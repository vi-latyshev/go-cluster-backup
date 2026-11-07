// Package redis snapshots one Redis instance with redis-cli --rdb, the binary
// the image already carries. redis-cli writes the RDB to a file of its own, so
// the snapshot goes through a temporary file in the configured scratch_dir
// (never in the process temp dir, a 64 Mi emptyDir in the cluster) and is
// streamed to dst from there; the temp file is removed on every path out. The
// password travels in REDISCLI_AUTH, never on the command line.
package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Config is the plugin config. Every field is required.
type Config struct {
	Address         string               `json:"address" required:"true" doc:"host:port of the Redis server"`
	Password        configuration.Secret `json:"password" doc:"file holding the server password; passed to redis-cli in REDISCLI_AUTH, never on the command line"`
	ScratchDir      string               `json:"scratch_dir" required:"true" doc:"existing writable directory the snapshot's temporary file is spooled into, e.g. the big emptyDir a run mounts; the process temp dir is not used"`
	ConnectAttempts int                  `json:"connect_attempts" doc:"TCP connect attempts before a snapshot gives up"`
	ConnectInterval model.Duration       `json:"connect_interval" doc:"wait between connect attempts; also the per-attempt dial timeout"`
}

type Source struct {
	cfg  Config
	host string
	port string
}

func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: ".rdb", Origin: "redis://" + s.cfg.Address}, nil
}

// Check runs at preflight: redis-cli is present, the scratch directory is
// writable, the server answers, and it accepts the configured password — a
// snapshot that would fail on WRONGPASS at 04:20 fails here instead.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := extcmd.FindBinary("redis-cli"); err != nil {
		return err
	}
	if err := extcmd.CheckScratchDir(s.cfg.ScratchDir); err != nil {
		return pluginerrors.Permanent(err)
	}
	if err := extcmd.WaitTCP(ctx, s.host, s.port, s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return pluginerrors.Transient(err)
	}
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "redis-cli", "-h", s.host, "-p", s.port, "PING")
	cmd.Env = extcmd.ReplaceEnv(os.Environ(), "REDISCLI_AUTH="+pass)
	out, err := extcmd.Output(ctx, cmd, classifyExit())
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) != "PONG" {
		return pluginerrors.Permanent(fmt.Errorf("redis-cli PING answered %q", strings.TrimSpace(out)))
	}
	return nil
}

// closeSnapshot and statSnapshot are the two steps that hand the snapshot
// redis-cli wrote back to this process. They are variables because no
// filesystem fails them on demand, and a branch no test reaches is a branch
// nobody has run.
var (
	closeSnapshot = func(f *os.File) error { return f.Close() }
	statSnapshot  = func(f *os.File) (os.FileInfo, error) { return f.Stat() }
)

// Dump writes the snapshot into dst. redis-cli --rdb writes a file, so the
// bytes land in a temp file first and are streamed from it; nothing is held
// whole in memory.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return err
	}
	if err := extcmd.WaitTCP(ctx, s.host, s.port, s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return pluginerrors.Transient(err)
	}
	tmp, err := os.CreateTemp(s.cfg.ScratchDir, "redis-rdb-*")
	if err != nil {
		return pluginerrors.Transient(err)
	}
	name := tmp.Name()
	// redis-cli opens the path itself; an already-open handle would only
	// conflict with it.
	if err := closeSnapshot(tmp); err != nil {
		_ = os.Remove(name)
		return pluginerrors.Transient(err)
	}
	defer func() { _ = os.Remove(name) }()

	cmd := exec.CommandContext(ctx, "redis-cli", "-h", s.host, "-p", s.port, "--rdb", name)
	cmd.Env = extcmd.ReplaceEnv(os.Environ(), "REDISCLI_AUTH="+pass)
	if _, err := extcmd.Output(ctx, cmd, classifyExit()); err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return pluginerrors.Transient(err)
	}
	defer func() { _ = f.Close() }()
	st, err := statSnapshot(f)
	if err != nil {
		return pluginerrors.Transient(err)
	}
	if st.Size() == 0 {
		return pluginerrors.Transient(errors.New("redis-cli exited 0 but wrote an empty snapshot"))
	}
	_, err = pluginio.CopyContext(ctx, dst, f)
	return err
}

// The patterns and the classifier they build are lazily-built singletons: a
// package initializer does no work.
var (
	transientRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)could not connect|connection refused|connection reset|connection timed out|timeout|broken pipe|loading`)
	})
	// A refused password or a denied command is not worth another attempt.
	permanentRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)wrongpass|invalid password|noauth|authentication required|denied|unknown command`)
	})
)

// classifyExit marks a failed redis-cli run with this plugin's patterns; text
// matching neither stays unclassified and the operation's owner decides what
// that means.
var classifyExit = sync.OnceValue(func() func(bin string, err error, detail string) error {
	return extcmd.Classifier(permanentRe(), transientRe())
})
