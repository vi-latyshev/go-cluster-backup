// Package extcmd is the shared plumbing of the plugins that drive external
// binaries and mount-bound files: bounded stderr capture, stdout capture with
// exit classification, environment replacement, a TCP reachability wait, a
// mounted password file read, and the scratch-directory writability probe.
// Each helper is one attempt and never retries: retries, backoff and budgets
// are the engine's, read from the config.
//
// The package is internal: it is the module's own library, not part of the
// plugin API a third-party plugin is written against.
package extcmd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// stderrLimit is how much of a command's stderr is kept for its error text:
// enough for a driver error, far from holding a command's whole output in
// memory.
const stderrLimit = 16 << 10

// BoundedBuffer keeps at most its limit of written bytes, discarding the rest:
// a failing command may stream for a long time, and only its first words are
// worth quoting.
type BoundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

// NewBoundedBuffer returns a buffer keeping the configured stderr limit.
func NewBoundedBuffer() *BoundedBuffer { return &BoundedBuffer{limit: stderrLimit} }

func (b *BoundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (b *BoundedBuffer) String() string { return strings.TrimSpace(b.buf.String()) }

// Output runs cmd without its own deadline (the caller's context is the budget
// for a preflight query or a short command) and returns what it printed on
// stdout. A non-zero exit is turned into an error by classify, which receives
// the binary path, the failure and the bounded stderr — the plugin's own
// regexes decide the class, and text matching neither pattern stays
// unclassified for the engine's config to judge. A cancelled context is
// returned as ctx.Err() before classify runs: cancellation is not the
// command's fault.
func Output(ctx context.Context, cmd *exec.Cmd, classify func(bin string, err error, detail string) error) (string, error) {
	stderr := NewBoundedBuffer()
	cmd.Stderr = stderr
	out, err := cmd.Output()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", classify(cmd.Path, err, stderr.String())
	}
	return string(out), nil
}

// Classifier binds ClassifyExit to one plugin's patterns: a plugin keeps its
// two regexes and one call to this, nothing else.
func Classifier(permanent, transient *regexp.Regexp) func(bin string, err error, detail string) error {
	return func(bin string, err error, detail string) error {
		return ClassifyExit(bin, err, detail, permanent, transient)
	}
}

// ClassifyExit marks a failed command with the class its output names: detail
// matching permanent wins, then transient; neither leaves it unclassified.
func ClassifyExit(bin string, err error, detail string, permanent, transient *regexp.Regexp) error {
	what := bin + " failed"
	if detail != "" {
		what += ": " + detail
	}
	switch {
	case permanent.MatchString(detail):
		return plugin.Permanent(fmt.Errorf("%s: %w", what, err))
	case transient.MatchString(detail):
		return plugin.Transient(fmt.Errorf("%s: %w", what, err))
	default:
		return fmt.Errorf("%s: %w", what, err)
	}
}

// Run runs one external binary with its own deadline composed onto ctx and
// returns everything it printed, stdout and stderr merged in the order they
// were written — sqlite's shell answers and complains on the same stream, and
// the caller quotes that text either way. env replaces the process
// environment wholesale when non-nil (the caller decides what the command
// inherits). A cancelled parent context is returned as ctx.Err().
func Run(ctx context.Context, timeout time.Duration, path string, env []string, args ...string) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, path, args...)
	cmd.Env = env
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil && ctx.Err() != nil {
		return out.String(), ctx.Err()
	}
	return out.String(), err
}

// ReplaceEnv returns env with every entry whose KEY matches one of kv's keys
// replaced by kv: a value inherited from the environment must never shadow the
// one the config chose, and exec passes duplicates through, where the first
// match would win.
func ReplaceEnv(env []string, kv ...string) []string {
	out := make([]string, 0, len(env)+len(kv))
	for _, e := range env {
		key, _, _ := strings.Cut(e, "=")
		replaced := false
		for _, pair := range kv {
			if k, _, _ := strings.Cut(pair, "="); k == key {
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, e)
		}
	}
	return append(out, kv...)
}

// WaitTCP waits for the address to accept a TCP connection, at most attempts
// times, waiting interval between attempts and timing out each dial at the
// same interval. A pod that appeared in the API a moment ago may take a second
// or two to become routable under the NetworkPolicy, which is why a dump does
// not call the server the moment its name resolves.
func WaitTCP(ctx context.Context, host, port string, attempts int, interval time.Duration) error {
	dialer := net.Dialer{Timeout: interval}
	var last error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if err == nil {
			_ = conn.Close()
			return nil
		}
		last = err
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("%s not reachable after %d attempts: %w", net.JoinHostPort(host, port), attempts, last)
}

// ReadPassword reads a mounted secret file at use time, never at construction:
// a missing file is a preflight failure, not a reason the whole config cannot
// even be built. A trailing newline (docker secrets style) is not part of it.
func ReadPassword(file string) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return "", plugin.Permanent(fmt.Errorf("reading password file: %w", err))
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// FindBinary locates one external binary the plugin's type declares and names
// it in the error: a missing binary is the deployment, and a preflight must
// fail here rather than at 04:20. The returned path is what runSQLite-style
// callers execute.
func FindBinary(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", plugin.Permanent(fmt.Errorf("%s is not in PATH: %w", name, err))
	}
	return path, nil
}

// closeProbe and removeProbe are the probe's own cleanup steps. They are
// variables because no filesystem fails them on demand, and a branch no test
// reaches is a branch nobody has run.
var (
	closeProbe  = func(f *os.File) error { return f.Close() }
	removeProbe = os.Remove
)

// CheckScratchDir proves dir exists, is a directory, and this process can
// create files in it — by creating one and removing it, because a mode test
// answers nothing (root ignores modes, and a mount's owner is kubelet, not
// this process). The returned errors are plain: the caller classifies them,
// and the message always names scratch_dir.
func CheckScratchDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("scratch_dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("scratch_dir %s is not a directory", dir)
	}
	probe, err := os.CreateTemp(dir, ".scratch-probe-")
	if err != nil {
		return fmt.Errorf("scratch_dir %s is not writable by this process: %w", dir, err)
	}
	name := probe.Name()
	if err := closeProbe(probe); err != nil {
		_ = removeProbe(name)
		return fmt.Errorf("scratch_dir %s: %w", dir, err)
	}
	if err := removeProbe(name); err != nil {
		return fmt.Errorf("scratch_dir %s: %w", dir, err)
	}
	return nil
}
