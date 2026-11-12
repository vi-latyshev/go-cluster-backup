package pgclient

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"sync"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	pluginio "github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
)

// Command builds one command of suite with the connection prefix and the
// password environment applied. The password reaches the process through
// PGPASSWORD, never argv; the caller reads the mounted secret at use time, so
// a rotated secret is the one this call uses.
func (c *Conn) Command(ctx context.Context, suite Suite, bin, pass string, args ...string) *exec.Cmd {
	argv := append(c.ConnArgs(), "--no-password")
	argv = append(argv, args...)
	cmd := exec.CommandContext(ctx, suite.Path(bin), argv...)
	cmd.Env = c.Env(pass)
	return cmd
}

// Stream starts cmd, copies its stdout into dst, and returns only after the
// command has exited. The bytes never pass through a buffer: a dst failure
// stops the command rather than being swallowed, and a non-zero exit is
// classified from the command's stderr.
func (c *Conn) Stream(ctx context.Context, cmd *exec.Cmd, dst io.Writer) (int64, error) {
	stdout, err := stdoutPipe(cmd)
	if err != nil {
		// A host out of descriptors is worth another attempt, never a panic.
		return 0, pluginerrors.Transient(fmt.Errorf("taking the command's stdout: %w", err))
	}
	stderr := extcmd.NewBoundedBuffer()
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, pluginerrors.Transient(err)
	}
	n, copyErr := pluginio.CopyContext(ctx, dst, stdout)
	if copyErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, copyErr
	}
	if err := cmd.Wait(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, classifyExit(cmd.Path, err, stderr.String())
	}
	return n, nil
}

// Run runs cmd with src as its stdin (nil leaves stdin empty) and returns the
// bytes fed to it. A cancelled context is the cancellation, not the command's
// kill.
func (c *Conn) Run(ctx context.Context, cmd *exec.Cmd, src io.Reader) (int64, error) {
	stderr := extcmd.NewBoundedBuffer()
	cmd.Stderr = stderr
	counted := &countingReader{r: src}
	if src != nil {
		cmd.Stdin = counted
	}
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return counted.n, ctxErr
		}
		return counted.n, classifyExit(cmd.Path, err, stderr.String())
	}
	return counted.n, nil
}

// Output runs cmd and returns what it printed on stdout. A non-zero exit is
// classified from the bounded stderr; a cancelled context is returned as
// ctx.Err() before classification runs.
func (c *Conn) Output(ctx context.Context, cmd *exec.Cmd) (string, error) {
	return extcmd.Output(ctx, cmd, classifyExit)
}

// DBNameArg passes a database name as a libpq conninfo value, never as a bare
// argument: a server's own database name may contain '=', a space or a leading
// '--', and a bare one either becomes another keyword's value or another
// option. Inside a quoted conninfo value a backslash escapes the next byte, so
// both it and the closing quote are escaped before the value is wrapped.
func DBNameArg(name string) string {
	return "--dbname=dbname='" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(name) + "'"
}

// stdoutPipe is the pipe a command's output streams through. It is a variable
// because os.Pipe can fail on a host out of descriptors, which no test can make
// it do on demand.
var stdoutPipe = func(cmd *exec.Cmd) (io.ReadCloser, error) { return cmd.StdoutPipe() }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

var (
	// A connection the server never answered is worth another attempt; a
	// credential the server refused is not.
	transientRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)connection refused|connection timed out|could not connect|connection reset|no route to host|i/o timeout|the database system is starting up|is in recovery|terminating connection due to|server closed the connection unexpectedly`)
	})
	permanentRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)password authentication failed|authentication failed|no pg_hba.conf entry|does not exist|permission denied|too many connections|server version mismatch`)
	})
)

// classifyExit turns a failed command into a framework-marked error with the
// PostgreSQL client's patterns; text that matches neither stays unclassified,
// and the execution config then decides what an unclassified error means.
func classifyExit(bin string, err error, detail string) error {
	return extcmd.ClassifyExit(bin, err, detail, permanentRe(), transientRe())
}
