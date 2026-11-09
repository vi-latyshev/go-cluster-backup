package postgres

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
)

// TestStreamCommandReportsAPipeItCannotTake: os.Pipe can fail on a host out of
// descriptors, and that is a retry rather than a panic.
func TestStreamCommandReportsAPipeItCannotTake(t *testing.T) {
	orig := stdoutPipe
	stdoutPipe = func(*exec.Cmd) (io.ReadCloser, error) {
		return nil, errors.New("injected pipe failure")
	}
	t.Cleanup(func() { stdoutPipe = orig })

	_, err := streamCommand(context.Background(), exec.Command("true"), io.Discard)
	if err == nil {
		t.Fatal("a command whose stdout pipe could not be taken was started")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestDbnameArg: a database name travels as a quoted conninfo value, so '='
// and a leading "--" inside it mean nothing and a quote or backslash cannot end
// the value early.
func TestDbnameArg(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"app", `--dbname=dbname='app'`},
		{"host=evil.example", `--dbname=dbname='host=evil.example'`},
		{"--file=/tmp/x", `--dbname=dbname='--file=/tmp/x'`},
		{"a'b", `--dbname=dbname='a\'b'`},
		{`a\b`, `--dbname=dbname='a\\b'`},
	} {
		if got := dbnameArg(tc.name); got != tc.want {
			t.Errorf("dbnameArg(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
