package pgclient

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
)

// TestStreamReportsAPipeItCannotTake: os.Pipe can fail on a host out of
// descriptors, and that is a retry rather than a panic.
func TestStreamReportsAPipeItCannotTake(t *testing.T) {
	orig := stdoutPipe
	stdoutPipe = func(*exec.Cmd) (io.ReadCloser, error) {
		return nil, errors.New("injected pipe failure")
	}
	t.Cleanup(func() { stdoutPipe = orig })

	_, err := (&Conn{}).Stream(context.Background(), exec.Command("true"), io.Discard)
	if err == nil {
		t.Fatal("a command whose stdout pipe could not be taken was started")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}
