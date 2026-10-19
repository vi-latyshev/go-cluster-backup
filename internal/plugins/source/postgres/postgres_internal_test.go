package postgres

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}
