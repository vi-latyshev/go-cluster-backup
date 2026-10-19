package local

// The one failure of Put no real directory can be made to produce: the metadata
// read that follows the rename. A filesystem can still refuse it (EIO, a network
// mount's ESTALE), so the seam is substituted here and the classification is
// asserted — the attempt must be retryable, not a panic.

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

func TestPutReportsAStatFailureAfterTheRename(t *testing.T) {
	s := &Storage{root: t.TempDir(), page: 10}
	saved := statPath
	statPath = func(string) (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
	t.Cleanup(func() { statPath = saved })

	_, err := s.Put(context.Background(), storage.PutRequest{Key: "k.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("a stat failure after the rename was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
}
