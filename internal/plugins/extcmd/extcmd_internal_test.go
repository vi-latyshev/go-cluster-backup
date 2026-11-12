package extcmd

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

// TestReadMountedReportsAFailedRead: a mounted file's bytes cannot be made to
// fail a read on demand, so the seam stands in for the filesystem and proves
// both the unbounded and the bounded reader report it.
func TestReadMountedReportsAFailedRead(t *testing.T) {
	orig := readAll
	t.Cleanup(func() { readAll = orig })
	readAll = func(io.Reader) ([]byte, error) { return nil, errors.New("injected read failure") }
	path := t.TempDir() + "/secret"
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, 4096} {
		if _, err := ReadSecretFile(path, limit); err == nil || !strings.Contains(err.Error(), "injected read failure") {
			t.Fatalf("ReadSecretFile(limit %d) = %v, want the read failure", limit, err)
		}
	}
}

// TestCheckScratchDirReportsAProbeItCannotCleanUp: the probe's own close and
// remove are the two ways a writable directory can still fail the check, and no
// filesystem fails them on demand.
func TestCheckScratchDirReportsAProbeItCannotCleanUp(t *testing.T) {
	origClose, origRemove := closeProbe, removeProbe
	t.Cleanup(func() { closeProbe, removeProbe = origClose, origRemove })

	closeProbe = func(f *os.File) error {
		_ = f.Close()
		return errors.New("injected close failure")
	}
	if err := CheckScratchDir(t.TempDir()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("a probe that could not be closed = %v, want a scratch_dir error", err)
	}

	closeProbe = origClose
	removeProbe = func(string) error { return errors.New("injected remove failure") }
	if err := CheckScratchDir(t.TempDir()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("a probe that could not be removed = %v, want a scratch_dir error", err)
	}
}
