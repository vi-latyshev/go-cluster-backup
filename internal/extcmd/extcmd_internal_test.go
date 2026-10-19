package extcmd

import (
	"errors"
	"os"
	"strings"
	"testing"
)

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
