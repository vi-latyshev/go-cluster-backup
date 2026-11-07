package notifier_test

import (
	"errors"
	"testing"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
)

// TestErrSkippedIsOnePermanentValue pins the marker's contract: every caller
// matches it with errors.Is, so every call must answer the same value, and the
// execution's retry loop stops at once, so the value is classified permanent.
func TestErrSkippedIsOnePermanentValue(t *testing.T) {
	err := notifier.ErrSkipped()
	if !errors.Is(err, notifier.ErrSkipped()) {
		t.Fatal("two ErrSkipped calls returned values errors.Is does not match")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("ErrSkipped class = %q, want permanent", class)
	}
}
