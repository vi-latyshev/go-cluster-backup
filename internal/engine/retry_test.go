package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

func TestBackoffCapsAnOvershootingDoubling(t *testing.T) {
	p := retryPolicy{initial: 10 * time.Millisecond, max: 15 * time.Millisecond}
	// 10ms doubles to 20ms, past the 15ms cap: the wait is clamped, never the
	// overshoot.
	if got := p.backoff(3); got != 15*time.Millisecond {
		t.Fatalf("backoff(3) = %v, want the 15ms cap", got)
	}
}

func TestSafeValueRecoversAPanic(t *testing.T) {
	_, err := safeValue(func() (int, error) { panic("value boom") })
	if err == nil || !strings.Contains(err.Error(), "value boom") {
		t.Fatalf("panic not recovered: %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("a recovered panic must be permanent, got %q", class)
	}
}

// --- fakes ------------------------------------------------------------------
