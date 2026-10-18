package yandexdisk

// The cleanup entry point deletes exactly what YDISK_LIVE_CLEANUP_ROOT names,
// so the resolution that reads that variable must refuse every location but a
// fresh conformance folder — offline, before any request. This test runs in
// the plain suite: no environment, no network. It pins the refusals for this
// plugin's base; the shared shape itself lives in storage/storagetest.

import (
	"errors"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func TestResolveCleanupRootRefusesEverythingButAConformanceFolder(t *testing.T) {
	const hex12 = "0123456789ab" // a fixed shape sample; the generator is random
	mustRefuse := []string{
		"",
		"app:/Backups",
		"app:/Backups/x",
		"app:/cluster-backup",
		"app:/cluster-backup/runs",
		"app:/cluster-backup-conformance-" + hex12 + "/../Backups",
		"app:/cluster-backup-conformance-" + hex12 + "/",
		"app:/cluster-backup-conformance-0123456789AB",
		"app:/cluster-backup-conformance-" + hex12 + "x",
	}
	for _, root := range mustRefuse {
		_, err := yandexConformance.Resolve(root)
		if root == "" {
			if !errors.Is(err, storagetest.ErrLocationUnset) {
				t.Errorf("the empty value: err %v, want the unset sentinel", err)
			}
			continue
		}
		if err == nil {
			t.Errorf("Resolve(%q) accepted a location outside the conformance shape", root)
		}
	}
	generated := yandexConformance.Generate(t)
	got, err := yandexConformance.Resolve(generated)
	if err != nil || got != generated {
		t.Errorf("Resolve(%q) = %q, %v; want the generated root accepted unchanged", generated, got, err)
	}
}
