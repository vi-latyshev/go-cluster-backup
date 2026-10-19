package s3_test

// The cleanup entry point deletes exactly what S3_LIVE_CLEANUP_PREFIX names,
// so the resolution that reads that variable must refuse every prefix but a
// fresh conformance one — offline, before any request. This test runs in the
// plain suite: no environment, no network. It pins the refusals for this
// plugin's base; the shared shape itself lives in storage/storagetest.

import (
	"errors"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func TestResolveCleanupPrefixRefusesEverythingButAConformancePrefix(t *testing.T) {
	const hex12 = "0123456789ab" // a fixed shape sample; the generator is random
	mustRefuse := []string{
		"",
		"cluster-backup",
		"cluster-backup/",
		"conformance-" + hex12 + "/../cluster-backup",
		"conformance-" + hex12 + "/",
		"conformance-0123456789AB",
		"conformance-" + hex12 + "x",
	}
	for _, prefix := range mustRefuse {
		_, err := s3Conformance.Resolve(prefix)
		if prefix == "" {
			if !errors.Is(err, storagetest.ErrLocationUnset) {
				t.Errorf("the empty value: err %v, want the unset sentinel", err)
			}
			continue
		}
		if err == nil {
			t.Errorf("Resolve(%q) accepted a prefix outside the conformance shape", prefix)
		}
	}
	generated := s3Conformance.Generate(t)
	got, err := s3Conformance.Resolve(generated)
	if err != nil || got != generated {
		t.Errorf("Resolve(%q) = %q, %v; want the generated prefix accepted unchanged", generated, got, err)
	}
}
