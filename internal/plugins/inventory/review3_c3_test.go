package inventory

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// TestR3_C3_MalformedDigestHasNoCredit is the reviewer's iteration-3 probe, kept
// as written: a non-canonical sha256 cannot match any computed digest, so the
// run is a problem run and its copy is not protection.
func TestR3_C3_MalformedDigestHasNoCredit(t *testing.T) {
	for _, digest := range []string{"not-a-sha256", strings.Repeat("a", 63), strings.Repeat("G", 64)} {
		t.Run(digest, func(t *testing.T) {
			doc := completeManifestDoc(t)
			setManifestPath(t, doc, "artifacts/0/sha256", digest)
			f := storagetest.NewFake()
			f.PutObject("runs/"+c3RunID+"/manifest.json", mustMarshal(t, doc), time.Now().UTC())
			f.PutObject("runs/"+c3RunID+"/databases/prod.dump.age", []byte("sealed!"), time.Now().UTC())
			s := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
			r := s.Runs[c3RunID]
			if r.ManifestProblem == "" || r.Stored.Verified || r.Stored.Manifest {
				t.Fatalf("unrestorable digest credited: %q, problem=%q verified=%v manifest=%v", digest, r.ManifestProblem, r.Stored.Verified, r.Stored.Manifest)
			}
		})
	}
}
