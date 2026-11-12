package backup_test

// The manifest is written with the metadata purpose: a storage that keeps
// payloads under one class rule must still serve every run's own record, even
// when its bucket default is a cold class. Artifacts stay payload.

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// purposeSink records the purpose of every whole-object Put by key.
type purposeSink struct {
	*storagetest.Fake
	mu       sync.Mutex
	purposes map[string]storage.Purpose
}

func (s *purposeSink) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	s.mu.Lock()
	s.purposes[req.Key] = req.Purpose
	s.mu.Unlock()
	return s.Fake.Put(ctx, req, body)
}

func (s *purposeSink) purpose(key string) (storage.Purpose, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.purposes[key]
	return p, ok
}

func TestManifestIsWrittenWithTheMetadataPurpose(t *testing.T) {
	w := newWorld(t)
	sink := &purposeSink{Fake: storagetest.NewFake(), purposes: map[string]storage.Purpose{}}
	w.ExtraStorages["primary"] = sink
	w.Storages["primary"] = sink.Fake
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	manifest := "runs/" + rep.RunID + "/manifest.json"
	if got, ok := sink.purpose(manifest); !ok || got != storage.PurposeMetadata {
		t.Fatalf("manifest purpose = (%q, %v), want the metadata purpose", got, ok)
	}
	artifact := ""
	for key := range sink.purposes {
		if strings.HasPrefix(key, "runs/"+rep.RunID+"/") && key != manifest {
			artifact = key
			break
		}
	}
	if artifact == "" {
		t.Fatalf("no artifact was written: %v", sink.purposes)
	}
	if got := sink.purposes[artifact]; got != storage.PurposePayload {
		t.Fatalf("artifact %s purpose = %q, want the payload purpose", artifact, got)
	}
}
