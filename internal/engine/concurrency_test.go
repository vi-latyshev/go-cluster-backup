package engine

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// putBarrier releases every Put once two are in flight, so two storages finish
// one artifact at once; concurrent records that the second arrival — not the
// timeout — did the releasing.
type putBarrier struct {
	mu         sync.Mutex
	waiting    int
	concurrent bool
	release    chan struct{}
}

func newPutBarrier() *putBarrier { return &putBarrier{release: make(chan struct{})} }

func (b *putBarrier) wait() {
	b.mu.Lock()
	b.waiting++
	n := b.waiting
	b.mu.Unlock()
	if n >= 2 {
		b.mu.Lock()
		b.concurrent = true
		b.mu.Unlock()
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		return
	}
	select {
	case <-b.release:
	case <-time.After(3 * time.Second): // a serial engine must still finish; the test then reports the serialization
	}
}

type barrierStorage struct {
	*storagetest.Fake
	barrier *putBarrier
}

func (s *barrierStorage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	s.barrier.wait()
	return s.Fake.Put(ctx, req, body)
}

// TestTwoStoragesFinishOneArtifactAtOnce races the concurrent upload path with
// two storages and parallelism 2: -race judges storageRunState and the verified
// matrix, and the barrier makes the overlap real rather than incidental.
func TestTwoStoragesFinishOneArtifactAtOnce(t *testing.T) {
	w := newWorld(t)
	barrier := newPutBarrier()
	fleet := map[string]*barrierStorage{}
	err := plugin.Register(w.registry, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type: "barrier", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
			s, ok := fleet[deps.Instance]
			if !ok {
				s = &barrierStorage{Fake: storagetest.NewFake(), barrier: barrier}
				fleet[deps.Instance] = s
			}
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{
		"alpha": storageInstance("barrier", "keep_last"),
		"beta":  storageInstance("barrier", "keep_last"),
	})
	cfg["run"].(map[string]any)["storage_parallelism"] = 2
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		"app": sourceInstance("fake", "databases", "app-db", []string{"zstd3"}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s: %+v", rep.Outcome, rep.Storages)
	}
	if !barrier.concurrent {
		t.Fatal("the two storages never had a Put in flight at once")
	}
	for _, name := range []string{"alpha", "beta"} {
		if got := storageResult(rep, name); got.Outcome != model.StorageVerified {
			t.Fatalf("storage %s %+v", name, got)
		}
	}
}
