package engine

import (
	"bytes"
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

// TestConcurrentRestoresShareOneSpoolDirectory: two restore fetches on one
// restore.spool_dir must each hand back exactly the bytes their manifest
// verified. The hooks interleave them inside Get, after one restore's bytes sit
// in the spool file and before the hash check and the re-read by name — a
// shared spool name lets the second restore truncate the first's file there.
func TestConcurrentRestoresShareOneSpoolDirectory(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, twoSources(t, w), "primary")
	fixture := runOnce(t, w, cfg)
	runs, err := fixture.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) != 1 || len(runs[0].Report.Artifacts) != 2 {
		t.Fatalf("fixture run: %+v, %v", runs, err)
	}
	runID := runs[0].RunID
	keys := map[string]string{}
	for _, ar := range runs[0].Report.Artifacts {
		keys[ar.Artifact.Name] = ar.Key
	}
	fake := w.storage(t, "primary")
	wantA, okA := fake.Bytes(keys["small"])
	wantB, okB := fake.Bytes(keys["large"])
	if !okA || !okB {
		t.Fatalf("the fixture objects are missing: %v", fake.Keys())
	}

	aWritten, bWritten := make(chan struct{}), make(chan struct{})
	aDone, bDone := make(chan struct{}), make(chan struct{})
	eA := w.newEngine(t, cfg)
	eB := w.newEngine(t, cfg)
	eA.storages[0].inst = &hookStorage{Fake: fake, key: keys["small"],
		onGet: func() { close(aWritten); <-bWritten }}
	eB.storages[0].inst = &hookStorage{Fake: fake, key: keys["large"],
		onGet: func() { close(bWritten); <-aDone }}

	var outA, outB bytes.Buffer
	var repA FetchReport
	var errA error
	go func() {
		repA, errA = eA.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "small"}, &outA)
		close(aDone)
	}()
	<-aWritten
	var errB error
	go func() {
		_, errB = eB.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "large"}, &outB)
		close(bDone)
	}()
	<-bWritten
	<-aDone
	<-bDone

	if errA != nil {
		t.Fatalf("the first restore failed: %v", errA)
	}
	if !repA.Verified || !bytes.Equal(outA.Bytes(), wantA) {
		t.Fatalf("the first restore returned %d bytes with verified=%v, want the %d bytes of %q: got %q",
			outA.Len(), repA.Verified, len(wantA), keys["small"], outA.Bytes())
	}
	if errB != nil {
		t.Fatalf("the second restore failed: %v", errB)
	}
	if !bytes.Equal(outB.Bytes(), wantB) {
		t.Fatalf("the second restore returned %q, want the bytes of %q", outB.Bytes(), keys["large"])
	}
}
