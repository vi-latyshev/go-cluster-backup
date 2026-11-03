package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// benchSource is a source a test can gate: it writes its chunks, and after one
// of them the first attempt blocks until the test releases it — which is how a
// test proves what a storage did while the producer was still writing.
type benchSource struct {
	chunks func(attempt int) [][]byte
	err    func(attempt int) error
	// gateAfter blocks after this many chunks on the first attempt.
	gateAfter int
	release   chan struct{}
	// blocked closes just before the gate wait; sleepAfter/sleep pause the first
	// attempt after a chunk in virtual time.
	blocked    chan struct{}
	blockedOne sync.Once
	sleepAfter int
	sleep      time.Duration

	mu       sync.Mutex
	dumps    int
	finished bool
}

func (s *benchSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat", Origin: "test://bench"}, nil
}

func (s *benchSource) Check(context.Context) error { return nil }

func (s *benchSource) Dump(ctx context.Context, dst io.Writer) error {
	s.mu.Lock()
	s.dumps++
	attempt := s.dumps
	s.mu.Unlock()
	for i, c := range s.chunks(attempt) {
		if _, err := dst.Write(c); err != nil {
			return err
		}
		if attempt == 1 && s.sleepAfter == i+1 && s.sleep > 0 {
			time.Sleep(s.sleep)
		}
		if attempt == 1 && s.gateAfter == i+1 {
			if s.blocked != nil {
				s.blockedOne.Do(func() { close(s.blocked) })
			}
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if s.err != nil {
		if err := s.err(attempt); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
	return nil
}

func (s *benchSource) done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

// overlapConfig is one artifact "wireguard" with no processor chain: the spool
// bytes are the source bytes, so a part's content is the source's own.
func overlapConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	cfg := baseConfig(t, storages)
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", nil, "required", "plain"),
	}
	return cfg
}

func addMultipart(w *world, name string, partSize int64) *fakeMultipart {
	mp := newFakeMultipart(partSize)
	w.extraStorages[name] = mp
	w.storages[name] = mp.Fake
	return mp
}

// TestOverlapStartsPartsWhileTheProducerWrites (A-O1, o.I2): the first part is
// accepted before the source's Dump returns — the upload is not waiting for the
// artifact to be whole.
func TestOverlapStartsPartsWhileTheProducerWrites(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	partSent := make(chan struct{})
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 1 {
			once.Do(func() { close(partSent) })
		}
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	go func() {
		<-partSent
		if src.done() {
			t.Error("the first part arrived only after the producer had finished")
		}
		close(src.release)
	}()
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 0 || completes != 1 {
		t.Fatalf("aborts=%d completes=%d, want a clean commit", aborts, completes)
	}
}

// TestOverlapAbortsWhenTheProducerFails (A-O2, o.I1/o.I3): a producer that wrote
// its bytes and then failed never completes the upload; the one session is
// aborted exactly once and the artifact fails.
func TestOverlapAbortsWhenTheProducerFails(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		err:       func(int) error { return errors.New("dump failed") },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	opened := make(chan struct{})
	var once sync.Once
	parts.beginHook = func(req storage.PutRequest) {
		if req.Size > 4 {
			once.Do(func() { close(opened) })
		}
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	go func() {
		<-opened
		close(src.release)
	}()
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want exactly one abort and no complete", aborts, completes)
	}
}

// TestOverlapAbortsOnArtifactOverflow (A-O3, o.I3): a producer that crosses
// run.max_artifact_bytes mid-stream aborts the open session once and sends no
// part after it.
func TestOverlapAbortsOnArtifactOverflow(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 3)
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123"), []byte("456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	parts.partHook = func(number int32) {
		if number == 1 {
			close(src.release)
		}
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(6)
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	_, aborts, completes, calls := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want one abort and no complete", aborts, completes)
	}
	if calls[2] != 0 {
		t.Fatalf("a part after the overflow was sent: %v", calls)
	}
}

// TestOverlapAbortsOnCancellation (A-O4, o.I3): a cancelled run aborts the open
// session once and returns — no goroutine waits for a producer nobody can stop.
func TestOverlapAbortsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	opened := make(chan struct{})
	var once sync.Once
	parts.beginHook = func(req storage.PutRequest) {
		if req.Size > 4 {
			once.Do(func() { close(opened) })
		}
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	go func() {
		<-opened
		cancel()
	}()
	rep, err := w.newEngine(t, cfg).Run(ctx, RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	_, aborts, _, _ := parts.stats()
	if aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1 after a cancelled upload", aborts)
	}
}

// TestCapStateBoundShrinkAndRelease (A-O5/A-O6, o.I4): the artifact bound is
// booked before the size is known, lowered to the exact size at the end, never
// raised, and released when the upload was aborted or never started.
func TestCapStateBoundShrinkAndRelease(t *testing.T) {
	c := newCapState(100, 10, true)
	if !c.reserve("k", 50, true) {
		t.Fatal("a bound that fits was refused")
	}
	if got := c.used(); got != 60 {
		t.Fatalf("used during the upload = %d, want the survey plus the bound", got)
	}
	c.shrink("k", 20)
	if got := c.used(); got != 30 {
		t.Fatalf("used after the size is known = %d, want the exact size", got)
	}
	c.shrink("k", 40)
	if got := c.used(); got != 30 {
		t.Fatalf("a shrink raised the reservation: %d", got)
	}
	c.unreserve("k")
	if got := c.used(); got != 10 {
		t.Fatalf("used after a released booking = %d, want the survey alone", got)
	}
}

// TestManualBoundRefusalUploadsExactAfterProduce (A-O5, o.I4): a manual run whose
// artifact bound does not fit the cap does not overlap — it asks for an upload
// only once, on the exact size, after the producer.
func TestManualBoundRefusalUploadsExactAfterProduce(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	var mu sync.Mutex
	var sizes []int64
	parts.beginHook = func(req storage.PutRequest) {
		mu.Lock()
		sizes = append(sizes, req.Size)
		mu.Unlock()
	}
	src := &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(100)
	withManualRetention(cfg, "a_parts", keepLastCfg(3))
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["max_total_bytes"] = int64(20)

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 1 || sizes[0] != 10 {
		t.Fatalf("begin sizes %v, want the one exact size after the producer", sizes)
	}
}

// TestManualExactSizeRefusalBeforeAnyWrite (A-O5, o.I4): a manual run whose
// exact artifact does not fit the cap is refused before a single part is sent.
func TestManualExactSizeRefusalBeforeAnyWrite(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	var begins atomic.Int64
	parts.beginHook = func(storage.PutRequest) { begins.Add(1) }
	src := &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(100)
	withManualRetention(cfg, "a_parts", keepLastCfg(3))
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["max_total_bytes"] = int64(5)

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a cap refusal", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); !r.CapExceeded {
		t.Fatalf("a_parts = %+v, want the cap refusal", r)
	}
	if got := begins.Load(); got != 0 {
		t.Fatalf("a refused manual run opened %d uploads", got)
	}
}

// TestOverlapOneSlotHoldsTheProducer (A-O8, o.I6): with one storage slot, the
// first multipart storage overlaps the producer and the second starts only after
// it — both copies complete.
func TestOverlapOneSlotHoldsTheProducer(t *testing.T) {
	w := newWorld(t)
	first := addMultipart(w, "a_parts", 4)
	second := addMultipart(w, "b_parts", 4)
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	var once sync.Once
	var firstEarly, secondEarly atomic.Bool
	hook := func(early *atomic.Bool) func(storage.PutRequest) {
		return func(storage.PutRequest) {
			if !src.done() {
				early.Store(true)
				once.Do(func() { close(src.release) })
			}
		}
	}
	first.beginHook = hook(&firstEarly)
	second.beginHook = hook(&secondEarly)
	cfg := overlapConfig(t, map[string]any{
		"a_parts": storageInstance("fake", "keep_last"),
		"b_parts": storageInstance("fake", "keep_last"),
	})
	cfg["run"].(map[string]any)["storage_parallelism"] = 1

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	if firstEarly.Load() == secondEarly.Load() {
		t.Fatalf("early begins: a_parts=%v b_parts=%v, want exactly one storage in the slot while the producer wrote",
			firstEarly.Load(), secondEarly.Load())
	}
	for _, name := range []string{"a_parts", "b_parts"} {
		if r := storageResult(rep, name); r.Outcome != model.StorageVerified {
			t.Fatalf("%s = %+v, want a verified copy", name, r)
		}
	}
}

// hookPutStorage records the whole-object Put of a storage without the
// capability: what proves the put happened only after the producer finished.
type hookPutStorage struct {
	*storagetest.Fake
	puts  atomic.Int64
	onPut func()
}

func (h *hookPutStorage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	h.puts.Add(1)
	if h.onPut != nil {
		h.onPut()
	}
	return h.Fake.Put(ctx, req, body)
}

// TestPlainStorageUploadsAfterProduce (A-O9, o.I7): a storage without the
// multipart capability is not called while the producer writes.
func TestPlainStorageUploadsAfterProduce(t *testing.T) {
	w := newWorld(t)
	store := &hookPutStorage{Fake: storagetest.NewFake()}
	w.extraStorages["plain"] = store
	w.storages["plain"] = store.Fake
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}), blocked: make(chan struct{}),
	}
	store.onPut = func() {
		if !src.done() {
			t.Error("a plain storage was called while the producer was still writing")
		}
	}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"plain": storageInstance("fake", "keep_last")})
	go func() {
		<-src.blocked
		if got := store.puts.Load(); got != 0 {
			t.Errorf("a put ran before the producer finished: %d", got)
		}
		close(src.release)
	}()
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
}

// TestOverlapDoesNotChargeProducerWaitToTheBudget (A-O10, o.I5): a storage whose
// calls take a little time keeps its budget while the producer spends most of
// the run writing.
func TestOverlapDoesNotChargeProducerWaitToTheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		addMultipart(w, "a_parts", 4)
		src := &benchSource{
			chunks:     func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
			sleepAfter: 1, sleep: 60 * time.Millisecond,
		}
		w.extraSources["vpn"] = src
		cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["time_budget"] = "50ms"

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v", rep.Artifacts[0])
		}
		if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageVerified {
			t.Fatalf("a_parts = %+v, want the producer's wait not charged to the storage", r)
		}
	})
}

// TestOverlapRetryUploadsTheSecondAttemptBytes (A-O11, o.I3/o.I7): an attempt 1
// upload is aborted once when the producer fails; the retry's bytes are what is
// stored, and the manifest records their hash.
func TestOverlapRetryUploadsTheSecondAttemptBytes(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	second := []byte("abcdefghij")
	src := &benchSource{
		chunks: func(attempt int) [][]byte {
			if attempt == 1 {
				return [][]byte{[]byte("0123456789")}
			}
			return [][]byte{second}
		},
		err: func(attempt int) error {
			if attempt == 1 {
				return errors.New("dump interrupted")
			}
			return nil
		},
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 2 {
			once.Do(func() { close(src.release) })
		}
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 1 || completes != 1 {
		t.Fatalf("aborts=%d completes=%d, want the failed attempt aborted and the retry committed", aborts, completes)
	}
	got, ok := parts.Bytes(rep.Artifacts[0].Key)
	if !ok || !bytes.Equal(got, second) {
		t.Fatalf("the stored object is %q, want the second attempt's bytes", got)
	}
	if rep.Artifacts[0].SHA256 != model.SHA256Hex(second) {
		t.Fatalf("the manifest hash %q does not match the second attempt", rep.Artifacts[0].SHA256)
	}
}

// TestOverlapPermanentPartFailsOneStorage (A-O13, o.I3): a permanent part error
// aborts that storage's upload once, leaves the other storage's copy, and the
// artifact stays ok.
func TestOverlapPermanentPartFailsOneStorage(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	parts.partErr = func(number int32, _ int) error {
		if number == 2 {
			return plugin.Permanent(errors.New("part refused"))
		}
		return nil
	}
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	opened := make(chan struct{})
	var once sync.Once
	parts.beginHook = func(req storage.PutRequest) {
		if req.Size > 4 {
			once.Do(func() { close(opened) })
		}
	}
	cfg := overlapConfig(t, twoWayStorages())
	go func() {
		<-opened
		close(src.release)
	}()
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v, want the failed copy", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other copy intact", r)
	}
	_, aborts, completes, _ := parts.stats()
	_, aborted := parts.sweepStats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d (%v), want one abort and no complete", aborts, completes, aborted)
	}
}

// TestOverlapBeginProbeErrorFailsTheStorage (A-O13 adjacent): a BeginMultipart
// that answers a real error while the producer is still writing fails that
// storage's copy — and the producer carries on for the others.
func TestOverlapBeginProbeErrorFailsTheStorage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: plugin.Permanent(errors.New("begin refused"))}
		w.extraStorages["a_parts"] = z
		w.storages["a_parts"] = z.Fake
		src := &benchSource{
			chunks:     func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
			sleepAfter: 1, sleep: 20 * time.Millisecond,
		}
		w.extraSources["vpn"] = src
		cfg := overlapConfig(t, twoWayStorages())
		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
		}
		if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed || !strings.Contains(r.Error, "begin refused") {
			t.Fatalf("a_parts = %+v, want the begin failure", r)
		}
	})
}

// TestOverlapProducerFailsBeforeAnyUpload: a producer that fails before a byte
// is written leaves the storage untouched — no upload opened, nothing reported
// against it.
func TestOverlapProducerFailsBeforeAnyUpload(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	src := &benchSource{
		chunks: func(int) [][]byte { return nil },
		err:    func(int) error { return plugin.Permanent(errors.New("dump refused")) },
	}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 0 || completes != 0 {
		t.Fatalf("a producer that wrote nothing opened or committed an upload: aborts=%d completes=%d", aborts, completes)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageSkipped {
		t.Fatalf("a_parts = %+v, want skipped", r)
	}
}

// TestOverlapFatalStorageIsNotFollowed: once a storage failed permanently, the
// next artifact does not open an upload there — its copy comes from the others.
func TestOverlapFatalStorageIsNotFollowed(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	parts.partErr = func(int32, int) error { return plugin.Permanent(errors.New("part refused")) }
	w.extraSources["vpn"] = &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	w.extraSources["vpn2"] = &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("abcdefghij")} }}
	cfg := overlapConfig(t, twoWayStorages())
	cfg["sources"].(map[string]any)["vpn2"] = sourceInstance("fake", "vpn", "wireguard2", nil, "required", "plain")
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	for _, ar := range rep.Artifacts {
		if ar.Outcome != model.ArtifactOK {
			t.Fatalf("artifact %s = %+v, want the plain storage's copy", ar.Artifact.Name, ar)
		}
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v, want the failed copy", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other copy", r)
	}
	begins, _, completes, _ := parts.stats()
	// artifact one opens one upload; the fatal storage is not asked again.
	if begins < 1 || completes != 0 {
		t.Fatalf("begins=%d completes=%d, want the fatal storage left alone after its failure", begins, completes)
	}
}

// TestFollowSourceNextHonoursCancellation: a follower waiting for a producer
// that may never write again ends with the run's context — never sleeps past it.
func TestFollowSourceNextHonoursCancellation(t *testing.T) {
	src := &followSource{g: newGrowth()}
	if _, _, err := src.next(context.Background(), -1); err != nil {
		t.Fatalf("a fresh growth: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := src.next(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled wait = %v, want the context's error", err)
	}
}

// TestManualAfterProduceBeginErrorFailsTheStorage: the fallback path a manual
// run takes when the artifact bound does not fit still opens its upload after
// the producer, and a refused Begin fails only that storage.
func TestManualAfterProduceBeginErrorFailsTheStorage(t *testing.T) {
	w := newWorld(t)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: plugin.Permanent(errors.New("begin refused"))}
	w.extraStorages["a_parts"] = z
	w.storages["a_parts"] = z.Fake
	src := &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(100)
	withManualRetention(cfg, "a_parts", keepLastCfg(3))
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["max_total_bytes"] = int64(20)

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed || !strings.Contains(r.Error, "begin refused") {
		t.Fatalf("a_parts = %+v, want the begin failure", r)
	}
}

// TestManualAfterProducePartErrorAborts: an after-produce upload whose part is
// refused is aborted once, and only that storage's copy fails.
func TestManualAfterProducePartErrorAborts(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	parts.partErr = func(int32, int) error { return plugin.Permanent(errors.New("part refused")) }
	src := &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	w.extraSources["vpn"] = src
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(100)
	withManualRetention(cfg, "a_parts", keepLastCfg(3))
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["max_total_bytes"] = int64(20)

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want one abort and no complete", aborts, completes)
	}
}

// TestFailedAbortKeepsTheOldAttemptCounted (C6, o.I4): an attempt whose Abort
// failed leaves parts that may still be billed. The retry must not reuse its
// booking, and the next manual write must see those bytes: the ledger never
// understates what may remain.
func TestFailedAbortKeepsTheOldAttemptCounted(t *testing.T) {
	w := newWorld(t)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: errors.New("abort failed")}
	w.extraStorages["a_parts"] = z
	w.storages["a_parts"] = z.Fake
	parts := z.fakeMultipart
	first := &benchSource{
		chunks: func(attempt int) [][]byte { return [][]byte{[]byte("0123456789")} },
		err: func(attempt int) error {
			if attempt == 1 {
				return errors.New("dump interrupted")
			}
			return nil
		},
		gateAfter: 1, release: make(chan struct{}),
	}
	parts.partHook = func(number int32) {
		if number == 1 {
			select {
			case <-first.release:
			default:
				close(first.release)
			}
		}
	}
	w.extraSources["vpn"] = first
	w.extraSources["vpn2"] = &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("012345678901234567890123456789")} }}
	cfg := overlapConfig(t, map[string]any{
		"a_parts": storageInstance("fake", "keep_last"),
		"b_plain": storageInstance("fake", "keep_last"),
	})
	cfg["catalog"] = instance("fake")
	cfg["sources"].(map[string]any)["vpn2"] = sourceInstance("fake", "vpn", "wireguard2", nil, "required", "plain")
	cfg["run"].(map[string]any)["max_artifact_bytes"] = int64(80)
	for _, name := range []string{"a_parts", "b_plain"} {
		withManualRetention(cfg, name, keepLastCfg(3))
	}
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["max_total_bytes"] = int64(100)

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	for _, ar := range rep.Artifacts {
		if ar.Outcome != model.ArtifactOK {
			t.Fatalf("artifact %s = %+v, want the plain storage's copy", ar.Artifact.Name, ar)
		}
	}
	_, _, completes, _ := parts.stats()
	if completes != 0 {
		t.Fatalf("completes=%d: the storage reused the key after its Abort failed", completes)
	}
	if _, ok := parts.Bytes(rep.Artifacts[0].Key); ok {
		t.Fatal("the retry uploaded to a storage whose first attempt could not be aborted")
	}
	// The leaked 80-byte booking plus the second artifact's 30 bytes exceed the
	// 100-byte cap; without it kept, the second upload would be admitted.
	if r := storageResult(rep, "a_parts"); !r.CapExceeded {
		t.Fatalf("a_parts = %+v, want the cap refusal the leftover booking causes", r)
	}
}

// TestEarlyProducerFailureReleasesFollowers (C7, o.I8): a producer that fails
// before its first attempt — spool reserve, statfs, Create — still wakes every
// follower at once; nothing waits for the run deadline.
func TestEarlyProducerFailureReleasesFollowers(t *testing.T) {
	modes := []struct {
		name  string
		setup func(t *testing.T, e *Engine) *spool.Spool
	}{
		{"reserve", func(t *testing.T, e *Engine) *spool.Spool {
			e.cfg.Run.SpoolReserveBytes = model.ByteSize(1 << 62)
			sp, err := spool.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return sp
		}},
		{"statfs", func(t *testing.T, e *Engine) *spool.Spool {
			dir := t.TempDir()
			sp, err := spool.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(dir); err != nil {
				t.Fatal(err)
			}
			return sp
		}},
		{"create", func(t *testing.T, e *Engine) *spool.Spool {
			dir := t.TempDir()
			sp, err := spool.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			return sp
		}},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newWorld(t)
				addMultipart(w, "a_parts", 4)
				cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
				e := w.newEngine(t, cfg)
				sp := tc.setup(t, e)
				plan := artifactPlan{
					instance: "vpn", srcType: "fake",
					src:      &benchSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }},
					artifact: model.Artifact{Group: "g", Name: "n"},
				}
				results := map[string]*storageRunState{}
				caps := map[string]*capState{}
				for _, sb := range e.storages {
					results[sb.name] = newStorageRunState(sb)
					caps[sb.name] = newCapState(1<<40, 0, true)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				start := time.Now()
				ar, _ := e.processArtifact(ctx, sp, plan, "r", model.RunKindScheduled, e.storages, results, caps)
				if ar.Outcome != model.ArtifactFailed {
					t.Fatalf("artifact = %+v, want a failure", ar)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("an immediate %s failure waited %s for the run deadline", tc.name, elapsed)
				}
			})
		})
	}
}
