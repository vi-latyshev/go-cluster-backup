package transfer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// scriptedSource is a producer a test can gate: it writes its chunks, and after
// one of them the first attempt blocks until the test releases it — which is how
// a test proves what a destination did while the producer was still writing.
type scriptedSource struct {
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

func (s *scriptedSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat", Origin: "test://scripted"}, nil
}

func (s *scriptedSource) Check(context.Context) error { return nil }

func (s *scriptedSource) Dump(ctx context.Context, dst io.Writer) error {
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

func (s *scriptedSource) done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

// artifact is the one artifact the producer's plan names.
func artifact() model.Artifact {
	return model.Artifact{Group: "g", Name: "n"}
}

// producePlan binds one scripted source to the producer.
func producePlan(src source.Source) pipeline.Plan {
	return pipeline.Plan{Source: src, SourceInstance: "src", SourceType: "scripted", Artifact: artifact()}
}

// newProducer is the run's producer with its artifact cap and producer attempts.
func newProducer(attempts int, maxArtifactBytes int64) *pipeline.Producer {
	return pipeline.New(pipeline.Options{
		Policy:           execution.Options{Attempts: attempts, Initial: time.Millisecond, Max: 2 * time.Millisecond, Unclassified: pluginerrors.ClassTransient},
		MaxArtifactBytes: maxArtifactBytes,
		ProgressInterval: time.Hour,
		Log:              testLog(),
	})
}

// produce runs one producer into the spool, feeding the follower's growth.
func produce(ctx context.Context, p *pipeline.Producer, sp *spool.Spool, plan pipeline.Plan, g *pipeline.Growth) (pipeline.Artifact, error) {
	res := pipeline.Resolved{Artifact: artifact()}
	res.Artifact.Ext = ".dat"
	return p.Produce(ctx, sp, plan, res, g)
}

// followResult is one overlapping upload as its goroutine finished it.
type followResult struct {
	out Outcome
	err error
}

// startFollow runs one Follow in its own goroutine, as the backup workflow does
// for every multipart destination.
func startFollow(tr *Transfer, h guard.Handle, sp *spool.Spool, g *pipeline.Growth, key string) <-chan followResult {
	ch := make(chan followResult, 1)
	go func() {
		out, err := tr.Follow(context.Background(), h, sp, g, Request{Key: key, Label: "g/n"})
		ch <- followResult{out: out, err: err}
	}()
	return ch
}

// ledgerWith binds one destination to a protection ledger with the given cap.
// A known survey makes a manual admission meaningful; unknown reports unknown
// occupancy.
func ledgerWith(t *testing.T, d guard.Destination, cap int64, known bool, used int64) *protection.Ledger {
	t.Helper()
	l := protection.NewLedger(protection.LedgerOptions{
		Destinations: []guard.Destination{d},
		Caps:         map[string]int64{d.Name(): cap},
		Retry:        testRetry(),
	})
	survey := inventory.Survey{Storage: d.Name(), OK: known}
	if used > 0 {
		survey.Runs = map[string]inventory.Run{"old": {Objects: []model.ObjectInfo{{Size: used}}}}
	}
	l.Start(map[string]inventory.Survey{d.Name(): survey})
	return l
}

// TestOverlapStartsPartsWhileTheProducerWrites: the first part is accepted
// before the source's Dump returns — the upload is not waiting for the artifact
// to be whole.
func TestOverlapStartsPartsWhileTheProducerWrites(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	partSent := make(chan struct{})
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 1 {
			once.Do(func() { close(partSent) })
		}
	}
	follow := startFollow(newTransfer(Options{}), h, sp, g, "runs/r/a.bin")
	go func() {
		<-partSent
		if src.done() {
			t.Error("the first part arrived only after the producer had finished")
		}
		close(src.release)
	}()
	pa, perr := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g)
	if perr != nil {
		t.Fatal(perr)
	}
	res := <-follow
	if res.err != nil || !res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the overlapping upload to handle the object", res.out, res.err)
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 0 || completes != 1 {
		t.Fatalf("aborts=%d completes=%d, want a clean commit", aborts, completes)
	}
	if got, ok := parts.Bytes("runs/r/a.bin"); !ok || !bytes.Equal(got, []byte("0123456789")) {
		t.Fatalf("the committed object is %q", got)
	}
	if res.out.Attempts == 0 || pa.Bytes != 10 {
		t.Fatalf("attempts=%d produced=%d, want counted calls over the ten bytes", res.out.Attempts, pa.Bytes)
	}
}

// TestOverlapAbortsWhenTheProducerFails: a producer that wrote its bytes and
// then failed never completes the upload; the one session is aborted exactly
// once, after every in-flight part returned, and the caller keeps the fallback.
func TestOverlapAbortsWhenTheProducerFails(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		err:       func(int) error { return errors.New("dump failed") },
		gateAfter: 1, release: make(chan struct{}),
	}
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 1 {
			once.Do(func() { close(src.release) })
		}
	}
	parts.partDelay = func(number int32, ctx context.Context) error {
		if number == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	follow := startFollow(newTransfer(Options{CallParallelism: 2}), h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err == nil {
		t.Fatal("the producer reported success")
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the whole-object fallback", res.out, res.err)
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want exactly one abort and no complete", aborts, completes)
	}
	_, abortInFlight, journal, _ := parts.flightStats()
	if len(abortInFlight) != 1 || abortInFlight[0] != 0 {
		t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
	}
	if after := partsBetweenTheAbortAndTheNextBegin(journal); len(after) != 0 {
		t.Fatalf("parts sent after the abort: %v", after)
	}
}

// partsBetweenTheAbortAndTheNextBegin lists the journal entries that sent a part
// between an abort and the next session: a correct upload has none.
func partsBetweenTheAbortAndTheNextBegin(journal []string) []string {
	var out []string
	aborted := false
	for _, entry := range journal {
		switch {
		case entry == "abort":
			aborted = true
		case entry == "begin":
			aborted = false
		case aborted && strings.HasPrefix(entry, "part "):
			out = append(out, entry)
		}
	}
	return out
}

// TestOverlapAbortsOnArtifactOverflow: a producer that crosses
// run.max_artifact_bytes mid-stream aborts the open session once and sends no
// part after it.
func TestOverlapAbortsOnArtifactOverflow(t *testing.T) {
	parts := newFakeMultipart(3)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123"), []byte("456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	parts.partHook = func(number int32) {
		if number == 1 {
			close(src.release)
		}
	}
	follow := startFollow(newTransfer(Options{}), h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 6), sp, producePlan(src), g); err == nil {
		t.Fatal("a producer past its cap reported success")
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the whole-object fallback", res.out, res.err)
	}
	_, aborts, completes, calls := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want one abort and no complete", aborts, completes)
	}
	if calls[2] != 0 {
		t.Fatalf("a part after the overflow was sent: %v", calls)
	}
}

// TestOverlapAbortsOnCancellation: a cancelled run aborts the open session once
// — after every in-flight part returned — and no part is sent after the abort.
func TestOverlapAbortsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 1 {
			once.Do(cancel)
		}
	}
	parts.partDelay = func(number int32, ctx context.Context) error {
		if number == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	tr := newTransfer(Options{CallParallelism: 2})
	done := make(chan followResult, 1)
	go func() {
		out, err := tr.Follow(ctx, h, sp, g, Request{Key: "runs/r/a.bin", Label: "g/n"})
		done <- followResult{out: out, err: err}
	}()
	if _, err := produce(ctx, newProducer(1, 1<<20), sp, producePlan(src), g); err == nil {
		t.Fatal("a cancelled producer reported success")
	}
	res := <-done
	if res.err == nil || !res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the cancelled upload reported", res.out, res.err)
	}
	if _, aborts, _, _ := parts.stats(); aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1 after a cancelled upload", aborts)
	}
	_, abortInFlight, journal, _ := parts.flightStats()
	if len(abortInFlight) != 1 || abortInFlight[0] != 0 {
		t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
	}
	if after := partsBetweenTheAbortAndTheNextBegin(journal); len(after) != 0 {
		t.Fatalf("parts sent after the abort: %v", after)
	}
}

// TestManualBoundRefusalFallsBackToTheExactSize: a manual run whose artifact
// bound does not fit the cap does not overlap — the caller uploads the exact
// size after the producer.
func TestManualBoundRefusalFallsBackToTheExactSize(t *testing.T) {
	parts := newFakeMultipart(4)
	dest := destination(t, "a_parts", parts)
	h := handle(t, dest)
	ledger := ledgerWith(t, dest, 20, true, 0)
	var mu sync.Mutex
	var sizes []int64
	parts.beginHook = func(req storage.PutRequest) {
		mu.Lock()
		sizes = append(sizes, req.Size)
		mu.Unlock()
	}
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789")} }}
	tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100, Manual: true})
	follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
	pa, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g)
	if err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err != nil || res.out.Handled || res.out.Parts != 0 {
		t.Fatalf("follow = %+v, %v; want the refused overlap", res.out, res.err)
	}
	if _, err := tr.Upload(context.Background(), h, sp, Request{Key: "runs/r/a.bin", Label: "g/n", File: pa.File, Size: pa.Bytes}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 1 || sizes[0] != 10 {
		t.Fatalf("begin sizes %v, want the one exact size after the producer", sizes)
	}
}

// TestFollowWithoutCapability: a whole-object destination is not called while
// the producer writes.
func TestFollowWithoutCapability(t *testing.T) {
	plain := storagetest.NewFake()
	h := handle(t, destination(t, "plain", plain))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}), blocked: make(chan struct{}),
	}
	tr := newTransfer(Options{})
	follow := make(chan followResult, 1)
	go func() {
		out, err := tr.Follow(context.Background(), h, sp, g, Request{Key: "runs/r/a.bin", Label: "g/n"})
		follow <- followResult{out: out, err: err}
	}()
	go func() {
		<-src.blocked
		if got := plain.CallCount(storagetest.OpPut, "runs/r/a.bin"); got != 0 {
			t.Errorf("a put ran before the producer finished: %d", got)
		}
		close(src.release)
	}()
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the whole-object path", res.out, res.err)
	}
}

// TestFollowProducerFinishesBeforeTheUploadOpens: an object that fits one
// request leaves the finished file to the caller, and the ledger shrinks the
// bound to the exact produced size.
func TestFollowProducerFinishesBeforeTheUploadOpens(t *testing.T) {
	parts := newFakeMultipart(1 << 20)
	dest := destination(t, "a_parts", parts)
	h := handle(t, dest)
	ledger := ledgerWith(t, dest, 100, true, 0)
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{chunks: func(int) [][]byte { return [][]byte{[]byte("tiny")} }}
	tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100})
	follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the whole-object path", res.out, res.err)
	}
	if _, _, _, calls := parts.stats(); len(calls) != 0 {
		t.Fatalf("an object that fits one request used the multipart path: %v", calls)
	}
	if got := ledger.Used("a_parts"); got != 4 {
		t.Fatalf("ledger used = %d, want the exact produced 4 bytes after the shrink", got)
	}
}

// TestOverlapDoesNotChargeProducerWaitToTheBudget: a destination whose calls
// take a little time keeps its budget while the producer spends most of the run
// writing.
func TestOverlapDoesNotChargeProducerWaitToTheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(4)
		h := handle(t, destination(t, "a_parts", parts))
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		src := &scriptedSource{
			chunks:     func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
			sleepAfter: 1, sleep: 60 * time.Millisecond,
		}
		clock := execution.NewClock(50 * time.Millisecond)
		tr := newTransfer(Options{Clock: clock})
		follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
		if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
			t.Fatal(err)
		}
		res := <-follow
		if res.err != nil || !res.out.Handled {
			t.Fatalf("follow = %+v, %v; want the overlapping upload to hold", res.out, res.err)
		}
		if got := clock.Total(); got != 0 {
			t.Fatalf("the producer's wait was charged to the destination: %s", got)
		}
	})
}

// TestOverlapRetryUploadsTheSecondAttemptBytes: an attempt 1 upload is aborted
// once when the producer fails; the retry's bytes are what the fallback upload
// stores.
func TestOverlapRetryUploadsTheSecondAttemptBytes(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	second := []byte("abcdefghij")
	src := &scriptedSource{
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
	var once sync.Once
	parts.partHook = func(number int32) {
		if number == 2 {
			once.Do(func() { close(src.release) })
		}
	}
	tr := newTransfer(Options{})
	follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
	pa, err := produce(context.Background(), newProducer(2, 1<<20), sp, producePlan(src), g)
	if err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the fallback after the producer's failure", res.out, res.err)
	}
	if _, err := tr.Upload(context.Background(), h, sp, Request{Key: "runs/r/a.bin", Label: "g/n", File: pa.File, Size: pa.Bytes}); err != nil {
		t.Fatal(err)
	}
	_, aborts, completes, _ := parts.stats()
	if aborts != 1 || completes != 1 {
		t.Fatalf("aborts=%d completes=%d, want the failed attempt aborted and the retry committed", aborts, completes)
	}
	if got, ok := parts.Bytes("runs/r/a.bin"); !ok || !bytes.Equal(got, second) {
		t.Fatalf("the stored object is %q, want the second attempt's bytes", got)
	}
	if got := parts.sessionCount(); got != 2 {
		t.Fatalf("sessions = %d, want the failed attempt's upload and the fallback's", got)
	}
}

// TestOverlapPermanentPartFailsTheUpload: a permanent part error aborts that
// destination's upload once — while the producer is still held, not after its
// long wait — and reports the part's own refusal, never its neighbour's
// cancellation.
func TestOverlapPermanentPartFailsTheUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(4)
		h := handle(t, destination(t, "a_parts", parts))
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		parts.partDelay = func(number int32, ctx context.Context) error {
			if number == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		parts.partErr = func(number int32, _ int) error {
			if number == 2 {
				return pluginerrors.Permanent(errors.New("part refused"))
			}
			return nil
		}
		var abortedEarly bool
		parts.abortHook = func() {}
		src := &scriptedSource{
			chunks:     func(int) [][]byte { return [][]byte{[]byte("0123456789abcd")} },
			sleepAfter: 1, sleep: 10 * time.Minute,
		}
		parts.abortHook = func() { abortedEarly = !src.done() }
		tr := newTransfer(Options{CallParallelism: 2})
		follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
		// The producer's own hold ends only after its long wait; the upload ends
		// through the part's refusal, which is what abortedEarly observes.
		if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
			t.Fatal(err)
		}
		res := <-follow
		if res.err == nil || !strings.Contains(res.err.Error(), "part refused") {
			t.Fatalf("follow = %v, want the part's own refusal", res.err)
		}
		if strings.Contains(res.err.Error(), "context canceled") || strings.Contains(res.err.Error(), "context deadline exceeded") {
			t.Fatalf("follow = %q: the neighbour's cancellation hid the cause", res.err)
		}
		if _, aborts, completes, calls := parts.stats(); aborts != 1 || completes != 0 || calls[3] != 0 {
			t.Fatalf("aborts=%d completes=%d calls=%v, want one abort, no complete and no part 3", aborts, completes, calls)
		}
		if !abortedEarly {
			t.Fatal("the abort waited for the producer's ten-minute hold to end")
		}
	})
}

// TestOverlapBeginProbeErrorFailsTheUpload: a BeginMultipart that answers a real
// error while the producer is still writing fails that destination's copy.
func TestOverlapBeginProbeErrorFailsTheUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: pluginerrors.Permanent(errors.New("begin refused"))}
		h := handle(t, destination(t, "a_parts", z))
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		src := &scriptedSource{
			chunks:     func(int) [][]byte { return [][]byte{[]byte("01234"), []byte("56789")} },
			sleepAfter: 1, sleep: 20 * time.Millisecond,
		}
		follow := startFollow(newTransfer(Options{}), h, sp, g, "runs/r/a.bin")
		if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
			t.Fatal(err)
		}
		res := <-follow
		if res.err == nil || !strings.Contains(res.err.Error(), "begin refused") || !res.out.Handled {
			t.Fatalf("follow = %+v, %v; want the begin failure", res.out, res.err)
		}
	})
}

// TestOverlapProducerFailsBeforeAnyUpload: a producer that fails before a byte
// is written leaves the destination untouched — no upload opened, nothing
// reported against it.
func TestOverlapProducerFailsBeforeAnyUpload(t *testing.T) {
	parts := newFakeMultipart(4)
	dest := destination(t, "a_parts", parts)
	h := handle(t, dest)
	ledger := ledgerWith(t, dest, 100, true, 0)
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks: func(int) [][]byte { return nil },
		err:    func(int) error { return pluginerrors.Permanent(errors.New("dump refused")) },
	}
	follow := startFollow(newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100}), h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err == nil {
		t.Fatal("the refused producer reported success")
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the destination untouched", res.out, res.err)
	}
	if begins, aborts, completes, _ := parts.stats(); begins != 0 || aborts != 0 || completes != 0 {
		t.Fatalf("a producer that wrote nothing opened or committed an upload: begins=%d aborts=%d completes=%d", begins, aborts, completes)
	}
	if got := ledger.Used("a_parts"); got != 0 {
		t.Fatalf("ledger used = %d, want the abandoned booking released", got)
	}
}

// TestFollowSourceNextHonoursCancellation: a follower waiting for a producer
// that may never write again ends with the run's context — never sleeps past it.
func TestFollowSourceNextHonoursCancellation(t *testing.T) {
	src := &followSource{g: pipeline.NewGrowth()}
	if _, _, err := src.next(context.Background(), -1); err != nil {
		t.Fatalf("a fresh growth: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := src.next(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled wait = %v, want the context's error", err)
	}
}

// TestFollowFinishFailureFailsTheUpload: an overlapping upload whose Complete
// commits nothing is caught by the Stat after it, aborted once, and reported as
// that destination's failure — the follow path's finish, not the whole-object
// fallback.
func TestFollowFinishFailureFailsTheUpload(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.completeDrop = true
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	parts.partHook = func(number int32) {
		if number == 1 {
			select {
			case <-src.release:
			default:
				close(src.release)
			}
		}
	}
	follow := startFollow(newTransfer(Options{CallParallelism: 2}), h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err == nil || !res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the failed finish", res.out, res.err)
	}
	if _, aborts, _, _ := parts.stats(); aborts != 1 {
		t.Fatalf("aborts = %d, want the dropped completion aborted once", aborts)
	}
	if _, ok := parts.Bytes("runs/r/a.bin"); ok {
		t.Fatalf("the dropped completion left an object: %v", parts.Keys())
	}
}

// TestFollowLedgerCommitAndUnreserve: a verified overlap turns the reserved
// bound into occupancy; a part refusal that aborts cleanly releases it.
func TestFollowLedgerCommitAndUnreserve(t *testing.T) {
	t.Run("success commits the exact size", func(t *testing.T) {
		parts := newFakeMultipart(4)
		dest := destination(t, "a_parts", parts)
		h := handle(t, dest)
		ledger := ledgerWith(t, dest, 100, true, 0)
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		src := &scriptedSource{
			chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
			gateAfter: 1, release: make(chan struct{}),
		}
		parts.partHook = func(int32) { close(src.release) }
		tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100})
		follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
		var once sync.Once
		parts.partHook = func(int32) { once.Do(func() { close(src.release) }) }
		if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
			t.Fatal(err)
		}
		if res := <-follow; res.err != nil || !res.out.Handled {
			t.Fatalf("follow = %+v, %v", res.out, res.err)
		}
		if got := ledger.Used("a_parts"); got != 10 {
			t.Fatalf("ledger used = %d, want the committed 10 bytes", got)
		}
	})
	t.Run("a part refusal releases the booking", func(t *testing.T) {
		parts := newFakeMultipart(4)
		dest := destination(t, "a_parts", parts)
		h := handle(t, dest)
		ledger := ledgerWith(t, dest, 100, true, 0)
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		parts.partErr = func(number int32, _ int) error {
			return pluginerrors.Permanent(errors.New("part refused"))
		}
		src := &scriptedSource{
			chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
			gateAfter: 1, release: make(chan struct{}),
		}
		parts.partHook = func(int32) { close(src.release) }
		tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100})
		follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
		if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
			t.Fatal(err)
		}
		if res := <-follow; res.err == nil {
			t.Fatal("a refused part reported success")
		}
		if got := ledger.Used("a_parts"); got != 0 {
			t.Fatalf("ledger used = %d, want the booking released after the abort", got)
		}
	})
}

// TestFailedAbortKeepsTheOldAttemptCounted: an attempt whose Abort failed leaves
// parts that may still be billed. The booking stays, and the next write sees
// those bytes: the ledger never understates what may remain.
func TestFailedAbortKeepsTheOldAttemptCounted(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: errors.New("abort failed")}
	dest := destination(t, "a_parts", z)
	h := handle(t, dest)
	ledger := ledgerWith(t, dest, 100, true, 0)
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		err:       func(int) error { return errors.New("dump interrupted") },
		gateAfter: 1, release: make(chan struct{}),
	}
	z.partHook = func(number int32) {
		if number == 1 {
			select {
			case <-src.release:
			default:
				close(src.release)
			}
		}
	}
	tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 80, Manual: true})
	follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err == nil {
		t.Fatal("the failed producer reported success")
	}
	res := <-follow
	if res.err == nil || !strings.Contains(res.err.Error(), "aborting the failed attempt") {
		t.Fatalf("follow = %v, want the failed abort", res.err)
	}
	if got := ledger.Used("a_parts"); got != 80 {
		t.Fatalf("ledger used = %d, want the leftover 80-byte booking", got)
	}
	if ledger.Admit("a_parts", "runs/r/b.bin", 30, true) {
		t.Fatal("a manual write was admitted beside a booking the failed abort kept")
	}
}

// TestEarlyProducerFailureReleasesFollowers: a producer that fails before its
// first attempt — spool reserve, statfs, Create — still wakes every follower at
// once; nothing waits for the run deadline.
func TestEarlyProducerFailureReleasesFollowers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(4)
		h := handle(t, destination(t, "a_parts", parts))
		sp := newSpool(t)
		g := pipeline.NewGrowth()
		p := pipeline.New(pipeline.Options{
			Policy:           testRetry(),
			ReserveBytes:     1 << 62,
			MaxArtifactBytes: 1 << 20,
			ProgressInterval: time.Hour,
			Log:              testLog(),
		})
		follow := startFollow(newTransfer(Options{}), h, sp, g, "runs/r/a.bin")
		start := time.Now()
		if _, err := produce(context.Background(), p, sp, producePlan(&scriptedSource{chunks: func(int) [][]byte { return nil }}), g); err == nil {
			t.Fatal("a spool without reserve produced an artifact")
		}
		res := <-follow
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("the reserve failure waited %s for the run deadline", elapsed)
		}
		if res.err != nil || res.out.Handled {
			t.Fatalf("follow = %+v, %v; want the destination untouched", res.out, res.err)
		}
		if begins, _, _, _ := parts.stats(); begins != 0 {
			t.Fatalf("begins = %d, want none", begins)
		}
	})
}

// TestOverlapProbeWaitsForTheFirstWholePart: while the produced bytes still fit
// one request the probe answers errors.ErrUnsupported, and the finished file
// goes out through the whole-object path.
func TestOverlapProbeWaitsForTheFirstWholePart(t *testing.T) {
	parts := newFakeMultipart(1 << 20)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("tiny")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	probed := make(chan struct{})
	var once sync.Once
	parts.beginHook = func(storage.PutRequest) { once.Do(func() { close(probed) }) }
	follow := startFollow(newTransfer(Options{}), h, sp, g, "runs/r/a.bin")
	go func() {
		<-probed
		close(src.release)
	}()
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err != nil || res.out.Handled {
		t.Fatalf("follow = %+v, %v; want the whole-object path", res.out, res.err)
	}
	if begins, aborts, completes, _ := parts.stats(); begins < 1 || aborts != 0 || completes != 0 {
		t.Fatalf("begins=%d aborts=%d completes=%d, want the probe only", begins, aborts, completes)
	}
}

// TestOverlapBeginErrorReleasesTheBooking: a probe refused with a real error
// releases the bound it booked before the session existed.
func TestOverlapBeginErrorReleasesTheBooking(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: pluginerrors.Permanent(errors.New("begin refused"))}
	dest := destination(t, "a_parts", z)
	h := handle(t, dest)
	ledger := ledgerWith(t, dest, 100, true, 0)
	sp := newSpool(t)
	g := pipeline.NewGrowth()
	src := &scriptedSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	z.beginHook = func(storage.PutRequest) { close(src.release) }
	tr := newTransfer(Options{Accounting: ledger.Accounting(), Storage: "a_parts", MaxArtifactBytes: 100})
	follow := startFollow(tr, h, sp, g, "runs/r/a.bin")
	if _, err := produce(context.Background(), newProducer(1, 1<<20), sp, producePlan(src), g); err != nil {
		t.Fatal(err)
	}
	res := <-follow
	if res.err == nil || !strings.Contains(res.err.Error(), "begin refused") {
		t.Fatalf("follow = %v, want the begin failure", res.err)
	}
	if got := ledger.Used("a_parts"); got != 0 {
		t.Fatalf("ledger used = %d, want the refused booking released", got)
	}
}
