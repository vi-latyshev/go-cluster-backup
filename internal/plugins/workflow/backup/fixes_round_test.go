package backup_test

// Iteration-1 fixes at the workflow boundary: C7 (the asynchronous-writer
// capability, not multipart-only), M4 (the invocation's one ownership scope and
// its bounded close) and C2 (the workflow receives a read port, nothing else).

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// asyncOnlyStorage hides every multipart method: its only signal is the explicit
// asynchronous-writer capability, like a backend whose Put answers 202 and
// settles later. A multipart-only classifier never sees it.
type asyncOnlyStorage struct{ *storagetest.Fake }

func (*asyncOnlyStorage) WritesAsynchronously() bool { return true }

// TestC7_AsyncCapabilityMarksBeforeTheFirstWrite: the explicit capability, with
// no multipart method at all, makes the run add its own id to the durable record
// BEFORE the first potentially asynchronous write; the settled run clears its
// own mark at the end.
func TestC7_AsyncCapabilityMarksBeforeTheFirstWrite(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	w.ExtraStorages["alpha"] = &asyncOnlyStorage{Fake: f}
	w.Storages["alpha"] = f
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("record")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "record", record)

	var firstPut []string
	f.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpPut && firstPut == nil {
			if unc := record.Uncertainty(); unc != nil {
				firstPut = append([]string(nil), unc.RunIDs...)
			}
		}
		return nil
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
	}
	if !slices.Contains(firstPut, rep.RunID) {
		t.Fatalf("the first asynchronous write ran without this run's id in the durable record: %v", firstPut)
	}
	if got := record.Uncertainty(); got != nil {
		t.Fatalf("a settled run kept its own mark: %+v", got)
	}
}

// TestC7_InheritedPrefixesSurviveTheSuccessor: a successor adds its own id to
// the pending set and never drops the inherited one; a settled successor clears
// only its own record, so the dead holder's prefix stays protected.
func TestC7_InheritedPrefixesSurviveTheSuccessor(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	w.ExtraStorages["alpha"] = &asyncOnlyStorage{Fake: f}
	w.Storages["alpha"] = f
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("record")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "record", record)
	record.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})

	var firstPut []string
	f.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpPut && firstPut == nil {
			if unc := record.Uncertainty(); unc != nil {
				firstPut = append([]string(nil), unc.RunIDs...)
			}
		}
		return nil
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
	}
	if !slices.Contains(firstPut, "dead") || !slices.Contains(firstPut, rep.RunID) {
		t.Fatalf("the first write's record does not carry both pending holders: %v", firstPut)
	}
	unc := record.Uncertainty()
	if unc == nil {
		t.Fatal("the settled successor cleared the inherited uncertainty")
	}
	if !slices.Contains(unc.RunIDs, "dead") {
		t.Fatalf("the successor dropped the dead holder's prefix: %+v", unc)
	}
	if slices.Contains(unc.RunIDs, rep.RunID) {
		t.Fatalf("the settled successor kept its own id: %+v", unc)
	}
}

// TestC7_NoDurableRecordRefusesAnAsyncWrite: without a catalog there is no
// record to mark, so the run refuses before the first asynchronous write.
func TestC7_NoDurableRecordRefusesAnAsyncWrite(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	w.ExtraStorages["alpha"] = &asyncOnlyStorage{Fake: f}
	w.Storages["alpha"] = f
	e := w.newAssembly(t, baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")}))

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || !strings.Contains(rep.Lease.Error, "durable writer lease") {
		t.Fatalf("an asynchronous run without a durable record was not refused: outcome=%q lease=%+v", rep.Outcome, rep.Lease)
	}
	puts := 0
	for _, ev := range f.Events() {
		if ev.Op == storagetest.OpPut {
			puts++
		}
	}
	if puts != 0 {
		t.Fatalf("a refused asynchronous run wrote %d objects", puts)
	}
}

// TestM4_IncompleteJoinPoisonsTheInvocationScope: a request that ignores
// cancellation outlives the join deadline; the invocation's one scope is
// poisoned, so the role's later Close refuses and closes nothing.
func TestM4_IncompleteJoinPoisonsTheInvocationScope(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	parts.beginStuck = make(chan struct{})
	src := &closingSource{fakeSource: fakeSource{Data: []byte("0123456789")}}
	w.ExtraSources["vpn"] = src
	cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	e := serviceCheck(t, w, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
		done <- err
	}()
	for begins := 0; begins == 0; {
		begins, _, _, _ = parts.stats()
		if begins > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the run returned before reaching the multipart begin: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("an incomplete join returned no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancel sequence did not stop at the join")
	}
	if err := e.Close(context.Background()); !errors.Is(err, lifecycle.ErrIncompleteJoin) {
		t.Fatalf("a poisoned invocation Close = %v, want ErrIncompleteJoin", err)
	}
	if got := src.closeCount(); got != 0 {
		t.Fatalf("a poisoned invocation closed its clients %d times", got)
	}
	close(parts.beginStuck)
	time.Sleep(20 * time.Millisecond)
}

// TestM4_CloseIsBoundedByTheCallersContext: the invocation owner's context
// bounds the close; a context already past its deadline starts no closer and
// reports the deadline instead of closing under code that may still run.
func TestM4_CloseIsBoundedByTheCallersContext(t *testing.T) {
	w := newWorld(t)
	src := &closingSource{}
	w.ExtraSources["vpn"] = src
	e := w.newAssembly(t, baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Close(ctx); err == nil {
		t.Fatal("a cancelled cleanup context still closed the invocation")
	}
	if got := src.closeCount(); got != 0 {
		t.Fatalf("a cancelled cleanup context started %d closers", got)
	}
}

// TestC2_WorkflowStorageIsAPort: the workflow's storage projection is a read
// port; there is no Destination, no Open and no Remover to reach a write or a
// delete from the Floor.
func TestC2_WorkflowStorageIsAPort(t *testing.T) {
	typ := reflect.TypeOf(backup.Storage{})
	if _, ok := typ.FieldByName("Destination"); ok {
		t.Fatal("the workflow storage still carries a Destination")
	}
	port, ok := typ.FieldByName("Port")
	if !ok {
		t.Fatal("the workflow storage carries no read port")
	}
	if port.Type != reflect.TypeOf(guard.Port{}) {
		t.Fatalf("workflow storage Port is %v, want guard.Port", port.Type)
	}
	for _, banned := range []string{"Remover", "Delete", "Put"} {
		if _, ok := reflect.TypeOf(guard.Port{}).MethodByName(banned); ok {
			t.Fatalf("guard.Port exposes %s to the workflow", banned)
		}
	}
}

// observeSecondFailingLease records the write-ahead marker, then fails the
// settlement observation: the run continues with occupancy unknown.
type observeSecondFailingLease struct {
	*catalogtest.Leaser
	observes int
}

func (l *observeSecondFailingLease) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	l.observes++
	if l.observes > 1 {
		return errors.New("the observation record is unavailable")
	}
	return l.Leaser.ObserveUncertainty(ctx, lease, digest)
}

// TestC7_TheSettlementObservationFailureKeepsTheRecord: the write-ahead marker
// succeeded, so the survey is settlement evidence, but recording its digest
// fails; the run keeps the record conservative and still finishes.
func TestC7_TheSettlementObservationFailureKeepsTheRecord(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("failing")
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	w.Notifiers["ops"] = &fakeNotifier{}
	record := &observeSecondFailingLease{Leaser: catalogtest.NewLeaser()}
	registerCatalogType(t, w, "failing", record)
	record.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("a failed settlement observation lowered the outcome to %q: %+v", rep.Outcome, rep.Storages)
	}
	if !slices.ContainsFunc(rep.Warnings, func(w model.Warning) bool {
		return w.Code == "catalog.uncertainty.settle.failed" &&
			strings.Contains(w.Message, "occupancy stays unknown")
	}) {
		t.Fatalf("the failed settlement is not a coded warning: %+v", rep.Warnings)
	}
	// The finished message is composed from the same report: the warning reaches
	// the notifier, not only the log.
	ev, ok := w.Notifiers["ops"].Finished()
	if !ok {
		t.Fatal("the finished event was not delivered")
	}
	if !slices.ContainsFunc(ev.Run.Warnings, func(w model.Warning) bool {
		return w.Code == "catalog.uncertainty.settle.failed"
	}) {
		t.Fatalf("the finished message lost the catalog warning: %+v", ev.Run.Warnings)
	}
	if record.Uncertainty() == nil {
		t.Fatal("a failed settlement observation cleared the record")
	}
}

// TestC7_ManualAsyncRunWithoutInheritedUncertaintyIsAdmitted reproduces the e2e
// regression: an empty async-capable storage, a manual run, no inherited
// uncertainty. The run's own pending id must not make the survey's occupancy
// unknown, so the manual write is admitted against max_total_bytes.
func TestC7_ManualAsyncRunWithoutInheritedUncertaintyIsAdmitted(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("a manual run on an empty async storage was not delivered: %+v", rep.Storages)
	}
	if got := storageResult(rep, "alpha"); got.Outcome != model.StorageVerified {
		t.Fatalf("the async storage did not verify the run: %+v", got)
	}
	if begins, _, completes, _ := parts.stats(); begins == 0 || completes == 0 {
		t.Fatalf("the async upload never ran: begins=%d completes=%d", begins, completes)
	}
	if unc := w.Catalog.Uncertainty(); unc != nil {
		t.Fatalf("a settled run kept its own durable mark: %+v", unc)
	}
}

// TestC7_ManualAsyncRunUnderAnInheritedHolderRefuses: another holder's pending
// prefix is exactly what unknown occupancy means; a manual run refuses there.
func TestC7_ManualAsyncRunUnderAnInheritedHolderRefuses(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newAssembly(t, cfg)
	w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("a manual run under an inherited holder was admitted: %+v", rep.Storages)
	}
	if got := storageResult(rep, "alpha"); !strings.Contains(got.Error, "cap_exceeded") && !strings.Contains(got.Error, "unknown") {
		t.Fatalf("the refusal does not name the unknown occupancy: %+v", got)
	}
	if begins, _, _, _ := parts.stats(); begins != 0 {
		t.Fatalf("a refused manual run started %d uploads", begins)
	}
	unc := w.Catalog.Uncertainty()
	if unc == nil || !slices.Contains(unc.RunIDs, "dead") {
		t.Fatalf("the inherited holder's prefix was dropped: %+v", unc)
	}
}

// TestC7_ScheduledAsyncRunKeepsItsOwnCapacityKnown: a scheduled run never
// refuses on the cap, but its own mark must not close cap eviction either; an
// inherited holder still closes it (the previous test proves that half).
func TestC7_ScheduledAsyncRunKeepsItsOwnCapacityKnown(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	withRetention(cfg, "alpha", keepLastN(10))
	var logBuf bytes.Buffer
	e := newAssemblyLogged(t, w, cfg, &logBuf)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	putCopiedRun(t, f, "alpha", "old", base, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("a scheduled run failed on its own async mark: %+v", rep.Storages)
	}
	if begins, _, _, _ := parts.stats(); begins == 0 {
		t.Fatal("the scheduled async upload never ran")
	}
	if strings.Contains(logBuf.String(), "storage.cap.unknown") {
		t.Fatalf("the run's own mark closed cap eviction:\n%s", logBuf.String())
	}
}
