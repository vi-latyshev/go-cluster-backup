package kubernetesconfigmap_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// markedLease hands the catalog the pending set the caller decided: its own run
// id on a clean record, every inherited id plus its own otherwise.
func markedLease(lease catalog.Lease, runIDs ...string) catalog.Lease {
	lease.Uncertainty = &catalog.Uncertainty{RunIDs: slices.Clone(runIDs), Since: time.Now().UTC()}
	return lease
}

// TestObserveUncertaintyRecordsTheDigest: a full survey's digest settles on the
// record's existing uncertainty, keeping its run and time; it survives the
// holder's release, is inherited by the next acquire and a takeover, and is
// removed with the uncertainty on clear.
func TestObserveUncertaintyRecordsTheDigest(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	observer := c.(catalog.UncertaintyObserver)
	ctx := context.Background()
	key := "configmaps/" + namespace + "/" + name

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	// Without an uncertainty the observation writes nothing.
	before := srv.Calls(key)
	if err := observer.ObserveUncertainty(ctx, held, "premature"); err != nil {
		t.Fatalf("ObserveUncertainty without an uncertainty = %v", err)
	}
	if got := srv.Calls(key) - before; got != 1 {
		t.Fatalf("the observation made %d requests, want its one read", got)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatal(err)
	}
	marked := readRecord(t, srv).Uncertainty
	if marked == nil {
		t.Fatal("MarkUncertainty recorded nothing")
	}
	if err := observer.ObserveUncertainty(ctx, held, "digest-1"); err != nil {
		t.Fatalf("ObserveUncertainty = %v", err)
	}
	observed := readRecord(t, srv).Uncertainty
	if observed == nil || observed.Digest != "digest-1" {
		t.Fatalf("the observation = %+v", observed)
	}
	if !slices.Equal(observed.RunIDs, marked.RunIDs) || !observed.Since.Equal(marked.Since) {
		t.Fatalf("the observation rewrote the uncertainty: %+v", observed)
	}

	// The digest belongs to the record: the holder's release keeps it.
	if err := c.Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	next, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Uncertainty == nil || next.Uncertainty.Digest != "digest-1" {
		t.Fatalf("the released record's digest was not inherited: %+v", next.Uncertainty)
	}

	// A takeover keeps the digest the record already carries.
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c", TakeoverToken: next.Token})
	if err != nil {
		t.Fatal(err)
	}
	if taken.Uncertainty == nil || taken.Uncertainty.Digest != "digest-1" {
		t.Fatalf("the takeover did not inherit the digest: %+v", taken.Uncertainty)
	}

	// The successor observes a fresh inventory; a clear removes it with the
	// uncertainty.
	if err := observer.ObserveUncertainty(ctx, taken, "digest-2"); err != nil {
		t.Fatalf("the successor's ObserveUncertainty = %v", err)
	}
	if observed := readRecord(t, srv).Uncertainty; observed == nil || observed.Digest != "digest-2" {
		t.Fatalf("the successor's observation = %+v", observed)
	}
	if err := c.ClearUncertainty(ctx, taken); err != nil {
		t.Fatalf("ClearUncertainty = %v", err)
	}
	if record := readRecord(t, srv); record.Uncertainty != nil {
		t.Fatalf("the cleared record carries %+v", record.Uncertainty)
	}
}

// TestObserveUncertaintyIgnoresAStaleHolder: a stale token writes nothing, and a
// cancelled context neither writes nor reports the observation.
func TestObserveUncertaintyIgnoresAStaleHolder(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	observer := c.(catalog.UncertaintyObserver)
	ctx := context.Background()
	key := "configmaps/" + namespace + "/" + name

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatal(err)
	}
	before := srv.Calls(key)
	if err := observer.ObserveUncertainty(ctx, catalog.Lease{Token: "stale-token"}, "stale-digest"); err != nil {
		t.Fatalf("a stale observation = %v", err)
	}
	if got := srv.Calls(key) - before; got != 1 {
		t.Fatalf("a stale observation made %d requests, want its one read", got)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	before = srv.Calls(key)
	if err := observer.ObserveUncertainty(cancelled, held, "cancelled-digest"); err == nil {
		t.Fatal("a cancelled observation returned success")
	}
	if got := srv.Calls(key) - before; got != 0 {
		t.Fatalf("a cancelled observation made %d requests, want none", got)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || record.Uncertainty.Digest != "" {
		t.Fatalf("a refused observation changed the record: %+v", record.Uncertainty)
	}
}

// TestUnencodableDigestIsPermanent: a digest the JSON encoder cannot render is
// refused as permanent and leaves the record as it was.
func TestUnencodableDigestIsPermanent(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatal(err)
	}
	err = c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, held, string([]byte{0xff}))
	if err == nil || !strings.Contains(err.Error(), "encode lease") {
		t.Fatalf("ObserveUncertainty = %v, want the encode refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || record.Uncertainty.Digest != "" {
		t.Fatalf("the unencodable digest changed the record: %+v", record.Uncertainty)
	}
}

// TestUnencodableRunIDIsPermanent: a pending set the JSON encoder cannot render
// is refused as permanent and leaves the record as it was.
func TestUnencodableRunIDIsPermanent(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.MarkUncertainty(ctx, markedLease(held, string([]byte{0xff})))
	if err == nil || !strings.Contains(err.Error(), "encode lease") {
		t.Fatalf("MarkUncertainty = %v, want the encode refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if record := readRecord(t, srv); record.Uncertainty != nil {
		t.Fatalf("the unencodable run id changed the record: %+v", record.Uncertainty)
	}
}

// TestMarkAndClearLifecycle: the record takes the pending set exactly as the
// caller hands it — so a successor keeps the inherited ids by handing them back
// with its own — a release keeps it, clearing removes it, and a lease without a
// set never clears it.
func TestMarkAndClearLifecycle(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ClearUncertainty(ctx, held); err != nil {
		t.Fatalf("ClearUncertainty on a known record = %v", err)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatalf("MarkUncertainty = %v", err)
	}
	marked := readRecord(t, srv)
	if marked.Uncertainty == nil || !slices.Equal(marked.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the mark = %+v", marked.Uncertainty)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a", "run-b")); err != nil {
		t.Fatalf("the second MarkUncertainty = %v", err)
	}
	if again := readRecord(t, srv); again.Uncertainty == nil || !slices.Equal(again.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("the second mark did not record the set as handed: %+v", again.Uncertainty)
	}
	if err := c.MarkUncertainty(ctx, held); err != nil {
		t.Fatalf("a mark without a set = %v", err)
	}
	if kept := readRecord(t, srv); kept.Uncertainty == nil || !slices.Equal(kept.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("a mark without a set changed the record: %+v", kept.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, held); err != nil {
		t.Fatalf("ClearUncertainty = %v", err)
	}
	if cleared := readRecord(t, srv); cleared.Uncertainty != nil {
		t.Fatalf("the cleared record carries %+v", cleared.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, held); err != nil {
		t.Fatalf("the second ClearUncertainty = %v", err)
	}
	if err := c.Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	if released := readRecord(t, srv); released.Holder != "" {
		t.Fatalf("the released record still names %q", released.Holder)
	}
}

// TestTakeoverKeepsTheRecordsExistingUncertainty: a takeover arms uncertainty
// only when the record has none; an uncertainty already there names the run
// whose provider operations may still land, not the holder being replaced.
func TestTakeoverKeepsTheRecordsExistingUncertainty(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		t.Fatal(err)
	}
	if taken.Uncertainty == nil || !slices.Equal(taken.Uncertainty.RunIDs, []string{"run-a"}) || taken.Uncertainty.Digest != "" {
		t.Fatalf("the takeover armed %+v, want run-a with no digest", taken.Uncertainty)
	}
	next, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c", TakeoverToken: taken.Token})
	if err != nil {
		t.Fatal(err)
	}
	if next.Uncertainty == nil || !slices.Equal(next.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the second takeover rewrote the uncertainty: %+v", next.Uncertainty)
	}
}

// TestStaleReleaseKeepsTheSuccessorsUncertainty: a run that lost its claim
// writes nothing, so the successor's claim and the uncertainty the takeover
// armed both stay exactly as they are.
func TestStaleReleaseKeepsTheSuccessorsUncertainty(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Release(ctx, first); err != nil {
		t.Fatalf("the stale release = %v", err)
	}
	if record := readRecord(t, srv); record.Holder != "run-b" || record.Uncertainty == nil || !slices.Equal(record.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the stale release changed the successor's record: %+v", record)
	}
	if err := c.Release(ctx, taken); err != nil {
		t.Fatal(err)
	}
	if record := readRecord(t, srv); record.Holder != "" || record.Uncertainty == nil || !slices.Equal(record.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the successor's release did not keep the uncertainty: %+v", record)
	}
}

// TestCASConflictIsTransient: a lost resourceVersion race is a transient error
// and leaves the record as it was — a failed mark is not a mark, a failed
// release is not a release.
func TestCASConflictIsTransient(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	srv.ConflictNextPut = true
	if _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"}); err == nil {
		t.Fatal("Acquire won a lost version race")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if data := srv.ConfigMapData(namespace, name); len(data) != 0 {
		t.Fatalf("the conflicted claim was written: %v", data)
	}

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	srv.ConflictNextPut = true
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err == nil {
		t.Fatal("a conflicted mark was reported as recorded")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if record := readRecord(t, srv); record.Uncertainty != nil {
		t.Fatalf("the conflicted mark left %+v", record.Uncertainty)
	}

	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatal(err)
	}
	srv.ConflictNextPut = true
	if err := c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, held, "digest"); err == nil {
		t.Fatal("a conflicted observation was reported as recorded")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || record.Uncertainty.Digest != "" {
		t.Fatalf("the conflicted observation left %+v", record.Uncertainty)
	}

	srv.ConflictNextPut = true
	if err := c.Release(ctx, held); err == nil {
		t.Fatal("a conflicted release was reported as released")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if record := readRecord(t, srv); record.Holder != "run-a" || record.Token != held.Token {
		t.Fatalf("the conflicted release changed the claim: %+v", record)
	}
}

// TestUncertaintyCallsSurfaceWhatTheyCannotRead: a denied read and a corrupt
// lease key are reported by Mark and Clear — neither may pretend the record is
// conservative or known.
func TestUncertaintyCallsSurfaceWhatTheyCannotRead(t *testing.T) {
	ctx := context.Background()
	lease := catalog.Lease{Holder: "run-a", Token: "t"}

	denied := kubeapitest.New(t)
	denied.AddConfigMap(namespace, name, map[string]string{})
	denied.Deny("configmaps/" + namespace + "/" + name)
	c := newCatalogIn(t, denied)
	for callName, call := range map[string]func() error{
		"Mark":    func() error { return c.MarkUncertainty(ctx, lease) },
		"Clear":   func() error { return c.ClearUncertainty(ctx, lease) },
		"Observe": func() error { return c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, lease, "d") },
	} {
		if err := call(); err == nil || !strings.Contains(err.Error(), "403") {
			t.Fatalf("%s on a denied read = %v, want a 403", callName, err)
		}
	}

	corrupt := kubeapitest.New(t)
	corrupt.AddConfigMap(namespace, name, map[string]string{leaseKey: "not a lease"})
	c = newCatalogIn(t, corrupt)
	for callName, call := range map[string]func() error{
		"Acquire": func() error { _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"}); return err },
		"Mark":    func() error { return c.MarkUncertainty(ctx, lease) },
		"Clear":   func() error { return c.ClearUncertainty(ctx, lease) },
		"Observe": func() error { return c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, lease, "d") },
		"Release": func() error { return c.Release(ctx, lease) },
	} {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "not a lease document") {
			t.Fatalf("%s on a corrupt lease key = %v", callName, err)
		}
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
			t.Fatalf("%s class = %q, want permanent", callName, class)
		}
	}
}

// TestAnOldLeaseRecordIsNotDecoded: the record shape that carried a quarantine
// is not read as a claim — the rollout deletes that ConfigMap instead.
func TestAnOldLeaseRecordIsNotDecoded(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{
		leaseKey: `{"holder":"run-a","kind":"manual","token":"t","acquired_at":"2026-11-07T00:00:00Z","quarantine_until":"2026-11-07T00:10:00Z"}`,
	})
	c := newCatalogIn(t, srv)
	err := c.Release(context.Background(), catalog.Lease{Token: "t"})
	if err == nil || !strings.Contains(err.Error(), "not a lease document") {
		t.Fatalf("Release on an old lease record = %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestReleaseSurfacesADeniedRead: RBAC that refuses the object fails the release
// instead of reporting a claim dropped it never saw.
func TestReleaseSurfacesADeniedRead(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	srv.Deny("configmaps/" + namespace + "/" + name)
	err := newCatalogIn(t, srv).Release(context.Background(), catalog.Lease{Token: "t"})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("Release = %v, want a 403", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestC7_MultiHolderSetRoundTripsThroughTheRecord: the durable record carries
// every pending holder across a takeover and a release, a stale holder cannot
// rewrite the successor's set, a lost version race is transient and writes
// nothing, and only a clear removes the whole set.
func TestC7_MultiHolderSetRoundTripsThroughTheRecord(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()

	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MarkUncertainty(ctx, markedLease(first, "run-a")); err != nil {
		t.Fatal(err)
	}
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", Kind: model.RunKindScheduled, TakeoverToken: first.Token})
	if err != nil {
		t.Fatal(err)
	}
	if taken.Uncertainty == nil || !slices.Equal(taken.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the takeover inherited %+v, want the predecessor alone", taken.Uncertainty)
	}
	if err := c.MarkUncertainty(ctx, markedLease(taken, "run-a", "run-b")); err != nil {
		t.Fatal(err)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || !slices.Equal(record.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("the record carries %+v, want both holders", record.Uncertainty)
	}
	if err := c.MarkUncertainty(ctx, markedLease(first, "run-a")); err != nil {
		t.Fatalf("a stale mark = %v", err)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || !slices.Equal(record.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("a stale holder rewrote the pending set: %+v", record.Uncertainty)
	}

	srv.ConflictNextPut = true
	if err := c.MarkUncertainty(ctx, markedLease(taken, "run-a", "run-b", "run-c")); err == nil {
		t.Fatal("a conflicted mark was reported as recorded")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if record := readRecord(t, srv); record.Uncertainty == nil || !slices.Equal(record.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("the conflicted mark changed the set: %+v", record.Uncertainty)
	}
	if err := c.Release(ctx, taken); err != nil {
		t.Fatal(err)
	}
	next, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if next.Uncertainty == nil || !slices.Equal(next.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("the released record carries %+v, want every holder", next.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, next); err != nil {
		t.Fatal(err)
	}
	if record := readRecord(t, srv); record.Uncertainty != nil {
		t.Fatalf("the cleared record carries %+v", record.Uncertainty)
	}
}

// TestCallsAgainstAnEmptyOrReleasedRecordAreNoOps: a record with no holder and
// no lease key at all carry no claim to mark, clear or release.
func TestCallsAgainstAnEmptyOrReleasedRecordAreNoOps(t *testing.T) {
	ctx := context.Background()
	lease := catalog.Lease{Token: "t"}

	empty := kubeapitest.New(t)
	empty.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, empty)
	if err := c.MarkUncertainty(ctx, lease); err != nil {
		t.Fatalf("MarkUncertainty on an empty record = %v", err)
	}
	if err := c.ClearUncertainty(ctx, lease); err != nil {
		t.Fatalf("ClearUncertainty on an empty record = %v", err)
	}
	if err := c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, lease, "d"); err != nil {
		t.Fatalf("ObserveUncertainty on an empty record = %v", err)
	}
	if err := c.Release(ctx, lease); err != nil {
		t.Fatalf("Release on an empty record = %v", err)
	}

	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c = newCatalogIn(t, srv)
	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkUncertainty(ctx, held); err != nil {
		t.Fatalf("MarkUncertainty after a release = %v", err)
	}
	if err := c.ClearUncertainty(ctx, held); err != nil {
		t.Fatalf("ClearUncertainty after a release = %v", err)
	}
	if err := c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, held, "d"); err != nil {
		t.Fatalf("ObserveUncertainty after a release = %v", err)
	}
	if err := c.Release(ctx, held); err != nil {
		t.Fatalf("the second Release = %v", err)
	}
	if record := readRecord(t, srv); record.Holder != "" {
		t.Fatalf("a call by the released holder changed the record: %+v", record)
	}
}
