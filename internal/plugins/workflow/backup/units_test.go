package backup

// The workflow's own unit branches: a spool file that is gone before its Put,
// and a Stat that reports the wrong size.

import (
	"bytes"

	"context"
	"encoding/json/jsontext"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/transfer"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type wrongSizeReader struct{}

func (wrongSizeReader) Describe(context.Context) (storage.StorageTraits, error) {
	return storage.StorageTraits{}, nil
}

func (wrongSizeReader) Stat(context.Context, string) (model.ObjectInfo, error) {
	return model.ObjectInfo{Size: 5}, nil
}

func (wrongSizeReader) List(context.Context, storage.ListRequest) (storage.ListPage, error) {
	return storage.ListPage{}, nil
}

func (wrongSizeReader) Get(context.Context, string, io.Writer) error { return nil }

func TestVerifyObjectRefusesAWrongSize(t *testing.T) {
	e := &Workflow{}
	err := e.verifyObject(context.Background(), guard.Read("alpha", wrongSizeReader{}), "k", "label", 10)
	if err == nil {
		t.Fatal("a wrong-size object was accepted")
	}
}

func TestPutWholeRefusesAMissingSpoolFile(t *testing.T) {
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Workflow{retry: testRetry()}
	pa := pipeline.Artifact{Artifact: model.Artifact{Group: "g", Name: "n"}, File: "gone", Bytes: 1}
	if err := e.putWhole(context.Background(), guard.Handle{}, &storageBinding{}, newStorageRunState(&storageBinding{name: "alpha"}), sp, pa, "k"); err == nil {
		t.Fatal("a missing spool file was uploaded")
	}
}

func testRetry() execution.Options { return execution.Options{Attempts: 1} }

func TestUploadsSettledIsFalseOnAFailedWrite(t *testing.T) {
	sb := &storageBinding{name: "alpha"}
	st := newStorageRunState(sb)
	st.markWritesStarted()
	st.setUploadError(errors.New("upload failed"))
	if uploadsSettled([]*storageBinding{sb}, map[string]*storageRunState{"alpha": st}) {
		t.Fatal("a failed write read as settled")
	}
	// A storage that never started a write cannot settle anything later: its
	// preflight exclusion does not keep the run's own pending id.
	idle := newStorageRunState(sb)
	idle.setUploadError(errors.New("permanently excluded before writes"))
	if !uploadsSettled([]*storageBinding{sb}, map[string]*storageRunState{"alpha": idle}) {
		t.Fatal("a pre-write exclusion read as an unsettled mutation")
	}
}

func TestShutdownWithInvalidBudgetsRefusesTheSequence(t *testing.T) {
	e := &Workflow{log: slog.New(slog.DiscardHandler)}
	err := e.shutdown(context.Background())
	if err == nil {
		t.Fatal("a shutdown under zero lifecycle budgets ran the cancel sequence")
	}
	if !strings.Contains(err.Error(), "join") {
		t.Fatalf("the refusal does not name the invalid budget: %v", err)
	}
}

func TestCancelledBeforeTheClaimDoesNotReleaseAnything(t *testing.T) {
	e := &Workflow{runConfigured: true, catalogConfigured: true, layout: goodLayout(),
		retry: testRetry(), log: slog.New(slog.DiscardHandler), lease: protection.NewLease(protection.LeaseOptions{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep, err := e.Run(ctx, RunRequest{Kind: model.RunKindScheduled})
	if err == nil && rep.Outcome != "" {
		t.Fatalf("a pre-cancelled run reported an outcome: %+v", rep)
	}
}

// multipartFake is a minimal multipart destination for the transfer path.
type multipartFake struct {
	*storagetest.Fake
	part int64
}

func (m *multipartFake) BeginMultipart(_ context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if req.Size <= m.part {
		return storage.MultipartSession{}, errors.ErrUnsupported
	}
	return storage.MultipartSession{Key: req.Key, UploadID: req.Key, PartSize: m.part}, nil
}

func (m *multipartFake) PutPart(_ context.Context, _ storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if _, err := io.Copy(io.Discard, part.Body); err != nil {
		return storage.PartResult{}, err
	}
	return storage.PartResult{Number: part.Number, ETag: "e"}, nil
}

func (m *multipartFake) CompleteMultipart(_ context.Context, session storage.MultipartSession, _ []storage.PartResult) error {
	m.PutObject(session.Key, []byte("0123456789"), time.Now().UTC())
	return nil
}

func (m *multipartFake) AbortMultipart(context.Context, storage.MultipartSession) error { return nil }

func (m *multipartFake) StaleMultipart(context.Context, time.Time) ([]storage.StaleUpload, error) {
	return nil, nil
}

func TestUploadObjectBooksAHandledMultipartUpload(t *testing.T) {
	fake := &multipartFake{Fake: storagetest.NewFake(), part: 4}
	reg := instances.NewRegistry()
	if err := storage.Register(reg, storage.FactorySpec[map[string]jsontext.Value]{
		Type: "multi", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return fake, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	dest, err := guard.Build(context.Background(), reg, "multi", jsontext.Value(`{}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	record := catalogtest.NewLeaser()
	lease := protection.NewLease(protection.LeaseOptions{Catalog: record, Log: slog.New(slog.DiscardHandler)})
	if _, _, err := lease.Acquire(context.Background(), model.RunKindScheduled, "unit"); err != nil {
		t.Fatal(err)
	}
	ledger := protection.NewLedger(protection.LedgerOptions{Destinations: []guard.Destination{dest}, Caps: map[string]int64{"alpha": 1 << 20}})
	ledger.BindLease(lease)
	ledger.Start(map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}})
	e := &Workflow{run: RunOptions{}, retry: testRetry(), log: slog.New(slog.DiscardHandler), ledger: ledger}
	authority := e.ledger.Authorize(time.Now().Add(time.Minute))
	defer authority.Close()
	h, err := dest.Port().Open(authority)
	if err != nil {
		t.Fatal(err)
	}
	sb := &storageBinding{name: "alpha", port: dest.Port(), cfg: Storage{Name: "alpha", TimeBudget: model.Duration(time.Minute), MaxTotalBytes: 1 << 20}}
	st := newStorageRunState(sb)
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	pa := pipeline.Artifact{Artifact: model.Artifact{Group: "g", Name: "n"}, File: w.Name(), Bytes: w.Size(), SHA256: w.SHA256()}
	tf := transfer.New(transfer.Options{Retry: testRetry(), Clock: st.clock, Accounting: e.ledger.Accounting(), Storage: "alpha", MaxArtifactBytes: 1 << 20, CallParallelism: 1})
	var buf bytes.Buffer
	e.log = slog.New(human.NewHandler(&buf, slog.LevelDebug))
	if !e.uploadObject(context.Background(), tf, h, sb, st, pa, sp, "k", model.RunKindScheduled) {
		t.Fatalf("a handled multipart upload was not booked:\n%s", buf.String())
	}
}

// TestShutdownWithoutAnOperationUsesTheInvocationScope: a cancel sequence on an
// invocation that never started an operation has nothing to join or release,
// and falls back to an empty invocation scope instead of a nil one.
func TestShutdownWithoutAnOperationUsesTheInvocationScope(t *testing.T) {
	e := &Workflow{log: slog.New(slog.DiscardHandler), lifecycle: lifecycle.Config{
		Join:       model.Duration(time.Minute),
		AbortTotal: model.Duration(2 * time.Minute),
		Close:      model.Duration(30 * time.Second),
	}}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatalf("a claim-free shutdown: %v", err)
	}
}

// TestUploadObjectRefusesAPermanentlyFailedStorage: the per-storage upload
// entry starts nothing once its permanent failure is known.
func TestUploadObjectRefusesAPermanentlyFailedStorage(t *testing.T) {
	fake := storagetest.NewFake()
	port := guard.Read("alpha", fake)
	st := newStorageRunState(&storageBinding{name: "alpha"})
	st.fatal = errors.New("permanent")
	e := &Workflow{run: RunOptions{}, retry: testRetry(), log: slog.New(slog.DiscardHandler),
		ledger: protection.NewLedger(protection.LedgerOptions{Caps: map[string]int64{"alpha": 1 << 20}})}
	if e.uploadObject(context.Background(), transfer.New(transfer.Options{}), guard.Handle{}, &storageBinding{name: "alpha", port: port}, st,
		pipeline.Artifact{Artifact: model.Artifact{Group: "g", Name: "n"}, Bytes: 1}, nil, "key", model.RunKindScheduled) {
		t.Fatal("a permanently failed storage accepted an upload")
	}
	if len(fake.Events()) != 0 {
		t.Fatalf("a permanently failed storage saw calls: %v", fake.Events())
	}
}
