package backup_test

// Helpers the restored workflow tests share: a restore section, a clock-bearing
// timestamp, a delivered run and a scheduled run that could not take the claim.

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// newAssemblyLogged builds the assembly with a handler the test keeps: the storage
// destinations capture their logger at construction, so a logger attached later
// never sees their events.
func newAssemblyLogged(t *testing.T, w *world, cfg map[string]any, buf *bytes.Buffer) *assembly.Assembly {
	t.Helper()
	w.syncLoader(t)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := assembly.New(context.Background(), raw, nil, assembly.Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(buf, slog.LevelInfo))})
	if err != nil {
		t.Fatalf("assembly.New: %v", err)
	}
	return e
}

// withRestore attaches a restore section to a config.
func withRestore(t *testing.T, cfg map[string]any, order ...string) map[string]any {
	t.Helper()
	cfg["restore"] = map[string]any{
		"spool_dir":           t.TempDir(),
		"spool_reserve_bytes": 0,
		"storage_order":       order,
		"unclassified_error":  "transient",
		"retry":               map[string]any{"attempts": 3, "initial_delay": "1ms", "max_delay": "5ms", "jitter": 0},
	}
	return cfg
}

func runScheduledFailed(t *testing.T, e *assembly.Assembly) model.RunReport {
	t.Helper()
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a scheduled run that could not take the claim returned an error instead of a report: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || rep.Lease.Error == "" {
		t.Fatalf("a scheduled run that could not take the claim did not fail with the lease reason: outcome=%q lease=%+v", rep.Outcome, rep.Lease)
	}
	return rep
}

// asyncStorage answers the multipart opening with errors.ErrUnsupported: a
// while every part call is a no-op.
type asyncStorage struct{ *storagetest.Fake }

func (asyncStorage) BeginMultipart(context.Context, storage.PutRequest) (storage.MultipartSession, error) {
	return storage.MultipartSession{}, errors.ErrUnsupported
}

func (asyncStorage) PutPart(context.Context, storage.MultipartSession, storage.Part) (storage.PartResult, error) {
	return storage.PartResult{}, nil
}

func (asyncStorage) CompleteMultipart(context.Context, storage.MultipartSession, []storage.PartResult) error {
	return nil
}

func (asyncStorage) AbortMultipart(context.Context, storage.MultipartSession) error { return nil }

func (asyncStorage) StaleMultipart(context.Context, time.Time) ([]storage.StaleUpload, error) {
	return nil, nil
}

// fakeMultipart is the workflow tests' multipart destination: the embedded fake
// keeps listings and Stat, and the parts API records what the transfer drove.
type fakeMultipart struct {
	*storagetest.Fake
	partSize  int64
	beginHook func(storage.PutRequest)
	mu        sync.Mutex
	begins    int
	aborts    int
	completes int
	parts     map[string][][]byte
	// partErr fails every PutPart; abortErr fails every AbortMultipart.
	partErr  error
	abortErr error
	// abortGate, when non-nil, holds the SECOND and later abort calls (the
	// cancel sequence's own) until it is closed; abortEntered signals the block.
	abortGate    chan struct{}
	abortEntered chan struct{}
	// beginGate, when non-nil, holds BeginMultipart ignoring cancellation.
	beginGate chan struct{}
	// beginStuck holds BeginMultipart ignoring cancellation entirely.
	beginStuck chan struct{}
	// sweepHold, when non-nil, holds StaleMultipart until it is closed;
	// sweepEntered signals the block.
	sweepHold    chan struct{}
	sweepEntered chan struct{}
	abortCalls   int
}

func newFakeMultipart(partSize int64) *fakeMultipart {
	return &fakeMultipart{Fake: storagetest.NewFake(), partSize: partSize, parts: map[string][][]byte{}}
}

func addMultipart(w *world, name string, partSize int64) *fakeMultipart {
	mp := newFakeMultipart(partSize)
	w.ExtraStorages[name] = mp
	w.Storages[name] = mp.Fake
	return mp
}

func (f *fakeMultipart) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	f.mu.Lock()
	f.begins++
	hook := f.beginHook
	gate := f.beginGate
	f.mu.Unlock()
	if stuck := f.beginStuck; stuck != nil && req.Size > f.partSize {
		<-stuck
		return storage.MultipartSession{}, errors.New("upload refused")
	}
	if gate != nil && req.Size > f.partSize {
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return storage.MultipartSession{}, errors.New("upload refused")
	}
	if hook != nil {
		hook(req)
	}
	if req.Size <= f.partSize {
		return storage.MultipartSession{}, errors.ErrUnsupported
	}
	return storage.MultipartSession{Key: req.Key, UploadID: req.Key, PartSize: f.partSize}, nil
}

func (f *fakeMultipart) PutPart(_ context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if f.partErr != nil {
		return storage.PartResult{}, f.partErr
	}
	body, err := io.ReadAll(part.Body)
	if err != nil {
		return storage.PartResult{}, err
	}
	f.mu.Lock()
	f.parts[session.UploadID] = append(f.parts[session.UploadID], body)
	f.mu.Unlock()
	return storage.PartResult{Number: part.Number, ETag: "etag"}, nil
}

func (f *fakeMultipart) CompleteMultipart(_ context.Context, session storage.MultipartSession, _ []storage.PartResult) error {
	f.mu.Lock()
	f.completes++
	var whole []byte
	for _, p := range f.parts[session.UploadID] {
		whole = append(whole, p...)
	}
	delete(f.parts, session.UploadID)
	f.mu.Unlock()
	f.PutObject(session.Key, whole, time.Now().UTC())
	return nil
}

func (f *fakeMultipart) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	f.mu.Lock()
	f.aborts++
	delete(f.parts, session.UploadID)
	err := f.abortErr
	f.abortCalls++
	second := f.abortCalls > 1
	gate, entered := f.abortGate, f.abortEntered
	f.mu.Unlock()
	if second && gate != nil {
		if entered != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (f *fakeMultipart) StaleMultipart(_ context.Context, _ time.Time) ([]storage.StaleUpload, error) {
	f.mu.Lock()
	hold, entered := f.sweepHold, f.sweepEntered
	f.mu.Unlock()
	if hold != nil {
		if entered != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
		}
		<-hold
	}
	return nil, nil
}

func (f *fakeMultipart) Inventory(context.Context) ([]storage.StaleUpload, error) { return nil, nil }

func (f *fakeMultipart) stats() (begins, aborts, completes int, _ map[int32]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.begins, f.aborts, f.completes, map[int32]int{}
}

// hookPutStorage calls onPut on every whole-object Put, so a test can prove no
// write started before a slow survey ended.
type hookPutStorage struct {
	*storagetest.Fake
	onPut func()
}

func (h *hookPutStorage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if h.onPut != nil {
		h.onPut()
	}
	return h.Fake.Put(ctx, req, body)
}

type slowListStorage struct {
	*storagetest.Fake
	delay time.Duration
}

func (s *slowListStorage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	time.Sleep(s.delay)
	return s.Fake.List(ctx, req)
}

// uncertainty this run armed, and the run must not fail over it.

// putRun fabricates one run on a fake storage with one artifact and, when
// verified, a manifest that calls this storage verified.
func putRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time, verified bool) {
	t.Helper()
	f.PutObject("runs/"+runID+"/artifact.bin", []byte(runID), at)
	if !verified {
		return
	}
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: kindOfID(runID), StartedAt: at, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// putCopiedRun plants one finished run holding the given artifacts.
func putCopiedRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time,
	outcome model.Outcome, artifacts [][2]string) {
	t.Helper()
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: kindOfID(runID), StartedAt: at, Outcome: outcome,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
	}
	for _, a := range artifacts {
		key := "runs/" + runID + "/" + a[0] + "/" + a[1] + ".bin"
		body := []byte(runID + "/" + a[0] + "/" + a[1])
		f.PutObject(key, body, at)
		rep.Artifacts = append(rep.Artifacts, model.ArtifactResult{
			Artifact: model.Artifact{Group: a[0], Name: a[1], Importance: model.ImportanceRequired},
			Outcome:  model.ArtifactOK, Key: key, Bytes: int64(len(body)), SHA256: model.SHA256Hex(body),
		})
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// runIDsOn lists the run directories a fake currently holds.
func runIDsOn(f *storagetest.Fake) map[string]bool {
	out := map[string]bool{}
	for _, k := range f.Keys() {
		rest, ok := strings.CutPrefix(k, "runs/")
		if !ok {
			continue
		}
		id, _, found := strings.Cut(rest, "/")
		if found && id != "" {
			out[id] = true
		}
	}
	return out
}

// kindOfID reads a run's kind from its id suffix.
func kindOfID(id string) model.RunKind {
	if strings.HasSuffix(id, "-manual") {
		return model.RunKindManual
	}
	return model.RunKindScheduled
}

// keepLastN is one keep_last policy config.
func keepLastN(n int) map[string]any {
	return map[string]any{"type": "keep_last", "config": map[string]any{
		"count": n, "min_keep": 1, "count_incomplete": true,
	}}
}

// gfsDaily1 keeps only the newest run of the newest day: two runs on one day
// leave exactly one, which is what makes the same-day rotation observable.

func gfsDaily1() map[string]any {
	return map[string]any{"type": "gfs", "config": map[string]any{
		"daily": 1, "weekly": 0, "monthly": 0, "timezone": "UTC",
		"min_keep": 1, "count_incomplete": true,
	}}
}

// TestGuardUnreadableManifestNeverReleasesThePin (review probe): one transient
// manifest read failure on night-1 — the run holding the volume's newest
// verified copy — must not turn into "night-1 has no manifest". Folding the
// read failure into absence is what let the fixture delete the only copy.

func artifactByName(rep model.RunReport, name string) model.ArtifactResult {
	for _, a := range rep.Artifacts {
		if a.Artifact.Name == name {
			return a
		}
	}
	return model.ArtifactResult{}
}

func storageResult(rep model.RunReport, name string) model.StorageResult {
	for _, s := range rep.Storages {
		if s.Storage == name {
			return s
		}
	}
	return model.StorageResult{}
}

type checkingStorage struct {
	*storagetest.Fake
	CheckErr  error
	CheckOnce error
	Checks    int
}

func checkingStorages(t *testing.T, w *world, typ string) map[string]*checkingStorage {
	t.Helper()
	fleet := map[string]*checkingStorage{}
	err := storage.Register(w.Registry, storage.FactorySpec[map[string]jsontext.Value]{
		Type: typ, Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, d instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			s, ok := fleet[d.Instance]
			if !ok {
				s = &checkingStorage{Fake: storagetest.NewFake()}
				fleet[d.Instance] = s
			}
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fleet
}

func serviceCheck(t *testing.T, w *world, cfg map[string]any) *assembly.Assembly {
	t.Helper()
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	return w.newAssembly(t, cfg)
}

func (s *checkingStorage) Check(context.Context) error {
	s.Checks++
	if s.Checks == 1 && s.CheckOnce != nil {
		return s.CheckOnce
	}
	return s.CheckErr
}

func foundSource(name, artifactName string, importance model.Importance) discovery.Found {
	return discovery.Found{
		Name: name, Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: artifactName,
			Importance: importance, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
}
