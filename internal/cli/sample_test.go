package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// multipartStorage is a storage that uploads one object as parts, so a sample
// run exercises the multipart path: the engine logs the part count, and the
// retry of the part this fake fails once.
type multipartStorage struct {
	*storagetest.Fake
	partSize int64

	mu     sync.Mutex
	calls  map[int32]int
	parts  map[string]map[int32][]byte
	nextID int
}

func newMultipartStorage(partSize int64) *multipartStorage {
	return &multipartStorage{Fake: storagetest.NewFake(), partSize: partSize,
		calls: map[int32]int{}, parts: map[string]map[int32][]byte{}}
}

func (m *multipartStorage) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return storage.MultipartSession{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := "upload-" + string(rune('a'+m.nextID-1))
	m.parts[id] = map[int32][]byte{}
	return storage.MultipartSession{Key: req.Key, UploadID: id, PartSize: m.partSize}, nil
}

func (m *multipartStorage) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.PartResult{}, err
	}
	m.mu.Lock()
	m.calls[part.Number]++
	call := m.calls[part.Number]
	m.mu.Unlock()
	// The first part of the first object fails once, transiently: the engine
	// retries it and the sample log carries the retry line.
	if part.Number == 1 && call == 1 {
		return storage.PartResult{}, plugin.Transient(errors.New("the connection to the endpoint was reset"))
	}
	body, err := io.ReadAll(part.Body)
	if err != nil {
		return storage.PartResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.parts[session.UploadID][part.Number] = body
	return storage.PartResult{Number: part.Number, ETag: "etag"}, nil
}

func (m *multipartStorage) CompleteMultipart(ctx context.Context, session storage.MultipartSession, parts []storage.PartResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []byte
	for i := int32(1); i <= int32(len(parts)); i++ {
		all = append(all, m.parts[session.UploadID][i]...)
	}
	m.PutObject(session.Key, all, time.Now())
	return nil
}

func (m *multipartStorage) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.parts, session.UploadID)
	return nil
}

func (m *multipartStorage) StaleMultipart(context.Context, time.Time) ([]storage.StaleUpload, error) {
	return nil, nil
}

// multipartRegister is the distribution of a test whose exchange storage
// uploads one object as parts: the harness's own types plus one that always
// answers with the given fake.
func multipartRegister(w *world, parts *multipartStorage) func(*plugin.Registry) error {
	return func(r *plugin.Registry) error {
		if err := w.register(r); err != nil {
			return err
		}
		return plugin.Register(r, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
			Type:      "multipart",
			Summary:   "a storage that uploads one object as parts",
			Stability: plugin.Experimental,
			Example:   []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (storage.Storage, error) {
				return parts, nil
			},
		})
	}
}

// TestRunLogSampleMultipartAndFailure is the whole story in one run: several
// artifacts, a multipart upload whose first part is retried, one artifact that
// fails, both storages, the catalog, the notifier and the closing summary. Run
// with -v to read the log it asserts on.
func TestRunLogSampleMultipartAndFailure(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	parts := newMultipartStorage(4)
	// The control-plane artifact's source fails its preflight check, which is
	// the failure the sample shows: scoped to that artifact, the run goes on.
	w.checks["broken"] = errors.New("the datastore refused the connection")

	body := `sources:
  shows:
    type: synthetic
    artifact: {group: databases, name: example-prod/shows, importance: required, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: "the-shows-of-the-run", dir: "", origin: "postgres://postgres-direct.example-prod.svc:5432/shows"}
  uploads:
    type: synthetic
    artifact: {group: volumes, name: infrastructure/uploads, importance: required, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: "uploads", dir: "", origin: "files:/var/lib/rancher/k3s/storage/uploads"}
  broken:
    type: synthetic
    artifact: {group: control-plane, name: k3s-state, importance: required, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: "state", dir: "", origin: "files:/var/lib/rancher/k3s/server/db"}
` + zstdYAML + `catalog:
  type: memory
  config: {}
notifiers:
  sink:
    type: memory
    config: {}
storages:
  exchange:
    type: multipart
    retention: ` + keepRetention + `
    max_total_bytes: ` + keepCap + `
    time_budget: 10s
    config: {}
` + faultyYAML
	// attempts 3 so the retried part is visible: the sample config's own retry
	// policy is one attempt, which is right for the fast tests and wrong for a
	// sample that must show a retry.
	cfg := mustWrite(t, dir, "config.yaml",
		strings.Replace(configYAML(dir, body), "attempts: 1, initial_delay: 1ms", "attempts: 3, initial_delay: 1ms", 1))

	register := multipartRegister(w, parts)

	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Register: register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"run", "--kind", "scheduled", "--config", cfg}, &stdout, &stderr)
	t.Logf("exit code = %d\n--- log (stderr) ---\n%s--- summary (stdout) ---\n%s", code, stderr.String(), stdout.String())

	if !strings.Contains(stderr.String(), "multipart ") {
		t.Errorf("the sample carries no multipart line:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "retry part 1 of ") {
		t.Errorf("the sample carries no retried-part line:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "✗ control-plane/k3s-state: ") {
		t.Errorf("the sample carries no failed-artifact line:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "processor zstd/squeeze: ") {
		t.Errorf("the sample carries no processor line:\n%s", stderr.String())
	}
}
