package s3_test

// The endpoint's max_object_bytes must hold for an upload whose final size is
// not known when it starts: a backup run whose producer keeps writing while
// parts are already accepted must be refused on the part that crosses the
// limit. The whole workflow is driven here, because "part number and size" is
// what the multipart protocol offers the plugin — no call ever sees the final
// object.

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

const (
	// objectLimit is the endpoint's max_object_bytes in these tests: two parts
	// of limitPartBytes, so a third part crosses it.
	objectLimit    = 10 << 20
	limitPartBytes = 5 << 20
)

// controlledSource writes its chunks in order. Growing holds after the first
// chunk until the endpoint accepted part 1, so the producer is still running
// while parts upload; a source that is not growing writes the whole object
// before any part is sent.
type controlledSource struct {
	chunks  [][]byte
	growing bool
	partOne <-chan struct{}
}

func (*controlledSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat"}, nil
}

func (s *controlledSource) Dump(ctx context.Context, dst io.Writer) error {
	for i, chunk := range s.chunks {
		if _, err := dst.Write(chunk); err != nil {
			return err
		}
		if s.growing && i == 0 {
			select {
			case <-s.partOne:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// TestGrowingUploadRespectsObjectLimit: an object that crosses max_object_bytes
// while it is still produced is refused on the crossing part — never committed —
// and one that stops exactly at the limit is stored.
func TestGrowingUploadRespectsObjectLimit(t *testing.T) {
	rep, f := runControlledSource(t, &controlledSource{
		growing: true,
		chunks:  [][]byte{bytes.Repeat([]byte("x"), 6<<20), bytes.Repeat([]byte("y"), 4<<20)},
	})
	if len(rep.Artifacts) == 0 {
		t.Fatalf("no artifacts in the report")
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("a 10 MiB object at the 10 MiB limit: outcome %q, error %q",
			rep.Artifacts[0].Outcome, rep.Artifacts[0].Error)
	}
	if got := f.Count("complete"); got != 1 {
		t.Errorf("completes: got %d, want 1", got)
	}
	if body, ok := f.objectBody("root/" + rep.Artifacts[0].Key); !ok || len(body) != objectLimit {
		t.Errorf("stored object %q: %d bytes, present %v; want %d", "root/"+rep.Artifacts[0].Key, len(body), ok, objectLimit)
	}

	rep, f = runControlledSource(t, &controlledSource{
		growing: true,
		chunks:  [][]byte{bytes.Repeat([]byte("x"), 6<<20), bytes.Repeat([]byte("y"), 6<<20)},
	})
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("a 12 MiB object at the 10 MiB limit: outcome %q", rep.Artifacts[0].Outcome)
	}
	if !bytes.Contains([]byte(rep.Storages[0].Error), []byte("max_object_bytes")) {
		t.Errorf("storage error %q does not name max_object_bytes", rep.Storages[0].Error)
	}
	if got := f.Count("part"); got != 2 {
		t.Errorf("parts: got %d, want the two that fit before the crossing one", got)
	}
	if got := f.Count("complete"); got != 0 {
		t.Errorf("completes: got %d, want none", got)
	}
	if got := f.Count("abort"); got != 1 {
		t.Errorf("aborts: got %d, want the one unfinished upload aborted", got)
	}
}

// TestFinishedUploadRespectsObjectLimit: the same bounds hold when the object is
// whole before any part is sent — the exact limit is stored, one byte more is
// refused before a single part.
func TestFinishedUploadRespectsObjectLimit(t *testing.T) {
	rep, f := runControlledSource(t, &controlledSource{
		chunks: [][]byte{bytes.Repeat([]byte("x"), objectLimit)},
	})
	if len(rep.Artifacts) == 0 {
		t.Fatalf("no artifacts in the report")
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("a 10 MiB object at the 10 MiB limit: outcome %q, error %q",
			rep.Artifacts[0].Outcome, rep.Artifacts[0].Error)
	}
	if got := f.Count("complete"); got != 1 {
		t.Errorf("completes: got %d, want 1", got)
	}

	rep, f = runControlledSource(t, &controlledSource{
		chunks: [][]byte{bytes.Repeat([]byte("x"), objectLimit+1)},
	})
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("a 10 MiB + 1 object at the 10 MiB limit: outcome %q", rep.Artifacts[0].Outcome)
	}
	if !bytes.Contains([]byte(rep.Storages[0].Error), []byte("above the endpoint limit")) {
		t.Errorf("storage error %q does not name the endpoint limit", rep.Storages[0].Error)
	}
	if got := f.Count("part"); got != 0 {
		t.Errorf("parts: got %d, want none past the limit", got)
	}
	if got := f.Count("complete"); got != 0 {
		t.Errorf("completes: got %d, want none", got)
	}
}

// runControlledSource runs one backup run with the real s3 plugin against the
// fake endpoint, its object limits set to objectLimit/limitPartBytes, and returns the
// report and the endpoint.
func runControlledSource(t *testing.T, src *controlledSource) (model.RunReport, *fakeS3Server) {
	t.Helper()
	f := newFakeS3(t, testBucket)
	endpoint := f.srv.URL
	if src.growing {
		partOne := make(chan struct{})
		src.partOne = partOne
		var once sync.Once
		inner := f.srv.Config.Handler
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inner.ServeHTTP(w, r)
			if r.Method == http.MethodPut && r.URL.Query().Get("partNumber") == "1" {
				once.Do(func() { close(partOne) })
			}
		}))
		t.Cleanup(srv.Close)
		endpoint = srv.URL
	}
	sc := baseConfig(t, f, func(m map[string]any) {
		m["endpoint"] = endpoint
		m["part_bytes"] = limitPartBytes
		m["max_request_bytes"] = limitPartBytes
		m["max_object_bytes"] = objectLimit
	})
	// The compiled distribution registers every plugin the binary runs, hosts and
	// roles included; the controlled source is one more compiled plugin of this
	// test distribution.
	r := instances.NewRegistry()
	lr := loader.New()
	if err := all.Compose(lr, r); err != nil {
		t.Fatal(err)
	}
	if err := loader.Add(lr, loader.Spec{ID: source.Kind.ID("controlled")}, func(lr *loader.Registry) error {
		return instances.AddFactory(lr, r, source.Kind, instances.FactorySpec[struct{}, source.Source]{
			Type: "controlled", Example: []byte(`{}`),
			New: func(context.Context, struct{}, instances.Deps) (source.Source, error) { return src, nil },
		})
	}); err != nil {
		t.Fatal(err)
	}
	// An asynchronous storage is admitted only under a durable writer lease, so
	// this distribution's catalog is the in-memory record the lease kit shares.
	if err := loader.Add(lr, loader.Spec{ID: catalog.Kind.ID("test")}, func(lr *loader.Registry) error {
		return instances.AddFactory(lr, r, catalog.Kind, instances.FactorySpec[struct{}, catalog.Catalog]{
			Type: "test", Example: []byte(`{}`),
			New: func(context.Context, struct{}, instances.Deps) (catalog.Catalog, error) {
				return catalogtest.NewLeaser(), nil
			},
		})
	}); err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{
		"run": map[string]any{"spool_dir": t.TempDir(), "spool_reserve_bytes": 0,
			"max_artifact_bytes": "20MiB", "deadline": "10s", "lease_hard_bound": "2s"},
		"sources": map[string]any{"data": map[string]any{"type": "controlled", "config": map[string]any{},
			"pipeline": []string{}, "rate_limit": 0,
			"artifact": map[string]any{"group": "g", "name": "n", "sensitivity": "plain", "importance": "required"}}},
		"storages": map[string]any{"s3": map[string]any{"type": "s3", "config": sc,
			"max_total_bytes": "100MiB", "time_budget": "10s", "call_parallelism": 1,
			"retention": map[string]any{"scheduled": map[string]any{"type": "keep_last",
				"config": map[string]any{"count": 3, "min_keep": 1, "count_incomplete": true}}}}},
		"catalog": map[string]any{"type": "test", "config": map[string]any{}},
	}
	cfg["layout"] = map[string]any{"run_prefix": "runs/{{ .Run.ID }}", "artifact_key": "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	run := cfg["run"].(map[string]any)
	for k, v := range map[string]any{
		"id_layout": "20060102T150405Z", "finish_allowance": "1s", "min_verified_storages": 1,
		"degraded_exit_code": 3, "failed_exit_code": 7, "storage_parallelism": 4,
		"unclassified_error": "transient", "notifier_failure_is_fatal": false, "progress_interval": "0s",
		"lease_wait": "0s", "lease_poll": "1ms", "holder_namespace_env": "", "holder_pod_env": "", "holder_uid_env": "",
		"writer_identity": "",
		"retry":           map[string]any{"attempts": 1, "initial_delay": "1ms", "max_delay": "1ms", "jitter": 0},
	} {
		run[k] = v
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := assembly.New(context.Background(), raw, nil,
		assembly.Deps{Factories: r, Loader: lr, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(context.Background()) }()
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	return rep, f
}
