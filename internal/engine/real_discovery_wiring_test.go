// Package engine_test holds wiring tests that must stand outside the engine
// package: they build the real plugin registry — the same one the binary builds
// — with the real kubernetes_labels discovery over a fake API server, so the
// Found.Problem contract and its retry rule are exercised end to end, through
// Run and PreflightResults, not invented per half.
package engine_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/cli"
	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

const wiringHostEnv = "GCB_WIRING_HOST"
const wiringPortEnv = "GCB_WIRING_PORT"

func wiringPod(t *testing.T, namespace, name string, labels map[string]string) string {
	t.Helper()
	pod := map[string]any{
		"metadata": map[string]any{"name": name, "namespace": namespace, "labels": labels},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "app"}}},
		"status":   map[string]any{"phase": "Running"},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// wiringPodEnv is wiringPod with one environment variable taken from a
// reference (a Secret or a ConfigMap), so a rule template can force a read of
// that object.
func wiringPodEnv(t *testing.T, namespace, name string, labels map[string]string, envName string, valueFrom map[string]any) string {
	t.Helper()
	pod := map[string]any{
		"metadata": map[string]any{"name": name, "namespace": namespace, "labels": labels},
		"spec": map[string]any{"containers": []any{map[string]any{
			"name": "app",
			"env":  []any{map[string]any{"name": envName, "valueFrom": valueFrom}},
		}}},
		"status": map[string]any{"phase": "Running"},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// wiringRootDir makes a real directory with one file: what a files source
// archives — the smallest honest stand-in for a dump, no external binary.
func wiringRootDir(t *testing.T, marker string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "payload.txt"), []byte("bytes of "+marker), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// podRule matches pods labelled backup.test/workload=true and archives the
// directory its rootTemplate renders.
func podRule(importance, rootTemplate string) map[string]any {
	return map[string]any{
		"name":     "volumes",
		"selector": "backup.test/workload=true",
		"phase":    "Running",
		"artifact": map[string]any{
			"group": "data", "name": "{{ .Name }}",
			"importance": importance, "sensitivity": "plain",
		},
		"covers_volumes": false,
		"pipeline":       []string{"zstd"},
		"min_matches":    1,
		"source": map[string]any{"type": "files", "config": map[string]any{
			"root":            rootTemplate,
			"ignore_vanished": false,
		}},
	}
}

// dbRule matches pods labelled backup.test/workload=db and archives the
// directory read through envFile — a rule that needs a resolved secret file.
func dbRule(importance string) map[string]any {
	r := podRule(importance, `{{ envFile 0 "ROOT" }}`)
	r["name"] = "dbs"
	r["selector"] = "backup.test/workload=db"
	return r
}

// wiringConfig is the run config of one scenario: a real kubernetes_labels
// discovery over the fake cluster's labelled pods, the given pod rules — most
// scenarios declare no claim of their own, so volumes is usually nil — zstd,
// one local storage and a keep_last retention. attempts is the engine's retry
// budget for every plugin call, Discover included.
func wiringConfig(t *testing.T, opts kubeapi.Options, secretsPath, spoolDir, storageRoot string, pods, volumes []any, attempts int, retryDelay string) jsontext.Value {
	t.Helper()
	cfg := map[string]any{
		"version": 1,
		"run": map[string]any{
			"id_layout":                 "20060102T150405Z",
			"spool_dir":                 spoolDir,
			"spool_reserve_bytes":       1 << 20,
			"max_artifact_bytes":        int64(1) << 20,
			"deadline":                  "2m",
			"finish_allowance":          "5s",
			"min_verified_storages":     1,
			"degraded_exit_code":        0,
			"failed_exit_code":          1,
			"storage_parallelism":       1,
			"unclassified_error":        "transient",
			"notifier_failure_is_fatal": false,
			"progress_interval":         "15s",
			"lease_wait":                "0s",
			"lease_poll":                "1s",
			"lease_hard_bound":          "1h",
			"holder_namespace_env":      "",
			"holder_pod_env":            "",
			"holder_uid_env":            "",
			"retry":                     map[string]any{"attempts": attempts, "initial_delay": retryDelay, "max_delay": retryDelay, "jitter": 0},
		},
		"layout": map[string]any{
			"run_prefix":       "runs/{{ .Run.ID }}",
			"artifact_key":     "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}",
			"manifest_key":     "manifest.json",
			"manual_id_suffix": "-manual",
		},
		// Every scenario is also restore-capable: the local storage is the
		// whole walk order, and a files restore goes side by side.
		"restore": map[string]any{
			"spool_dir":           spoolDir,
			"spool_reserve_bytes": 1 << 20,
			"storage_order":       []any{"local"},
			"unclassified_error":  "transient",
			"retry":               map[string]any{"attempts": attempts, "initial_delay": retryDelay, "max_delay": retryDelay, "jitter": 0},
			"apply": map[string]any{"files": map[string]any{
				"preserve_owner": true, "side_by_side_dir": ".restore-{{ .Label }}",
			}},
		},
		"discoveries": map[string]any{"workloads": map[string]any{
			"type": "kubernetes_labels",
			"config": map[string]any{
				"api_host_env":       wiringHostEnv,
				"api_port_env":       wiringPortEnv,
				"token_file":         opts.TokenFile,
				"ca_file":            opts.CAFile,
				"request_timeout":    "5s",
				"secrets_path":       secretsPath,
				"list_page_size":     50,
				"exclude":            map[string]any{"label": "backup.test/ignore", "value": "true"},
				"pods":               pods,
				"volumes":            volumes,
				"undeclared_volumes": map[string]any{"name": "undeclared", "importance": "required"},
			},
		}},
		"processors": map[string]any{"zstd": map[string]any{"type": "zstd", "config": map[string]any{
			"level": "fastest", "concurrency": 1, "window_bytes": 1 << 20, "decode_window_bytes": 1 << 20,
		}}},
		"storages": map[string]any{"local": map[string]any{
			"type": "local", "retention": map[string]any{"scheduled": map[string]any{"type": "keep_last", "config": map[string]any{
				"count": 3, "min_keep": 1, "count_incomplete": true}}},
			"max_total_bytes": int64(1) << 30,
			"time_budget":     "1m",
			"config":          map[string]any{"root": storageRoot, "page_size": 100},
		}},
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func wiringEngine(t *testing.T, cfg jsontext.Value) *engine.Engine {
	t.Helper()
	reg := plugin.NewRegistry()
	if err := cli.Generic(reg); err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(context.Background(), cfg, nil, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func artifactsByName(rep model.RunReport) map[string]model.ArtifactResult {
	out := map[string]model.ArtifactResult{}
	for _, a := range rep.Artifacts {
		out[a.Artifact.Name] = a
	}
	return out
}

// TestRealDiscoveryScopesOneBrokenWorkload: one of three labelled pods cannot
// render its source (it misses the label the rule reads), and the run still
// produces and delivers the other two artifacts — the broken one is ONE failed
// artifact carrying its problem, the outcome follows that artifact's
// importance, and preflight shows the discovery itself as ok with one FAILED
// row for the broken object.
func TestRealDiscoveryScopesOneBrokenWorkload(t *testing.T) {
	scenarios := []struct {
		name        string
		importance  string
		wantOutcome model.Outcome
		wantExit    int
	}{
		{"required", string(model.ImportanceRequired), model.OutcomeFailed, 1},
		{"best_effort", string(model.ImportanceBestEffort), model.OutcomeDegraded, 0},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv := kubeapitest.New(t)
			opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)

			srv.AddPod(wiringPod(t, "ns", "good-1", map[string]string{"backup.test/workload": "true", "backup.test/root": wiringRootDir(t, "good-1")}))
			srv.AddPod(wiringPod(t, "ns", "good-2", map[string]string{"backup.test/workload": "true", "backup.test/root": wiringRootDir(t, "good-2")}))
			// The broken workload: labelled to match, but the label the rule's
			// root template reads is missing.
			srv.AddPod(wiringPod(t, "ns", "broken", map[string]string{"backup.test/workload": "true"}))

			storageRoot := t.TempDir()
			cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
				[]any{podRule(tc.importance, `{{ label "backup.test/root" }}`)}, nil, 2, "10ms")
			e := wiringEngine(t, cfg)

			rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatalf("Run did not start: %v", err)
			}
			if rep.Outcome != tc.wantOutcome || rep.ExitCode != tc.wantExit {
				t.Fatalf("outcome = %s (exit %d), want %s (exit %d)\nartifacts: %+v",
					rep.Outcome, rep.ExitCode, tc.wantOutcome, tc.wantExit, rep.Artifacts)
			}
			byName := artifactsByName(rep)
			if len(byName) != 3 {
				t.Fatalf("the run reports %d artifacts, want 3: %+v", len(byName), rep.Artifacts)
			}
			for _, name := range []string{"good-1", "good-2"} {
				a, ok := byName[name]
				if !ok || a.Outcome != model.ArtifactOK || a.Bytes == 0 || a.Key == "" {
					t.Errorf("healthy artifact %q = %+v, want a delivered object", name, a)
				}
			}
			broken, ok := byName["broken"]
			if !ok || broken.Outcome != model.ArtifactFailed {
				t.Fatalf("broken artifact = %+v, want failed", broken)
			}
			for _, want := range []string{"pod ns/broken", `has no label "backup.test/root"`} {
				if !strings.Contains(broken.Error, want) {
					t.Errorf("broken artifact error %q does not name %q", broken.Error, want)
				}
			}
			// Objects counts every uploaded object: the two healthy artifacts
			// and the run manifest.
			if len(rep.Storages) != 1 || rep.Storages[0].Outcome != model.StorageVerified || rep.Storages[0].Objects != 3 {
				t.Errorf("storages = %+v, want one verified storage holding the two healthy artifacts and the manifest", rep.Storages)
			}
			if _, err := os.Stat(filepath.Join(storageRoot, "runs", rep.RunID, "manifest.json")); err != nil {
				t.Errorf("the run manifest is not on the storage: %v", err)
			}

			// Preflight — the path `validate --preflight` uses, with an empty
			// selection (every declared instance): the discovery row stays ok,
			// every healthy found source has its own ok row, and the broken
			// object has one FAILED row naming it.
			rows := map[string]engine.PreflightResult{}
			for _, r := range e.PreflightResults(ctx, engine.PreflightRequest{}) {
				rows[r.Kind+"/"+r.Instance] = r
			}
			if discovery, ok := rows["discovery/workloads"]; !ok || !discovery.OK {
				t.Fatalf("discovery row = %+v, want ok", discovery)
			}
			for _, name := range []string{"volumes/ns/good-1", "volumes/ns/good-2"} {
				if row, ok := rows["source/"+name]; !ok || !row.OK {
					t.Errorf("source row %s = %+v, want ok", name, row)
				}
			}
			brokenRow, ok := rows["source/volumes/ns/broken"]
			if !ok || brokenRow.OK {
				t.Fatalf("broken source row = %+v, want FAILED", brokenRow)
			}
			if !strings.Contains(brokenRow.Error, `has no label "backup.test/root"`) {
				t.Errorf("broken source row error %q does not name the problem", brokenRow.Error)
			}
		})
	}
}

// TestRealDiscoveryRetriesRetryableProblem: one 5xx is aimed at the SECOND
// read of the key, so it lands in the run's Discover — the first read is the
// scoped preflight's Check. The plugin marks the Found retryable, the engine's
// retry policy re-runs Discover, and the artifact ends delivered. This fails
// both when the engine loop is removed and when the plugin stops marking.
func TestRealDiscoveryRetriesRetryableProblem(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		key       string
		valueFrom map[string]any
		seed      func(*kubeapitest.Server, string)
	}{
		{
			"secret",
			"secrets/ns/cfgdata",
			map[string]any{"secretKeyRef": map[string]any{"name": "cfgdata", "key": "ROOT"}},
			func(srv *kubeapitest.Server, dir string) {
				srv.AddSecret("ns", "cfgdata", map[string]string{"ROOT": dir})
			},
		},
		{
			"configmap",
			"configmaps/ns/cfgdata",
			map[string]any{"configMapKeyRef": map[string]any{"name": "cfgdata", "key": "ROOT"}},
			func(srv *kubeapitest.Server, dir string) {
				srv.AddConfigMap("ns", "cfgdata", map[string]string{"ROOT": dir})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
			tc.seed(srv, wiringRootDir(t, tc.name))
			srv.AddPod(wiringPodEnv(t, "ns", "app", map[string]string{"backup.test/workload": "true"}, "ROOT", tc.valueFrom))
			srv.FailNth(tc.key, 500, 2)

			storageRoot := t.TempDir()
			cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
				[]any{podRule(string(model.ImportanceRequired), `{{ envValue 0 "ROOT" }}`)}, nil, 2, "10ms")
			e := wiringEngine(t, cfg)

			rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatalf("Run did not start: %v", err)
			}
			if rep.Outcome != model.OutcomeDelivered {
				t.Fatalf("outcome = %s, want delivered after one retry; artifacts: %+v", rep.Outcome, rep.Artifacts)
			}
			if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactOK {
				t.Fatalf("artifacts = %+v, want the one artifact ok", rep.Artifacts)
			}
			if len(rep.Storages) != 1 || rep.Storages[0].Outcome != model.StorageVerified {
				t.Fatalf("storages = %+v, want one verified copy", rep.Storages)
			}
		})
	}
}

// TestRealDiscoveryRetryableProblemPastBudgetIsOneArtifact: a 5xx that answers
// every attempt on ONE object's ConfigMap read outlives the retry budget, and
// the last attempt stands — that object is one failed artifact carrying the
// 5xx, the other two are delivered with bytes on the storage, and the outcome
// follows the rule's importance. One failing dependency never costs the night.
func TestRealDiscoveryRetryableProblemPastBudgetIsOneArtifact(t *testing.T) {
	scenarios := []struct {
		name        string
		importance  string
		wantOutcome model.Outcome
		wantExit    int
	}{
		{"required", string(model.ImportanceRequired), model.OutcomeFailed, 1},
		{"best_effort", string(model.ImportanceBestEffort), model.OutcomeDegraded, 0},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv := kubeapitest.New(t)
			opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
			for _, name := range []string{"good-1", "good-2", "bad"} {
				srv.AddConfigMap("ns", name, map[string]string{"ROOT": wiringRootDir(t, name)})
				srv.AddPod(wiringPodEnv(t, "ns", name, map[string]string{"backup.test/workload": "true"}, "ROOT",
					map[string]any{"configMapKeyRef": map[string]any{"name": name, "key": "ROOT"}}))
			}
			// The bad object's ConfigMap answers 5xx on every attempt.
			srv.Fail("configmaps/ns/bad", 500)

			storageRoot := t.TempDir()
			cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
				[]any{podRule(tc.importance, `{{ envValue 0 "ROOT" }}`)}, nil, 2, "10ms")
			e := wiringEngine(t, cfg)

			rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatalf("Run did not start: %v", err)
			}
			if rep.Outcome != tc.wantOutcome || rep.ExitCode != tc.wantExit {
				t.Fatalf("outcome = %s (exit %d), want %s (exit %d)\nartifacts: %+v",
					rep.Outcome, rep.ExitCode, tc.wantOutcome, tc.wantExit, rep.Artifacts)
			}
			byName := artifactsByName(rep)
			if len(byName) != 3 {
				t.Fatalf("the run reports %d artifacts, want 3: %+v", len(byName), rep.Artifacts)
			}
			for _, name := range []string{"good-1", "good-2"} {
				a, ok := byName[name]
				if !ok || a.Outcome != model.ArtifactOK || a.Bytes == 0 {
					t.Errorf("healthy artifact %q = %+v, want a delivered object", name, a)
				}
			}
			bad, ok := byName["bad"]
			if !ok || bad.Outcome != model.ArtifactFailed {
				t.Fatalf("bad artifact = %+v, want failed", bad)
			}
			if !strings.Contains(bad.Error, "500") || !strings.Contains(bad.Error, "pod ns/bad") {
				t.Errorf("bad artifact error %q does not carry the 5xx and the object", bad.Error)
			}
			if len(rep.Storages) != 1 || rep.Storages[0].Outcome != model.StorageVerified || rep.Storages[0].Objects != 3 {
				t.Errorf("storages = %+v, want one verified storage holding the two healthy artifacts and the manifest", rep.Storages)
			}

			// Preflight retries the same way and, at the budget end, shows the
			// still-broken object as its own FAILED row while the healthy ones
			// stay ok.
			rows := map[string]engine.PreflightResult{}
			for _, r := range e.PreflightResults(ctx, engine.PreflightRequest{}) {
				rows[r.Kind+"/"+r.Instance] = r
			}
			if row, ok := rows["discovery/workloads"]; !ok || !row.OK {
				t.Fatalf("discovery row = %+v, want ok", row)
			}
			for _, name := range []string{"volumes/ns/good-1", "volumes/ns/good-2"} {
				if row, ok := rows["source/"+name]; !ok || !row.OK {
					t.Errorf("source row %s = %+v, want ok", name, row)
				}
			}
			if row, ok := rows["source/volumes/ns/bad"]; !ok || row.OK || !strings.Contains(row.Error, "500") {
				t.Errorf("bad source row = %+v, want FAILED carrying the 500", row)
			}
		})
	}
}

// TestRealDiscoveryUnusableSecretsPathCostsOnlySecretArtifacts: with
// secrets_path pointing at a file, the rule whose source reads a secret file
// fails — its one artifact — while the volume rule, which writes no secret
// file, is delivered. The unusable path is permanent: no attempt answers it
// differently, and the run's outcome follows the failed rule's importance.
func TestRealDiscoveryUnusableSecretsPathCostsOnlySecretArtifacts(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddPod(wiringPod(t, "ns", "media", map[string]string{"backup.test/workload": "true", "backup.test/root": wiringRootDir(t, "media")}))
	dbDir := wiringRootDir(t, "db")
	srv.AddPod(wiringPodEnv(t, "ns", "db", map[string]string{"backup.test/workload": "db"}, "ROOT",
		map[string]any{"secretKeyRef": map[string]any{"name": "creds", "key": "ROOT"}}))
	srv.AddSecret("ns", "creds", map[string]string{"ROOT": dbDir})

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, blocker, t.TempDir(), storageRoot,
		[]any{dbRule(string(model.ImportanceRequired)), podRule(string(model.ImportanceBestEffort), `{{ label "backup.test/root" }}`)}, nil, 2, "10ms")
	e := wiringEngine(t, cfg)

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome = %s, want failed: the required dbs artifact cannot be produced", rep.Outcome)
	}
	byName := artifactsByName(rep)
	if len(byName) != 2 {
		t.Fatalf("the run reports %d artifacts, want 2: %+v", len(byName), rep.Artifacts)
	}
	if media, ok := byName["media"]; !ok || media.Outcome != model.ArtifactOK || media.Bytes == 0 {
		t.Errorf("the volume artifact = %+v, want it delivered despite the unusable secrets path", media)
	}
	if db, ok := byName["db"]; !ok || db.Outcome != model.ArtifactFailed || !strings.Contains(db.Error, blocker) {
		t.Errorf("the secret-file artifact = %+v, want it failed naming the unusable path", db)
	}
	if len(rep.Storages) != 1 || rep.Storages[0].Outcome != model.StorageVerified || rep.Storages[0].Objects != 2 {
		t.Errorf("storages = %+v, want one verified storage holding the volume artifact and the manifest", rep.Storages)
	}
}

// TestRealDiscoveryListCallIsRetried: a 5xx on the SECOND pods list — the
// run's Discover, past the scoped preflight's own list — is a discovery-level
// transient, retried by the engine under the same policy; the artifact ends
// delivered.
func TestRealDiscoveryListCallIsRetried(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddPod(wiringPod(t, "ns", "dbs", map[string]string{"backup.test/workload": "true", "backup.test/root": wiringRootDir(t, "dbs")}))
	srv.FailNth("pods", 500, 2)

	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
		[]any{podRule(string(model.ImportanceRequired), `{{ label "backup.test/root" }}`)}, nil, 2, "10ms")
	e := wiringEngine(t, cfg)

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome = %s, want delivered after retrying the list; artifacts: %+v", rep.Outcome, rep.Artifacts)
	}
}

// TestRealDiscoveryListCallPastBudgetFailsTheDiscovery pins the discovery-level
// rule: a list call that answers 5xx on every attempt is retried to the budget
// and then fails the discovery as a whole — the run reports one required
// artifact `discovery/workloads` carrying the 5xx and produces nothing else,
// because without a listing there is nothing to scope a failure to.
func TestRealDiscoveryListCallPastBudgetFailsTheDiscovery(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddPod(wiringPod(t, "ns", "dbs", map[string]string{"backup.test/workload": "true", "backup.test/root": wiringRootDir(t, "dbs")}))
	srv.Fail("pods", 500)

	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), t.TempDir(),
		[]any{podRule(string(model.ImportanceRequired), `{{ label "backup.test/root" }}`)}, nil, 2, "10ms")
	e := wiringEngine(t, cfg)

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome = %s (artifacts %+v), want failed", rep.Outcome, rep.Artifacts)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Artifact.Name != "discovery/workloads" || rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("artifacts = %+v, want the single failed discovery artifact", rep.Artifacts)
	}
	if !strings.Contains(rep.Artifacts[0].Error, "500") {
		t.Errorf("the discovery artifact does not carry the 500: %q", rep.Artifacts[0].Error)
	}
}

// TestRealDiscoveryCancellationDuringRetryWait: cancelling the run while a
// retryable problem waits for its next pass makes the summary carry the
// cancellation — the run's discovery artifact says "context canceled", never
// the stale 5xx of the pass before it.
func TestRealDiscoveryCancellationDuringRetryWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddConfigMap("ns", "cfg", map[string]string{"ROOT": wiringRootDir(t, "app")})
	srv.AddPod(wiringPodEnv(t, "ns", "app", map[string]string{"backup.test/workload": "true"}, "ROOT",
		map[string]any{"configMapKeyRef": map[string]any{"name": "cfg", "key": "ROOT"}}))
	srv.Fail("configmaps/ns/cfg", 500)

	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), t.TempDir(),
		[]any{podRule(string(model.ImportanceRequired), `{{ envValue 0 "ROOT" }}`)}, nil, 3, "2s")
	e := wiringEngine(t, cfg)

	// Wait for the two reads that precede the run's retry wait — the scoped
	// check's and the first Discover pass's — then cancel inside the wait.
	base := srv.Calls("configmaps/ns/cfg")
	go func() {
		for srv.Calls("configmaps/ns/cfg") < base+2 {
			time.Sleep(2 * time.Millisecond)
		}
		cancel()
	}()

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want the cancellation or a report", err)
		}
		return
	}
	var discoveryArtifact model.ArtifactResult
	found := false
	for _, a := range rep.Artifacts {
		if a.Artifact.Name == "discovery/workloads" {
			discoveryArtifact, found = a, true
		}
	}
	if !found {
		t.Fatalf("no discovery artifact in the cancelled run: %+v", rep.Artifacts)
	}
	if !strings.Contains(discoveryArtifact.Error, "context canceled") {
		t.Errorf("the discovery artifact error %q does not carry the cancellation", discoveryArtifact.Error)
	}
	if strings.Contains(discoveryArtifact.Error, "500") {
		t.Errorf("the stale 5xx came back instead of the cancellation: %s", discoveryArtifact.Error)
	}
}

// TestRestoreTargetDiscoveredStopsWhenTheTargetIsHealthy: an unrelated object's
// ConfigMap answering 5xx must not hold the restore for the whole retry budget
// — the pass that found the target healthy is the pass that resolves it. The
// fake counts the reads, so one pass is one read.
func TestRestoreTargetDiscoveredStopsWhenTheTargetIsHealthy(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddPod(wiringPodEnv(t, "ns", "good", map[string]string{"backup.test/workload": "true"}, "ROOT",
		map[string]any{"configMapKeyRef": map[string]any{"name": "good", "key": "ROOT"}}))
	srv.AddConfigMap("ns", "good", map[string]string{"ROOT": wiringRootDir(t, "good")})
	srv.AddPod(wiringPodEnv(t, "ns", "bad", map[string]string{"backup.test/workload": "true"}, "ROOT",
		map[string]any{"configMapKeyRef": map[string]any{"name": "bad", "key": "ROOT"}}))
	srv.AddConfigMap("ns", "bad", map[string]string{"ROOT": wiringRootDir(t, "bad")})
	srv.Fail("configmaps/ns/bad", 500)

	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
		[]any{podRule(string(model.ImportanceBestEffort), `{{ envValue 0 "ROOT" }}`)}, nil, 2, "10ms")
	e := wiringEngine(t, cfg)
	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	if a := artifactsByName(rep)["good"]; a.Outcome != model.ArtifactOK {
		t.Fatalf("the healthy artifact = %+v, want it delivered for the restore", a)
	}

	baseBad, basePods := srv.Calls("configmaps/ns/bad"), srv.Calls("pods")
	if _, err := e.Apply(ctx, engine.ApplyRunRequest{
		Fetch:  engine.FetchRequest{RunID: rep.RunID, Artifact: "good", Decode: true},
		Target: "discovered",
		Mode:   source.ApplySideBySide,
	}); err != nil {
		t.Fatalf("apply with a healthy target: %v", err)
	}
	if got := srv.Calls("configmaps/ns/bad") - baseBad; got != 1 {
		t.Errorf("the unrelated retryable object was read %d times, want 1: the restore waited for it", got)
	}
	// One pass lists pods twice: once per pod rule, and once for the pods that
	// carry the exclusion marker — the accounting of claims always runs, so its
	// marker pass does too.
	if got := srv.Calls("pods") - basePods; got != 2 {
		t.Errorf("the discovery made %d pod listings, want 2 (one pass)", got)
	}
}

// TestRestoreTargetDiscoveredRetriesItsOwnProblem: when the target's own read
// blips once, that problem buys another pass — the restore resolves after the
// retry, and the fake counts the two reads.
func TestRestoreTargetDiscoveredRetriesItsOwnProblem(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)
	srv.AddPod(wiringPodEnv(t, "ns", "good", map[string]string{"backup.test/workload": "true"}, "ROOT",
		map[string]any{"configMapKeyRef": map[string]any{"name": "good", "key": "ROOT"}}))
	srv.AddConfigMap("ns", "good", map[string]string{"ROOT": wiringRootDir(t, "good")})
	srv.AddConfigMap("ns", "other", map[string]string{"ROOT": wiringRootDir(t, "other")})
	srv.AddPod(wiringPodEnv(t, "ns", "other", map[string]string{"backup.test/workload": "true"}, "ROOT",
		map[string]any{"configMapKeyRef": map[string]any{"name": "other", "key": "ROOT"}}))

	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
		[]any{podRule(string(model.ImportanceRequired), `{{ envValue 0 "ROOT" }}`)}, nil, 2, "10ms")
	e := wiringEngine(t, cfg)
	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}

	// The next read of the target's own key fails once: it lands in the
	// restore's first Discover pass. FailNth counts the requests since it was
	// armed, and nothing reads this key between the run and the apply.
	base := srv.Calls("configmaps/ns/good")
	srv.FailNth("configmaps/ns/good", 500, 1)
	if _, err := e.Apply(ctx, engine.ApplyRunRequest{
		Fetch:  engine.FetchRequest{RunID: rep.RunID, Artifact: "good", Decode: true},
		Target: "discovered",
		Mode:   source.ApplySideBySide,
	}); err != nil {
		t.Fatalf("apply with a target that blipped once: %v", err)
	}
	if got := srv.Calls("configmaps/ns/good") - base; got != 2 {
		t.Errorf("the target's key was read %d times, want 2: the blip was not retried", got)
	}
}

// wiringClaimRule declares a claim as a file volume: the selector matches the
// claims a manifest declared, and the source reads the volume the claim is bound
// to — the path the plugin resolves from the PersistentVolume.
func wiringClaimRule(importance string) map[string]any {
	return map[string]any{
		"name":     "volumes",
		"selector": "backup.test/engine=files", "phase": "Bound",
		"artifact": map[string]any{"group": "volumes", "name": "{{ .Namespace }}/{{ .Name }}",
			"importance": importance, "sensitivity": "plain"},
		"pipeline": []string{"zstd"}, "min_matches": 0,
		"source": map[string]any{"type": "files", "config": map[string]any{
			"root": "{{ volumePath }}", "ignore_vanished": true,
		}},
	}
}

// wiringClaim is one PersistentVolumeClaim fixture bound to a named volume.
func wiringClaim(t *testing.T, namespace, name string, labels map[string]string, volumeName string) string {
	t.Helper()
	claim := map[string]any{
		"metadata": map[string]any{"name": name, "namespace": namespace, "labels": labels},
		"spec":     map[string]any{"volumeName": volumeName},
		"status":   map[string]any{"phase": "Bound"},
	}
	raw, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestRealDiscoveryOptOutCoverage drives the whole opt-out decision table
// through the real plugin and the real engine: a claim declared as a file volume
// is read at the path of its own PersistentVolume, a claim a pod with a declared
// engine mounts is covered by that pod's dump, a marked claim is ignored
// visibly, and a claim nobody accounted for is ONE failed artifact naming it —
// while everything else is delivered.
func TestRealDiscoveryOptOutCoverage(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)

	// The declared claim's data, and the volume it is bound to: the path the
	// rule reads is the one the cluster reports, not a mount made for this test.
	declared := wiringRootDir(t, "declared")
	srv.AddPV(`{"metadata":{"name":"pv-declared"},"spec":{"local":{"path":"` + declared + `"}}}`)
	srv.AddPVC(wiringClaim(t, "ns", "declared", map[string]string{"backup.test/engine": "files"}, "pv-declared"))

	// A claim no rule declares and no pod covers.
	srv.AddPVC(wiringClaim(t, "ns", "orphan", map[string]string{"backup.test/kind": "pvc"}, "pv-orphan"))

	// Two claims the run decides not to copy: the marker on one, the pod that
	// mounts the other.
	srv.AddPVC(wiringClaim(t, "ns", "ignored", map[string]string{"backup.test/ignore": "true"}, "pv-ignored"))
	srv.AddPVC(wiringClaim(t, "ns", "covered", map[string]string{"backup.test/kind": "pvc"}, "pv-covered"))
	pod := map[string]any{
		"metadata": map[string]any{"name": "db-0", "namespace": "ns",
			"labels": map[string]string{"backup.test/engine": "postgres", "backup.test/root": wiringRootDir(t, "dump")}},
		"spec": map[string]any{
			"containers": []any{map[string]any{"name": "postgres"}},
			"volumes":    []any{map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "covered"}}},
		},
		"status": map[string]any{"phase": "Running"},
	}
	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	srv.AddPod(string(rawPod))

	rules := []any{
		func() map[string]any {
			r := podRule("required", `{{ label "backup.test/root" }}`)
			r["name"] = "databases"
			r["selector"] = "backup.test/engine=postgres"
			r["covers_volumes"] = true
			r["artifact"].(map[string]any)["name"] = "{{ .Namespace }}/db"
			return r
		}(),
	}
	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot, rules, []any{wiringClaimRule("required")}, 2, "10ms")
	e := wiringEngine(t, cfg)

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	byName := artifactsByName(rep)
	if len(byName) != 3 {
		t.Fatalf("the run reports %d artifacts, want the dump, the declared claim and the orphan: %+v", len(byName), rep.Artifacts)
	}
	if a, ok := byName["ns/db"]; !ok || a.Outcome != model.ArtifactOK {
		t.Errorf("the pod's dump = %+v, want delivered", a)
	}
	declaredArtifact, ok := byName["ns/declared"]
	if !ok || declaredArtifact.Outcome != model.ArtifactOK {
		t.Fatalf("the declared claim = %+v, want delivered from its volume", declaredArtifact)
	}
	orphan, ok := byName["ns/orphan"]
	if !ok || orphan.Outcome != model.ArtifactFailed {
		t.Fatalf("the undeclared claim = %+v, want ONE failed artifact", orphan)
	}
	for _, want := range []string{"persistentvolumeclaim ns/orphan", "is backed up by no rule", "backup.test/ignore=true"} {
		if !strings.Contains(orphan.Error, want) {
			t.Errorf("the undeclared claim's error %q does not name %q", orphan.Error, want)
		}
	}
	if orphan.Artifact.Importance != model.ImportanceRequired || rep.Outcome != model.OutcomeFailed {
		t.Errorf("the undeclared claim did not fail the run: %+v", rep.Outcome)
	}
	// The covered claim is not copied: no artifact carries its name.
	if _, ok := byName["ns/covered"]; ok {
		t.Errorf("a claim the pod's dump covers was copied anyway: %+v", byName["ns/covered"])
	}
	// The declared claim's artifact came from ITS volume: the source's origin is
	// the path volumePath resolved, which is the directory this test created.
	if declaredArtifact.Source.Origin != "file://"+declared {
		t.Errorf("the declared claim was read from %q, want its own volume %q", declaredArtifact.Source.Origin, declared)
	}
	if declaredArtifact.Bytes == 0 {
		t.Error("the declared claim's artifact is empty")
	}

	// Both decisions are reached by the accounting rule — the one whose selector
	// reaches every claim — and the record carries the group of the rule that
	// would have produced an artifact for the object, never the accounting
	// setting's own name.
	wantExcluded := map[string]model.Exclusion{
		// No rule would have produced one: the accounting deploys no artifact,
		// so the record names no group at all.
		"ns/ignored": {Name: "ns/ignored", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker backup.test/ignore=true"},
		// The claim's data is what the pod rule's own artifact carries, so the
		// record names that rule's group.
		"ns/covered": {Name: "ns/covered", Group: "data", Kind: model.ExclusionCovered, Reason: `covered by the dump of pod ns/db-0 (rule "databases")`},
	}
	if len(rep.Excluded) != len(wantExcluded) {
		t.Fatalf("excluded %+v, want one record per decision", rep.Excluded)
	}
	for _, x := range rep.Excluded {
		want, ok := wantExcluded[x.Name]
		if !ok {
			t.Fatalf("unexpected exclusion %+v", x)
		}
		if x.Kind != want.Kind || x.Group != want.Group || x.Reason != want.Reason {
			t.Fatalf("exclusion %+v, want %+v", x, want)
		}
	}

	// Preflight reports the same decisions and the same failure.
	rows := map[string]engine.PreflightResult{}
	for _, r := range e.PreflightResults(ctx, engine.PreflightRequest{}) {
		rows[r.Kind+"/"+r.Instance] = r
	}
	if row, ok := rows["source/undeclared/ns/ignored"]; !ok || !row.OK || row.Excluded != model.ExclusionIgnored {
		t.Errorf("the ignored claim's preflight row = %+v", row)
	}
	if row, ok := rows["source/undeclared/ns/covered"]; !ok || !row.OK || row.Excluded != model.ExclusionCovered {
		t.Errorf("the covered claim's preflight row = %+v", row)
	}
	if row, ok := rows["source/undeclared/ns/orphan"]; !ok || row.OK {
		t.Errorf("the undeclared claim's preflight row = %+v, want FAILED", row)
	}
}

// TestRealDiscoveryKeepsACoveredClaimThroughALateStumble: a claim covered by the
// dump of the pod mounting it must not turn into a failure because the LAST
// discovery pass stumbled on that pod. The knob makes the pass worth repeating
// (another object's read fails once, retryably), and the covering pod's own read
// then fails in the second pass only: the pod and its claim both keep the
// verdict the first pass reached.
func TestRealDiscoveryKeepsACoveredClaimThroughALateStumble(t *testing.T) {
	ctx := context.Background()
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, wiringHostEnv, wiringPortEnv)

	// Two engine pods, each reading its source config through a Secret, each
	// mounting its own claim.
	for _, name := range []string{"a", "b"} {
		srv.AddSecret("ns", name, map[string]string{"ROOT": wiringRootDir(t, name)})
		pod := map[string]any{
			"metadata": map[string]any{"name": name + "-0", "namespace": "ns", "labels": map[string]string{"backup.test/workload": "true"}},
			"spec": map[string]any{
				"containers": []any{map[string]any{
					"name": "app",
					"env": []any{map[string]any{"name": "ROOT", "valueFrom": map[string]any{
						"secretKeyRef": map[string]any{"name": name, "key": "ROOT"},
					}}},
				}},
				"volumes": []any{map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "data-" + name}}},
			},
			"status": map[string]any{"phase": "Running"},
		}
		raw, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}
		srv.AddPod(string(raw))
		srv.AddPVC(wiringClaim(t, "ns", "data-"+name, map[string]string{"backup.test/kind": "pvc"}, "pv-"+name))
	}
	// Each secret is read once per pass, and the run makes three passes before
	// its discoveries: the scoped check first, then two Discover passes with
	// attempts: 2. So request 2 of b is the first Discover pass (the retryable
	// problem that buys the second), and request 3 of a is the LAST pass — the
	// stumble the merge has to survive.
	srv.FailNth("secrets/ns/b", 500, 2)
	srv.FailNth("secrets/ns/a", 500, 3)

	storageRoot := t.TempDir()
	cfg := wiringConfig(t, opts, t.TempDir(), t.TempDir(), storageRoot,
		[]any{func() map[string]any {
			r := podRule("required", `{{ envValue 0 "ROOT" }}`)
			r["name"] = "databases"
			r["selector"] = "backup.test/workload=true"
			r["covers_volumes"] = true
			r["artifact"].(map[string]any)["name"] = "{{ .Namespace }}/{{ .Name }}"
			return r
		}()}, nil, 2, "1ms")
	e := wiringEngine(t, cfg)

	rep, err := e.Run(ctx, engine.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("Run did not start: %v", err)
	}
	byName := artifactsByName(rep)
	for _, name := range []string{"ns/a-0", "ns/b-0"} {
		a, ok := byName[name]
		if !ok {
			t.Fatalf("artifact %q missing: %+v", name, rep.Artifacts)
		}
		if a.Outcome != model.ArtifactOK {
			t.Fatalf("artifact %q = %+v, want the verdict the first pass reached", name, a)
		}
	}
	covered := map[string]bool{}
	for _, x := range rep.Excluded {
		if x.Kind == model.ExclusionCovered {
			covered[x.Name] = true
		}
	}
	for _, name := range []string{"ns/data-a", "ns/data-b"} {
		if !covered[name] {
			t.Fatalf("the claim %q is not covered by the dump of its pod: %+v", name, rep.Excluded)
		}
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("the run reports %d artifacts, want the two dumps: %+v", len(rep.Artifacts), rep.Artifacts)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("outcome %q, want the run to keep the verdicts of the earlier pass: %+v", rep.Outcome, rep.Artifacts)
	}
}
