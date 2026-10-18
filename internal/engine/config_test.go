package engine

import (
	"context"
	"encoding/json/jsontext"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"go.yaml.in/yaml/v3"
)

func newEngineRaw(t *testing.T, w *world, raw jsontext.Value) (*Engine, error) {
	t.Helper()
	e, err := New(context.Background(), raw, nil, w.registry, testLogger())
	if e != nil {
		t.Cleanup(func() { _ = e.Close() })
	}
	return e, err
}

func TestConfigMissingFieldsWithPaths(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	delete(cfg["run"].(map[string]any), "id_layout")
	delete(cfg["run"].(map[string]any), "deadline")
	st := cfg["storages"].(map[string]any)["primary"].(map[string]any)
	delete(st, "time_budget")
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("a config with missing fields was accepted")
	}
	msg := err.Error()
	for _, want := range []string{
		"missing field /run/id_layout",
		"missing field /run/deadline",
		"missing field /storages/primary/time_budget",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}
}

func TestConfigUnknownMemberWithPath(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["storages"].(map[string]any)["primary"].(map[string]any)["evict_befor_upload"] = true // typo
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("unknown member accepted")
	}
	if !strings.Contains(err.Error(), "evict_befor_upload") || !strings.Contains(err.Error(), "/storages/primary") {
		t.Fatalf("error lacks member or path: %v", err)
	}
}

func TestConfigUnknownTypeListsKnown(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("nosuch", "keep_last")})
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("unknown type accepted")
	}
	if !strings.Contains(err.Error(), `unknown storage type "nosuch"`) || !strings.Contains(err.Error(), "fake") {
		t.Fatalf("error does not list known types: %v", err)
	}
}

func TestConfigTwoInstancesOfOneType(t *testing.T) {
	w := newWorld(t)
	w.processors["p1"] = &fakeProcessor{ext: ".one"}
	w.processors["p2"] = &fakeProcessor{ext: ".two"}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"p1": instance("fake"), "p2": instance("fake")}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []string{"p1", "p2"}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newEngineRaw(t, w, raw); err != nil {
		t.Fatal(err)
	}
	if w.processors["p1"].ext != ".one" || w.processors["p2"].ext != ".two" {
		t.Fatal("the two instances were not built independently")
	}
}

func TestConfigEnumsAndRanges(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	run := cfg["run"].(map[string]any)
	run["unclassified_error"] = "maybe"
	run["min_verified_storages"] = 0
	run["storage_parallelism"] = 0
	run["retry"].(map[string]any)["jitter"] = 1.5
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["artifact"].(map[string]any)["sensitivity"] = "weird"
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("invalid enums accepted")
	}
	msg := err.Error()
	for _, want := range []string{
		"unclassified_error", "min_verified_storages", "storage_parallelism",
		"jitter", "sensitivity",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}
}

func TestConfigDanglingReferences(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["storages"].(map[string]any)["primary"].(map[string]any)["retention"] = map[string]any{"scheduled": map[string]any{"type": "nosuch", "config": map[string]any{}}}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []string{"ghost"}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("dangling references accepted")
	}
	msg := err.Error()
	for _, want := range []string{"unknown retention", "ghost"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q (all references must be checked):\n%s", want, msg)
		}
	}
}

func TestConfigVersion(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["version"] = 2
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newEngineRaw(t, w, raw); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("version 2 accepted: %v", err)
	}
}

func TestConfigUnknownProcessorInPipeline(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []string{"ghost"}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newEngineRaw(t, w, raw); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("pipeline referencing an unknown processor accepted: %v", err)
	}
}

func TestConfigErrorsCollect(t *testing.T) {
	// Several independent errors across the file arrive together.
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	delete(cfg["run"].(map[string]any), "spool_dir")
	delete(cfg["layout"].(map[string]any), "manifest_key")
	delete(cfg["storages"].(map[string]any)["primary"].(map[string]any), "time_budget")
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("broken config accepted")
	}
	msg := err.Error()
	for _, want := range []string{"spool_dir", "manifest_key", "time_budget"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q (all errors must arrive together):\n%s", want, msg)
		}
	}
}

// yamlToJSON mirrors what the distribution CLI does in step 3: YAML in, the JSON
// bytes the core decodes out.
func yamlToJSON(t *testing.T, y string) jsontext.Value {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(y), &v); err != nil {
		t.Fatal(err)
	}
	raw, err := jsonMarshal(normalizeYAML(v))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// normalizeYAML turns yaml.v3's map[string]any into JSON-marshalable data.
func normalizeYAML(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := map[string]any{}
		for k, val := range x {
			m[k] = normalizeYAML(val)
		}
		return m
	case map[any]any:
		m := map[string]any{}
		for k, val := range x {
			m[k.(string)] = normalizeYAML(val)
		}
		return m
	case []any:
		for i := range x {
			x[i] = normalizeYAML(x[i])
		}
		return x
	default:
		return v
	}
}

func registerStep2Names(t *testing.T, w *world) {
	t.Helper()
	w.registerType(t, "discovery", "kubernetes_labels")
	w.registerType(t, "source", "postgres")
	w.registerType(t, "source", "redis")
	w.registerType(t, "source", "files")
	w.registerType(t, "source", "sqlite")
	w.registerType(t, "source", "staged")
	w.registerType(t, "processor", "zstd")
	w.registerType(t, "processor", "age")
	w.registerType(t, "storage", "yandex_disk")
	w.registerType(t, "storage", "s3")
	w.registerType(t, "catalog", "kubernetes_configmap")
	w.registerType(t, "notifier", "webhook")
}

// clusterRunYAML is a run-shaped configuration of the plan, section 6 — the run
// half of what the deployment's one file carries. The engine must decode it with
// nothing else registered than plugins named exactly like the step-2 ones.
const clusterRunYAML = `
version: 1
run:
  id_layout: "20060102T150405Z"
  spool_dir: /backup/spool
  spool_reserve_bytes: 1073741824
  max_artifact_bytes: 3221225472
  deadline: 85m
  finish_allowance: 2m
  min_verified_storages: 1
  degraded_exit_code: 0
  failed_exit_code: 1
  storage_parallelism: 1
  unclassified_error: transient
  notifier_failure_is_fatal: false
  progress_interval: 15s
  lease_wait: 30s
  lease_poll: 1s
  lease_hard_bound: 8400s
  holder_namespace_env: ""
  holder_pod_env: ""
  holder_uid_env: ""
  retry: {attempts: 4, initial_delay: 5s, max_delay: 40s, jitter: 0.2}
layout:
  run_prefix: "runs/{{ .Run.ID }}"
  artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"
  manifest_key: "manifest.json"
  manual_id_suffix: "-manual"
discoveries:
  workloads:
    type: kubernetes_labels
    config:
      api_host_env: KUBERNETES_SERVICE_HOST
      api_port_env: KUBERNETES_SERVICE_PORT
      token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      request_timeout: 30s
      secrets_dir: /tmp/discovered
      rules:
        - name: postgres
          resource: pods
          selector: "backup.example/enabled=true,backup.example/type=database,backup.example/engine=postgres"
          phase: Running
          artifact: {group: databases, name: '{{ .Namespace }}/postgres-{{ envValue 0 "POSTGRES_DB" }}', importance: required, sensitivity: plain}
          pipeline: [zstd3]
          source:
            type: postgres
            config:
              address: '{{ label "backup.example/service-name" }}.{{ .Namespace }}.svc.cluster.local:{{ port 0 0 }}'
              database: '{{ envValue 0 "POSTGRES_DB" }}'
              username: '{{ envValue 0 "POSTGRES_USER" }}'
              password: {file: '{{ envFile 0 "POSTGRES_PASSWORD" }}'}
              format: custom
              compression: none
              connect_attempts: 6
              connect_interval: 5s
        - name: volumes
          resource: persistentvolumeclaims
          selector: "backup.example/enabled=true,backup.example/type=pvc"
          phase: Bound
          artifact: {group: volumes, name: '{{ .Namespace }}/{{ .Name }}', importance: required, sensitivity: plain}
          pipeline: [zstd3]
          source:
            type: files
            config:
              root: '/volumes/{{ .Namespace }}/{{ .Name }}'
              ignore_vanished: true
sources:
  vpn_peers:
    type: sqlite
    artifact: {group: vpn, name: wireguard, importance: required, sensitivity: secret}
    pipeline: [zstd3, seal]
    config: {path: /vpn-config/wireguard.db, busy_timeout: 10s, integrity_check: quick}
  control_plane:
    type: staged
    artifact: {group: control-plane, name: k3s-state, importance: required, sensitivity: secret}
    pipeline: []
    config: {dir: /backup/staged, name: k3s-state}
processors:
  zstd3: {type: zstd, config: {level: 3, concurrency: 1, window_bytes: 8388608}}
  seal:  {type: age,  config: {recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"]}}
storages:
  primary:
    type: yandex_disk
    retention:
      scheduled: {type: gfs, config: {daily: 30, weekly: 12, monthly: 12, timezone: UTC, min_keep: 1, count_incomplete: true}}
      manual: {type: keep_last, config: {count: 5, min_keep: 1, count_incomplete: true}}
    max_total_bytes: 70000000000
    time_budget: 30m
    config:
      api_url: https://cloud-api.yandex.net/v1/disk
      token: {file: /run/secrets/primary/token}
      root: "app:/cluster-backup"
      permanent_delete: true
      request_timeout: 30s
      commit_poll_interval: 5s
      commit_poll_timeout: 2m
  offsite:
    type: s3
    retention:
      scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}
    max_total_bytes: 14000000000
    time_budget: 30m
    config:
      endpoint: https://s3.example.com
      region: ru-central-1
      bucket: "example-bucket"
      prefix: cluster-backup
      access_key_id: {file: /run/secrets/s3/access-key-id}
      secret_access_key: {file: /run/secrets/s3/secret-access-key}
      addressing: path
      storage_class: STANDARD
      request_checksum: when_required
      part_bytes: 16777216
      max_request_bytes: 5368709120
      max_object_bytes: 5497558138880
      request_timeout: 5m
catalog:
  type: kubernetes_configmap
  config:
    api_host_env: KUBERNETES_SERVICE_HOST
    api_port_env: KUBERNETES_SERVICE_PORT
    token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    request_timeout: 30s
    namespace: infrastructure
    name: cluster-backup-catalog
    key: catalog.json
    max_runs: 60
notifiers:
  ops:
    type: webhook
    config:
      url: http://receiver.example.com/notify
      method: POST
      headers: {Content-Type: application/json}
      timeout: 10s
      success_status: [200, 202]
      events:
        - name: run_started
          body: '{"thread":"backup","title":"x"}'
`

func TestClusterRunYAMLDecodes(t *testing.T) {
	w := newWorld(t)
	registerStep2Names(t, w)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	// The staged artifact carries its producer's chain: the stand-in reports it
	// the way the real staged source does, and the chain's sealing step is what
	// keeps the secret invariant.
	w.extraSources["control_plane"] = &chainSource{data: []byte("staged"), ext: ".tar.zst.age",
		chain: []model.ProcessorStep{
			{Instance: "zstd3", Type: "zstd", Ext: ".zst"},
			{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
		}}
	raw := yamlToJSON(t, clusterRunYAML)
	e, err := newEngineRaw(t, w, raw)
	if err != nil {
		t.Fatalf("the cluster config does not decode: %v", err)
	}
	// The whole run can be planned: discovery finds nothing, both static sources
	// resolve, the secret invariant holds (seal seals), storages are described.
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("dry run of the cluster config: %+v", rep)
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	for _, a := range rep.Artifacts {
		if a.Outcome != model.ArtifactOK {
			t.Fatalf("artifact %+v", a)
		}
	}
}

// clusterStageYAML is a stage-shaped configuration of the plan, section 6 — plus
// its own exit codes, which the stage section needs because it has no run
// section.
const clusterStageYAML = `
version: 1
layout: {run_prefix: "runs/{{ .Run.ID }}", artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}", manifest_key: manifest.json, manual_id_suffix: "-manual"}
stage:
  dir: /backup/staged
  degraded_exit_code: 0
  failed_exit_code: 1
  sources:
    control_plane:
      type: k3s_state
      artifact: {group: control-plane, name: k3s-state, importance: required, sensitivity: secret}
      pipeline: [zstd3, seal]
      config: {datastore: /k3s-db/state.db, token: /k3s-token, scratch_dir: /plain, busy_timeout: 10s, integrity_check: quick}
processors:
  zstd3: {type: zstd, config: {level: 3, concurrency: 1, window_bytes: 8388608}}
  seal: {type: age, config: {recipients: ["age1example"]}}
`

func TestClusterStageYAMLDecodesAndStages(t *testing.T) {
	w := newWorld(t)
	w.registerType(t, "source", "k3s_state")
	w.registerType(t, "processor", "age")
	w.registerType(t, "processor", "zstd")
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	w.processors["zstd3"] = &fakeProcessor{ext: ".zst"}
	w.sources["control_plane"] = &fakeSource{data: []byte("sealed snapshot")}
	dir := t.TempDir()
	cfg := clusterStageYAML
	cfg = strings.Replace(cfg, "dir: /backup/staged", "dir: "+dir, 1)
	raw := yamlToJSON(t, cfg)
	e, err := newEngineRaw(t, w, raw)
	if err != nil {
		t.Fatalf("the stage config does not decode: %v", err)
	}
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("stage report %+v", rep)
	}
	body, err := os.ReadFile(filepath.Join(dir, "k3s-state.dat.zst.age"))
	if err != nil {
		t.Fatalf("the staged artifact is missing: %v", err)
	}
	if string(body) != "sealed snapshot" {
		t.Fatalf("staged bytes %q", body)
	}
}

// notifierBodyYAML renders the run_finished template of the plan, section 6,
// through the core function set: the plan's nested if/else must be expressible.
func TestClusterNotifierTemplateRenders(t *testing.T) {
	const body = `{"thread":"backup",
 "title":{{ if eq .Run.Outcome "delivered" }}"✅ Backup completed"{{ else if eq .Run.Outcome "degraded" }}"⚠️ Backup DEGRADED"{{ else }}"❌ Backup FAILED"{{ end }},
 "lines":[{"text":{{ json (printf "📦 %s" .Run.RunID) }}},
  {"text":"⏱ Duration: ","mono":{{ json (duration .Run.Duration) }}}
  {{- range .Run.Artifacts }},
  {"text":{{ json .Artifact.Name }},"mono":{{ json (printf "%s in %s" (bytes .Bytes) (duration .Duration)) }}}
  {{- end }}
  {{- range .Run.Storages }},
  {"text":{{ json .Storage }},"mono":{{ json (printf "%s, %s, kept %d, deleted %d" .Outcome (bytes .Bytes) .Kept (len .Deleted)) }}}
  {{- end }}]}`
	var cfg struct {
		Body plugin.Template `json:"body"`
	}
	raw, err := jsonMarshal(map[string]any{"body": body})
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Decode(raw, &cfg); err != nil {
		t.Fatalf("the notifier body template does not parse: %v", err)
	}
	data := struct{ Run model.RunReport }{
		Run: model.RunReport{
			RunID:    "20261012T042000Z",
			Duration: model.Duration(4*time.Minute + 39*time.Second),
			Outcome:  model.OutcomeDegraded,
			Artifacts: []model.ArtifactResult{
				{Artifact: model.Artifact{Name: "db/pg"}, Outcome: model.ArtifactOK, Bytes: 3616189605, Duration: model.Duration(time.Minute)},
			},
			Storages: []model.StorageResult{
				{Storage: "primary", Outcome: model.StorageVerified, Bytes: 3616189605, Kept: 9, Deleted: []string{"A", "B"}},
			},
		},
	}
	out, err := cfg.Body.Render(data)
	if err != nil {
		t.Fatalf("the notifier body does not render: %v", err)
	}
	for _, want := range []string{"⚠️ Backup DEGRADED", "20261012T042000Z", "4m39s", "3.37 GiB", "kept 9, deleted 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered body lacks %q:\n%s", want, out)
		}
	}
	// And it is valid JSON, which is what the notifier will send.
	var parsed map[string]any
	if err := jsonUnmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("rendered body is not JSON: %v\n%s", err, out)
	}
}

// TestRestoreApplyNullIsRefused: `apply.postgres:` with the options block
// commented out is a null, and an options block is never silently absent — the
// refusal lands at construction rather than at the first restore, which is the
// moment nothing else is left to do about it.
func TestRestoreApplyNullIsRefused(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["restore"] = map[string]any{
		"spool_dir": t.TempDir(), "spool_reserve_bytes": 0,
		"storage_order":      []any{"primary"},
		"unclassified_error": "transient",
		"retry":              map[string]any{"attempts": 1, "initial_delay": "1ms", "max_delay": "1ms", "jitter": 0},
		"apply":              map[string]any{"postgres": nil},
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("an apply options block written as null passed construction")
	}
	if want := "/restore/apply/postgres is null"; !strings.Contains(err.Error(), want) {
		t.Errorf("the refusal does not name the null: %v", err)
	}
}

func validRetryConfig() RetryConfig {
	return RetryConfig{
		Attempts:     1,
		InitialDelay: model.Duration(time.Millisecond),
		MaxDelay:     model.Duration(time.Millisecond),
		Jitter:       0,
	}
}

func validRunConfig() *RunConfig {
	return &RunConfig{
		IDLayout:            "20060102T150405Z",
		SpoolDir:            "/spool",
		SpoolReserveBytes:   0,
		MaxArtifactBytes:    1,
		Deadline:            model.Duration(time.Second),
		FinishAllowance:     model.Duration(time.Second),
		MinVerifiedStorages: 1,
		StorageParallelism:  1,
		UnclassifiedError:   plugin.ClassTransient,
		Retry:               validRetryConfig(),
		LeaseWait:           model.Duration(0),
		LeasePoll:           model.Duration(time.Millisecond),
		LeaseHardBound:      model.Duration(time.Second),
	}
}

func validArtifactConfig() ArtifactConfig {
	return ArtifactConfig{
		Group:       "g",
		Name:        "n",
		Importance:  model.ImportanceRequired,
		Sensitivity: model.SensitivityPlain,
	}
}

func engineBaseConfig() *Config {
	return &Config{Version: 1, Run: validRunConfig(), Layout: LayoutConfig{ManualIDSuffix: "-manual"}}
}

func TestConfigValidateRejectsEveryField(t *testing.T) {
	withRestore := func(mutate func(*RestoreConfig)) func(*Config) {
		return func(c *Config) {
			c.Restore = &RestoreConfig{SpoolDir: "/r", StorageOrder: []string{"s"}, UnclassifiedError: plugin.ClassTransient, Retry: validRetryConfig()}
			mutate(c.Restore)
		}
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"version", func(c *Config) { c.Version = 2 }, "unsupported config version"},
		{"id_layout", func(c *Config) { c.Run.IDLayout = "" }, "/run/id_layout is empty"},
		{"spool_dir", func(c *Config) { c.Run.SpoolDir = "" }, "/run/spool_dir is empty"},
		{"spool_reserve_bytes", func(c *Config) { c.Run.SpoolReserveBytes = -1 }, "spool_reserve_bytes is negative"},
		{"max_artifact_bytes", func(c *Config) { c.Run.MaxArtifactBytes = 0 }, "max_artifact_bytes must be at least 1"},
		{"deadline", func(c *Config) { c.Run.Deadline = 0 }, "deadline must be positive"},
		{"finish_allowance", func(c *Config) { c.Run.FinishAllowance = 0 }, "finish_allowance must be positive"},
		{"run exit codes", func(c *Config) { c.Run.DegradedExitCode = -1 }, "exit codes must not be negative"},
		{"lease_wait", func(c *Config) { c.Run.LeaseWait = model.Duration(-1) }, "lease_wait must not be negative"},
		{"lease_poll", func(c *Config) { c.Run.LeasePoll = 0 }, "lease_poll must be positive"},
		{"lease_hard_bound", func(c *Config) { c.Run.LeaseHardBound = 0 }, "lease_hard_bound must be positive"},
		{"manual_id_suffix empty", func(c *Config) { c.Layout.ManualIDSuffix = "" }, "manual_id_suffix is empty"},
		{"manual_id_suffix slash", func(c *Config) { c.Layout.ManualIDSuffix = "a/b" }, "must not carry"},
		{"manual_id_suffix dotdot", func(c *Config) { c.Layout.ManualIDSuffix = "a..b" }, "must not carry"},
		{"restore spool_reserve_bytes", withRestore(func(r *RestoreConfig) { r.SpoolReserveBytes = -1 }), "/restore/spool_reserve_bytes is negative"},
		{"restore storage_order", withRestore(func(r *RestoreConfig) { r.StorageOrder = nil }), "/restore/storage_order is empty"},
		{"stage dir", func(c *Config) { c.Stage = &StageConfig{Dir: ""} }, "/stage/dir is empty"},
		{"stage exit codes", func(c *Config) { c.Stage = &StageConfig{Dir: "/d", DegradedExitCode: -1} }, "/stage exit codes must not be negative"},
		{"storage max_total_bytes", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", MaxTotalBytes: -1, TimeBudget: model.Duration(time.Second)}}
		}, "max_total_bytes must not be negative"},
		{"retention type empty", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", TimeBudget: model.Duration(time.Second), Retention: map[string]*Instance{"scheduled": nil}}}
		}, "/type is empty"},
		{"retention unknown kind", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", TimeBudget: model.Duration(time.Second), Retention: map[string]*Instance{"weekly": {Type: "keep_last"}}}}
		}, "unknown run kind"},
		{"pipeline empty ref", func(c *Config) {
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: validArtifactConfig(), Pipeline: []string{""}}}
		}, "pipeline/0 is empty"},
		{"artifact group", func(c *Config) {
			a := validArtifactConfig()
			a.Group = ""
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "/group is empty"},
		{"artifact name", func(c *Config) {
			a := validArtifactConfig()
			a.Name = ""
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "/name is empty"},
		{"artifact importance", func(c *Config) {
			a := validArtifactConfig()
			a.Importance = "maybe"
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "importance must be"},
		{"artifact sensitivity", func(c *Config) {
			a := validArtifactConfig()
			a.Sensitivity = "weird"
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "sensitivity must be"},
		{"run retry initial_delay", func(c *Config) { c.Run.Retry.InitialDelay = 0 }, "initial_delay must be positive"},
		{"run retry max_delay", func(c *Config) {
			c.Run.Retry.InitialDelay = model.Duration(2 * time.Millisecond)
			c.Run.Retry.MaxDelay = model.Duration(time.Millisecond)
		}, "max_delay must not be smaller"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := engineBaseConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate accepted a broken config (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q lacks %q", err.Error(), tc.want)
			}
		})
	}
}

func TestConfigValidateAcceptsACompleteRunConfig(t *testing.T) {
	if err := engineBaseConfig().Validate(); err != nil {
		t.Fatalf("a complete config was refused: %v", err)
	}
	// A config with no run and no restore section is the restore-only shape: it
	// carries no run knobs and is still valid.
	only := &Config{Version: 1, Layout: LayoutConfig{ManualIDSuffix: "-manual"}}
	if err := only.Validate(); err != nil {
		t.Fatalf("a run-less config was refused: %v", err)
	}
}

// --- engine.go: New's refusal branches --------------------------------------
