package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all/components"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// harnessLoader is the compiled view of the harness configuration: the hostless
// production composition, the doubles the harness declares, and the CLI host's
// own roles. The host package cannot import the composition root that imports
// it, so the generated hostless half and the host's AddHost are composed here.
func harnessLoader(t *testing.T) *loader.Registry {
	t.Helper()
	lr := loader.New()
	if err := components.Compose(lr, instances.NewRegistry()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		"storage/faulty", "discovery/fakediscovery", "catalog/memory", "notifier/memory", "source/synthetic",
	} {
		if err := loader.Add(lr, loader.Spec{ID: id, Summary: "harness fake"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := AddHost(lr, Options{Program: "backup"}); err != nil {
		t.Fatal(err)
	}
	return lr
}

// TestValidateReportsEveryErrorOfOnePhase: a broken config is never answered
// with only its first problem. The decode phase reports every missing field and
// unknown member with its path; once the file decodes, the reference phase
// reports every dangling name.
func TestValidateReportsEveryErrorOfOnePhase(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()

	broken := mustWrite(t, dir, "decode.yaml", `run:
  spool_dir: /spool
  deadline: 85m
  min_verified_storages: 1
  degraded_exit_code: 0
  failed_exit_code: 1
  storage_parallelism: 1
  unclassified_error: transient
  notifier_failure_is_fatal: false
  progress_interval: 15s
  retry: {attempts: 4, initial_delay: 5s, max_delay: 40s, jitter: 0.2}
  oops_member: true
layout:
  run_prefix: "runs/{{ .Run.ID }}"
  artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"
`)
	code, _, stderr := w.main(context.Background(), "validate", "--config", broken)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	for _, want := range []string{
		"missing field /run/spool_reserve_bytes",
		"missing field /run/max_artifact_bytes",
		`unknown object member name "oops_member" within "/run"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not carry %q:\n%s", want, stderr)
		}
	}
	// What the distribution's defaults document fills is not a missing field:
	// this file states no id_layout and no manifest_key, and both are defaults.
	for _, gone := range []string{"missing field /run/id_layout", "missing field /layout/manifest_key"} {
		if strings.Contains(stderr, gone) {
			t.Errorf("stderr carries %q, which the defaults document fills:\n%s", gone, stderr)
		}
	}

	dangling := mustWrite(t, dir, "references.yaml", `layout:
  run_prefix: "runs/{{ .Run.ID }}"
  artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"
  manifest_key: manifest.json
  manual_id_suffix: "-manual"
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
processors:
  squeeze: {type: zstd, config: {level: fastest, concurrency: 1, window_bytes: 1024, decode_window_bytes: 1024}}
storages:
  exchange:
    type: local
    retention: {scheduled: {type: ghost_retention, config: {}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {root: `+filepath.Join(dir, "exchange")+`, page_size: 100}
`)
	code, _, stderr = w.main(context.Background(), "validate", "--config", dangling)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	for _, want := range []string{
		`/sources/thing/pipeline/0: unknown processor instance "ghost_processor"`,
		`/storages/exchange/retention/scheduled: unknown retention type "ghost_retention"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not carry %q:\n%s", want, stderr)
		}
	}
}

// TestValidateReadsTheCompiledGraph: validate reports the same build problems
// `plugins check` and `doctor` do — a broken effective dependency fails it even
// when the configuration file itself is sound.
func TestValidateReadsTheCompiledGraph(t *testing.T) {
	lr := loader.New()
	if err := loader.Add(lr, loader.Spec{ID: "probe/broken", Requires: []string{"probe/ghost"}}, nil); err != nil {
		t.Fatal(err)
	}
	w := newWorld()
	dir := t.TempDir()
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(filepath.Join(dir, "exchange"))))
	var stdout, stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}, Loader: lr},
		[]string{"validate", "--config", cfg}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stderr.String())
	}
	if !strings.Contains(stderr.String(), `unregistered plugin "probe/ghost"`) {
		t.Errorf("the compiled graph's problem is not printed: %s", stderr.String())
	}
}

// TestValidateAggregatesSelectionAndReferenceProblems: one validate reports the
// configuration's reference errors and the diagnostics only the compiled set
// can judge — a selection naming an unknown plugin — in the same answer, never
// one phase hiding the other. Reference checks are pure: they report their
// problems without building an instance the refused selection would have
// disabled.
func TestValidateAggregatesSelectionAndReferenceProblems(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	body := `plugins:
  disabled: [source/ghost]
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
storages:
` + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	code, _, stderr := w.main(context.Background(), "validate", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stderr)
	}
	for _, want := range []string{
		`unknown plugin "source/ghost"`,
		`/sources/thing/pipeline/0: unknown processor instance "ghost_processor"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the aggregated answer does not carry %q:\n%s", want, stderr)
		}
	}
}

// TestValidatePreflightRunsPluginChecks: plain validate never touches a plugin
// Check; --preflight reports one line per instance and exits non-zero when any
// of them failed — and a failed check fails only its own line.
func TestValidatePreflightRunsPluginChecks(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))

	code, stdout, stderr := w.main(context.Background(), "validate", "--config", cfg)
	if code != 0 || !strings.Contains(stdout, "valid") || strings.Contains(stdout, "preflight") {
		t.Fatalf("validate: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if stderr != "" {
		t.Fatalf("plain validate ran plugin checks: %s", stderr)
	}

	code, stdout, stderr = w.main(context.Background(), "validate", "--config", cfg, "--preflight")
	if code != 0 {
		t.Fatalf("validate --preflight: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	for _, want := range []string{"source/thing: ok", "storage/exchange: ok", "preflight: 0 of 4 instances failed"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("preflight output does not carry %q:\n%s", want, stdout)
		}
	}

	w.checks["thing"] = errors.New("test: credentials rejected")
	code, stdout, stderr = w.main(context.Background(), "validate", "--config", cfg, "--preflight")
	if code != exitFailure {
		t.Fatalf("a failing check: exit %d, stdout %s", code, stdout)
	}
	if !strings.Contains(stdout, "source/thing: FAILED: test: credentials rejected") {
		t.Errorf("the failed instance is not named with its error:\n%s", stdout)
	}
	if !strings.Contains(stdout, "storage/exchange: ok") || !strings.Contains(stdout, "preflight: 1 of 4 instances failed") {
		t.Errorf("the other instances are not reported next to the failure:\n%s", stdout)
	}
	if !strings.Contains(stderr, "1 of 4 instances failed") {
		t.Errorf("the failure is not summarised on stderr: %s", stderr)
	}
}

// TestValidatePreflightCoversDiscoveredSources: a preflight walks every
// discovery, builds what it finds and reports one row per source — including a
// found object that already carries a problem — and the summary counts them.
func TestValidatePreflightCoversDiscoveredSources(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	w.found["workloads"] = []discovery.Found{
		{
			Name:     "found_ok",
			Type:     "synthetic",
			Config:   jsontext.Value(`{"payload": "x", "dir": "", "origin": "test:found"}`),
			Artifact: model.Artifact{Name: "found", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Pipeline: []string{"as_is"},
		},
		{Name: "found_broken", Problem: "the pod has no POSTGRES_DB variable"},
	}
	body := `discoveries:
  workloads:
    type: fakediscovery
    config: {}
` + zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))

	code, stdout, stderr := w.main(context.Background(), "validate", "--config", cfg, "--preflight")
	if code != exitFailure {
		t.Fatalf("a found object with a problem did not fail the preflight: exit %d\nstdout: %s", code, stdout)
	}
	if !strings.Contains(stdout, "source/found_ok: ok") {
		t.Errorf("the built discovered source is not reported ok:\n%s", stdout)
	}
	if !strings.Contains(stdout, "source/found_broken: FAILED: the pod has no POSTGRES_DB variable") {
		t.Errorf("the found object's problem is not reported:\n%s", stdout)
	}
	if !strings.Contains(stdout, "preflight: 1 of 6 instances failed") {
		t.Errorf("the summary does not count the discovered rows:\n%s", stdout)
	}
	if !strings.Contains(stderr, "1 of 6 instances failed") {
		t.Errorf("the failure is not summarised on stderr: %s", stderr)
	}
}

// scopedConfig is the config of the scoping tests: one named discovery whose
// found source is checked, one declared source the selection must leave alone,
// and the storages and retentions every preflight carries.
func scopedConfig(t *testing.T, w *world, dir string) string {
	t.Helper()
	w.found["workloads"] = []discovery.Found{{
		Name:     "found_ok",
		Type:     "synthetic",
		Config:   jsontext.Value(`{"payload": "x", "dir": "", "origin": "test:found"}`),
		Artifact: model.Artifact{Name: "found", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"as_is"},
	}}
	root := filepath.Join(dir, "exchange")
	body := `discoveries:
  workloads:
    type: fakediscovery
    config: {}
sources:
  static_source:
    type: synthetic
    artifact: {group: g, name: static, importance: required, sensitivity: plain}
    pipeline: []
    config: {payload: x, dir: "", origin: "test:static"}
` + zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	return mustWrite(t, dir, "config.yaml", configYAML(dir, body))
}

// TestValidatePreflightScopedChecksOnlyTheNamedInstances: --instance narrows the
// preflight to what it names — a named discovery brings the sources it finds —
// and leaves every other instance untouched: no Check runs on an excluded
// instance, and the summary says exactly what was not checked.
func TestValidatePreflightScopedChecksOnlyTheNamedInstances(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := scopedConfig(t, w, dir)

	code, stdout, stderr := w.main(context.Background(),
		"validate", "--config", cfg, "--preflight", "--instance", "discovery/workloads")
	if code != 0 {
		t.Fatalf("scoped preflight: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	for _, want := range []string{
		"  discovery/workloads: ok",
		"  source/found_ok: ok",
		"preflight: 0 of 2 checked instances failed",
		"preflight: 4 configured instances not checked: processor/squeeze, source/static_source, storage/exchange, storage/offsite",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("scoped output does not carry %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "source/static_source: ok") {
		t.Errorf("an excluded instance was checked:\n%s", stdout)
	}
	if n := w.sources["static_source"].checkCount(); n != 0 {
		t.Errorf("the excluded source's Check ran %d times", n)
	}
	if n := w.sources["found_ok"].checkCount(); n != 1 {
		t.Errorf("the named discovery's found source was checked %d times, want 1", n)
	}
}

// TestValidatePreflightUnknownInstance: a name that is not a configured
// instance is a usage error listing the valid ones, and nothing is checked.
func TestValidatePreflightUnknownInstance(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := scopedConfig(t, w, dir)

	code, stdout, stderr := w.main(context.Background(),
		"validate", "--config", cfg, "--preflight", "--instance", "storage/absent", "--instance", "discovery/workloads")
	if code != exitUsage {
		t.Fatalf("unknown instance: exit %d, want %d\nstderr: %s", code, exitUsage, stderr)
	}
	if !strings.Contains(stderr, "unknown instance storage/absent") || !strings.Contains(stderr, "discovery/workloads") {
		t.Errorf("the usage error does not name the unknown instance with the valid list: %s", stderr)
	}
	if stdout != "" {
		t.Errorf("a usage error answered on stdout:\n%s", stdout)
	}
	if n := w.sources["static_source"].checkCount(); n != 0 {
		t.Errorf("the excluded source's Check ran %d times", n)
	}
	if s := w.sources["found_ok"]; s != nil && s.checkCount() != 0 {
		t.Error("a check ran before the usage error")
	}
}

// TestValidateInstanceRequiresPreflight: the selection flag alone is a usage
// error — offline validation always reads the whole file.
func TestValidateInstanceRequiresPreflight(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := scopedConfig(t, w, dir)

	code, stdout, stderr := w.main(context.Background(),
		"validate", "--config", cfg, "--instance", "storage/exchange")
	if code != exitUsage {
		t.Fatalf("--instance without --preflight: exit %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "--instance requires --preflight") {
		t.Errorf("the usage error does not name the flag: %s", stderr)
	}
	if stdout != "" {
		t.Errorf("a usage error answered on stdout:\n%s", stdout)
	}
}

// TestValidatePreflightReportsExclusions: an object a discovery excluded is a
// line of its own — no failure, carrying the decision — and the summary counts
// it apart from the instances that were checked. Its type is one no registry
// knows, so a build of it would be a failed line: the OK row is the proof that
// nothing was built for it.
func TestValidatePreflightReportsExclusions(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	w.found["workloads"] = []discovery.Found{{
		Name:     "excluded_thing",
		Type:     "a-type-no-registry-knows",
		Artifact: model.Artifact{Group: "volumes", Name: "ns/media", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Excluded: model.ExclusionIgnored, ExcludedReason: "carries the exclusion marker backup.example/ignore=true",
	}}
	body := `discoveries:
  workloads:
    type: fakediscovery
    config: {}
` + zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))

	code, stdout, stderr := w.main(context.Background(), "validate", "--config", cfg, "--preflight")
	if code != exitOK {
		t.Fatalf("an exclusion failed the preflight: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	for _, want := range []string{
		"source/excluded_thing: excluded (ignored): carries the exclusion marker backup.example/ignore=true",
		"preflight: 0 of 4 instances failed",
		"preflight: 1 objects excluded",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("preflight output does not carry %q:\n%s", want, stdout)
		}
	}
}

// TestValidateRefusesAPipelineThatCannotEncode: an age instance with no
// recipient cannot seal anything, and a pipeline through it is a configuration
// fault the run must not discover one artifact at a time — four retries in, on
// the night the sealed artifact was due.
func TestValidateRefusesAPipelineThatCannotEncode(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	file := mustWrite(t, dir, "backup.yaml", configYAML(dir, `sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, sensitivity: secret}
    pipeline: [seal]
    config: {payload: hi, dir: "", origin: "test:thing"}
processors:
  seal: {type: age, config: {recipients: [], identities: [{file: /run/secrets/identity.txt}]}}
storages:
`+exchangeYAML(dir)+faultyYAML))
	code, _, stderr := w.main(context.Background(), "validate", "--config", file)
	if code == exitOK {
		t.Fatal("a pipeline with a processor that cannot encode passed validate")
	}
	for _, want := range []string{`/sources/thing/pipeline/0: processor "seal"`, "no recipients"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not name %q: %s", want, stderr)
		}
	}
}

// TestValidateRefusesATerminationGraceBelowTheCleanupBound: the chart's
// terminationGracePeriodSeconds must cover every cleanup bound the effective
// configuration adds up — the run's release window and the lifecycle budgets —
// and the refusal is aggregated with the file's other diagnostics, never
// instead of them.
func TestValidateRefusesATerminationGraceBelowTheCleanupBound(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `plugins:
  disabled: [source/ghost]
storages:
`+exchangeYAML(root)+faultyYAML))
	var stdout, stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Loader: harnessLoader(t),
			Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"validate", "--config", cfg, "--termination-grace-seconds", "60"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\nstdout: %s", code, exitFailure, stdout.String())
	}
	for _, want := range []string{
		"--termination-grace-seconds 60 is below the cleanup bound 4m5s",
		`unknown plugin "source/ghost"`,
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the aggregated refusal does not carry %q:\n%s", want, stderr.String())
		}
	}
}

// TestValidateAcceptsASufficientTerminationGrace: a grace that covers the bound
// is not a failure, and without the flag the check never runs.
func TestValidateAcceptsASufficientTerminationGrace(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, "storages:\n"+exchangeYAML(root)+faultyYAML))
	opts := Options{Program: "backup", Factories: w.factories(), Loader: harnessLoader(t),
		Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}}
	var stdout, stderr strings.Builder
	if code := Main(context.Background(), opts,
		[]string{"validate", "--config", cfg, "--termination-grace-seconds", "300"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit %d, want %d\n%s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "valid") {
		t.Errorf("a sufficient grace is not reported valid:\n%s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Main(context.Background(), opts,
		[]string{"validate", "--config", cfg}, &stdout, &stderr); code != exitOK {
		t.Fatalf("without the flag: exit %d, want %d\n%s", code, exitOK, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Main(context.Background(), opts,
		[]string{"validate", "--config", cfg, "--termination-grace-seconds", "-1"}, &stdout, &stderr); code != exitUsage {
		t.Errorf("a negative grace: exit %d, want %d", code, exitUsage)
	}
}

// TestValidateReportsAnUnreadableLifecycleBlock: a deployment overriding one
// cleanup budget with a value its type cannot read is a configuration fault the
// grace check names at its path.
func TestValidateReportsAnUnreadableLifecycleBlock(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `plugins:
  config:
    lifecycle:
      join: not-a-duration
storages:
`+exchangeYAML(root)+faultyYAML))
	var stdout, stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Loader: harnessLoader(t),
			Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"validate", "--config", cfg, "--termination-grace-seconds", "300"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "/plugins/config/lifecycle") {
		t.Errorf("the unreadable block is not named: %s", stderr.String())
	}
}

// TestM4_SelectionDiagnosticsSurviveAnIndependentDecodeError: a file with an
// unknown member and a selection naming an unknown plugin names both in one
// answer. The full decode fails on the member; the plugins block is checked on
// its own so the selection problem is not hidden behind it.
func TestM4_SelectionDiagnosticsSurviveAnIndependentDecodeError(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `oops: true
plugins:
  disabled: [source/ghost]
storages:
`+exchangeYAML(root)+faultyYAML))
	code, _, stderr := w.main(context.Background(), "validate", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stderr)
	}
	for _, want := range []string{
		`unknown object member name "oops"`,
		`unknown plugin "source/ghost"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the aggregated answer does not carry %q:\n%s", want, stderr)
		}
	}
}

// TestM3_InvalidCleanupBudgetsAreRefused: the grace gate refuses an effective
// lifecycle block whose budgets are outside their ranges or whose sum does not
// fit, instead of reading a non-positive result as "no deadline". The negative
// join probe is the reviewer's counterexample: on ee0585f the sum was <= 0,
// RequiredGrace returned zero and `--termination-grace-seconds 1` accepted the
// file.
func TestM3_InvalidCleanupBudgetsAreRefused(t *testing.T) {
	cases := []struct {
		name                       string
		join, abort, close, margin string
		want                       string
	}{
		{name: "a negative join", join: "-10m", abort: "2m", close: "30s", margin: "30s",
			want: "lifecycle: join must be positive"},
		{name: "a zero join", join: "0s", abort: "2m", close: "30s", margin: "30s",
			want: "lifecycle: join must be positive"},
		{name: "an overflowing block", join: "2562047h47m16.854775807s", abort: "2562047h47m16.854775807s",
			close: "30s", margin: "30s", want: "lifecycle: cleanup bound overflows time.Duration"},
		// The lifecycle block's own sum fits, but the release window the grace
		// also carries tips the total over time.Duration.
		{name: "a release window that overflows the sum", join: "2562047h47m16.854775800s", abort: "1ns",
			close: "1ns", margin: "0s", want: "lifecycle: cleanup bound overflows time.Duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld()
			dir := t.TempDir()
			root := filepath.Join(dir, "exchange")
			cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, fmt.Sprintf(`plugins:
  config:
    lifecycle:
      join: %s
      abort_total: %s
      close: %s
      margin: %s
storages:
`, tc.join, tc.abort, tc.close, tc.margin)+exchangeYAML(root)+faultyYAML))
			code, stdout, stderr := w.main(context.Background(),
				"validate", "--config", cfg, "--termination-grace-seconds", "1")
			if code != exitFailure {
				t.Fatalf("exit %d, want %d\nstdout: %s", code, exitFailure, stdout)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("the refusal does not carry %q:\n%s", tc.want, stderr)
			}
		})
	}
}
