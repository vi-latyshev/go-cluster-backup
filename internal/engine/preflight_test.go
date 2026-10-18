package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// checkingStorage is the fault-injecting storage plus a Check a test can fail:
// the engine's preflight scoping is what makes a storage whose Check fails
// cost only that storage.
type checkingStorage struct {
	*storagetest.Fake
	checkErr  error
	checkOnce error
	checks    int
}

func (s *checkingStorage) Check(context.Context) error {
	s.checks++
	if s.checks == 1 && s.checkOnce != nil {
		return s.checkOnce
	}
	return s.checkErr
}

func checkingStorages(t *testing.T, w *world, typ string) map[string]*checkingStorage {
	t.Helper()
	fleet := map[string]*checkingStorage{}
	err := plugin.Register(w.registry, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type: typ, Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
			s, ok := fleet[deps.Instance]
			if !ok {
				s = &checkingStorage{Fake: storagetest.NewFake()}
				fleet[deps.Instance] = s
			}
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fleet
}

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

func serviceCheck(t *testing.T, w *world, cfg map[string]any) *Engine {
	t.Helper()
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	return w.newEngine(t, cfg)
}

// TestRunScopesSourceCheckFailure: a required source whose Check fails is a
// failed artifact — every other artifact is still produced, uploaded to every
// storage and named in the notification.
func TestRunScopesSourceCheckFailure(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"broken": sourceInstance("fake", "databases", "broken-db", []string{"zstd3"}, "required", "plain"),
		"vpn":    sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
	}
	e := serviceCheck(t, w, cfg)
	w.sources["broken"].checkErr = errors.New("credentials rejected")

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("outcome %s exit %d, want failed/7", rep.Outcome, rep.ExitCode)
	}
	broken := artifactByName(rep, "broken-db")
	if broken.Outcome != model.ArtifactFailed || !strings.Contains(broken.Error, "credentials rejected") {
		t.Fatalf("broken artifact %+v", broken)
	}
	if ok := artifactByName(rep, "wireguard"); ok.Outcome != model.ArtifactOK {
		t.Fatalf("the healthy artifact failed too: %+v", ok)
	}
	if w.sources["broken"].dumpCount() != 0 {
		t.Fatal("a source whose Check failed was dumped")
	}
	for _, name := range []string{"primary", "offsite"} {
		f := w.storage(t, name)
		manifest := false
		for _, key := range f.Keys() {
			if strings.Contains(key, "broken-db") {
				t.Fatalf("storage %s holds the failed artifact at %s", name, key)
			}
			if strings.HasSuffix(key, "/manifest.json") {
				manifest = true
			}
		}
		if len(f.Keys()) == 0 || !manifest {
			t.Fatalf("storage %s: %v", name, f.Keys())
		}
		if got := storageResult(rep, name); got.Outcome != model.StorageVerified {
			t.Fatalf("storage %s outcome %s", name, got.Outcome)
		}
	}
	ev, ok := w.notifiers["ops"].last()
	if !ok || ev.Name != "run_finished" {
		t.Fatalf("notification %+v", ev)
	}
	reported := artifactByName(ev.Run, "broken-db")
	if reported.Outcome != model.ArtifactFailed || !strings.Contains(reported.Error, "credentials rejected") {
		t.Fatalf("the notification does not carry the failure: %+v", reported)
	}
}

// TestRunBestEffortSourceCheckFailureDegrades: a best_effort source's failed
// Check degrades the run and stops nothing.
func TestRunBestEffortSourceCheckFailureDegrades(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"cache": sourceInstance("fake", "caches", "search", []string{"zstd3"}, "best_effort", "plain"),
		"vpn":   sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
	}
	e := serviceCheck(t, w, cfg)
	w.sources["cache"].checkErr = errors.New("redis refused")

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded || rep.ExitCode != 3 {
		t.Fatalf("outcome %s exit %d, want degraded/3", rep.Outcome, rep.ExitCode)
	}
	if ok := artifactByName(rep, "wireguard"); ok.Outcome != model.ArtifactOK {
		t.Fatalf("the healthy artifact failed: %+v", ok)
	}
}

// TestRunScopesStorageCheckFailure: a storage whose Check fails is out for the
// run; the other holds everything, the manifest included.
func TestRunScopesStorageCheckFailure(t *testing.T) {
	w := newWorld(t)
	fleet := checkingStorages(t, w, "checking")
	cfg := baseConfig(t, map[string]any{
		"primary": storageInstance("checking", "keep_last"),
		"offsite": storageInstance("checking", "keep_last"),
	})
	e := serviceCheck(t, w, cfg)
	fleet["offsite"].checkErr = plugin.Permanent(errors.New("no credentials"))

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded || rep.ExitCode != 3 {
		t.Fatalf("outcome %s exit %d, want degraded/3", rep.Outcome, rep.ExitCode)
	}
	if got := storageResult(rep, "offsite"); got.Outcome != model.StorageFailed || !strings.Contains(got.Error, "no credentials") {
		t.Fatalf("the storage that failed its check: %+v", got)
	}
	if got := storageResult(rep, "primary"); got.Outcome != model.StorageVerified {
		t.Fatalf("the healthy storage: %+v", got)
	}
	if keys := fleet["offsite"].Keys(); len(keys) != 0 {
		t.Fatalf("the storage that failed its check received objects: %v", keys)
	}
	keys := fleet["primary"].Keys()
	if len(keys) == 0 {
		t.Fatal("the healthy storage holds nothing")
	}
	manifest := false
	for _, key := range keys {
		if strings.HasSuffix(key, "/manifest.json") {
			manifest = true
		}
	}
	if !manifest {
		t.Fatalf("the healthy storage has no manifest: %v", keys)
	}
}

func TestRunAllStoragesCheckFailure(t *testing.T) {
	w := newWorld(t)
	fleet := checkingStorages(t, w, "checking")
	cfg := baseConfig(t, map[string]any{
		"primary": storageInstance("checking", "keep_last"),
		"offsite": storageInstance("checking", "keep_last"),
	})
	e := serviceCheck(t, w, cfg)
	fleet["primary"].checkErr = plugin.Permanent(errors.New("no credentials"))
	fleet["offsite"].checkErr = plugin.Permanent(errors.New("endpoint refused"))

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("outcome %s exit %d, want failed/7", rep.Outcome, rep.ExitCode)
	}
	for name, f := range fleet {
		if keys := f.Keys(); len(keys) != 0 {
			t.Fatalf("storage %s holds %v", name, keys)
		}
	}
}

// TestRunDiscoveryFailureIsScoped: a discovery that cannot see the cluster —
// its Check or its Discover — loses its artifacts only; the statically
// configured sources are still backed up, and the run is never delivered.
func TestRunDiscoveryFailureIsScoped(t *testing.T) {
	cases := []struct {
		name string
		fail func(d *fakeDiscovery)
	}{
		{"check", func(d *fakeDiscovery) { d.checkErr = errors.New("api unreachable") }},
		{"discover", func(d *fakeDiscovery) { d.err = errors.New("api unreachable") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, twoStorages())
			cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
			e := serviceCheck(t, w, cfg)
			tc.fail(w.discoveries["workloads"])

			rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeFailed {
				t.Fatalf("outcome %s, want failed", rep.Outcome)
			}
			if ok := artifactByName(rep, "wireguard"); ok.Outcome != model.ArtifactOK {
				t.Fatalf("the static source was lost with the discovery: %+v", ok)
			}
			missing := artifactByName(rep, "discovery/workloads")
			if missing.Outcome != model.ArtifactFailed || !strings.Contains(missing.Error, "api unreachable") {
				t.Fatalf("the missing discovery is not named: %+v", missing)
			}
			if len(w.storage(t, "primary").Keys()) == 0 {
				t.Fatal("nothing was delivered")
			}
		})
	}
}

// TestRunProcessorCheckFailureNeverSendsASecretUnsealed: an artifact whose
// sealing processor failed its Check fails as an artifact and is never
// produced; artifacts on other pipelines proceed.
func TestRunProcessorCheckFailureNeverSendsASecretUnsealed(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "seal"}, "best_effort", "secret"),
		"app": sourceInstance("fake", "databases", "app-db", []string{"zstd3"}, "required", "plain"),
	}
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	e := serviceCheck(t, w, cfg)
	w.processors["seal"].checkErr = errors.New("identity unreadable")

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded {
		t.Fatalf("outcome %s, want degraded", rep.Outcome)
	}
	secret := artifactByName(rep, "wireguard")
	if secret.Outcome != model.ArtifactFailed || !strings.Contains(secret.Error, "identity unreadable") {
		t.Fatalf("the secret artifact %+v", secret)
	}
	if w.sources["vpn"].dumpCount() != 0 {
		t.Fatal("a secret artifact was produced through a processor that failed its check")
	}
	for _, name := range []string{"primary", "offsite"} {
		for _, key := range w.storage(t, name).Keys() {
			if strings.Contains(key, "wireguard") {
				t.Fatalf("storage %s holds the unsealed artifact at %s", name, key)
			}
		}
	}
	if ok := artifactByName(rep, "app-db"); ok.Outcome != model.ArtifactOK {
		t.Fatalf("the plain artifact failed too: %+v", ok)
	}
}

// TestRunRetriesATransientCheckFailure: a check that fails once with a
// transient error and succeeds on the retry does not degrade the run.
func TestRunRetriesATransientCheckFailure(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := serviceCheck(t, w, cfg)
	w.sources["vpn"].checkOnce = plugin.Transient(errors.New("connection reset"))

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("outcome %s exit %d, want delivered/0", rep.Outcome, rep.ExitCode)
	}
	if n := w.sources["vpn"].checkCount(); n < 2 {
		t.Fatalf("the check ran %d times, want a retry", n)
	}
}

// TestRunRefusesBeforeAnyWorkWhenARequiredBinaryIsMissing: a binary the plugin
// type declares and PATH does not have is the deployment — the run stops
// before any plugin work, and preflight names it.
func TestRunRefusesBeforeAnyWorkWhenARequiredBinaryIsMissing(t *testing.T) {
	const missingBinary = "gcb-definitely-not-a-binary"
	w := newWorld(t)
	err := plugin.Register(w.registry, source.Kind, plugin.Spec[map[string]jsontext.Value, source.Source]{
		Type: "needy", Example: []byte(`{}`), Requires: []string{missingBinary},
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (source.Source, error) {
			s, ok := w.sources[deps.Instance]
			if !ok {
				s = &fakeSource{data: []byte("x")}
				w.sources[deps.Instance] = s
			}
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("needy", "vpn", "wireguard", []string{"zstd3"}, "required", "plain")}
	e := w.newEngine(t, cfg)

	_, err = e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err == nil || !strings.Contains(err.Error(), missingBinary) {
		t.Fatalf("a missing required binary did not stop the run: %v", err)
	}
	if n := w.sources["vpn"].checkCount(); n != 0 {
		t.Fatalf("the run did plugin work anyway: %d checks", n)
	}
	if keys := w.storage(t, "primary").Keys(); len(keys) != 0 {
		t.Fatalf("the run wrote %v", keys)
	}
	res := e.PreflightResults(context.Background(), PreflightRequest{})
	found := false
	for _, r := range res {
		if r.Kind == "source" && r.Instance == "vpn" {
			found = true
			if r.OK || !strings.Contains(r.Error, missingBinary) {
				t.Fatalf("preflight result %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("preflight has no result for the source: %+v", res)
	}
}

// TestPreflightResultsCoverEveryInstance: one row per instance, all of them,
// each with its kind, its name and its own outcome.
func TestPreflightResultsCoverEveryInstance(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	e := w.newEngine(t, cfg)
	w.sources["vpn"].checkErr = errors.New("credentials rejected")

	res := e.PreflightResults(context.Background(), PreflightRequest{})
	want := map[instanceKey]bool{
		{"source", "vpn"}:      false,
		{"processor", "zstd3"}: true,
		{"storage", "primary"}: true, {"storage", "offsite"}: true,
		{"notifier", "ops"}:    true,
		{"catalog", "catalog"}: true,
	}
	if len(res) != len(want) {
		t.Fatalf("%d results, want %d: %+v", len(res), len(want), res)
	}
	for _, r := range res {
		wantOK, ok := want[instanceKey{r.Kind, r.Instance}]
		if !ok {
			t.Fatalf("unexpected result %+v", r)
		}
		if r.OK != wantOK {
			t.Fatalf("result %+v, want ok=%v", r, wantOK)
		}
		if !r.OK && !strings.Contains(r.Error, "credentials rejected") {
			t.Fatalf("result %+v does not carry the failure", r)
		}
	}
}

// TestStageScopesSourceCheckFailure: stage follows the same rule — the failing
// source's artifact is the only thing lost.
func TestStageScopesSourceCheckFailure(t *testing.T) {
	w := newWorld(t)
	cfg := map[string]any{
		"version": 1,
		"layout":  configLayout(),
		"stage": map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{
				"broken": sourceInstance("fake", "control-plane", "k3s-state", []string{}, "required", "plain"),
				"vpn":    sourceInstance("fake", "vpn", "wireguard", []string{}, "best_effort", "plain"),
			}},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	w.sources["broken"].checkErr = errors.New("no snapshot")
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("outcome %s exit %d, want failed/7", rep.Outcome, rep.ExitCode)
	}
	if ok := artifactByName(rep, "wireguard"); ok.Outcome != model.ArtifactOK {
		t.Fatalf("the healthy staged artifact: %+v", ok)
	}
	if bad := artifactByName(rep, "k3s-state"); bad.Outcome != model.ArtifactFailed || !strings.Contains(bad.Error, "no snapshot") {
		t.Fatalf("the failed staged artifact: %+v", bad)
	}
}

// discoveryRows counts the rows one instance got, so a double report is a
// failed assertion rather than a cosmetic surprise in the summary.
func discoveryRows(t *testing.T, res []PreflightResult, name string) []PreflightResult {
	t.Helper()
	var rows []PreflightResult
	for _, r := range res {
		if r.Kind == "discovery" && r.Instance == name {
			rows = append(rows, r)
		}
	}
	return rows
}

// TestPreflightReportsADiscoveryOnce: one row per instance, whatever a
// discovery does. A failed Check means its Discover is not run at all and the
// one row says so; a failed Discover turns the discovery's own row failed
// instead of adding a second; every found source gets its own row.
func TestPreflightReportsADiscoveryOnce(t *testing.T) {
	t.Run("a discovery whose Check failed gets one row", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, twoStorages())
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
		cfg["notifiers"] = map[string]any{"ops": instance("fake")}
		e := w.newEngine(t, cfg)
		w.discoveries["workloads"].checkErr = errors.New("api unreachable")
		w.discoveries["workloads"].found = []discovery.Found{foundSource("ghost", "ghost", model.ImportanceBestEffort)}

		res := e.PreflightResults(context.Background(), PreflightRequest{})
		rows := discoveryRows(t, res, "workloads")
		if len(rows) != 1 {
			t.Fatalf("%d rows for one discovery, want 1: %+v", len(rows), res)
		}
		if rows[0].OK || !strings.Contains(rows[0].Error, "api unreachable") ||
			!strings.Contains(rows[0].Error, "not checked") {
			t.Fatalf("row %+v, want the check failure with its sources reported unchecked", rows[0])
		}
		if got := w.discoveries["workloads"].discovers; got != 0 {
			t.Fatalf("Discover ran %d times on a discovery that failed its Check", got)
		}
		for _, r := range res {
			if r.Instance == "ghost" {
				t.Fatalf("a source was reported for a discovery that never ran: %+v", r)
			}
		}
		// 1 processor + 2 storages + 1 source + 1 discovery + 1 notifier: the
		// retention lives inside its storage and is not an instance of its own.
		if len(res) != 6 {
			t.Fatalf("%d rows for 6 instances: %+v", len(res), res)
		}
	})

	t.Run("a discovery whose Discover failed gets one row", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, twoStorages())
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
		cfg["notifiers"] = map[string]any{"ops": instance("fake")}
		e := w.newEngine(t, cfg)
		w.discoveries["workloads"].err = errors.New("api unreachable")

		res := e.PreflightResults(context.Background(), PreflightRequest{})
		rows := discoveryRows(t, res, "workloads")
		if len(rows) != 1 {
			t.Fatalf("%d rows for one discovery, want 1: %+v", len(rows), res)
		}
		if rows[0].OK || !strings.Contains(rows[0].Error, "api unreachable") {
			t.Fatalf("row %+v, want the discover failure", rows[0])
		}
		if len(res) != 6 {
			t.Fatalf("%d rows for 6 instances: %+v", len(res), res)
		}
	})

	t.Run("a working discovery adds one row per found source", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, twoStorages())
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
		cfg["notifiers"] = map[string]any{"ops": instance("fake")}
		e := w.newEngine(t, cfg)
		w.sources["broken-db"] = &fakeSource{checkErr: errors.New("credentials rejected")}
		w.discoveries["workloads"].found = []discovery.Found{
			foundSource("good-db", "good-db", model.ImportanceBestEffort),
			foundSource("broken-db", "broken-db", model.ImportanceBestEffort),
		}

		res := e.PreflightResults(context.Background(), PreflightRequest{})
		if rows := discoveryRows(t, res, "workloads"); len(rows) != 1 || !rows[0].OK {
			t.Fatalf("discovery rows %+v, want one passing row", rows)
		}
		got := map[string]PreflightResult{}
		found := 0
		for _, r := range res {
			if r.Kind == "source" && (r.Instance == "good-db" || r.Instance == "broken-db") {
				got[r.Instance] = r
				found++
			}
		}
		if found != 2 {
			t.Fatalf("%d rows for two found sources: %+v", found, res)
		}
		if !got["good-db"].OK {
			t.Fatalf("good source row %+v", got["good-db"])
		}
		if got["broken-db"].OK || !strings.Contains(got["broken-db"].Error, "credentials rejected") {
			t.Fatalf("broken source row %+v", got["broken-db"])
		}
		if len(res) != 8 {
			t.Fatalf("%d rows, want 6 instances + 2 found sources: %+v", len(res), res)
		}
	})
}

// TestPreflightClosesTheSourcesItBuilds: preflight builds every discovered
// source once per walk and closes it right there — repeated calls leave the
// Engine's lifetime closer list unchanged and each built source is closed
// once.
func TestPreflightClosesTheSourcesItBuilds(t *testing.T) {
	w := newWorld(t)
	target := &closingRestorer{}
	w.extraSources["found"] = target
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("found", "wireguard", model.ImportanceBestEffort)}

	before := len(e.closers)
	for i := 1; i <= 2; i++ {
		res := e.PreflightResults(context.Background(), PreflightRequest{})
		ok := false
		for _, r := range res {
			if r.Kind == "source" && r.Instance == "found" && r.OK {
				ok = true
			}
		}
		if !ok {
			t.Fatalf("preflight %d did not report the found source: %+v", i, res)
		}
		if got := target.closeCount(); got != i {
			t.Fatalf("after preflight %d Close ran %d times, want once per walk", i, got)
		}
	}
	if got := len(e.closers); got != before {
		t.Fatalf("the engine's lifetime closer list grew by %d over the preflights", got-before)
	}
}

// TestPreflightTargetsListsWithoutChecking: the target list is pure data — the
// declared instances in a stable order, with no Check, no binary lookup and no
// Discover behind it.
func TestPreflightTargetsListsWithoutChecking(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("found-db", "found-db", model.ImportanceBestEffort)}

	targets := e.PreflightTargets(context.Background())
	want := []PreflightTarget{
		{Kind: "discovery", Name: "workloads"},
		{Kind: "processor", Name: "zstd3"},
		{Kind: "source", Name: "vpn"},
		{Kind: "storage", Name: "offsite"},
		{Kind: "storage", Name: "primary"},
	}
	if len(targets) != len(want) {
		t.Fatalf("targets %+v, want %+v", targets, want)
	}
	for i := range want {
		if targets[i] != want[i] {
			t.Fatalf("targets %+v, want %+v", targets, want)
		}
	}
	if got := w.sources["vpn"].checkCount(); got != 0 {
		t.Fatalf("listing targets ran %d source checks", got)
	}
	if got := w.processors["zstd3"].checks; got != 0 {
		t.Fatalf("listing targets ran %d processor checks", got)
	}
	if got := w.discoveries["workloads"].discovers; got != 0 {
		t.Fatalf("listing targets ran Discover %d times", got)
	}
}

// TestPreflightRequestScopesTheChecks: the selection is applied before any
// Check, binary lookup or Discover — an unselected instance is not touched and
// an unselected discovery contributes no rows, while a selected discovery
// brings its found sources along.
func TestPreflightRequestScopesTheChecks(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn":   sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		"cache": sourceInstance("fake", "caches", "search", []string{"zstd3"}, "best_effort", "plain"),
	}
	cfg["discoveries"] = map[string]any{"workloads": instance("fake"), "ghost": instance("fake")}
	e := w.newEngine(t, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("found-db", "found-db", model.ImportanceBestEffort)}
	w.discoveries["ghost"].found = []discovery.Found{foundSource("ghost-db", "ghost-db", model.ImportanceBestEffort)}

	res := e.PreflightResults(context.Background(), PreflightRequest{Instances: []PreflightTarget{
		{Kind: "source", Name: "vpn"},
		{Kind: "discovery", Name: "workloads"},
	}})
	want := map[string]bool{"source/vpn": true, "discovery/workloads": true, "source/found-db": true}
	if len(res) != len(want) {
		t.Fatalf("rows %+v, want %v", res, want)
	}
	for _, r := range res {
		if !want[r.Kind+"/"+r.Instance] {
			t.Fatalf("unexpected row %+v", r)
		}
		if !r.OK {
			t.Fatalf("selected row failed: %+v", r)
		}
	}
	if got := w.sources["vpn"].checkCount(); got != 1 {
		t.Fatalf("the selected source's Check ran %d times, want 1", got)
	}
	if got := w.sources["found-db"].checkCount(); got != 1 {
		t.Fatalf("a found source of the selected discovery was not checked: %d", got)
	}
	if got := w.sources["cache"].checkCount(); got != 0 {
		t.Fatalf("the unselected source's Check ran %d times", got)
	}
	if got := w.processors["zstd3"].checks; got != 0 {
		t.Fatalf("the unselected processor's Check ran %d times", got)
	}
	if got := w.discoveries["workloads"].discovers; got != 1 {
		t.Fatalf("the selected discovery ran Discover %d times, want 1", got)
	}
	if got := w.discoveries["ghost"].discovers; got != 0 {
		t.Fatalf("the unselected discovery ran Discover %d times", got)
	}
}

// TestPreflightEmptyRequestIsTheFullPreflight: no selection means every
// declared instance and every discovery — row for row the same walk as naming
// every target explicitly.
func TestPreflightEmptyRequestIsTheFullPreflight(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("found-db", "found-db", model.ImportanceBestEffort)}

	full := e.PreflightResults(context.Background(), PreflightRequest{})
	explicit := e.PreflightResults(context.Background(), PreflightRequest{Instances: e.PreflightTargets(context.Background())})
	if len(full) != len(explicit) {
		t.Fatalf("full %+v, explicit %+v", full, explicit)
	}
	for i := range full {
		if full[i] != explicit[i] {
			t.Fatalf("row %d differs: %+v, %+v", i, full[i], explicit[i])
		}
	}
	seen := map[string]bool{}
	for _, r := range full {
		seen[r.Kind+"/"+r.Instance] = true
	}
	for _, target := range e.PreflightTargets(context.Background()) {
		if !seen[target.Kind+"/"+target.Name] {
			t.Fatalf("a declared instance has no row: %+v", target)
		}
	}
	if !seen["source/found-db"] {
		t.Fatalf("the discovered source has no row: %+v", full)
	}
}

// TestPreflightUnknownTargetIsAFailedRow: a selection naming an instance that
// is not part of this config is a caller error, never silence.
func TestPreflightUnknownTargetIsAFailedRow(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, twoStorages())
	e := w.newEngine(t, cfg)

	res := e.PreflightResults(context.Background(), PreflightRequest{Instances: []PreflightTarget{
		{Kind: "source", Name: "ghost"},
		{Kind: "source", Name: "vpn"},
	}})
	var unknown *PreflightResult
	for i := range res {
		if res[i].Kind == "source" && res[i].Instance == "ghost" {
			unknown = &res[i]
		}
	}
	if unknown == nil {
		t.Fatalf("an unknown target was silently ignored: %+v", res)
	}
	if unknown.OK || !strings.Contains(unknown.Error, "unknown instance source/ghost") {
		t.Fatalf("unknown row %+v", *unknown)
	}
	if got := w.sources["vpn"].checkCount(); got != 1 {
		t.Fatalf("the selected instance was not checked next to the unknown one: %d", got)
	}
}

// TestStageChecksNothingButItsOwnSources: the stage's check set is its own
// sources and the processors their pipelines name. A run-side instance whose
// Check fails — a source, a storage, a discovery — costs the stage nothing: this
// container has other mounts, and the run side's credentials are not its own.
func TestStageChecksNothingButItsOwnSources(t *testing.T) {
	w := newWorld(t)
	dir := t.TempDir()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["layout"] = configLayout()
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["stage"] = map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
		"sources": map[string]any{"control": sourceInstance("fake", "control-plane", "state", []string{}, "required", "plain")}}
	e := w.newEngine(t, cfg)
	// Every run-side instance fails its check; the stage's own source is fine.
	w.sources["vpn"].checkErr = errors.New("credentials rejected")
	w.discoveries["workloads"].checkErr = errors.New("api unreachable")
	w.notifiers["ops"].checkErr = errors.New("receiver refused")
	w.sources["control"] = &fakeSource{data: []byte("staged bytes")}

	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("stage report %+v, want the run side's failures to cost it nothing", rep)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if got := w.sources["vpn"].checkCount(); got != 0 {
		t.Fatalf("the stage checked the run side's source %d times", got)
	}

	// The same config's run does check them, and the failure is scoped to that
	// instance: the run's own source fails its artifact and the rest proceeds.
	rep, err = e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if w.sources["vpn"].checkCount() == 0 {
		t.Fatal("the run did not check its own source")
	}
	for _, a := range rep.Artifacts {
		if a.Source.Instance == "vpn" && a.Outcome != model.ArtifactFailed {
			t.Fatalf("artifact %+v, want the failed check to fail its own artifact", a)
		}
	}
}

// TestPreflightTargetsNameAStageSourceOfItsOwn: the same name may be declared on
// both sides — the staged source the run reads back and the source the stage
// produces it with — so a target carries the side in its kind, and the stage's
// own source is selectable under stage_source.
func TestPreflightTargetsNameAStageSourceOfItsOwn(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["stage"] = map[string]any{"dir": t.TempDir(), "degraded_exit_code": 0, "failed_exit_code": 0,
		"sources": map[string]any{"control_plane": sourceInstance("fake", "control-plane", "k3s-state", []string{}, "required", "plain")}}
	e := w.newEngine(t, cfg)

	var kinds []string
	for _, target := range e.PreflightTargets(context.Background()) {
		if target.Name == "control_plane" {
			kinds = append(kinds, target.Kind)
		}
	}
	if !slices.Contains(kinds, "stage_source") {
		t.Fatalf("the stage's own source is not selectable: %v", kinds)
	}

	// Selecting it checks it, and the run side's source of the same name is not
	// the instance that was asked for.
	res := e.PreflightResults(context.Background(), PreflightRequest{
		Instances: []PreflightTarget{{Kind: "stage_source", Name: "control_plane"}},
	})
	rows := 0
	for _, r := range res {
		if r.Kind == "stage_source" && r.Instance == "control_plane" {
			rows++
		}
	}
	if rows != 1 {
		t.Fatalf("the selected stage source got %d rows: %+v", rows, res)
	}
	// The run side has no source of that name in this config: the selection says
	// so instead of checking the stage's own instance by mistake.
	unknown := e.PreflightResults(context.Background(), PreflightRequest{
		Instances: []PreflightTarget{{Kind: "source", Name: "control_plane"}},
	})
	if len(unknown) != 1 || unknown[0].OK || !strings.Contains(unknown[0].Error, "unknown instance") {
		t.Fatalf("a selection naming the run side's source: %+v", unknown)
	}
}

// TestPreflightTargetsCancelledContext: a cancelled context lists nothing —
// the target list is a pure read and a dead context is not worth reading.
func TestPreflightTargetsCancelledContext(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := e.PreflightTargets(ctx); got != nil {
		t.Fatalf("PreflightTargets on a cancelled context = %+v, want nil", got)
	}
}

// decodeProcessor is a processor with the restore-side DecodeChecker
// capability, which `validate --preflight` asks about.
type decodeProcessor struct {
	*fakeProcessor
	decodes int
}

func (p *decodeProcessor) CheckDecode(context.Context) error {
	p.decodes++
	return nil
}

// TestPreflightChecksTheDecodeSide: a processor that implements DecodeChecker
// has its decode side checked when the caller asks for it.
func TestPreflightChecksTheDecodeSide(t *testing.T) {
	w := newWorld(t)
	dec := &decodeProcessor{fakeProcessor: &fakeProcessor{}}
	registerStageProcessor(t, w, "zzdecode", func() processor.Processor { return dec })
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"seal"}, "required", "plain")}
	cfg["processors"] = map[string]any{"seal": instance("zzdecode")}
	e := w.newEngine(t, cfg)

	res := e.PreflightResults(context.Background(), PreflightRequest{})
	found := false
	for _, r := range res {
		if r.Kind == "processor" && r.Instance == "seal" {
			found = true
			if !r.OK {
				t.Fatalf("the decode-checking processor failed: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("the processor has no preflight row: %+v", res)
	}
	if dec.decodes != 1 {
		t.Fatalf("CheckDecode ran %d times, want once", dec.decodes)
	}
}

// TestRunDiscoveriesSkipsADiscoveryWithNoRow: a discovery the row list does
// not carry contributes nothing — no Discover, no found sources.
func TestRunDiscoveriesSkipsADiscoveryWithNoRow(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	got := e.runDiscoveries(context.Background(), e.retry, []preflightRow{}, nil)
	if len(got) != 0 {
		t.Fatalf("runDiscoveries added %+v from a discovery with no row", got)
	}
	if n := w.discoveries["workloads"].discovers; n != 0 {
		t.Fatalf("Discover ran %d times for a discovery with no row", n)
	}
}

// TestInstanceTypeUnknownKind: a kind with no declaration has no type.
func TestInstanceTypeUnknownKind(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if got := e.instanceType("nope", "x"); got != "" {
		t.Fatalf("instanceType of an unknown kind = %q, want empty", got)
	}
	// A stage_source on a config without a stage section also has no type.
	if got := e.instanceType("stage_source", "x"); got != "" {
		t.Fatalf("instanceType(stage_source) without a stage = %q, want empty", got)
	}
}

// --- lease.go ----------------------------------------------------------------
