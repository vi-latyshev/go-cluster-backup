package kubernetesconfigmap_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/kubernetesconfigmap"
)

const (
	testHostEnv = "GCB_TEST_HOST"
	testPortEnv = "GCB_TEST_PORT"

	namespace = "infrastructure"
	name      = "cluster-backup-catalog"
	key       = "catalog.json"
)

var base = time.Date(2026, 10, 10, 4, 20, 0, 0, time.UTC)

type configFixture struct {
	APIHostEnv      string `json:"api_host_env"`
	APIPortEnv      string `json:"api_port_env"`
	TokenFile       string `json:"token_file"`
	CAFile          string `json:"ca_file"`
	RequestTimeout  string `json:"request_timeout"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	Key             string `json:"key"`
	LeaseKeySuffix  string `json:"lease_key_suffix"`
	MaxRuns         int    `json:"max_runs"`
	MaxHistoryBytes int64  `json:"max_history_bytes"`
}

func registry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := kubernetesconfigmap.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func newCatalogIn(t *testing.T, srv *kubeapitest.Server, maxRuns int) catalog.Catalog {
	t.Helper()
	return newCatalogInBytes(t, srv, maxRuns, 512<<10)
}

func newCatalogInBytes(t *testing.T, srv *kubeapitest.Server, maxRuns int, maxBytes int64) catalog.Catalog {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s",
		Namespace:      namespace, Name: name, Key: key, LeaseKeySuffix: ".lease",
		MaxRuns: maxRuns, MaxHistoryBytes: maxBytes,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := plugin.Build(context.Background(), registry(t), catalog.Kind, "kubernetes_configmap",
		jsontext.Value(raw), plugin.Deps{Instance: "catalog", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func report(id string, at time.Time, outcome model.Outcome) model.RunReport {
	return model.RunReport{RunID: id, StartedAt: at, Outcome: outcome}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, registry(t))
}

func TestCatalogConformance(t *testing.T) {
	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		srv := kubeapitest.New(t)
		// The manifest creates the object; an empty history lives in it.
		srv.AddConfigMap(namespace, name, map[string]string{})
		return newCatalogIn(t, srv, 60)
	})
}

// TestAbsentConfigMap: reads of an object that does not exist yet see an empty
// history, and a save reports not_found — nothing here creates it.
func TestAbsentConfigMap(t *testing.T) {
	srv := kubeapitest.New(t)
	c := newCatalogIn(t, srv, 10)
	ctx := context.Background()

	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil || len(runs) != 0 {
		t.Fatalf("Load = %v, %v", runs, err)
	}
	if err := c.Forget(ctx, []string{"r1"}); err != nil {
		t.Fatalf("Forget = %v", err)
	}
	err = c.Save(ctx, report("r1", base, model.OutcomeRunning))
	if err == nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("Save = %v, want the missing ConfigMap named", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
	if srv.ConfigMapData(namespace, name) != nil {
		t.Fatal("Save created the ConfigMap")
	}
}

// TestSaveUpsertsAndTrims: the history keeps the newest max_runs entries, and
// the final outcome of a run replaces its own "running" entry.
func TestSaveUpsertsAndTrims(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 2)
	ctx := context.Background()

	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(ctx, report("r2", base.AddDate(0, 0, 1), model.OutcomeRunning)); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(ctx, report("r3", base.AddDate(0, 0, 2), model.OutcomeDelivered)); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(ctx, report("r2", base.AddDate(0, 0, 1), model.OutcomeDelivered)); err != nil {
		t.Fatal(err)
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].RunID != "r3" || runs[1].RunID != "r2" || runs[1].Outcome != model.OutcomeDelivered {
		t.Fatalf("stored runs = %+v", runs)
	}
	if raw := srv.ConfigMapData(namespace, name)[key]; raw == "" {
		t.Fatal("the history was not written under the configured key")
	}
}

// TestLoadSortsWhatItReads: entries are served newest first even when the
// stored order is not.
func TestLoadSortsWhatItReads(t *testing.T) {
	srv := kubeapitest.New(t)
	stored, err := json.Marshal([]model.RunReport{
		report("old", base, model.OutcomeDelivered),
		report("new", base.AddDate(0, 0, 5), model.OutcomeDelivered),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.AddConfigMap(namespace, name, map[string]string{key: string(stored)})
	c := newCatalogIn(t, srv, 10)

	runs, err := c.Load(context.Background(), catalog.LoadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].RunID != "new" || runs[1].RunID != "old" {
		t.Fatalf("Load = %+v", runs)
	}
	runs, err = c.Load(context.Background(), catalog.LoadRequest{Limit: 1})
	if err != nil || len(runs) != 1 || runs[0].RunID != "new" {
		t.Fatalf("limited Load = %+v, %v", runs, err)
	}
}

// TestConflictIsTransient: a stale resourceVersion comes back as a transient
// conflict; the engine's retry repeats the whole read-modify-write and the
// second attempt succeeds.
func TestConflictIsTransient(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 10)
	ctx := context.Background()

	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err != nil {
		t.Fatal(err)
	}
	// The next update loses the version race: someone wrote between this
	// Save's read and its write.
	srv.ConflictNextPut = true
	err := c.Save(ctx, report("r2", base.AddDate(0, 0, 1), model.OutcomeDelivered))
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("Save = %v, want a 409", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if err := c.Save(ctx, report("r2", base.AddDate(0, 0, 1), model.OutcomeDelivered)); err != nil {
		t.Fatalf("the retry after the conflict failed: %v", err)
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil || len(runs) != 2 {
		t.Fatalf("Load = %+v, %v", runs, err)
	}
}

// TestSizeGuard: a history over the limit fails with a message naming the
// limit and max_runs, instead of letting the API server refuse the write.
func TestSizeGuard(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 10)
	run := report("r1", base, model.OutcomeDelivered)
	run.Artifacts = nil
	err := c.Save(context.Background(), model.RunReport{
		RunID: run.RunID, StartedAt: run.StartedAt, Outcome: run.Outcome,
		Storages: []model.StorageResult{{Storage: "yandex", Error: strings.Repeat("x", 600<<10)}},
	})
	if err == nil || !strings.Contains(err.Error(), "max_runs or max_history_bytes") {
		t.Fatalf("Save = %v, want the size limit named", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if data := srv.ConfigMapData(namespace, name); len(data) != 0 {
		t.Fatalf("the oversized history was written: %v", data)
	}
}

// TestForbiddenIsPermanent: RBAC that refuses the ConfigMap fails the save and
// the preflight with a permanent error naming the status.
func TestForbiddenIsPermanent(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Deny("configmaps/" + namespace + "/" + name)
	c := newCatalogIn(t, srv, 10)
	ctx := context.Background()

	err := c.Save(ctx, report("r1", base, model.OutcomeDelivered))
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("Save = %v, want a 403", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	checker, ok := c.(plugin.Checker)
	if !ok {
		t.Fatal("the catalog does not implement plugin.Checker")
	}
	if err := checker.Check(ctx); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("Check = %v, want a 403", err)
	}
}

// TestCheckToleratesAnAbsentConfigMap: the first run legitimately finds the
// object missing, and preflight must not refuse to start because of it.
func TestCheckToleratesAnAbsentConfigMap(t *testing.T) {
	srv := kubeapitest.New(t)
	c := newCatalogIn(t, srv, 10)
	checker, ok := c.(plugin.Checker)
	if !ok {
		t.Fatal("the catalog does not implement plugin.Checker")
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("Check = %v", err)
	}
}

// TestCorruptHistoryIsRefused: bytes that are not a run history are a
// permanent error, not a silent wipe.
func TestCorruptHistoryIsRefused(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{key: "not json"})
	c := newCatalogIn(t, srv, 10)
	_, err := c.Load(context.Background(), catalog.LoadRequest{})
	if err == nil || !strings.Contains(err.Error(), key) {
		t.Fatalf("Load = %v, want the corrupt key named", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

func TestCancelledContext(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err == nil {
		t.Error("Save returned success on a cancelled context")
	}
	if _, err := c.Load(ctx, catalog.LoadRequest{}); err == nil {
		t.Error("Load returned success on a cancelled context")
	}
	if err := c.Forget(ctx, []string{"r1"}); err == nil {
		t.Error("Forget returned success on a cancelled context")
	}
}

func TestConfigValidation(t *testing.T) {
	base := func(mutate func(*configFixture)) jsontext.Value {
		cfg := configFixture{
			APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
			TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
			RequestTimeout: "10s", Namespace: namespace, Name: name, Key: key,
			MaxRuns: 10, MaxHistoryBytes: 512 << 10,
		}
		mutate(&cfg)
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return jsontext.Value(raw)
	}
	cases := map[string]struct {
		raw  jsontext.Value
		want string
	}{
		"relative token file":   {base(func(c *configFixture) { c.TokenFile = "token" }), "/token_file"},
		"empty key":             {base(func(c *configFixture) { c.Key = "" }), "/key is empty"},
		"empty name":            {base(func(c *configFixture) { c.Name = "" }), "/name is empty"},
		"zero max_runs":         {base(func(c *configFixture) { c.MaxRuns = 0 }), "/max_runs"},
		"zero history bound":    {base(func(c *configFixture) { c.MaxHistoryBytes = 0 }), "/max_history_bytes"},
		"history bound too big": {base(func(c *configFixture) { c.MaxHistoryBytes = 1 << 20 }), "ConfigMap object limit"},
		"zero timeout":          {base(func(c *configFixture) { c.RequestTimeout = "0s" }), "/request_timeout"},
	}
	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			err := registry(t).Check("catalog", "kubernetes_configmap", tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Check = %v, want a message containing %q", err, tc.want)
			}
		})
	}
}

// TestHistoryOverByteLimitDropsOldest: the byte bound trims the oldest entries
// until the history fits — it is a backstop under max_runs, and refusing the
// whole save would fail every later Save on an unchanged, still oversized
// history. What trimming cannot fix (the newest entry alone over the bound)
// stays a named permanent error and leaves the stored history untouched.
func TestHistoryOverByteLimitDropsOldest(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	ctx := context.Background()

	// The newest entry is the last-good one, which byte trimming protects; the
	// older, failed entry is what the backstop may drop.
	small := report("r1", base, model.OutcomeFailed)
	fat := report("r2", base.Add(time.Minute), model.OutcomeDelivered)
	fat.Storages = []model.StorageResult{{Storage: "yandex", Error: strings.Repeat("x", 400)}}
	smallRaw, err := json.Marshal([]model.RunReport{small})
	if err != nil {
		t.Fatal(err)
	}
	fatRaw, err := json.Marshal([]model.RunReport{fat})
	if err != nil {
		t.Fatal(err)
	}
	// The bound admits the fat run alone but not the small one beside it.
	c := newCatalogInBytes(t, srv, 10, int64(len(fatRaw)+8))
	if err := c.Save(ctx, small); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(ctx, fat); err != nil {
		t.Fatalf("Save with an over-bound history = %v, want the oldest entry dropped", err)
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].RunID != "r2" {
		t.Fatalf("stored runs = %+v, want r2 alone", runs)
	}
	if len(fatRaw) <= len(smallRaw) {
		t.Fatalf("the fixture sizes did not order as intended: small %d, fat %d", len(smallRaw), len(fatRaw))
	}

	huge := report("r3", base.Add(2*time.Minute), model.OutcomeDelivered)
	huge.Storages = []model.StorageResult{{Storage: "yandex", Error: strings.Repeat("x", 900)}}
	err = c.Save(ctx, huge)
	if err == nil || !strings.Contains(err.Error(), "the newest run alone is") {
		t.Fatalf("Save = %v, want the single-entry refusal", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if runs, _ = c.Load(ctx, catalog.LoadRequest{}); len(runs) != 1 || runs[0].RunID != "r2" {
		t.Fatalf("the refused save changed the history: %+v", runs)
	}
}

// TestLeaseLifecycle: the writer lease lives in a second key of the same
// ConfigMap, taken under the resourceVersion CAS. A held claim refuses; a claim
// is taken over only by a request carrying its exact token (the engine sets it
// after proving the holder terminated); Release with a foreign token is a no-op;
// and the run history in the other key is untouched by any of it.
func TestLeaseLifecycle(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 60)
	l, ok := c.(catalog.Leaser)
	if !ok {
		t.Fatal("the configmap catalog does not hold the writer lease")
	}
	ctx := context.Background()
	first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "20260101T040000Z", Kind: model.RunKindManual,
		HolderPod: "backup-a", HolderUID: "uid-a"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Token == "" || first.HolderPod != "backup-a" || first.HolderUID != "uid-a" {
		t.Fatalf("the claim does not record its identity: %+v", first)
	}
	// A held claim refuses, and names itself.
	held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "other", Kind: model.RunKindScheduled})
	if !errors.Is(err, catalog.ErrLeaseHeld) {
		t.Fatalf("a held claim was taken over: %v", err)
	}
	if held.Holder != first.Holder || held.Token != first.Token {
		t.Fatalf("the refusal does not name the holder: %+v", held)
	}
	// The wrong takeover token is refused: the engine must prove THIS claim.
	if _, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "other", Kind: model.RunKindScheduled,
		TakeoverToken: "not-the-token"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		t.Fatalf("a foreign takeover token was accepted: %v", err)
	}
	// The exact token takes it over.
	taken, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "20260101T050000Z", Kind: model.RunKindScheduled,
		TakeoverToken: first.Token})
	if err != nil {
		t.Fatalf("a proven-dead claim was not taken over: %v", err)
	}
	if taken.Holder != "20260101T050000Z" || taken.Token == first.Token {
		t.Fatalf("takeover returned %+v", taken)
	}
	// Release with the stale token is a no-op; the new holder's claim survives.
	if err := l.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	if held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "third", Kind: model.RunKindScheduled}); !errors.Is(err, catalog.ErrLeaseHeld) || held.Holder != taken.Holder {
		t.Fatalf("a stale release dropped the new holder's claim: %+v, %v", held, err)
	}
	// The run history in the other key is untouched by any of this.
	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err != nil {
		t.Fatal(err)
	}
	if runs, err := c.Load(ctx, catalog.LoadRequest{}); err != nil || len(runs) != 1 {
		t.Fatalf("the run history was disturbed by the lease: %+v, %v", runs, err)
	}
	// The holder's own release drops the claim; the next acquire takes it.
	if err := l.Release(ctx, taken); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "next", Kind: model.RunKindManual}); err != nil {
		t.Fatalf("a released claim was not takeable: %v", err)
	}
}

// TestHolderStateProvesTermination: only a pod found with the recorded UID in
// phase Succeeded/Failed is DEFINITIVE termination. An absent pod or a different
// UID is proof only of the object being gone — a forced deletion can produce it
// while the process still runs — so it is reported non-definitively and the
// engine applies its hard bound. A running or pending pod, a partial identity
// and a failed read are all "not terminated".
func TestHolderStateProvesTermination(t *testing.T) {
	cases := []struct {
		name       string
		pod        string
		namespace  string
		uid        string
		terminated bool
		definitive bool
	}{
		{"absent pod", "", "infrastructure", "uid-a", true, false},
		{"recreated pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-new"},"status":{"phase":"Running"}}`, "infrastructure", "uid-a", true, false},
		{"succeeded pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Succeeded"}}`, "infrastructure", "uid-a", true, true},
		{"failed pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Failed"}}`, "infrastructure", "uid-a", true, true},
		{"running pod with a terminated container status", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Running","containerStatuses":[{"state":{"terminated":{}}}]}}`, "infrastructure", "uid-a", false, false},
		{"pending pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Pending"}}`, "infrastructure", "uid-a", false, false},
		{"a different namespace finds no pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Succeeded"}}`, "other", "uid-a", true, false},
		{"partial identity", "", "infrastructure", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			srv.AddConfigMap(namespace, name, map[string]string{})
			if tc.pod != "" {
				srv.AddPod(tc.pod)
			}
			l := newCatalogIn(t, srv, 60).(catalog.Liveness)
			terminated, definitive, err := l.HolderState(context.Background(), catalog.Lease{
				Holder: "h", HolderNamespace: tc.namespace, HolderPod: "backup-a", HolderUID: tc.uid})
			if err != nil {
				t.Fatalf("HolderState: %v", err)
			}
			if terminated != tc.terminated || definitive != tc.definitive {
				t.Fatalf("HolderState = (%v, %v), want (%v, %v)", terminated, definitive, tc.terminated, tc.definitive)
			}
		})
	}
	// A read that fails is not a verdict.
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	srv.Fail("pods", 500)
	l := newCatalogIn(t, srv, 60).(catalog.Liveness)
	if terminated, _, err := l.HolderState(context.Background(), catalog.Lease{
		Holder: "h", HolderNamespace: "infrastructure", HolderPod: "backup-a", HolderUID: "uid-a"}); terminated || err == nil {
		t.Fatalf("a failed liveness read = %v, %v; want not terminated with an error", terminated, err)
	}
}

// TestMaxRunsIsPerKind (C7): max_runs applies to each kind's own window, so a
// manual flood cannot evict the scheduled history the nightly health reads.
func TestMaxRunsIsPerKind(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 2)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := c.Save(ctx, report(fmt.Sprintf("s%d", i), base.Add(time.Duration(i)*time.Minute), model.OutcomeDelivered)); err != nil {
			t.Fatal(err)
		}
		manual := report(fmt.Sprintf("m%d-manual", i), base.Add(time.Duration(i)*time.Minute+30*time.Second), model.OutcomeDelivered)
		manual.Kind = model.RunKindManual
		if err := c.Save(ctx, manual); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	count := map[model.RunKind]int{}
	for _, r := range runs {
		count[model.NormalizeKind(r.Kind)]++
	}
	if count[model.RunKindScheduled] != 2 || count[model.RunKindManual] != 2 {
		t.Fatalf("windows = %v, want 2 scheduled and 2 manual", count)
	}
	seen := map[string]bool{}
	for _, r := range runs {
		seen[r.RunID] = true
	}
	for _, id := range []string{"s3", "s2", "m3-manual", "m2-manual"} {
		if !seen[id] {
			t.Fatalf("%s was evicted from its own kind's window: %v", id, seen)
		}
	}
}

// TestByteTrimDropsManualFirstAndProtectsScheduledHealth (C7): the byte backstop
// sheds extra manual entries first, and never the newest scheduled running or
// last-good record.
func TestByteTrimDropsManualFirstAndProtectsScheduledHealth(t *testing.T) {
	ctx := context.Background()
	running := report("running", base.Add(4*time.Minute), model.OutcomeRunning)
	lastGood := report("lastgood", base.Add(2*time.Minute), model.OutcomeDelivered)
	older := report("older", base.Add(time.Minute), model.OutcomeDelivered)
	manualRun := report("manual", base.Add(3*time.Minute), model.OutcomeDelivered)
	manualRun.RunID = "manual-manual"
	manualRun.Kind = model.RunKindManual

	// Newest first: the newest entry (the running one) always stays; the manual
	// entry sits behind it and is what the backstop sheds.
	base3 := []model.RunReport{running, manualRun, lastGood, older}
	full, err := json.Marshal(base3)
	if err != nil {
		t.Fatal(err)
	}
	// A bound that fits the scheduled trio and the newest manual? We size it so
	// the manual entry has to go and the scheduled trio fits.
	withoutManual, err := json.Marshal([]model.RunReport{running, lastGood, older})
	if err != nil {
		t.Fatal(err)
	}
	bound := int64(len(withoutManual) + 8)
	if int64(len(full)) <= bound {
		t.Fatalf("the fixture does not overflow: full %d, bound %d", len(full), bound)
	}

	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogInBytes(t, srv, 10, bound)
	for _, r := range []model.RunReport{older, lastGood, manualRun, running} {
		if err := c.Save(ctx, r); err != nil {
			t.Fatalf("Save %s: %v", r.RunID, err)
		}
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range runs {
		seen[r.RunID] = true
	}
	if seen["manual-manual"] {
		t.Fatalf("the manual entry survived the byte backstop: %v", seen)
	}
	if !seen["running"] || !seen["lastgood"] {
		t.Fatalf("the byte backstop dropped a scheduled health record: %v", seen)
	}
}
