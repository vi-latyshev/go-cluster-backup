package kubernetesconfigmap_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/kubernetesconfigmap"
)

// farFuture is a year the JSON encoder cannot render: the only value that makes
// an otherwise infallible Marshal of a lease or a run history fail.
var farFuture = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)

func buildCatalog(t *testing.T, cfg configFixture) catalog.Catalog {
	t.Helper()
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

func leaseConfig(hostEnv, portEnv, tokenFile, caFile string) configFixture {
	return configFixture{
		APIHostEnv: hostEnv, APIPortEnv: portEnv, TokenFile: tokenFile, CAFile: caFile,
		RequestTimeout: "10s", Namespace: namespace, Name: name, Key: key, LeaseKeySuffix: ".lease",
		MaxRuns: 10, MaxHistoryBytes: 512 << 10,
	}
}

// TestNewToleratesAMissingLoggerAndRefusesACancelledContext: the constructor is
// the only place the plugin may be handed no logger, and a cancelled context
// stops it before it builds anything.
func TestNewToleratesAMissingLoggerAndRefusesACancelledContext(t *testing.T) {
	cfg := kubernetesconfigmap.Config{
		APIHostEnv: "GCB_HOST", APIPortEnv: "GCB_PORT", TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
		RequestTimeout: model.Duration(10 * time.Second), Namespace: namespace, Name: name, Key: key,
		LeaseKeySuffix: ".lease", MaxRuns: 10, MaxHistoryBytes: 512 << 10,
	}
	c, err := kubernetesconfigmap.New(context.Background(), cfg, plugin.Deps{})
	if err != nil || c == nil {
		t.Fatalf("New without a logger = %v, %v", c, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := kubernetesconfigmap.New(ctx, cfg, plugin.Deps{}); err == nil {
		t.Fatal("New built a catalog on a cancelled context")
	}
}

// TestValidateNamesEveryMissingField: the fields the schema cannot express as a
// JSON shape are reported one by one.
func TestValidateNamesEveryMissingField(t *testing.T) {
	cfg := kubernetesconfigmap.Config{
		TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
		RequestTimeout: model.Duration(10 * time.Second), Key: key,
		LeaseKeySuffix: ".lease", MaxRuns: 10, MaxHistoryBytes: 512 << 10,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("a config with no host, port or namespace validated")
	}
	for _, want := range []string{"/api_host_env is empty", "/api_port_env is empty", "/namespace is empty"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate = %v, want a message containing %q", err, want)
		}
	}
}

// TestAClientThatCannotBeBuiltIsPermanent: every read path reports the same
// construction failure, so a misconfigured deployment fails loudly instead of
// retrying a name that will never resolve.
func TestAClientThatCannotBeBuiltIsPermanent(t *testing.T) {
	const unsetHostEnv = "GCB_COVERAGE_UNSET_HOST"
	t.Setenv(unsetHostEnv, "")
	cfg := leaseConfig(unsetHostEnv, testPortEnv, "/var/run/token", "/var/run/ca.crt")
	c := buildCatalog(t, cfg)
	ctx := context.Background()

	failures := map[string]func() error{
		"Load":   func() error { _, err := c.Load(ctx, catalog.LoadRequest{}); return err },
		"Save":   func() error { return c.Save(ctx, report("r1", base, model.OutcomeDelivered)) },
		"Forget": func() error { return c.Forget(ctx, []string{"r1"}) },
		"Check":  func() error { return c.(plugin.Checker).Check(ctx) },
		"Acquire": func() error {
			_, err := c.(catalog.Leaser).Acquire(ctx, catalog.LeaseRequest{Holder: "run"})
			return err
		},
		"Release": func() error { return c.(catalog.Leaser).Release(ctx, catalog.Lease{Token: "t"}) },
		"HolderState": func() error {
			_, _, err := c.(catalog.Liveness).HolderState(ctx, catalog.Lease{
				HolderNamespace: namespace, HolderPod: "backup-a", HolderUID: "uid-a"})
			return err
		},
		"PreviousJobFailure": func() error {
			_, err := c.(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-a")
			return err
		},
	}
	for name, call := range failures {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatal("a client that cannot be built was not reported")
			}
			if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
				t.Fatalf("class = %q, want permanent: %v", class, err)
			}
		})
	}
}

// TestForgetSurfacesAReadFailure: a read the plugin cannot make is reported, so
// the engine retries it instead of believing the runs were forgotten.
func TestForgetSurfacesAReadFailure(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	srv.Deny("configmaps/" + namespace + "/" + name)
	if err := newCatalogIn(t, srv, 10).Forget(context.Background(), []string{"r1"}); err == nil {
		t.Fatal("a denied read was reported as a successful Forget")
	}
}

// TestSaveRefusesARunTheHistoryCannotEncode: a run whose own timestamp cannot be
// rendered is a named permanent error, not a silent drop from the history.
func TestSaveRefusesARunTheHistoryCannotEncode(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv, 10)
	err := c.Save(context.Background(), report("r1", farFuture, model.OutcomeDelivered))
	if err == nil || !strings.Contains(err.Error(), "encode run history") {
		t.Fatalf("Save = %v, want the encode refusal", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestSaveRefusesWhenTheByteBoundCanShedNothing: the byte backstop drops what it
// may — extra manual entries first, then older scheduled ones — and when every
// entry left is one it must keep, the save is refused and the history is left
// exactly as it was.
func TestSaveRefusesWhenTheByteBoundCanShedNothing(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	running := report("running", base, model.OutcomeRunning)
	lastGood := report("lastgood", base.Add(time.Minute), model.OutcomeDelivered)
	single, err := json.Marshal([]model.RunReport{running})
	if err != nil {
		t.Fatal(err)
	}
	both, err := json.Marshal([]model.RunReport{lastGood, running})
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(both)) <= int64(len(single)) {
		t.Fatalf("the fixture does not overflow the bound: %d then %d", len(single), len(both))
	}
	c := newCatalogInBytes(t, srv, 10, int64(len(single)))
	ctx := context.Background()

	if err := c.Save(ctx, running); err != nil {
		t.Fatalf("Save %s: %v", running.RunID, err)
	}
	err = c.Save(ctx, lastGood)
	if err == nil || !strings.Contains(err.Error(), "the newest run alone is") {
		t.Fatalf("Save = %v, want the refusal naming the byte limit", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{})
	if err != nil || len(runs) != 1 || runs[0].RunID != "running" {
		t.Fatalf("the refused save changed the history: %+v, %v", runs, err)
	}
}
