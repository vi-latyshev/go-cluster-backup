package all

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/cli"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// The command layer's pre-outcome exit codes (internal contract): a command
// that fails before an outcome has one of these, and the assertions below pin
// them exactly as the command suite did.
const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

// floorManifest is the tracked manifest the generator writes beside the
// composition root: the same plugin set, checked in, so a test can prove the
// generated Go and the manifest agree.
type floorManifest struct {
	Plugins []struct {
		ID string `json:"id"`
	} `json:"plugins"`
}

// readFloorManifest reads the tracked manifest from the package directory.
func readFloorManifest(t *testing.T) floorManifest {
	t.Helper()
	data, err := os.ReadFile("floor.json")
	if err != nil {
		t.Fatalf("read floor.json: %v", err)
	}
	var manifest floorManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode floor.json: %v", err)
	}
	if len(manifest.Plugins) == 0 {
		t.Fatal("floor.json lists no plugin")
	}
	return manifest
}

// command builds one command's compiled graph and factory registry together:
// the distribution, the CLI host and, when a test needs it, a test double. It
// is the same composition the production root performs.
func command(t *testing.T) (*loader.Registry, *instances.Registry) {
	t.Helper()
	lr := loader.New()
	factories := instances.NewRegistry()
	if err := Compose(lr, factories); err != nil {
		t.Fatalf("compose the distribution: %v", err)
	}
	return lr, factories
}

// commandWithDouble adds the `source/synthetic` test double to the compiled set,
// because a command's schema and selection read compiled descriptors: the host
// suite asserted the schema offers one alternative per registered source type,
// and the distribution's own set need not carry a test type.
func commandWithDouble(t *testing.T) (*loader.Registry, *instances.Registry) {
	t.Helper()
	lr, factories := command(t)
	if err := loader.Add(lr, loader.Spec{ID: "source/synthetic", Summary: "a source that dumps fixed bytes"}, func(lr *loader.Registry) error {
		return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
			Type:      "synthetic",
			Summary:   "a source that dumps fixed bytes",
			Stability: instances.Experimental,
			Example:   []byte(`{}`),
			New: func(context.Context, map[string]jsontext.Value, instances.Deps) (source.Source, error) {
				return syntheticSource{}, nil
			},
		})
	}); err != nil {
		t.Fatalf("register the test double: %v", err)
	}
	return lr, factories
}

// syntheticSource is the minimal source a schema alternative needs.
type syntheticSource struct{}

func (syntheticSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "test://synthetic"}, nil
}

func (syntheticSource) Dump(context.Context, io.Writer) error { return nil }

// main runs one command through the real command layer and captures its output.
func main(t *testing.T, lr *loader.Registry, factories *instances.Registry, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr strings.Builder
	code := cli.Main(context.Background(), cli.Options{
		Program:   cli.Program,
		Loader:    lr,
		Factories: factories,
		Defaults:  [][]byte{defaults.Defaults},
	}, args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestRegenerateInstallsEveryManifestPlugin proves the one registration entry
// point installs exactly the compiled distribution: every manifest row is
// registered into the loader and its factories into the factory registry.
func TestRegenerateInstallsEveryManifestPlugin(t *testing.T) {
	manifest := readFloorManifest(t)
	r := loader.New()
	factories := instances.NewRegistry()
	if err := Compose(r, factories); err != nil {
		t.Fatalf("compose: %v", err)
	}
	registered := make(map[string]bool, len(manifest.Plugins))
	for _, p := range loader.Plugins(r) {
		registered[p.ID] = true
	}
	for _, want := range manifest.Plugins {
		if !registered[want.ID] {
			t.Errorf("plugin %s is in floor.json but was not registered", want.ID)
		}
	}
	if len(registered) != len(manifest.Plugins) {
		t.Errorf("registered %d plugins, floor.json lists %d", len(registered), len(manifest.Plugins))
	}
	if got := len(factories.Descriptors()); got == 0 {
		t.Error("no factory descriptor reached the factory registry")
	}
}

// TestRegisterOwnsAFreshFactoryRegistry proves the production entry point takes
// no registry: it creates one and installs the whole distribution over it.
func TestRegisterOwnsAFreshFactoryRegistry(t *testing.T) {
	r := loader.New()
	if err := Register(r); err != nil {
		t.Fatalf("register: %v", err)
	}
	ids := make(map[string]bool, len(loader.Plugins(r)))
	for _, p := range loader.Plugins(r) {
		ids[p.ID] = true
	}
	for _, want := range []string{"cli", "workflow-backup", "reporting"} {
		if !ids[want] {
			t.Errorf("plugin %s was not registered", want)
		}
	}
}

// TestRegisterRefusesASecondRegistration proves the composition root owns one
// registry: a second call reports the import graph already set.
func TestRegisterRefusesASecondRegistration(t *testing.T) {
	r := loader.New()
	if err := Register(r); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := Register(r); err == nil {
		t.Fatal("a second registration was accepted")
	}
}

// TestRegisterReportsADuplicatePlugin proves a plugin already registered under
// a manifest id stops the build of the distribution.
func TestRegisterReportsADuplicatePlugin(t *testing.T) {
	manifest := readFloorManifest(t)
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: manifest.Plugins[0].ID, Summary: "probe"}, nil); err != nil {
		t.Fatalf("probe registration: %v", err)
	}
	if err := Register(r); err == nil {
		t.Fatal("a duplicate plugin was accepted")
	}
}

// TestPluginsListAndDescribe: the listing names every compiled type, the
// description carries the fields and the example of one.
func TestPluginsListAndDescribe(t *testing.T) {
	lr, factories := command(t)
	code, stdout, stderr := main(t, lr, factories, "plugins")
	if code != exitOK {
		t.Fatalf("plugins: exit %d\nstderr: %s", code, stderr)
	}
	for _, want := range []string{"source/postgres", "storage/yandex_disk", "processor/age", "catalog/kubernetes_configmap"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("listing does not name %s:\n%s", want, stdout)
		}
	}

	code, stdout, stderr = main(t, lr, factories, "plugins", "describe", "source/postgres")
	if code != exitOK {
		t.Fatalf("plugins describe: exit %d\nstderr: %s", code, stderr)
	}
	for _, want := range []string{"pg_dump", "connect_interval", `"address": "postgres.example.svc.cluster.local:5432"`, "[secret]"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("description does not carry %q:\n%s", want, stdout)
		}
	}

	code, _, stderr = main(t, lr, factories, "plugins", "describe", "source/absent")
	if code != exitUsage || !strings.Contains(stderr, "source/postgres") {
		t.Fatalf("unknown type: exit %d, stderr %s", code, stderr)
	}
	code, _, _ = main(t, lr, factories, "plugins", "describe")
	if code != exitUsage {
		t.Fatalf("describe without an argument: exit %d", code)
	}
}

// TestSchemaDescribesEveryRegisteredType: the document is valid JSON and offers
// one instance alternative per registered type of a kind.
func TestSchemaDescribesEveryRegisteredType(t *testing.T) {
	lr, factories := commandWithDouble(t)
	code, stdout, stderr := main(t, lr, factories, "schema")
	if code != exitOK {
		t.Fatalf("schema: exit %d\nstderr: %s", code, stderr)
	}
	if !jsontext.Value(stdout).IsValid() {
		t.Fatalf("schema output is not valid JSON:\n%s", stdout)
	}
	for _, want := range []string{
		`"$schema": "https://json-schema.org/draft/2020-12/schema"`,
		`"const": "postgres"`,
		`"const": "synthetic"`,
		`"const": "yandex_disk"`,
		`"const": "kubernetes_labels"`,
		`"x-secret": true`,
		`"oneOf"`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("schema does not carry %s", want)
		}
	}
}

// TestDoctorNamesEveryMissingBinary: a stub PATH proves the check can fail; the
// stub binaries then prove it can pass.
func TestDoctorNamesEveryMissingBinary(t *testing.T) {
	empty := t.TempDir()
	t.Setenv("PATH", empty)
	var stdout, stderr strings.Builder
	lr, factories := command(t)
	code := cli.Main(context.Background(), cli.Options{
		Program: cli.Program, Loader: lr, Factories: factories, Defaults: [][]byte{defaults.Defaults},
	}, []string{"doctor"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("doctor on an empty PATH: exit %d, want %d", code, exitFailure)
	}
	for _, missing := range []string{"pg_dump", "pg_restore", "psql", "redis-cli", "sqlite3"} {
		if !strings.Contains(stdout.String(), missing+": missing") {
			t.Errorf("doctor does not name %s missing:\n%s", missing, stdout.String())
		}
		if !strings.Contains(stderr.String(), missing) {
			t.Errorf("the failure does not name %s:\n%s", missing, stderr.String())
		}
	}

	stub := t.TempDir()
	for _, bin := range []string{"pg_dump", "pg_restore", "psql", "redis-cli", "sqlite3"} {
		if err := os.WriteFile(filepath.Join(stub, bin), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", stub)
	stdout.Reset()
	stderr.Reset()
	code = cli.Main(context.Background(), cli.Options{
		Program: cli.Program, Loader: lr, Factories: factories, Defaults: [][]byte{defaults.Defaults},
	}, []string{"doctor"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("doctor with every binary present: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), filepath.Join(stub, "pg_dump")) {
		t.Errorf("doctor does not report where a binary was found:\n%s", stdout.String())
	}
}

// TestMainRejectsAnEmptyDistribution guards the command layer's own contract.
func TestMainRejectsAnEmptyDistribution(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := cli.Main(context.Background(), cli.Options{}, nil, &stdout, &stderr); code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "Program") {
		t.Fatalf("the failure does not name the missing option: %s", stderr.String())
	}
}

// TestPluginsRegistrationIsDeterministic pins one property of the recipe: two
// registries built from the same distribution list the same types — a duplicate
// or a missing registration fails the build of the registry, not a run.
func TestPluginsRegistrationIsDeterministic(t *testing.T) {
	_, first := command(t)
	_, second := command(t)
	a, b := first.Descriptors(), second.Descriptors()
	if len(a) != len(b) {
		t.Fatalf("%d types against %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].Type != b[i].Type {
			t.Fatalf("descriptor %d: %s/%s against %s/%s", i, a[i].Kind, a[i].Type, b[i].Kind, b[i].Type)
		}
	}
}

// TestComposeReportsADuplicateHost proves the host half names the same loader as
// the hostless half: a host already registered stops the build of the graph.
func TestComposeReportsADuplicateHost(t *testing.T) {
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: "cli", Summary: "probe"}, nil); err != nil {
		t.Fatalf("probe registration: %v", err)
	}
	if err := Compose(r, instances.NewRegistry()); err == nil {
		t.Fatal("a duplicate host was accepted")
	}
}
