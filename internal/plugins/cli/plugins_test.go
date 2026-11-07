package cli

import (
	"context"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// probeRegistry is a compiled registry with one descriptor and one default
// role, the smallest build `plugins` and `plugins check` can read.
func probeRegistry(t *testing.T) *loader.Registry {
	t.Helper()
	lr := loader.New()
	if err := loader.Add(lr, loader.Spec{ID: "source/probe", Summary: "a probe"}, func(lr *loader.Registry) error {
		if err := loader.Register(lr, instances.Descriptors, instances.Descriptor{
			ID: "source/probe", Kind: "source", Type: "probe", Summary: "a probe", Stability: instances.Experimental,
		}); err != nil {
			return err
		}
		return loader.Role(lr, loader.RoleSpec{
			Name: "cli", Default: true, Diagnostic: true,
			Run:      func(context.Context, loader.Session) int { return exitOK },
			Disabled: func(loader.Session) int { return exitFailure },
		})
	}); err != nil {
		t.Fatal(err)
	}
	return lr
}

// TestPluginsReadsTheCompiledSchema: the listing and `describe` read the
// loader's compiled descriptors, so a plugin the selection disables keeps its
// schema, its fields and its example — a disabled backend must be documentable.
func TestPluginsReadsTheCompiledSchema(t *testing.T) {
	lr := probeRegistry(t)
	loader.ApplySelection(lr, []string{"source/probe"})
	if loader.Active(lr, "source/probe") {
		t.Fatal("the probe was not disabled")
	}
	var stdout, stderr strings.Builder
	opts := Options{Program: "backup", Loader: lr}
	if code := pluginsCommand(opts, instances.NewRegistry(), nil, nil, &stdout, &stderr); code != exitOK {
		t.Fatalf("plugins: exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "source/probe") {
		t.Errorf("a disabled plugin's descriptor is missing:\n%s", stdout.String())
	}
	stdout.Reset()
	if code := pluginsCommand(opts, instances.NewRegistry(), nil, []string{"describe", "source/probe"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("plugins describe: exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "source/probe (experimental)") {
		t.Errorf("describe does not read the compiled descriptor:\n%s", stdout.String())
	}
}

// TestC6_SelectionDiagnosticsResolveAgainstTheCompiledSet: the diagnostics use
// the plugin-owned resolver — the loader's compiled IDs and its bundle table —
// never a private compiled list with an empty bundle table, and a build without
// the loader registry is refused instead of treating everything as active.
func TestC6_SelectionDiagnosticsResolveAgainstTheCompiledSet(t *testing.T) {
	lr := loader.New()
	reg := instances.NewRegistry()
	for _, id := range []string{"storage/s3", "storage/yandex_disk"} {
		if err := loader.Add(lr, loader.Spec{ID: id, Summary: id}, nil); err != nil {
			t.Fatal(err)
		}
	}
	knownBundle := false
	for _, b := range loader.All(lr, configuration.Bundles) {
		if b.Name == configuration.LocalOnly().Name {
			knownBundle = true
		}
	}
	if !knownBundle {
		if err := loader.Add(lr, loader.Spec{ID: "test/bundles"}, func(lr *loader.Registry) error {
			return configuration.AddBundle(lr, configuration.LocalOnly())
		}); err != nil {
			t.Fatal(err)
		}
	}
	opts := Options{Program: "backup", Loader: lr}
	local := &configuration.Config{Plugins: &configuration.Plugins{Bundles: []string{configuration.LocalOnly().Name}}}
	for _, err := range configDiagnostics(opts, reg, local) {
		if strings.Contains(err.Error(), "unknown bundle") {
			t.Fatalf("the compiled bundle table is not read: %v", err)
		}
	}
	ghost := &configuration.Config{Plugins: &configuration.Plugins{Disabled: []string{"source/ghost"}}}
	found := false
	for _, err := range configDiagnostics(opts, reg, ghost) {
		if strings.Contains(err.Error(), `unknown plugin "source/ghost"`) {
			found = true
		}
	}
	if !found {
		t.Error("an unknown disabled plugin was not reported")
	}
	// A present section needs its role: whether a provider survives is the
	// loader's answer, not the compiled list's.
	withRun := &configuration.Config{Run: &configuration.RunConfig{}}
	found = false
	for _, err := range configDiagnostics(opts, reg, withRun) {
		if strings.Contains(err.Error(), `needs the "backup-job" role`) {
			found = true
		}
	}
	if providers := configuration.RolesFrom(lr)[configuration.RoleBackup]; len(providers) == 0 && !found {
		t.Error("a run section without an active backup-job provider was accepted")
	}
	if errs := configDiagnostics(Options{}, reg, &configuration.Config{}); len(errs) == 0 {
		t.Error("a build without the loader registry was not refused")
	}
}

// TestPluginsCheckRunsTheSharedValidator: a healthy build answers one ok line;
// a broken effective dependency is the validator's to report, and it prints
// every problem with a non-zero exit.
func TestPluginsCheckRunsTheSharedValidator(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := pluginsCommand(Options{Program: "backup", Loader: probeRegistry(t)}, instances.NewRegistry(), nil,
		[]string{"check"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("plugins check: exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "plugins: ok (1 compiled)") {
		t.Errorf("the healthy answer does not count the compiled plugins:\n%s", stdout.String())
	}

	lr := probeRegistry(t)
	if err := loader.Add(lr, loader.Spec{ID: "probe/broken", Requires: []string{"probe/ghost"}}, nil); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := pluginsCommand(Options{Program: "backup", Loader: lr}, instances.NewRegistry(), nil,
		[]string{"check"}, &stdout, &stderr); code != exitFailure {
		t.Fatalf("a broken dependency: exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), `unregistered plugin "probe/ghost"`) {
		t.Errorf("the validator's problem is not printed: %s", stderr.String())
	}

	if code := pluginsCommand(Options{Program: "backup"}, instances.NewRegistry(), nil,
		[]string{"check", "extra"}, &stdout, &stderr); code != exitUsage {
		t.Errorf("plugins check accepted an argument: exit %d", code)
	}

	// A build whose composition root has not threaded the loader has no compiled
	// graph to judge; the command still answers.
	stdout.Reset()
	stderr.Reset()
	if code := pluginsCommand(Options{Program: "backup"}, instances.NewRegistry(), nil,
		[]string{"check"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("plugins check without a loader: exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "plugins: ok") {
		t.Errorf("the unthreaded answer is not printed: %s", stdout.String())
	}
	if code := pluginsCommand(Options{Program: "backup", Loader: probeRegistry(t)}, instances.NewRegistry(), nil,
		[]string{"check", "-h"}, &stdout, &stderr); code != exitOK {
		t.Errorf("plugins check -h: exit %d", code)
	}
}

// TestWriteFieldsMarksEveryClassification: a struct member's children carry the
// classification, a member the document fills carries its default, and one the
// deployment states is required.
func TestWriteFieldsMarksEveryClassification(t *testing.T) {
	fields := []instances.Field{
		{Name: "nested", Type: "struct", Doc: "a struct", Fields: []instances.Field{
			{Name: "inner", Type: "string", Required: true, Doc: "required"},
		}},
		{Name: "defaulted", Type: "string", Doc: "defaulted"},
		{Name: "stated", Type: "string", Required: true, Doc: "required"},
		{Name: "opaque", Type: "string", Required: true, Secret: true, Doc: "a secret"},
	}
	var out strings.Builder
	writeFields(&errWriter{w: &out}, fields, jsontext.Value(`{"nested":{},"defaulted":"v"}`), 1)
	text := out.String()
	for _, want := range []string{
		"nested struct", "  inner string [required]", `defaulted string [default: "v"]`,
		"stated string [required]", "opaque string [secret] [required]",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("writeFields does not carry %q:\n%s", want, text)
		}
	}
}

// TestWriteDescriptorRefusesAnExampleItCannotIndent: a descriptor handed an
// example that is not JSON is reported, never printed raw.
func TestWriteDescriptorRefusesAnExampleItCannotIndent(t *testing.T) {
	var stdout, stderr strings.Builder
	d := instances.Descriptor{ID: "source/probe", Kind: "source", Type: "probe", Stability: instances.Experimental,
		Summary: "a probe", Example: jsontext.Value("{not json")}
	if code := writeDescriptor(&stdout, &stderr, "backup", nil, d); code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "example") {
		t.Errorf("the failure does not name the example: %s", stderr.String())
	}
}

// TestPluginsDescribeRefusesBadArguments: the description takes exactly one
// KIND/TYPE and names the known types when it does not know the one asked for.
func TestPluginsDescribeRefusesBadArguments(t *testing.T) {
	w := newWorld()
	code, _, stderr := w.main(context.Background(), "plugins", "describe")
	if code != exitUsage || !strings.Contains(stderr, "exactly one KIND/TYPE") {
		t.Fatalf("no argument: exit %d, stderr %s", code, stderr)
	}
	code, _, stderr = w.main(context.Background(), "plugins", "describe", "source/absent")
	if code != exitUsage || !strings.Contains(stderr, `unknown plugin "source/absent"`) {
		t.Fatalf("unknown type: exit %d, stderr %s", code, stderr)
	}
}
