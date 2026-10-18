package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// TestPluginsListAndDescribe: the listing names every registered type, the
// description carries the fields and the example of one.
func TestPluginsListAndDescribe(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "plugins")
	if code != 0 {
		t.Fatalf("plugins: exit %d\nstderr: %s", code, stderr)
	}
	for _, want := range []string{"source/postgres", "storage/yandex_disk", "processor/age", "catalog/kubernetes_configmap"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("listing does not name %s:\n%s", want, stdout)
		}
	}

	code, stdout, stderr = w.main(context.Background(), "plugins", "describe", "source/postgres")
	if code != 0 {
		t.Fatalf("plugins describe: exit %d\nstderr: %s", code, stderr)
	}
	for _, want := range []string{"pg_dump", "connect_interval", `"address": "postgres.example.svc.cluster.local:5432"`, "[secret]"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("description does not carry %q:\n%s", want, stdout)
		}
	}

	code, _, stderr = w.main(context.Background(), "plugins", "describe", "source/absent")
	if code != exitUsage || !strings.Contains(stderr, "source/postgres") {
		t.Fatalf("unknown type: exit %d, stderr %s", code, stderr)
	}
	code, _, _ = w.main(context.Background(), "plugins", "describe")
	if code != exitUsage {
		t.Fatalf("describe without an argument: exit %d", code)
	}
}

// TestSchemaDescribesEveryRegisteredType: the document is valid JSON, pins the
// version and offers one instance alternative per registered type of a kind.
func TestSchemaDescribesEveryRegisteredType(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "schema")
	if code != 0 {
		t.Fatalf("schema: exit %d\nstderr: %s", code, stderr)
	}
	if !jsontext.Value(stdout).IsValid() {
		t.Fatalf("schema output is not valid JSON:\n%s", stdout)
	}
	for _, want := range []string{
		`"$schema": "https://json-schema.org/draft/2020-12/schema"`,
		`"const": 1`,
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
	code := Main(context.Background(),
		Options{Program: "go-cluster-backup", Register: Generic, Defaults: [][]byte{defaults.Defaults}},
		[]string{"doctor"}, &stdout, &stderr)
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
	code = Main(context.Background(),
		Options{Program: "go-cluster-backup", Register: Generic, Defaults: [][]byte{defaults.Defaults}},
		[]string{"doctor"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("doctor with every binary present: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), filepath.Join(stub, "pg_dump")) {
		t.Errorf("doctor does not report where a binary was found:\n%s", stdout.String())
	}
}

// TestMainRejectsAnEmptyDistribution guards main's own contract.
func TestMainRejectsAnEmptyDistribution(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := Main(context.Background(), Options{}, nil, &stdout, &stderr); code != exitFailure {
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
	first, second := plugin.NewRegistry(), plugin.NewRegistry()
	if err := Generic(first); err != nil {
		t.Fatal(err)
	}
	if err := Generic(second); err != nil {
		t.Fatal(err)
	}
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

// TestGenericRefusesADuplicateRegistration: the registration list is checked by
// the registry it is handed, so a type already registered stops the build of the
// distribution rather than a run.
func TestGenericRefusesADuplicateRegistration(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := plugin.Register(reg, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type:    "local",
		Summary: "occupies the type the distribution registers",
		Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (storage.Storage, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := Generic(reg); err == nil {
		t.Fatal("a duplicate registration was accepted")
	}
}
