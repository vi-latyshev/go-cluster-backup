package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// TestDoctorRunsTheSharedValidator: doctor reads the same validator as
// `plugins check` and `validate`, so a compiled graph whose effective
// dependencies are broken fails the image build before any binary is looked up.
func TestDoctorRunsTheSharedValidator(t *testing.T) {
	lr := loader.New()
	if err := loader.Add(lr, loader.Spec{ID: "probe/broken", Requires: []string{"probe/ghost"}}, nil); err != nil {
		t.Fatal(err)
	}
	w := newWorld()
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}, Loader: lr},
		[]string{"doctor"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stderr.String())
	}
	if !strings.Contains(stderr.String(), `unregistered plugin "probe/ghost"`) {
		t.Errorf("the shared validator's problem is not printed: %s", stderr.String())
	}
}

// withBarePlugin registers the world's plugins plus one that carries no example:
// doctor has nothing to build for it, so it is skipped rather than guessed at.
func (w *world) withBarePlugin(r *instances.Registry) error {
	if err := w.register(r); err != nil {
		return err
	}
	return storage.Register(r, storage.FactorySpec[map[string]jsontext.Value]{
		Type:      "bare",
		Summary:   "a storage with no example at all",
		Stability: instances.Experimental,
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			return w.storage("bare"), nil
		},
	})
}

// bareFactories is the factory registry of the world plus the bare plugin, the
// shape Options.Factories takes.
func (w *world) bareFactories() *instances.Registry {
	r := instances.NewRegistry()
	if err := w.withBarePlugin(r); err != nil {
		panic(err)
	}
	return r
}

// TestDoctorCoversEveryDescriptor: a type with no example is skipped, and a
// doctor whose answer cannot be written ends the command.
func TestDoctorCoversEveryDescriptor(t *testing.T) {
	w := newWorld()
	var stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.bareFactories(), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, failWriter{}, &stderr)
	if code != exitFailure || !strings.Contains(stderr.String(), "stream is broken") {
		t.Fatalf("exit %d, stderr %s", code, stderr.String())
	}
}

// TestDoctorBuildsEveryPluginWithItsDefaults: the startup check reads a
// configuration value by its Go type, and an enum a plugin validates in its own
// constructor is a plain string there — so a bad one in a defaults document
// would pass every command until the night it is used. `doctor` closes that:
// every registered type's own complete example is merged with the block its
// distribution gives it and built, which is the image build's last step.
//
// The document below is the module's own, with one value changed: the union
// refuses two documents setting one key, and this keeps the test's document the
// only source of the field it breaks.
func TestDoctorBuildsEveryPluginWithItsDefaults(t *testing.T) {
	const broken = "    body_format: yaml"
	if !strings.Contains(string(defaults.Defaults), broken) {
		t.Fatalf("the module's document no longer carries %q", broken)
	}
	doc := []byte(strings.Replace(string(defaults.Defaults), broken, "    body_format: json", 1))
	w := newWorld()
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Defaults: [][]byte{doc}},
		[]string{"doctor"}, &stdout, &stderr)
	if code == exitOK {
		t.Fatalf("a defaults document with an enum no plugin accepts passed doctor:\n%s", stdout.String())
	}
	// The document itself is well-formed: the value is a string, and only the
	// plugin that decodes it knows the accepted set.
	if strings.Contains(stderr.String(), "must be a JSON") || strings.Contains(stderr.String(), "unknown key") {
		t.Fatalf("the refusal came from the document check, not from building the plugin:\n%s", stderr.String())
	}
	for _, want := range []string{"notifier/webhook", "json"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the refusal does not name %q: %s", want, stderr.String())
		}
	}
}

// closingStorage is a storage instance that records every Close call: the raw
// instance stays behind the guarded destination, which the invocation owner
// closes exactly once.
type closingStorage struct {
	*storagetest.Fake
	closes *int
}

func (c closingStorage) Close() error {
	*c.closes = *c.closes + 1
	return nil
}

// closingFactories is the world plus a storage whose raw instance is a closer,
// the shape Options.Factories takes.
func (w *world) closingFactories(closes *int) *instances.Registry {
	r := instances.NewRegistry()
	if err := w.register(r); err != nil {
		panic(err)
	}
	if err := storage.Register(r, storage.FactorySpec[map[string]jsontext.Value]{
		Type:      "closing",
		Summary:   "a storage whose raw instance is closed if a consumer ever holds it",
		Stability: instances.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			return closingStorage{Fake: w.storage("closing"), closes: closes}, nil
		},
	}); err != nil {
		panic(err)
	}
	return r
}

// TestC2_DoctorClosesThroughTheGuardedDestination: doctor constructs a storage
// example through the guarded builder, keeps its read port for the answer and
// closes the destination through the storage boundary exactly once; the raw
// instance (here one that records Close calls) never reaches the command layer.
func TestC2_DoctorClosesThroughTheGuardedDestination(t *testing.T) {
	w := newWorld()
	closes := 0
	var stdout, stderr bytes.Buffer
	Main(context.Background(),
		Options{Program: "backup", Factories: w.closingFactories(&closes), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, &stdout, &stderr)
	if closes != 1 {
		t.Errorf("constructed storage closed %d times, want 1", closes)
	}
	if !strings.Contains(stdout.String(), "storage examples:") {
		t.Errorf("doctor does not report the storage examples it built:\n%s", stdout.String())
	}
}

// TestDoctorRefusesAStorageWhoseExampleFails: a storage example that cannot be
// built under the defaults stops the image, named by its factory ID.
func TestDoctorRefusesAStorageWhoseExampleFails(t *testing.T) {
	w := newWorld()
	r := instances.NewRegistry()
	if err := w.register(r); err != nil {
		panic(err)
	}
	if err := storage.Register(r, storage.FactorySpec[map[string]jsontext.Value]{
		Type:      "failing",
		Summary:   "a storage that refuses its own example",
		Stability: instances.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			return nil, errors.New("this storage refuses every configuration")
		},
	}); err != nil {
		panic(err)
	}
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Factories: r, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stdout.String())
	}
	if !strings.Contains(stderr.String(), "storage/failing") {
		t.Errorf("the failed storage example is not named by its factory ID: %s", stderr.String())
	}
}

// failingCloseStorage is a constructed storage whose Close refuses: doctor
// reports the release failure rather than hiding it.
type failingCloseStorage struct {
	*storagetest.Fake
}

func (f failingCloseStorage) Close() error {
	return errors.New("this storage cannot release its handle")
}

// TestDoctorReportsAStorageItCannotClose: closing a constructed example goes
// through the guarded destination, and a refused release is a diagnostic of the
// build check.
func TestDoctorReportsAStorageItCannotClose(t *testing.T) {
	w := newWorld()
	r := instances.NewRegistry()
	if err := w.register(r); err != nil {
		panic(err)
	}
	if err := storage.Register(r, storage.FactorySpec[map[string]jsontext.Value]{
		Type:      "close_fails",
		Summary:   "a storage that cannot release its handle",
		Stability: instances.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			return failingCloseStorage{Fake: w.storage("close_fails")}, nil
		},
	}); err != nil {
		panic(err)
	}
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Factories: r, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\n%s", code, exitFailure, stdout.String())
	}
	for _, want := range []string{"storage/close_fails", "closing its example"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("the close failure does not carry %q: %s", want, stderr.String())
		}
	}
}

// TestDoctorSucceedsWhenEveryBinaryIsOnPath: a complete stub PATH satisfies
// every declared binary, so the build check answers ok and the command exits
// zero.
func TestDoctorSucceedsWhenEveryBinaryIsOnPath(t *testing.T) {
	w := newWorld()
	reg := w.factories()
	binaries := map[string]bool{}
	for _, d := range reg.Descriptors() {
		for _, bin := range d.Binaries {
			binaries[bin] = true
		}
	}
	dir := t.TempDir()
	for bin := range binaries {
		if err := os.WriteFile(filepath.Join(dir, bin), nil, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The PostgreSQL clients are a configured suite, not a metadata lookup:
	// doctor proves them locally, so the stub carries all four of one major and
	// each answers --version. redis-cli and sqlite3 are only looked up.
	for _, bin := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
		script := "#!/bin/sh\necho \"" + bin + " (PostgreSQL) 18.6\"\n"
		if err := os.WriteFile(filepath.Join(dir, bin), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(),
		Options{Program: "backup", Factories: reg, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("exit %d, want %d\n%s%s", code, exitOK, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "storage examples:") {
		t.Errorf("doctor did not report the storage examples it built:\n%s", stdout.String())
	}
}
