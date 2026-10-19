package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// withBarePlugin registers the world's plugins plus one that carries no example:
// doctor has nothing to build for it, so it is skipped rather than guessed at.
func (w *world) withBarePlugin(r *plugin.Registry) error {
	if err := w.register(r); err != nil {
		return err
	}
	return plugin.Register(r, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type:      "bare",
		Summary:   "a storage with no example at all",
		Stability: plugin.Experimental,
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
			return w.storage(deps.Instance), nil
		},
	})
}

// TestDoctorCoversEveryDescriptor: a type with no example is skipped, and a
// doctor whose answer cannot be written ends the command.
func TestDoctorCoversEveryDescriptor(t *testing.T) {
	w := newWorld()
	var stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Register: w.withBarePlugin, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
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
		Options{Program: "backup", Register: w.register, Defaults: [][]byte{doc}},
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
