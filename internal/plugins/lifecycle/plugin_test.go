package lifecycle

import (
	"reflect"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// TestApplyPublishesTheSettingsType: the plugin registers its own settings, and
// the schema resolves plugins.config.lifecycle to Config's type.
func TestApplyPublishesTheSettingsType(t *testing.T) {
	factories := instances.NewRegistry()
	if err := Apply(loader.New(), factories); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	typ, ok := factories.ProviderConfigType(Spec.ID)
	if !ok {
		t.Fatalf("no settings type is registered under %q", Spec.ID)
	}
	if typ != reflect.TypeFor[Config]() {
		t.Fatalf("settings type = %v, want %v", typ, reflect.TypeFor[Config]())
	}
}
