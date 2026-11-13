package instances_test

import (
	"context"
	"log/slog"
	"reflect"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// TestRestoreApplyCapabilityMatchesTheRestorer: a source type's config declares
// the offline restore.apply check exactly when a built instance of the type can
// restore. A missed checker would refuse a working block; a checker without the
// restorer would accept a block Apply then answers ErrUnsupported for. The pair
// is proven over the compiled distribution, one built example per type — the
// same surface doctor builds.
func TestRestoreApplyCapabilityMatchesTheRestorer(t *testing.T) {
	_, reg := everyPlugin(t)
	doc, err := yamlToJSON(t, defaults.Defaults)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := configuration.LoadDefaults(reg, []jsontext.Value{doc})
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, d := range reg.Descriptors() {
		if d.Kind != source.Kind.Name() || len(d.Example) == 0 {
			continue
		}
		ct, ok := reg.ConfigType(d.Kind, d.Type)
		if !ok {
			t.Fatalf("%s: no config type registered", d.ID)
		}
		_, hasChecker := reflect.New(ct).Interface().(source.ApplyOptionsChecker)
		merged, err := defs.DefaultsConfig(reg, d.Kind, d.Type, d.Example)
		if err != nil {
			t.Fatalf("%s: its defaults do not merge under the example: %v", d.ID, err)
		}
		inst, err := reg.BuildAny(context.Background(), d.Kind, d.Type, merged,
			instances.Deps{Instance: d.Type, Log: slog.New(slog.DiscardHandler)})
		if err != nil {
			t.Fatalf("%s: its own example does not build: %v", d.ID, err)
		}
		_, restores := inst.(source.Restorer)
		if hasChecker != restores {
			t.Errorf("%s: config implements ApplyOptionsChecker = %t, instance implements Restorer = %t; "+
				"the two declarations of one fact must agree", d.ID, hasChecker, restores)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no source type with an example was checked")
	}
}
