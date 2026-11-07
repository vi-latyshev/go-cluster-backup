package instances

import (
	"context"
	"fmt"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// Descriptors is the compiled schema view of every factory. The kind is a
// schema kind: All returns a descriptor whether its plugin is active or
// disabled, so config merge, shape validation, `plugins describe`, `schema` and
// `config effective` never depend on the deployment's selection.
var Descriptors = loader.NewKind[Descriptor]("instances.descriptors", loader.KindOptions[Descriptor]{Schema: true})

// AddFactory registers one factory and contributes its descriptor to the
// compiled schema in one call — the whole body of a backend's loader.Add apply:
//
//	loader.Add(r, postgres.Spec, func(lr *loader.Registry) error {
//		return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{...})
//	})
//
// factories is the composition root's one shared Registry: every compiled
// factory lives there whatever the selection, which is what CheckShape,
// ConfigType and Build read. A registration error aborts loader.Add, so a
// half-registered process is never started.
func AddFactory[C, T any](lr *loader.Registry, factories *Registry, k Kind[T], s FactorySpec[C, T]) error {
	d, err := register(factories, k, s)
	if err != nil {
		return err
	}
	return loader.Register(lr, Descriptors, d)
}

// BuildActive constructs one instance only when the factory's plugin is active
// in the loader's selected set: a disabled backend is refused by name, never
// built behind the deployment's back. An unknown factory falls through to
// Build, so a typo is still reported as an unknown type rather than as a
// disabled plugin.
func BuildActive[T any](ctx context.Context, lr *loader.Registry, factories *Registry, k Kind[T], typ string, raw jsontext.Value, d Deps) (T, error) {
	var zero T
	id := k.ID(typ)
	if _, ok := factories.ConfigType(k.name, typ); ok && !loader.Active(lr, id) {
		return zero, fmt.Errorf("%s: plugin is disabled", id)
	}
	return Build(ctx, factories, k, typ, raw, d)
}
