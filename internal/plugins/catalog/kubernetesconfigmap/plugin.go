package kubernetesconfigmap

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(catalog.Kind.Name(), "kubernetes_configmap"),
	Summary: "the writer lease in one key of one ConfigMap",
}

// Apply contributes the kubernetes_configmap catalog factory and its descriptor
// to the compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, catalog.Kind, instances.FactorySpec[Config, catalog.Catalog]{
		Type:      "kubernetes_configmap",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(example),
		New:       New,
	})
}
