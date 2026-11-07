package kuberneteslabels

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(discovery.Kind.Name(), "kubernetes_labels"),
	Summary: "labelled pods and persistent volume claims listed from the Kubernetes API",
}

// Apply contributes the kubernetes_labels discovery factory and its descriptor
// to the compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, discovery.Kind, instances.FactorySpec[Config, discovery.Discoverer]{
		Type:      "kubernetes_labels",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(example),
		Rules:     RuleSources,
		New:       New,
	})
}
