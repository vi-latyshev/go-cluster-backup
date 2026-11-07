package configuration

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity. The configuration contract is imported
// directly by every provider; the one contribution is the distribution's bundle
// table, which is configuration data, not loader policy.
var Spec = loader.Spec{
	ID:      "configuration",
	Summary: "the one configuration document: strict decode, the unified plugins root, defaults merge and offline rule checks",
}

// Apply publishes the distribution's compiled bundle table: the named
// selections a deployment may list under plugins.bundles. The table is
// configuration data; the loader only holds it.
func Apply(lr *loader.Registry) error {
	return AddBundle(lr, LocalOnly())
}
