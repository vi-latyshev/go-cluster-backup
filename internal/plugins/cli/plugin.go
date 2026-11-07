package cli

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// Spec is the compiled identity of the CLI host. It is Floor: every process role
// is dispatched here, so a deployment cannot disable the host.
var Spec = loader.Spec{
	ID:      "cli",
	Summary: "the command host: one process role per container",
	Floor:   true,
}

// Program is the binary name the host's usage text and error prefixes use by
// default.
const Program = "go-cluster-backup"

// Apply registers the host's roles inside the loader's plugin registration. The
// generated composition root calls loader.Add(Spec, apply) and passes this
// function as apply; there is no second registration path.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return addRoles(lr, Options{
		Program:   Program,
		Loader:    lr,
		Factories: factories,
		Defaults:  [][]byte{defaults.Defaults},
	})
}

// addRoles registers one role per process entrypoint for the plugin currently
// being added.
func addRoles(lr *loader.Registry, opts Options) error {
	for _, role := range hostRoles(opts) {
		if err := loader.Role(lr, role); err != nil {
			return err
		}
	}
	return nil
}
