package cli

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// AddHost is the white-box tests' seam into a components-only graph: it performs
// the same loader.Add(Spec, Apply) the generated composition root performs, with
// the test's own Options. Production has exactly one registration path — the
// generated root's Apply — and no exported helper for it.
func AddHost(r *loader.Registry, opts Options) error {
	opts.Loader = r
	return loader.Add(r, Spec, func(lr *loader.Registry) error {
		return addRoles(lr, opts)
	})
}
