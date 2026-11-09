package reporting

import (
	"errors"

	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/errors"
	"example.test/app/internal/plugins/model"
)

// ErrReporting is a plain sentinel beside the plugin error package.
var ErrReporting = errors.New("reporting")

// Spec is the compiled plugin identity of the previous-run reporter.
var Spec = loader.Spec{
	ID:       "reporting",
	Summary:  "derives the previous run from the storage survey",
	Requires: []string{"model"},
}

// Apply contributes the reporting provider.
func Apply(lr *loader.Registry) error {
	_ = pluginerrors.Spec
	return nil
}
