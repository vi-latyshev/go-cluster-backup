package cli

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
)

// openAssembly merges the distribution's defaults under the configuration file
// and decodes the result through the assembly: every plugin instance is built
// and every reference between instances is checked, which is the whole of
// `validate` and the first half of every other command. The assembly resolves
// the deployment's selection before it builds anything, so a disabled plugin,
// an unknown bundle or a section whose role has no active provider refuses
// here, before any command performs its first operational call.
func openAssembly(ctx context.Context, opts Options, defs *configuration.Defaults, path string, log *slog.Logger) (*assembly.Assembly, error) {
	if opts.Loader == nil {
		return nil, fmt.Errorf("%s: the compiled loader registry is required: without it no selection exists", path)
	}
	raw, err := configuration.ReadConfig(path)
	if err != nil {
		return nil, err
	}
	e, err := assembly.New(ctx, raw, defs, assembly.Deps{Factories: opts.Factories, Loader: opts.Loader, Log: log})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return e, nil
}
