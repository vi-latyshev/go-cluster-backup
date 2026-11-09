package zstd

import (
	"context"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(processor.Kind.Name(), "zstd"),
	Summary: "zstd frame compression, streaming, with the window and concurrency from the config",
}

// Apply contributes the zstd processor factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, processor.Kind, instances.FactorySpec[Config, processor.Processor]{
		Type:      "zstd",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"level": "better", "concurrency": 1, "window_bytes": 67108864, "decode_window_bytes": 134217728}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (processor.Processor, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Decode ran Config.Validate before New: the level, the
			// concurrency and both windows are already admitted, and a breach
			// of that claim is a panic, not a config error this call can see.
			must.Do(cfg, cfg.Validate())
			return &Processor{cfg: cfg}, nil
		},
	})
}
