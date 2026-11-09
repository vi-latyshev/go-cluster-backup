package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "redis"),
	Summary: "one Redis instance, snapshotted with redis-cli --rdb",
}

// Apply contributes the redis source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "redis",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Binaries:  []string{"redis-cli"},
		Example:   []byte(`{"address": "redis.example.svc.cluster.local:6379", "password": {"file": "/run/secrets/redis/password"}, "scratch_dir": "/scratch", "connect_attempts": 6, "connect_interval": "5s"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			host, port, err := net.SplitHostPort(cfg.Address)
			if err != nil {
				return nil, fmt.Errorf("address %q must be host:port: %w", cfg.Address, err)
			}
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return nil, fmt.Errorf("address %q: port %q is not a number", cfg.Address, port)
			}
			if cfg.Password.File == "" {
				return nil, errors.New("password/file is empty")
			}
			if cfg.ScratchDir == "" {
				return nil, errors.New("scratch_dir is empty")
			}
			if !filepath.IsAbs(cfg.ScratchDir) {
				return nil, fmt.Errorf("scratch_dir %q must be absolute", cfg.ScratchDir)
			}
			if cfg.ConnectAttempts < 1 {
				return nil, fmt.Errorf("connect_attempts must be at least 1, got %d", cfg.ConnectAttempts)
			}
			if cfg.ConnectInterval.Std() <= 0 {
				return nil, errors.New("connect_interval must be positive")
			}
			return &Source{cfg: cfg, host: host, port: port}, nil
		},
	})
}
