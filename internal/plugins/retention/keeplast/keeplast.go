// Package keeplast is the retention policy "keep the newest N runs". It carries
// no byte budget: the configured cap belongs to the engine, which partitions
// runs by kind and applies the cap over every kind at once.
package keeplast

import (
	"context"
	"fmt"
	"slices"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
)

// Config is the policy's own config. CountIncomplete is the deployment's to state:
// it decides whether a run nobody could verify is ever deleted, which is a
// delete mode.
type Config struct {
	Count           int  `json:"count" required:"true" doc:"how many newest runs stay"`
	MinKeep         int  `json:"min_keep" doc:"floor: the newest this many runs always stay"`
	CountIncomplete bool `json:"count_incomplete" required:"true" doc:"runs without a verified manifest occupy retention slots"`
}

// Register adds the keep_last policy to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, retention.Kind, plugin.Spec[Config, retention.Retention]{
		Type:      "keep_last",
		Summary:   "keep the newest N runs",
		Stability: plugin.Stable,
		Example:   []byte(`{"count": 3, "min_keep": 1, "count_incomplete": true}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (retention.Retention, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Count < 1 {
				return nil, fmt.Errorf("count must be at least 1, got %d", cfg.Count)
			}
			if cfg.MinKeep < 1 {
				return nil, fmt.Errorf("min_keep must be at least 1, got %d", cfg.MinKeep)
			}
			if cfg.MinKeep > cfg.Count {
				return nil, fmt.Errorf("min_keep %d must not exceed count %d", cfg.MinKeep, cfg.Count)
			}
			return &Policy{cfg: cfg}, nil
		},
	})
}

type Policy struct {
	cfg Config
}

func (p *Policy) Decide(ctx context.Context, req retention.DecideRequest) (retention.Decision, error) {
	if err := ctx.Err(); err != nil {
		return retention.Decision{}, err
	}
	runs := slices.Clone(req.Runs)
	retention.NewestFirst(runs)
	keep := map[string]bool{}
	var floor []string
	counted := 0
	for _, r := range runs {
		if !r.Verified && !p.cfg.CountIncomplete {
			// An unfinished run is neither counted nor dropped: retention has no
			// trustworthy manifest to judge it by.
			keep[r.RunID] = true
			continue
		}
		if counted < p.cfg.Count {
			if counted < p.cfg.MinKeep {
				floor = append(floor, r.RunID)
			}
			keep[r.RunID] = true
			counted++
		}
	}
	d := retention.Decided(runs, keep)
	d.KeepFloor = floor
	return d, nil
}
