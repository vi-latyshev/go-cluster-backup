// Package keepversions is the retention policy "keep the newest N successful
// copies of every live artifact". A run is not the unit of keep: an artifact
// identity ("<group>/<name>") is, so a database dropped from the source stops
// holding retention slots as soon as the newest runs no longer name it, while a
// run that failed but still holds a good copy of a live artifact keeps counting
// towards that artifact's history.
//
// It carries no byte budget: the configured cap belongs to the protection
// ledger, which partitions runs by kind and applies the cap over every kind at
// once.
package keepversions

import (
	"context"
	"slices"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
)

type Config struct {
	Count   int `json:"count" required:"true" doc:"how many newest successful copies of each live artifact stay"`
	MinKeep int `json:"min_keep" doc:"floor: the newest this many successful copies of each live artifact always stay"`
	// CountIncomplete decides what happens to a run holding no successful copy
	// at all on this storage — a killed run, or one whose uploads all failed.
	// false keeps it, outside the slots, because retention has no copy to judge
	// it by; true keeps it only while it ranks among the newest count runs.
	CountIncomplete bool `json:"count_incomplete" required:"true" doc:"runs without a successful copy occupy retention slots"`
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
	// Liveness is read from the newest count runs alone, whatever their outcome:
	// a run that failed still saw its source's artifacts, so an identity is live
	// while a recent run names it. An identity that only older runs name has been
	// dropped from the source, and keeping its last copies forever is the bug
	// this policy exists to avoid.
	live := map[string]bool{}
	for i := 0; i < min(p.cfg.Count, len(runs)); i++ {
		for _, identity := range runs[i].ObservedArtifacts {
			live[identity] = true
		}
	}
	keep := map[string]bool{}
	floor := map[string]bool{}
	for identity := range live {
		holders := 0
		for _, r := range runs {
			if !slices.Contains(r.Artifacts, identity) {
				continue
			}
			if holders < p.cfg.Count {
				keep[r.RunID] = true
			}
			if holders < p.cfg.MinKeep {
				floor[r.RunID] = true
			}
			holders++
		}
	}
	for rank, r := range runs {
		if len(r.Artifacts) > 0 {
			// The run holds a successful copy of something: its keep is decided
			// by the live identities above. A copy of a dead identity neither
			// keeps it nor earns it the fallback.
			continue
		}
		if !p.cfg.CountIncomplete || rank < p.cfg.Count {
			keep[r.RunID] = true
		}
	}
	d := retention.Decided(runs, keep)
	for _, r := range runs {
		if floor[r.RunID] {
			d.KeepFloor = append(d.KeepFloor, r.RunID)
		}
	}
	return d, nil
}
