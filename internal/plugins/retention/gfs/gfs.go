// Package gfs is the retention policy "grandfather-father-son": the newest run
// of each of the newest N calendar days, each of the newest M ISO weeks and
// each of the newest K calendar months, plus a floor of the newest runs. It
// carries no byte budget: the configured cap belongs to the engine, which
// partitions runs by kind and applies the cap over every kind at once.
package gfs

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
)

type Config struct {
	Daily           int    `json:"daily" required:"true" doc:"how many newest calendar days keep their newest run"`
	Weekly          int    `json:"weekly" required:"true" doc:"how many newest ISO weeks keep their newest run"`
	Monthly         int    `json:"monthly" required:"true" doc:"how many newest calendar months keep their newest run; 0 turns the tier off"`
	Timezone        string `json:"timezone" doc:"IANA time zone the calendar (days, weeks, months) is read in, e.g. UTC"`
	MinKeep         int    `json:"min_keep" doc:"floor: the newest this many runs always stay"`
	CountIncomplete bool   `json:"count_incomplete" required:"true" doc:"runs without a verified manifest occupy retention slots: a period's slot goes to its newest run this build can judge, and to a run it cannot only when the period has no other; false keeps such runs in addition, outside the tiers"`
}

// Register adds the gfs policy to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, retention.Kind, plugin.Spec[Config, retention.Retention]{
		Type:      "gfs",
		Summary:   "keep the newest run per day, week and month",
		Stability: plugin.Stable,
		Example:   []byte(`{"daily": 30, "weekly": 12, "monthly": 12, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (retention.Retention, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Daily < 1 {
				return nil, fmt.Errorf("daily must be at least 1, got %d", cfg.Daily)
			}
			if cfg.Weekly < 0 {
				return nil, fmt.Errorf("weekly must not be negative, got %d", cfg.Weekly)
			}
			if cfg.Monthly < 0 {
				return nil, fmt.Errorf("monthly must not be negative, got %d", cfg.Monthly)
			}
			if cfg.MinKeep < 1 {
				return nil, fmt.Errorf("min_keep must be at least 1, got %d", cfg.MinKeep)
			}
			loc, err := time.LoadLocation(cfg.Timezone)
			if err != nil {
				return nil, err
			}
			return &Policy{cfg: cfg, loc: loc}, nil
		},
	})
}

type Policy struct {
	cfg Config
	loc *time.Location
}

func (p *Policy) Decide(ctx context.Context, req retention.DecideRequest) (retention.Decision, error) {
	if err := ctx.Err(); err != nil {
		return retention.Decision{}, err
	}
	runs := slices.Clone(req.Runs)
	retention.NewestFirst(runs)
	// Each period of each tier holds ONE slot, and the slot goes to the run of
	// that period this build can judge best: its verified run first, then one
	// whose manifest it read, then the objects of a run that was killed before
	// it wrote one — otherwise a killed run at a Sunday or month end would be
	// the week's or month's only representative and the complete run beside it
	// would age out with it.
	type pick struct {
		runID string
		rank  int
	}
	// runRank is what a period may keep, best first. A run with a readable
	// manifest outranks one without even when neither is verified: under a
	// standing failure of every run, the one that at least reported an outcome
	// is the one worth keeping for the night the failure is read.
	runRank := func(r model.StoredRun) int {
		switch {
		case r.Verified:
			return 2
		case r.Manifest:
			return 1
		}
		return 0
	}
	picksFor := func(picks map[string]*pick, key string, r model.StoredRun, rank map[string]int) {
		pk := picks[key]
		if pk == nil {
			pk = &pick{}
			picks[key] = pk
			rank[key] = len(rank) + 1
		}
		if rank := runRank(r); pk.runID == "" || rank > pk.rank {
			pk.runID, pk.rank = r.RunID, rank
		}
	}
	dayPicks, weekPicks, monthPicks := map[string]*pick{}, map[string]*pick{}, map[string]*pick{}
	dayRank, weekRank, monthRank := map[string]int{}, map[string]int{}, map[string]int{}
	keep := map[string]bool{}
	for _, r := range runs {
		if !r.Verified && !p.cfg.CountIncomplete {
			// An unfinished run is neither counted nor dropped: retention has no
			// trustworthy manifest to judge it by.
			keep[r.RunID] = true
			continue
		}
		t := r.At.In(p.loc)
		isoYear, isoWeek := t.ISOWeek()
		picksFor(dayPicks, t.Format("2006-01-02"), r, dayRank)
		picksFor(weekPicks, fmt.Sprintf("%04d-W%02d", isoYear, isoWeek), r, weekRank)
		picksFor(monthPicks, t.Format("2006-01"), r, monthRank)
	}
	// A period's rank is its age among the periods of that tier — runs arrive
	// newest first, so the first time a period appears is its rank; the tiers
	// keep the newest N of them, each through its own pick.
	for key, pk := range dayPicks {
		if dayRank[key] <= p.cfg.Daily {
			keep[pk.runID] = true
		}
	}
	for key, pk := range weekPicks {
		if weekRank[key] <= p.cfg.Weekly {
			keep[pk.runID] = true
		}
	}
	for key, pk := range monthPicks {
		if monthRank[key] <= p.cfg.Monthly {
			keep[pk.runID] = true
		}
	}
	for i := 0; i < min(p.cfg.MinKeep, len(runs)); i++ {
		keep[runs[i].RunID] = true
	}
	d := retention.Decided(runs, keep)
	// The floor is the newest min_keep runs, which the loop above kept: the
	// engine's cap eviction honours it, so a byte budget never cuts deeper than
	// the policy itself would. The window mirrors the old byte budget's: an
	// unfinished run count_incomplete keeps is outside it.
	var floorCandidates []model.StoredRun
	for _, r := range runs {
		if !keep[r.RunID] {
			continue
		}
		if !r.Verified && !p.cfg.CountIncomplete {
			continue
		}
		floorCandidates = append(floorCandidates, r)
	}
	for i := 0; i < min(p.cfg.MinKeep, len(floorCandidates)); i++ {
		d.KeepFloor = append(d.KeepFloor, floorCandidates[i].RunID)
	}
	return d, nil
}
