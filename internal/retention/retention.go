// Package retention is the seam that decides which stored runs stay on ONE storage.
// The engine calls it with the run list it read from that storage's manifests and
// applies the decision with the cross-storage guard (never the last verified copy).
// The interface obeys the seam rules documented in package plugin.
package retention

import (
	"context"
	"sort"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

type DecideRequest struct {
	// Now is the run's start time; the policy never reads the wall clock itself.
	Now time.Time
	// Runs is every run present on one storage.
	Runs []model.StoredRun
}

// Decision classifies every run of the request exactly once: Keep and Drop are
// disjoint and their union is every run id in the request. A policy judges only
// the runs it is handed — the engine partitions by run kind before the call —
// and it carries no byte budget: the configured cap is the engine's, applied
// once over every kind, so it cannot come to mean two different things.
type Decision struct {
	Keep []string
	Drop []string
	// KeepFloor names the runs the policy's own floor protects — the newest
	// min_keep runs it would never delete itself. The engine's cap eviction
	// honours them too, so a byte budget cannot cut deeper than the policy's
	// floor; the engine never reads the policy's opaque config for this.
	KeepFloor []string
}

type Retention interface {
	// Decide is pure: it computes from the request only. A cancelled context
	// returns promptly with an error.
	Decide(ctx context.Context, req DecideRequest) (Decision, error)
}

var Kind = plugin.NewKind[Retention]("retention")

// Budget trims kept — newest first — to maxBytes: it returns how many of the
// newest entries survive, dropping the oldest first and never dropping below
// minKeep. It is the one definition of a byte budget in this module, applied by
// the engine — the owner of the configured cap — to a storage's remaining
// scheduled history once the cheaper evictions are spent, so the budget cannot
// come to mean two different things.
func Budget(kept []model.StoredRun, maxBytes int64, minKeep int) int {
	n := len(kept)
	var total int64
	for i, r := range kept {
		total += r.Bytes
		if total > maxBytes && i+1 > minKeep {
			n = i
			break
		}
	}
	return n
}

// NewestFirst sorts runs newest first, ties broken by run id: every policy
// reads its calendar in this order, and the engine hands the list in no
// particular order.
func NewestFirst(runs []model.StoredRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		if !runs[i].At.Equal(runs[j].At) {
			return runs[i].At.After(runs[j].At)
		}
		return runs[i].RunID < runs[j].RunID
	})
}

// Decided builds the decision from a keep set: every run of the request is
// classified exactly once, in the order given.
func Decided(runs []model.StoredRun, keep map[string]bool) Decision {
	d := Decision{}
	for _, r := range runs {
		if keep[r.RunID] {
			d.Keep = append(d.Keep, r.RunID)
			continue
		}
		d.Drop = append(d.Drop, r.RunID)
	}
	return d
}
