package inventory

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// StoredRunView is one run as a restore listing reports it.
type StoredRunView struct {
	RunID string
	// Kind is the run's identity, read from its id — available even for a run
	// that never wrote a manifest.
	Kind        model.RunKind
	Storage     string
	HasManifest bool
	Report      model.RunReport // zero when the manifest is missing
}

// StoredRuns lists the runs every storage holds, including runs without a
// manifest. Storages are surveyed in the caller's order; a storage that cannot
// be listed fails the whole listing, never a partial view.
func (inv Inventory) StoredRuns(ctx context.Context, prefix string, storages []Storage) ([]StoredRunView, error) {
	var out []StoredRunView
	for _, s := range storages {
		sur := inv.SurveyStorage(ctx, prefix, s)
		if !sur.OK {
			return nil, fmt.Errorf("stored runs: storage %s: %w", s.Name, sur.Err)
		}
		ids := slices.Sorted(maps.Keys(sur.Runs))
		sort.Slice(ids, func(i, j int) bool {
			a, b := sur.Runs[ids[i]].Stored, sur.Runs[ids[j]].Stored
			if !a.At.Equal(b.At) {
				return a.At.After(b.At)
			}
			return a.RunID > b.RunID
		})
		for _, id := range ids {
			sr := sur.Runs[id]
			view := StoredRunView{RunID: id, Kind: sr.Stored.Kind, Storage: s.Name, HasManifest: sr.Manifest != nil}
			if sr.Manifest != nil {
				view.Report = *sr.Manifest
			}
			out = append(out, view)
		}
	}
	return out, nil
}
