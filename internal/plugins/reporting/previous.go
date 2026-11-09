package reporting

import (
	"context"
	"sort"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// SurveyedRun is one run as one surveyed storage holds it: the input the
// previous-run record is derived from. The caller flattens every storage's
// survey into these records; a storage whose survey failed contributes none, so
// an unreadable storage never answers for a run.
type SurveyedRun struct {
	// Storage is the instance name of the storage that reported this run. When
	// two storages disagree about a run's manifest, the lexicographically
	// smallest name decides, so the answer does not depend on map order.
	Storage string
	RunID   string
	Kind    model.RunKind
	// StartedAt is the manifest's start time when the manifest was read, and the
	// newest object's modification time otherwise.
	StartedAt time.Time
	// Manifest is true when this storage holds the run's manifest and this build
	// read it. Outcome is meaningful only then.
	Manifest bool
	Outcome  model.Outcome
}

// Previous returns .Run.Previous: the newest scheduled run the storages hold —
// unfinished when none of them kept its manifest — the newest scheduled run that
// reported delivered or degraded, and the previous Job's failure read from the
// cluster. The Job read never fails the run: an unreadable status is reported in
// Unavailable.
func (r *Reporter) Previous(ctx context.Context, runs []SurveyedRun) model.PreviousRun {
	prev := derivePrevious(runs)
	failure, err := r.PreviousJobFailure(ctx)
	switch {
	case err != nil:
		prev.Unavailable = "previous run status unavailable: " + err.Error()
	case failure != nil:
		prev.Failure = failure
	}
	return prev
}

// derivePrevious reduces every storage's view to one answer. A manual run is
// neither the previous scheduled night nor a last-good candidate: its absence
// from this answer is what keeps a manual failure from reading as a missed
// night. Ordering is newest first by StartedAt, the run id breaking ties so the
// answer is stable.
func derivePrevious(runs []SurveyedRun) model.PreviousRun {
	merged := mergeRuns(runs)
	prev := model.PreviousRun{}
	for _, run := range merged {
		if run.Kind != model.RunKindScheduled {
			continue
		}
		if prev.RunID == "" {
			prev.RunID = run.RunID
			prev.Unfinished = !run.Manifest
		}
		if run.Manifest && (run.Outcome == model.OutcomeDelivered || run.Outcome == model.OutcomeDegraded) {
			prev.LastGoodAt = run.StartedAt
			break
		}
	}
	return prev
}

// mergeRuns folds every storage's record of one run id into one: the newest
// start time seen, whether any storage kept a readable manifest, and that
// manifest's outcome. Records without a run id are not runs.
func mergeRuns(runs []SurveyedRun) []SurveyedRun {
	byID := map[string]*SurveyedRun{}
	for i := range runs {
		run := &runs[i]
		if run.RunID == "" {
			continue
		}
		merged := byID[run.RunID]
		if merged == nil {
			merged = &SurveyedRun{RunID: run.RunID, Kind: run.Kind, StartedAt: run.StartedAt}
			byID[run.RunID] = merged
		}
		if run.Manifest && (!merged.Manifest || run.Storage < merged.Storage) {
			// The manifest carries the run's real start time; an object's
			// modification time never stands in for it once one storage read it.
			merged.Manifest = true
			merged.Outcome = run.Outcome
			merged.StartedAt = run.StartedAt
			merged.Storage = run.Storage
		} else if !merged.Manifest && run.StartedAt.After(merged.StartedAt) {
			merged.StartedAt = run.StartedAt
		}
	}
	out := make([]SurveyedRun, 0, len(byID))
	for _, run := range byID {
		out = append(out, *run)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.After(out[j].StartedAt)
		}
		return out[i].RunID < out[j].RunID
	})
	return out
}
