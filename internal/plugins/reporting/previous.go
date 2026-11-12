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

// PreviousJob returns the cluster's half of .Run.Previous, read once per run
// right after the writer lease decision and carried by both events: the failed
// earlier attempts of this run's own Job, the previous Job of this run's own
// CronJob and how many Jobs of it failed in a row. The whole walk runs under
// the configured walk_timeout; each request additionally ends at
// request_timeout, so a request ends at whichever comes first. The Job read
// never fails the run: an unreadable status is reported in Unavailable, beside
// every fact the reads before the error established.
func (r *Reporter) PreviousJob(ctx context.Context) model.PreviousRun {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.WalkTimeout.Std())
	defer cancel()
	attempts, failure, err := r.walk(ctx)
	prev := model.PreviousRun{Attempts: attempts, Failure: failure}
	if err != nil {
		prev.Unavailable = "previous run status unavailable: " + err.Error()
	}
	return prev
}

// Previous combines the cluster's half read at the start with the storage
// half: the newest scheduled run the storages hold — unfinished when none of
// them kept its manifest — and the newest scheduled run that reported delivered
// or degraded. The storage half exists only for a finished event; the cluster
// half comes from the one start read, or from its one allowed re-read.
func Previous(job model.PreviousRun, runs []SurveyedRun) model.PreviousRun {
	prev := derivePrevious(runs)
	prev.Attempts, prev.Failure, prev.Unavailable = job.Attempts, job.Failure, job.Unavailable
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
