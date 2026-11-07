package reporting_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
)

var previousBase = time.Date(2026, 10, 10, 3, 0, 0, 0, time.UTC)

// quietReporter is a reporter outside a pod: the previous-run fields come from
// the surveyed runs alone, and no API server is ever read.
func quietReporter(t *testing.T) *reporting.Reporter {
	t.Helper()
	t.Setenv(testPodEnv, "")
	rep, err := reporting.New(context.Background(), reporting.Config{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: "/token", CAFile: "/ca",
		RequestTimeout: model.Duration(time.Second),
		NamespaceEnv:   testNamespaceEnv, PodEnv: testPodEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func surveyedRun(storage, id string, at time.Time, manifest bool, outcome model.Outcome) reporting.SurveyedRun {
	return reporting.SurveyedRun{
		Storage: storage, RunID: id, Kind: model.RunKindScheduled,
		StartedAt: at, Manifest: manifest, Outcome: outcome,
	}
}

// TestPreviousDerivesTheNewestScheduledRun: the newest scheduled run is the
// previous one, the newest scheduled delivered or degraded run is the last good
// one, and a manual run is neither — a manual failure must never read as a
// missed night.
func TestPreviousDerivesTheNewestScheduledRun(t *testing.T) {
	runs := []reporting.SurveyedRun{
		surveyedRun("alpha", "s-early", previousBase, true, model.OutcomeDelivered),
		surveyedRun("alpha", "s-new", previousBase.Add(time.Hour), true, model.OutcomeFailed),
		{Storage: "alpha", RunID: "", Kind: model.RunKindScheduled,
			StartedAt: previousBase.Add(3 * time.Hour), Manifest: true, Outcome: model.OutcomeDelivered},
		{Storage: "alpha", RunID: "m-new", Kind: model.RunKindManual,
			StartedAt: previousBase.Add(2 * time.Hour), Manifest: true, Outcome: model.OutcomeDelivered},
	}
	prev := quietReporter(t).Previous(context.Background(), runs)
	if prev.RunID != "s-new" || prev.Unfinished {
		t.Fatalf("previous = %+v, want the newest scheduled run, finished", prev)
	}
	if !prev.LastGoodAt.Equal(previousBase) {
		t.Fatalf("last good = %s, want the older delivered run", prev.LastGoodAt)
	}
	if prev.Failure != nil || prev.Unavailable != "" {
		t.Fatalf("a run outside a pod carries cluster facts: %+v", prev)
	}
}

// TestPreviousMarksAnUnfinishedRun: a scheduled run no storage kept a manifest
// for started and never reported an outcome, and the last good run before it
// still answers.
func TestPreviousMarksAnUnfinishedRun(t *testing.T) {
	runs := []reporting.SurveyedRun{
		surveyedRun("alpha", "s-good", previousBase, true, model.OutcomeDegraded),
		surveyedRun("alpha", "s-lost", previousBase.Add(time.Hour), false, ""),
		{Storage: "alpha", RunID: "m-lost", Kind: model.RunKindManual,
			StartedAt: previousBase.Add(2 * time.Hour)},
	}
	prev := quietReporter(t).Previous(context.Background(), runs)
	if prev.RunID != "s-lost" || !prev.Unfinished {
		t.Fatalf("previous = %+v, want the unfinished scheduled run", prev)
	}
	if !prev.LastGoodAt.Equal(previousBase) {
		t.Fatalf("last good = %s, want the degraded run before it", prev.LastGoodAt)
	}
}

// TestPreviousMergesEveryStorageOfOneRun: the same run is one run whichever
// storages hold it — its manifest on any of them finishes it, its start time
// comes from that manifest rather than from an object's later modification
// time, and two disagreeing manifests are decided by the storage name so the
// answer never depends on map order.
func TestPreviousMergesEveryStorageOfOneRun(t *testing.T) {
	started := previousBase.Add(time.Hour)
	merged := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("alpha", "s-1", previousBase.Add(2*time.Hour), false, ""),
		surveyedRun("beta", "s-1", started, true, model.OutcomeDelivered),
	})
	if merged.RunID != "s-1" || merged.Unfinished {
		t.Fatalf("previous = %+v, want the run finished by the manifest on beta", merged)
	}
	if !merged.LastGoodAt.Equal(started) {
		t.Fatalf("last good = %s, want the manifest's own start %s", merged.LastGoodAt, started)
	}

	disagreeing := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("beta", "s-1", started, true, model.OutcomeDelivered),
		surveyedRun("alpha", "s-1", started, true, model.OutcomeFailed),
	})
	if !disagreeing.LastGoodAt.IsZero() {
		t.Fatalf("last good = %s, want none: the alpha manifest says failed", disagreeing.LastGoodAt)
	}

	// No manifest anywhere: the newest object's modification time is the run's
	// only instant, and the newest storage's reading of it wins.
	manifestless := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("beta", "s-1", started.Add(time.Minute), false, ""),
		surveyedRun("alpha", "s-1", started, false, ""),
	})
	if !manifestless.LastGoodAt.IsZero() || manifestless.RunID != "s-1" {
		t.Fatalf("previous = %+v, want the manifestless run", manifestless)
	}
	if prev := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("alpha", "s-old", previousBase, false, ""),
		surveyedRun("beta", "s-old", started, false, ""),
	}); !prev.Unfinished {
		t.Fatalf("previous = %+v, want the unfinished run", prev)
	}
}

// TestPreviousIsEmptyWithoutRuns: nothing surveyed answers nothing.
func TestPreviousIsEmptyWithoutRuns(t *testing.T) {
	for _, runs := range [][]reporting.SurveyedRun{nil, {}} {
		prev := quietReporter(t).Previous(context.Background(), runs)
		if prev.RunID != "" || prev.Unfinished || !prev.LastGoodAt.IsZero() || prev.Failure != nil || prev.Unavailable != "" {
			t.Fatalf("previous = %+v, want the empty answer", prev)
		}
	}
}

// TestPreviousBreaksATieByRunID: two runs that started in the same instant are
// ordered by run id, so every storage's answer names the same previous run.
func TestPreviousBreaksATieByRunID(t *testing.T) {
	prev := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("alpha", "s-b", previousBase, true, model.OutcomeDelivered),
		surveyedRun("alpha", "s-a", previousBase, true, model.OutcomeDelivered),
	})
	if prev.RunID != "s-a" {
		t.Fatalf("previous run = %q, want the lexicographically first id", prev.RunID)
	}
	if !prev.LastGoodAt.Equal(previousBase) {
		t.Fatalf("last good = %s, want the tie's start time", prev.LastGoodAt)
	}
}

// TestPreviousIncludesThePreviousJobFailure: the cluster's record of the failed
// previous Job reaches the same answer as the surveyed runs.
func TestPreviousIncludesThePreviousJobFailure(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	prev := newReporterIn(t, srv).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("alpha", "s-early", previousBase, true, model.OutcomeDelivered),
	})
	if prev.RunID != "s-early" || !prev.LastGoodAt.Equal(previousBase) {
		t.Fatalf("previous = %+v, want the surveyed run", prev)
	}
	if prev.Failure == nil || prev.Failure.Job != "backup-prev-job" {
		t.Fatalf("the previous Job's failure is not in the answer: %+v", prev)
	}
	if prev.Unavailable != "" {
		t.Fatalf("a readable previous Job reported unavailable: %q", prev.Unavailable)
	}
}

// TestPreviousSurvivesAnUnreadablePreviousJob: a missing grant is visible as
// "previous run status unavailable" and the surveyed answer still stands.
func TestPreviousSurvivesAnUnreadablePreviousJob(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("jobs")
	prev := newReporterIn(t, srv).Previous(context.Background(), []reporting.SurveyedRun{
		surveyedRun("alpha", "s-early", previousBase, true, model.OutcomeDelivered),
	})
	if !strings.HasPrefix(prev.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("unavailable = %q, want the surfaced read failure", prev.Unavailable)
	}
	if prev.RunID != "s-early" || !prev.LastGoodAt.Equal(previousBase) {
		t.Fatalf("previous = %+v, want the surveyed answer intact", prev)
	}
	if prev.Failure != nil {
		t.Fatalf("an unreadable previous Job produced a failure: %+v", prev.Failure)
	}
}

// TestC4_MissingKindIsNotAPreviousRun: the current stored format always carries
// a kind, so a surveyed record without one is a problem run. It must never be
// re-read as a scheduled run — neither as the previous one, finished or
// unfinished, nor as last good — and a valid scheduled run beside it still
// answers.
func TestC4_MissingKindIsNotAPreviousRun(t *testing.T) {
	goodAt := previousBase
	brokenAt := previousBase.Add(2 * time.Hour)

	prev := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		{Storage: "alpha", RunID: "s-good", Kind: model.RunKindScheduled,
			StartedAt: goodAt, Manifest: true, Outcome: model.OutcomeDelivered},
		{Storage: "alpha", RunID: "s-nokind",
			StartedAt: brokenAt, Manifest: true, Outcome: model.OutcomeDelivered},
	})
	if prev.RunID != "s-good" || prev.Unfinished {
		t.Fatalf("previous = %+v, want the valid scheduled run, not the kindless one", prev)
	}
	if !prev.LastGoodAt.Equal(goodAt) {
		t.Fatalf("last good = %s, want %s: a kindless manifest is not protection", prev.LastGoodAt, goodAt)
	}

	if prev := quietReporter(t).Previous(context.Background(), []reporting.SurveyedRun{
		{Storage: "alpha", RunID: "s-nokind", StartedAt: brokenAt},
	}); prev.RunID != "" || prev.Unfinished {
		t.Fatalf("previous = %+v, want none: a kindless run is a problem run", prev)
	}
}

// TestM8_PreviousWithoutRunsStillReadsThePreviousJob: a refused run performs no
// storage survey, so the finished event asks for the previous record with no
// surveyed runs at all — the previous Job's status must still be read.
func TestM8_PreviousWithoutRunsStillReadsThePreviousJob(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	prev := newReporterIn(t, srv).Previous(context.Background(), nil)
	if prev.RunID != "" || prev.Unfinished || !prev.LastGoodAt.IsZero() {
		t.Fatalf("previous = %+v, want no surveyed run", prev)
	}
	if prev.Failure == nil || prev.Failure.Job != "backup-prev-job" {
		t.Fatalf("the previous Job's failure is not in the answer: %+v", prev)
	}
}
