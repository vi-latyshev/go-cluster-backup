package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestPrintRunReportNamesFailuresAndStats: the closing block names each failed
// artifact with the actionable half of its reason, and says what an artifact is
// not a copy of.
func TestPrintRunReportNamesFailuresAndStats(t *testing.T) {
	rep := model.RunReport{
		RunID: "r1", Kind: model.RunKindScheduled, Outcome: model.OutcomeDegraded, ExitCode: 3,
		Artifacts: []model.ArtifactResult{
			{Artifact: model.Artifact{Group: "g", Name: "ok"}, Outcome: model.ArtifactOK,
				RawBytes: 2048, Bytes: 1024,
				Stats: model.DumpStats{SkippedEntries: 2, PaddedFiles: 1}},
			{Artifact: model.Artifact{Group: "g", Name: "bad"}, Outcome: model.ArtifactFailed,
				Error: "the whole reason", Hint: "do this instead"},
		},
		Previous: model.PreviousRun{RunID: "r0", LastGoodAt: time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)},
		Storages: []model.StorageResult{{Storage: "exchange", Outcome: "ok", Objects: 1, Bytes: 1024}},
	}
	var out strings.Builder
	printRunReport(&out, rep, false)
	text := out.String()
	for _, want := range []string{
		"scheduled run r1 degraded in 0s (exit 3)",
		"previous run r0 (unfinished=false, last good 2026-10-18T04:13:51Z)",
		"artifacts: 1 ok, 1 failed, 0 excluded",
		"✗ g/bad: do this instead",
		"↳ g/ok: skipped 2, padded 1",
		"exchange: ok, 1 objects, 1 KiB, kept 0, deleted 0, freed 0 B",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the report does not carry %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "the whole reason") {
		t.Errorf("the report buried the actionable half under the full reason:\n%s", text)
	}
}

// TestPrintRunReportOfAStage: a stage has no run id, so its summary is the
// stage's own line.
func TestPrintRunReportOfAStage(t *testing.T) {
	var out strings.Builder
	printRunReport(&out, model.RunReport{Outcome: model.OutcomeDelivered}, false)
	if got := out.String(); !strings.Contains(got, "stage delivered in 0s (exit 0)") {
		t.Errorf("stage summary = %q", got)
	}
}
