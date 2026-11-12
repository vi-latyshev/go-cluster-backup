package backup

// The notifier and previous-run subjects of the workflow: the parts of Run's
// helpers a test drives directly, beside the skeleton proofs in assembly.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
)

type recordingNotifier struct {
	events []model.Event
}

func (n *recordingNotifier) Check(context.Context) error { return nil }

func (n *recordingNotifier) Notify(_ context.Context, ev model.Event) error {
	n.events = append(n.events, ev)
	return nil
}

// TestNotifySkipsANotifierWhoseCheckFailed: a notifier that failed its Check is
// not called, counts as failed, and its type is looked up for the log prefix.
func TestNotifySkipsANotifierWhoseCheckFailed(t *testing.T) {
	n := &recordingNotifier{}
	w := &Workflow{
		notifiers:       []notifierBinding{{name: "ops", inst: n}},
		notifierTypeMap: map[string]string{"ops": "fake"},
		log:             slog.New(slog.DiscardHandler),
	}
	checks := failedChecks{instanceKey{kind: "notifier", name: "ops"}: errors.New("check failed")}
	failed, delivered := w.notify(context.Background(), model.Event{Name: model.EventStarted}, checks)
	if !failed || delivered {
		t.Fatalf("a notifier whose Check failed: failed %v delivered %v", failed, delivered)
	}
	if len(n.events) != 0 {
		t.Fatalf("a notifier whose Check failed was called: %v", n.events)
	}
	if got := w.notifierType("absent"); got != "" {
		t.Fatalf("notifierType(absent) = %q, want empty", got)
	}
	if got := w.notifierType("ops"); got != "fake" {
		t.Fatalf("notifierType(ops) = %q", got)
	}
}

// TestPreviousSkipsAFailedSurveyAndRendersTheRecord: a storage whose survey
// failed answers for no run, and the run log names the unavailable status, the
// previous Job's failure and the own Job's failed earlier attempts exactly as
// the record carries them.
func TestPreviousSkipsAFailedSurveyAndRendersTheRecord(t *testing.T) {
	var buf bytes.Buffer
	w := &Workflow{log: slog.New(human.NewHandler(&buf, slog.LevelInfo))}
	at := time.Now().UTC()
	order := []*storageBinding{{name: "broken"}, {name: "alpha"}}
	surveys := map[string]inventory.Survey{
		"broken": {Storage: "broken", OK: false},
		"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
			"20261010T042000Z": {
				Stored:   model.StoredRun{RunID: "20261010T042000Z", Kind: model.RunKindScheduled, At: at, Manifest: true},
				Manifest: &model.RunReport{Outcome: model.OutcomeDelivered},
			},
		}},
	}
	job := model.PreviousRun{
		Unavailable: "the status grant is missing",
		Failure:     &model.PreviousFailure{Job: "backup-20261009", Reason: "OOMKilled"},
		Attempts:    &model.AttemptFailure{Job: "backup-20261010", Failed: 2, Deleted: 1},
	}
	prev := reporting.Previous(job, surveyedRuns(order, surveys))
	if prev.RunID != "20261010T042000Z" || prev.Unfinished {
		t.Fatalf("previous %+v, want the delivered run of the readable storage", prev)
	}
	if prev.Failure == nil || prev.Failure.Job != "backup-20261009" {
		t.Fatalf("the cluster half is not in the answer: %+v", prev)
	}
	w.reportPrevious(prev)
	w.reportPreviousJob(job)
	log := buf.String()
	for _, want := range []string{
		"20261010T042000Z", "the status grant is missing", "backup-20261009", "OOMKilled",
		"backup-20261010",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("the run log lacks %q:\n%s", want, log)
		}
	}
}

var _ notifier.Notifier = (*recordingNotifier)(nil)
