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
// failed answers for no run, and the run log names the unavailable and failed
// previous Job exactly as the record carries them.
func TestPreviousSkipsAFailedSurveyAndRendersTheRecord(t *testing.T) {
	reporter, err := reporting.New(context.Background(), reporting.Config{
		APIHostEnv: "GCB_PREV_HOST", APIPortEnv: "GCB_PREV_PORT",
		TokenFile: "/var/run/secrets/token", CAFile: "/var/run/secrets/ca.crt",
		RequestTimeout: model.Duration(time.Second),
		NamespaceEnv:   "GCB_PREV_NAMESPACE", PodEnv: "GCB_PREV_POD",
	})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	w := &Workflow{reporter: reporter, log: slog.New(human.NewHandler(&buf, slog.LevelInfo))}
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
	prev := reporter.Previous(context.Background(), surveyedRuns(order, surveys))
	if prev.RunID != "20261010T042000Z" || prev.Unfinished {
		t.Fatalf("previous %+v, want the delivered run of the readable storage", prev)
	}
	w.reportPrevious(model.PreviousRun{
		RunID: "20261010T042000Z", Unfinished: true,
		Unavailable: "the status grant is missing",
		Failure:     &model.PreviousFailure{Job: "backup-20261009", Reason: "OOMKilled"},
	})
	log := buf.String()
	for _, want := range []string{"the status grant is missing", "backup-20261009", "OOMKilled"} {
		if !strings.Contains(log, want) {
			t.Errorf("the run log lacks %q:\n%s", want, log)
		}
	}
}

var _ notifier.Notifier = (*recordingNotifier)(nil)
