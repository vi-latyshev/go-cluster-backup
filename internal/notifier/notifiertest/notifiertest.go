// Package notifiertest is the conformance suite every notifier implementation runs:
//
//	notifiertest.Run(t, func(t *testing.T) notifier.Notifier { return newTestNotifier(t) })
package notifiertest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
)

type Check struct {
	Name string
	Run  func(t *testing.T, n notifier.Notifier) error
}

var Checks = []Check{
	{"notify", checkNotify},
	{"returns on cancellation", checkCancel},
}

func Run(t *testing.T, newFn func(t *testing.T) notifier.Notifier) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(t, newFn(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Event is the sample event every notifier must be able to deliver: one
// artifact, one storage and one exclusion of each kind, so a template that
// walks the report meets every record the run can carry.
func Event() model.Event {
	return model.Event{
		Name: "run_finished",
		At:   time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC),
		Run: model.RunReport{
			RunID:     "20261012T042000Z",
			StartedAt: time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC),
			Duration:  model.Duration(5 * time.Minute),
			Outcome:   model.OutcomeDelivered,
			Artifacts: []model.ArtifactResult{{
				Artifact: model.Artifact{Name: "databases/example/postgres", Group: "databases", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
				Outcome:  model.ArtifactOK,
				Bytes:    1 << 20,
				Duration: model.Duration(30 * time.Second),
			}},
			Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
			Excluded: []model.Exclusion{
				{Name: "volumes/example/cache", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker"},
				{Name: "volumes/example/data", Group: "volumes", Kind: model.ExclusionCovered, Reason: `covered by the dump of pod "postgres-0"`},
			},
		},
	}
}

func checkNotify(_ *testing.T, n notifier.Notifier) error {
	if err := n.Notify(context.Background(), Event()); err != nil {
		return fmt.Errorf("Notify: %w", err)
	}
	return nil
}

func checkCancel(t *testing.T, n notifier.Notifier) error {
	return plugintest.MustReturnWithError(t, 15*time.Second, "Notify", func(ctx context.Context) error {
		return n.Notify(ctx, Event())
	})
}
