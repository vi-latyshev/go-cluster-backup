package backup_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

type reviewMarkerFailure struct {
	*catalogtest.Leaser
	observes int
}

func (r *reviewMarkerFailure) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	r.observes++
	if r.observes == 3 {
		return errors.New("catalog marker unavailable")
	}
	return r.Leaser.ObserveUncertainty(ctx, lease, digest)
}
func TestR2_M7_MarkerFailureRefusesBeforeSurvey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		config := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		config["catalog"] = instance("review_marker")
		record := &reviewMarkerFailure{Leaser: catalogtest.NewLeaser()}
		registerCatalogType(t, w, "review_marker", record)
		record.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
		failedLists := 0
		for i := 0; i < 3; i++ {
			e := w.newAssembly(t, config)
			f := w.storage(t, "alpha")
			if i == 1 {
				f.Fault = func(op storagetest.Op, _ string, _ int) error {
					if op == storagetest.OpList {
						failedLists++
						return pluginerrors.Permanent(errors.New("survey unavailable"))
					}
					return nil
				}
			} else {
				f.Fault = nil
			}
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if i == 1 {
				if failedLists != 0 {
					t.Fatalf("the run whose marker failed still surveyed the storage (%d lists)", failedLists)
				}
				if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || rep.Lease.Error == "" {
					t.Fatalf("the run whose marker failed was not refused: %+v", rep)
				}
			}
			if i != 1 && rep.Outcome == model.OutcomeFailed {
				t.Fatalf("full-survey run failed: %+v", rep.Storages)
			}
			time.Sleep(time.Second)
		}
		if record.Uncertainty() == nil {
			t.Fatal("failed middle survey was bridged by two successful nonconsecutive surveys")
		}
	})
}
