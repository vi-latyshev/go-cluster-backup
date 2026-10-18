// Package catalogtest is the conformance suite every catalog implementation runs:
//
//	catalogtest.Run(t, func(t *testing.T) catalog.Catalog { return newTestCatalog(t) })
//
// newFn must return a fresh, empty catalog for every call.
package catalogtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
)

type Check struct {
	Name string
	Run  func(t *testing.T, c catalog.Catalog) error
}

var Checks = []Check{
	{"save and load", checkSaveLoad},
	{"save upserts by run id", checkUpsert},
	{"load is newest first and limited", checkOrderAndLimit},
	{"forget removes and tolerates unknown ids", checkForget},
	{"returns on cancellation", checkCancel},
}

func Run(t *testing.T, newFn func(t *testing.T) catalog.Catalog) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(t, newFn(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var base = time.Date(2026, 10, 10, 4, 20, 0, 0, time.UTC)

func report(id string, at time.Time, outcome model.Outcome) model.RunReport {
	return model.RunReport{RunID: id, StartedAt: at, Outcome: outcome}
}

func loadAll(t *testing.T, c catalog.Catalog) ([]model.RunReport, error) {
	t.Helper()
	return c.Load(context.Background(), catalog.LoadRequest{})
}

func checkSaveLoad(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err != nil {
		return fmt.Errorf("Save: %w", err)
	}
	runs, err := loadAll(t, c)
	if err != nil {
		return fmt.Errorf("Load: %w", err)
	}
	if len(runs) != 1 || runs[0].RunID != "r1" || runs[0].Outcome != model.OutcomeDelivered {
		return fmt.Errorf("Load returned %+v", runs)
	}
	return nil
}

func checkUpsert(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	if err := c.Save(ctx, report("r1", base, model.OutcomeRunning)); err != nil {
		return fmt.Errorf("Save running: %w", err)
	}
	if err := c.Save(ctx, report("r1", base, model.OutcomeDelivered)); err != nil {
		return fmt.Errorf("Save final: %w", err)
	}
	runs, err := loadAll(t, c)
	if err != nil {
		return fmt.Errorf("Load: %w", err)
	}
	if len(runs) != 1 {
		return fmt.Errorf("saving the same run id twice produced %d entries", len(runs))
	}
	if runs[0].Outcome != model.OutcomeDelivered {
		return fmt.Errorf("upsert kept outcome %q, want delivered", runs[0].Outcome)
	}
	return nil
}

func checkOrderAndLimit(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	for i, id := range []string{"r1", "r2", "r3"} {
		if err := c.Save(ctx, report(id, base.AddDate(0, 0, i), model.OutcomeDelivered)); err != nil {
			return fmt.Errorf("Save: %w", err)
		}
	}
	runs, err := c.Load(ctx, catalog.LoadRequest{Limit: 2})
	if err != nil {
		return fmt.Errorf("Load: %w", err)
	}
	if len(runs) != 2 || runs[0].RunID != "r3" || runs[1].RunID != "r2" {
		return fmt.Errorf("Load limit 2 returned %+v, want r3 then r2", runIDs(runs))
	}
	runs, err = loadAll(t, c)
	if err != nil {
		return fmt.Errorf("Load: %w", err)
	}
	if len(runs) != 3 || runs[0].RunID != "r3" || runs[2].RunID != "r1" {
		return fmt.Errorf("Load all returned %+v, want r3 r2 r1", runIDs(runs))
	}
	return nil
}

func runIDs(runs []model.RunReport) []string {
	var ids []string
	for _, r := range runs {
		ids = append(ids, r.RunID)
	}
	return ids
}

func checkForget(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	for _, id := range []string{"r1", "r2"} {
		if err := c.Save(ctx, report(id, base, model.OutcomeDelivered)); err != nil {
			return fmt.Errorf("Save: %w", err)
		}
	}
	if err := c.Forget(ctx, []string{"r1", "never-saved"}); err != nil {
		return fmt.Errorf("Forget: %w", err)
	}
	runs, err := loadAll(t, c)
	if err != nil {
		return fmt.Errorf("Load: %w", err)
	}
	if len(runs) != 1 || runs[0].RunID != "r2" {
		return fmt.Errorf("after Forget, Load returned %v", runIDs(runs))
	}
	return nil
}

func checkCancel(t *testing.T, c catalog.Catalog) error {
	ops := []struct {
		name string
		fn   func(ctx context.Context) error
	}{
		{"Save", func(ctx context.Context) error { return c.Save(ctx, report("r1", base, model.OutcomeDelivered)) }},
		{"Load", func(ctx context.Context) error { _, err := c.Load(ctx, catalog.LoadRequest{}); return err }},
		{"Forget", func(ctx context.Context) error { return c.Forget(ctx, []string{"r1"}) }},
	}
	for _, op := range ops {
		if err := plugintest.MustReturnWithError(t, 5*time.Second, op.name, op.fn); err != nil {
			return err
		}
	}
	return nil
}
