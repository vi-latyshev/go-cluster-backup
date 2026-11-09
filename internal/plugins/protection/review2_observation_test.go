package protection

import (
	"context"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"testing"
)

func TestR2_M7_FailedBeginCannotBridgeFailedSurvey(t *testing.T) {
	ctx := context.Background()
	names := []string{"primary"}
	full := map[string]inventory.Survey{"primary": {OK: true}}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	first := testLease(cat, nil)
	if _, _, err := first.Acquire(ctx, model.RunKindScheduled, "first"); err != nil {
		t.Fatal(err)
	}
	if err := first.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "first", names, full); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	middle := testLease(cat, nil)
	if _, _, err := middle.Acquire(ctx, model.RunKindScheduled, "middle"); err != nil {
		t.Fatal(err)
	}
	cat.observeErr = errors.New("catalog unavailable")
	if err := middle.BeginObservation(ctx); err == nil {
		t.Fatal("expected marker failure")
	}
	// Production run.go:391-431 still surveys, then skips Settle on observationBroken.
	failed := map[string]inventory.Survey{"primary": {OK: false}}
	if failed["primary"].OK {
		t.Fatal("fixture must be failed survey")
	}
	cat.observeErr = nil
	if err := middle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	last := testLease(cat, nil)
	if _, _, err := last.Acquire(ctx, model.RunKindScheduled, "last"); err != nil {
		t.Fatal(err)
	}
	if err := last.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	settled, err := last.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "last", names, full)
	if err != nil {
		t.Fatal(err)
	}
	if settled || cat.Uncertainty() == nil {
		t.Fatal("failed middle survey did not break consecutive observations")
	}
}
