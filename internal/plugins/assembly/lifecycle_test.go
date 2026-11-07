package assembly

// The lifecycle budgets under plugins.config.lifecycle: decoded strictly, or
// absent, which leaves no bound configured.

import (
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
)

func TestBuildLifecycleReadsTheBudgetBlock(t *testing.T) {
	e := &Assembly{merged: jsontext.Value(`{"plugins":{"bundles":[],"disabled":[],"config":{"lifecycle":{` +
		`"join":"1m","abort_total":"2m","close":"30s","margin":"30s"}}}}`)}
	if err := e.buildLifecycle(); err != nil {
		t.Fatal(err)
	}
	if e.lifecycle.Join.Std().Minutes() != 1 || e.lifecycle.AbortTotal.Std().Minutes() != 2 {
		t.Fatalf("lifecycle = %+v", e.lifecycle)
	}
}

func TestBuildLifecycleAcceptsAnAbsentBlock(t *testing.T) {
	e := &Assembly{merged: jsontext.Value(`{"plugins":{"bundles":[],"disabled":[],"config":{}}}`)}
	if err := e.buildLifecycle(); err != nil {
		t.Fatal(err)
	}
	if e.lifecycle != (lifecycle.Config{}) {
		t.Fatalf("lifecycle = %+v", e.lifecycle)
	}
}

func TestBuildLifecycleRefusesABadBudget(t *testing.T) {
	e := &Assembly{merged: jsontext.Value(`{"plugins":{"bundles":[],"disabled":[],"config":{"lifecycle":{` +
		`"join":"soon","abort_total":"2m","close":"30s","margin":"30s"}}}}`)}
	err := e.buildLifecycle()
	if err == nil || !strings.Contains(err.Error(), "/plugins/config/lifecycle") {
		t.Fatalf("err = %v", err)
	}
}
