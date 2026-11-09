package loader_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// TestC6_SelectionProblemsRefuseAnOperationalRoleBeforeRun: the loader applies
// the deployment's selection before it dispatches a role. A disable the loader
// refuses to honour — here a Floor plugin — surfaces in Health, and an
// operational role never reaches Run: the refusal is the only outcome, so no
// construction can happen behind the deployment's back.
func TestC6_SelectionProblemsRefuseAnOperationalRoleBeforeRun(t *testing.T) {
	ran := false
	register := func(r *loader.Registry) error {
		addSelectable(t, r, loader.Spec{ID: "workflow-backup", Floor: true})
		withSelection(t, r, "config", "workflow-backup")
		addRole(t, r, "backup-plugin", "backup-job", false, func(context.Context, loader.Session) int {
			ran = true
			return 0
		})
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"backup-job"}})
	if code != 1 || ran {
		t.Fatalf("Main = %d, ran %v, stderr %q, want the selection refusal before Run", code, ran, stderr)
	}
	if !strings.Contains(stderr, `selection: floor plugin "workflow-backup" cannot be disabled`) {
		t.Fatalf("stderr %q, want the floor problem named", stderr)
	}
}

// TestC6_LibraryDisablesRefuseAnOperationalRoleBeforeRun: a library is
// mandatory infrastructure for the plugins that import it, so the loader keeps
// it active and reports the refused disable. An operational role then refuses
// before Run exactly as it does for a Floor plugin.
func TestC6_LibraryDisablesRefuseAnOperationalRoleBeforeRun(t *testing.T) {
	ran := false
	register := func(r *loader.Registry) error {
		if err := loader.SetImports(r, loader.Imports{"backup-plugin": {"model"}}); err != nil {
			return err
		}
		addPlugin(t, r, loader.Spec{ID: "model"}, nil)
		withSelection(t, r, "config", "model")
		addRole(t, r, "backup-plugin", "backup-job", false, func(context.Context, loader.Session) int {
			ran = true
			return 0
		})
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"backup-job"}})
	if code != 1 || ran {
		t.Fatalf("Main = %d, ran %v, stderr %q, want the library refusal before Run", code, ran, stderr)
	}
	if !strings.Contains(stderr, `selection: library plugin "model" is always active and cannot be disabled`) {
		t.Fatalf("stderr %q, want the library problem named", stderr)
	}
}
