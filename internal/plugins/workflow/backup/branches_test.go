package backup

// Run's refusal branches: what a run refuses before it touches anything.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
)

func branchWorkflow() *Workflow {
	return &Workflow{runConfigured: true, catalogConfigured: true, layout: goodLayout(), retry: retryOnce(), log: slog.New(slog.DiscardHandler)}
}

// goodLayout is one valid key layout: a run prefix with the literal survey head.
func goodLayout() layout.Layout {
	type doc struct {
		Prefix configuration.Template `json:"prefix"`
	}
	var d doc
	if err := configuration.Decode(jsontext.Value(`{"prefix":"runs/{{ .Run.ID }}"}`), &d); err != nil {
		panic(err)
	}
	return layout.New(layout.Config{RunPrefix: d.Prefix})
}

func retryOnce() execution.Options { return execution.Options{Attempts: 1} }

func TestRunRefusesWithoutARunSection(t *testing.T) {
	w := branchWorkflow()
	w.runConfigured = false
	if _, err := w.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err == nil || !strings.Contains(err.Error(), "needs the run section") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunRefusesAnInvalidKind(t *testing.T) {
	if _, err := branchWorkflow().Run(context.Background(), RunRequest{}); err == nil || !strings.Contains(err.Error(), "kind must be") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunRefusesABadRunPrefix(t *testing.T) {
	w := branchWorkflow()
	w.layout = layout.New(layout.Config{RunPrefix: configuration.Template{}})
	if _, err := w.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err == nil || !strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunRefusesAManualRunWithoutACatalog(t *testing.T) {
	w := branchWorkflow()
	w.catalogConfigured = false
	if _, err := w.Run(context.Background(), RunRequest{Kind: model.RunKindManual}); err == nil || !strings.Contains(err.Error(), "needs a catalog") {
		t.Fatalf("err = %v", err)
	}
}

func branchStorage(name string, scheduled, manual bool, cap model.ByteSize) *storageBinding {
	return &storageBinding{name: name, cfg: Storage{Name: name, MaxTotalBytes: cap}, retention: map[model.RunKind]retention.Retention{
		model.RunKindScheduled: keepLast(),
		model.RunKindManual:    keepLast(),
	}}
}

// stubPolicy keeps everything.
type stubPolicy struct{}

func (stubPolicy) Decide(context.Context, retention.DecideRequest) (retention.Decision, error) {
	return retention.Decision{}, nil
}

func keepLast() retention.Retention { return stubPolicy{} }

func TestRunRefusesMissingPoliciesAndCaps(t *testing.T) {
	t.Run("scheduled retention", func(t *testing.T) {
		w := branchWorkflow()
		sb := branchStorage("alpha", false, false, 1)
		sb.retention = map[model.RunKind]retention.Retention{}
		w.storages = []*storageBinding{sb}
		err := firstError(w)
		if !strings.Contains(err, "retention/scheduled is missing") {
			t.Fatalf("err = %s", err)
		}
	})
	t.Run("cap", func(t *testing.T) {
		w := branchWorkflow()
		w.storages = []*storageBinding{branchStorage("alpha", true, true, 0)}
		if err := firstError(w); !strings.Contains(err, "max_total_bytes is missing") {
			t.Fatalf("err = %s", err)
		}
	})
	t.Run("manual retention", func(t *testing.T) {
		w := branchWorkflow()
		sb := branchStorage("alpha", true, true, 1)
		delete(sb.retention, model.RunKindManual)
		w.storages = []*storageBinding{sb}
		if err := firstErrorKind(w, model.RunKindManual); !strings.Contains(err, "retention/manual is missing") {
			t.Fatalf("err = %s", err)
		}
	})
}

func TestRunRefusesAMissingBinary(t *testing.T) {
	w := branchWorkflow()
	w.instances = []planning.Instance{{Kind: "source", Name: "vpn"}}
	w.binaries = func(string, string) []string { return []string{"gcb-no-such-binary"} }
	w.storages = []*storageBinding{branchStorage("alpha", true, true, 1)}
	err := firstError(w)
	if !strings.Contains(err, "preflight:") || !strings.Contains(err, "gcb-no-such-binary") {
		t.Fatalf("err = %s", err)
	}
}

func firstError(w *Workflow) string { return firstErrorKind(w, model.RunKindScheduled) }

func firstErrorKind(w *Workflow, kind model.RunKind) string {
	_, err := w.Run(context.Background(), RunRequest{Kind: kind})
	if err == nil {
		return ""
	}
	return err.Error()
}
