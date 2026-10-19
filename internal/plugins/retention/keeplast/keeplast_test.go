package keeplast_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention/retentiontest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := keeplast.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func newPolicy(t *testing.T, cfg string) retention.Retention {
	t.Helper()
	p, err := plugin.Build(context.Background(), testRegistry(t), retention.Kind, "keep_last", jsontext.Value(cfg),
		plugin.Deps{Instance: "keep_last", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func decide(t *testing.T, p retention.Retention, runs ...model.StoredRun) retention.Decision {
	t.Helper()
	now := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	d, err := p.Decide(context.Background(), retention.DecideRequest{Now: now, Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func run(id string, daysAgo int, bytes int64) model.StoredRun {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	return model.StoredRun{RunID: id, At: base.AddDate(0, 0, -daysAgo), Bytes: bytes, Verified: true}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestRetentionConformance(t *testing.T) {
	retentiontest.Run(t, func() retention.Retention {
		return newPolicy(t, `{"count": 3, "min_keep": 1, "count_incomplete": true}`)
	})
}

func TestCountKeepsTheNewest(t *testing.T) {
	p := newPolicy(t, `{"count": 3, "min_keep": 1, "count_incomplete": false}`)
	d := decide(t, p, run("a", 0, 10), run("b", 1, 10), run("c", 2, 10), run("d", 3, 10))
	if !slices.Equal(d.Drop, []string{"d"}) {
		t.Fatalf("decision %+v", d)
	}
}

func TestIncompleteRunsAreKeptAndNotCounted(t *testing.T) {
	unfinished := run("unfinished", 0, 10)
	unfinished.Verified = false
	p := newPolicy(t, `{"count": 2, "min_keep": 1, "count_incomplete": false}`)
	d := decide(t, p, unfinished, run("a", 1, 10), run("b", 2, 10), run("c", 3, 10))
	if !slices.Contains(d.Keep, "unfinished") {
		t.Fatalf("unfinished run dropped: %+v", d)
	}
	if !slices.Contains(d.Keep, "a") || !slices.Contains(d.Keep, "b") || !slices.Contains(d.Drop, "c") {
		t.Fatalf("unfinished run occupied a slot: %+v", d)
	}

	p = newPolicy(t, `{"count": 2, "min_keep": 1, "count_incomplete": true}`)
	d = decide(t, p, unfinished, run("a", 1, 10), run("b", 2, 10), run("c", 3, 10))
	if !slices.Contains(d.Drop, "b") {
		t.Fatalf("counted-incomplete did not occupy a slot: %+v", d)
	}
}

// TestBuildReturnsOnCancelledContext: construction owes a cancelled context the
// same prompt return as every other call.
func TestBuildReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := plugin.Build(ctx, testRegistry(t), retention.Kind, "keep_last",
		jsontext.Value(`{"count": 3, "min_keep": 1, "count_incomplete": true}`),
		plugin.Deps{Instance: "keep_last", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("Build with a cancelled context succeeded")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	r := testRegistry(t)
	for _, raw := range []string{
		`{"count": 0, "min_keep": 1, "count_incomplete": false}`,
		`{"count": 3, "min_keep": 0, "count_incomplete": false}`,
		`{"count": 2, "min_keep": 3, "count_incomplete": false}`,
	} {
		if _, err := r.BuildAny(context.Background(), "retention", "keep_last", jsontext.Value(raw), plugin.Deps{Instance: "keep_last"}); err == nil {
			t.Errorf("config %s accepted", raw)
		}
	}
}

// TestTwoRunsOneNightOccupyTwoSlots (critic sub-claim, confirmed): count counts
// runs, not nights — a night with a retry run costs two of the seven slots.
func TestTwoRunsOneNightOccupyTwoSlots(t *testing.T) {
	p := newPolicy(t, `{"count": 2, "min_keep": 1, "count_incomplete": true}`)
	// Two nights, two runs each — a morning run and a retry in the evening.
	n1 := time.Date(2026, 10, 10, 4, 0, 0, 0, time.UTC)
	n2 := time.Date(2026, 10, 11, 4, 0, 0, 0, time.UTC)
	runs := []model.StoredRun{
		{RunID: "n2-evening", At: n2.Add(18 * time.Hour), Bytes: 10, Verified: true},
		{RunID: "n2-morning", At: n2, Bytes: 10, Verified: true},
		{RunID: "n1-evening", At: n1.Add(18 * time.Hour), Bytes: 10, Verified: true},
		{RunID: "n1-morning", At: n1, Bytes: 10, Verified: true},
	}
	d := decide(t, p, runs...)
	slices.Sort(d.Keep)
	slices.Sort(d.Drop)
	if !slices.Equal(d.Keep, []string{"n2-evening", "n2-morning"}) {
		t.Fatalf("kept %v, want both runs of the newest night", d.Keep)
	}
	if !slices.Equal(d.Drop, []string{"n1-evening", "n1-morning"}) {
		t.Fatalf("dropped %v, want the whole older night", d.Drop)
	}
}
