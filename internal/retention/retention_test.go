package retention_test

import (
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
)

func run(id string, at time.Time) model.StoredRun { return model.StoredRun{RunID: id, At: at} }

// TestNewestFirstOrdersByTimeAndTiesByID pins the one order every policy reads
// its calendar in: newest first, and two runs of the same instant by ascending
// run id — flipping either comparison flips the asserted order.
func TestNewestFirstOrdersByTimeAndTiesByID(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	runs := []model.StoredRun{
		run("b", t0), run("old", t0.Add(-time.Hour)), run("newer", t0.Add(time.Hour)), run("a", t0),
	}
	retention.NewestFirst(runs)
	got := []string{runs[0].RunID, runs[1].RunID, runs[2].RunID, runs[3].RunID}
	want := []string{"newer", "a", "b", "old"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}

	retention.NewestFirst(nil) // an empty request is not a panic
	retention.NewestFirst([]model.StoredRun{run("only", t0)})
}

// TestDecidedIsAPartition: every run of the request lands in exactly one of
// Keep and Drop, in request order. Swapping the two appends, iterating the keep
// map instead of the runs (random order, phantom ids), or dropping the else
// branch breaks the assertions.
func TestDecidedIsAPartition(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	runs := []model.StoredRun{run("a", t0), run("b", t0), run("c", t0)}

	cases := []struct {
		name     string
		keep     map[string]bool
		wantKeep []string
		wantDrop []string
	}{
		{"partial", map[string]bool{"a": true, "c": true}, []string{"a", "c"}, []string{"b"}},
		{"none kept", map[string]bool{}, nil, []string{"a", "b", "c"}},
		{"all kept", map[string]bool{"a": true, "b": true, "c": true}, []string{"a", "b", "c"}, nil},
		// An id that is not a run of the request is ignored: the decision
		// classifies the request, not the map.
		{"unknown id in the set", map[string]bool{"a": true, "zz": true}, []string{"a"}, []string{"b", "c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := retention.Decided(runs, tc.keep)
			assertIDs(t, "keep", d.Keep, tc.wantKeep)
			assertIDs(t, "drop", d.Drop, tc.wantDrop)
		})
	}
}

func assertIDs(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	}
}

// TestBudgetKeepsTheNewestThatFit pins the one byte-budget rule: entries are
// taken newest first while the running total fits, the boundary (total equal
// the budget) keeps the entry, and the floor outranks the budget. Flipping the
// comparison, dropping the floor clause or returning everything breaks it.
func TestBudgetKeepsTheNewestThatFit(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	runs := []model.StoredRun{
		{RunID: "new", At: t0, Bytes: 10},
		{RunID: "mid", At: t0.Add(-time.Hour), Bytes: 10},
		{RunID: "old", At: t0.Add(-2 * time.Hour), Bytes: 10},
	}
	cases := []struct {
		name     string
		maxBytes int64
		minKeep  int
		want     int
	}{
		{"two fit", 25, 1, 2},
		{"the boundary counts as fitting", 30, 1, 3},
		{"one fits", 15, 1, 1},
		{"nothing fits, the floor holds", 5, 1, 1},
		{"the floor outranks the budget", 5, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retention.Budget(runs, tc.maxBytes, tc.minKeep); got != tc.want {
				t.Fatalf("Budget(%d, %d) = %d, want %d", tc.maxBytes, tc.minKeep, got, tc.want)
			}
		})
	}
}
