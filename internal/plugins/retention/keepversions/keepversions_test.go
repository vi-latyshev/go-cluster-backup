package keepversions_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keepversions"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/retentiontest"
)

func testPlugin(t *testing.T) (*loader.Registry, *instances.Registry) {
	t.Helper()
	lr := loader.New()
	factories := instances.NewRegistry()
	if err := loader.Add(lr, keepversions.Spec, func(lr *loader.Registry) error {
		return keepversions.Apply(lr, factories)
	}); err != nil {
		t.Fatal(err)
	}
	return lr, factories
}

func testRegistry(t *testing.T) *instances.Registry {
	t.Helper()
	_, factories := testPlugin(t)
	return factories
}

func newPolicy(t *testing.T, cfg string) retention.Retention {
	t.Helper()
	p, err := instances.Build(context.Background(), testRegistry(t), retention.Kind, "keep_versions", jsontext.Value(cfg),
		instances.Deps{Instance: "keep_versions", Log: slog.New(slog.DiscardHandler)})
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

// holder is one run with a successful copy of identities and an observed list.
func holder(id string, daysAgo int, artifacts, observed []string) model.StoredRun {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	return model.StoredRun{RunID: id, At: base.AddDate(0, 0, -daysAgo), Bytes: 10, Verified: true,
		Manifest: true, Artifacts: artifacts, ObservedArtifacts: observed}
}

func sorted(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

// TestApplyContributesCompiledDescriptor: registration is memory-only and the
// descriptor stays visible whatever the selection.
func TestApplyContributesCompiledDescriptor(t *testing.T) {
	lr, _ := testPlugin(t)
	ds := loader.All(lr, instances.Descriptors)
	if len(ds) != 1 {
		t.Fatalf("descriptors %+v", ds)
	}
	if ds[0].ID != "retention/keep_versions" || ds[0].ID != keepversions.Spec.ID {
		t.Fatalf("descriptor id %q, spec id %q", ds[0].ID, keepversions.Spec.ID)
	}
}

func TestRetentionConformance(t *testing.T) {
	retentiontest.Run(t, func() retention.Retention {
		return newPolicy(t, `{"count": 3, "min_keep": 1, "count_incomplete": true}`)
	})
}

// TestKeepsNewestCopiesOfALiveIdentity: a live identity keeps its newest count
// successful holders wherever they rank among the runs, so an older run with no
// successful copy is dropped while older copies of the live identity stay.
func TestKeepsNewestCopiesOfALiveIdentity(t *testing.T) {
	p := newPolicy(t, `{"count": 2, "min_keep": 1, "count_incomplete": true}`)
	d := decide(t, p,
		holder("new", 0, []string{"db/a"}, []string{"db/a"}),
		holder("older-good", 1, []string{"db/a"}, []string{"db/a"}),
		holder("failed", 2, nil, []string{"db/a"}),
		holder("oldest-good", 3, []string{"db/a"}, []string{"db/a"}),
	)
	if !slices.Equal(sorted(d.Keep), []string{"new", "older-good"}) {
		t.Fatalf("kept %v, want the two newest successful copies", d.Keep)
	}
	if !slices.Equal(sorted(d.Drop), []string{"failed", "oldest-good"}) {
		t.Fatalf("dropped %v, want the failed run in count and the oldest copy", d.Drop)
	}
}

// TestFailedRunWithAGoodCopyCounts: a run's outcome does not decide its copy's
// rank — the newest successful copy of a live identity stays even when the run
// itself failed.
func TestFailedRunWithAGoodCopyCounts(t *testing.T) {
	unverified := holder("latest", 0, []string{"db/a"}, []string{"db/a"})
	unverified.Verified = false
	p := newPolicy(t, `{"count": 1, "min_keep": 1, "count_incomplete": true}`)
	d := decide(t, p, unverified, holder("older", 1, []string{"db/a"}, []string{"db/a"}))
	if !slices.Equal(d.Keep, []string{"latest"}) {
		t.Fatalf("kept %v, want the newest successful copy on the failed run", d.Keep)
	}
}

// TestDeadIdentityAddsNoPolicyKeep: an identity named only by runs outside the
// newest count is dead, and its successful copies earn no keep — only the
// protection floor still pins the newest one.
func TestDeadIdentityAddsNoPolicyKeep(t *testing.T) {
	p := newPolicy(t, `{"count": 1, "min_keep": 1, "count_incomplete": true}`)
	live := holder("new", 0, []string{"db/live"}, []string{"db/live"})
	dropped := holder("dropped", 1, []string{"db/dropped"}, nil)
	d := decide(t, p, live, dropped)
	if !slices.Equal(d.Keep, []string{"new"}) {
		t.Fatalf("kept %v, want only the live identity's copy", d.Keep)
	}
	if !slices.Contains(d.Drop, "dropped") {
		t.Fatalf("a dead identity's copy was kept: %+v", d)
	}
}

// TestNoSuccessfulCopyFallsBackPerCountIncomplete: a run with no successful copy
// at all is kept outside the slots when count_incomplete is false, and only
// while it ranks among the newest count runs when it is true. Its keep never
// depends on whether its observed identities are live.
func TestNoSuccessfulCopyFallsBackPerCountIncomplete(t *testing.T) {
	runs := []model.StoredRun{
		holder("new", 0, []string{"db/a"}, []string{"db/a"}),
		holder("dead", 1, nil, []string{"db/gone"}),
	}
	p := newPolicy(t, `{"count": 1, "min_keep": 1, "count_incomplete": false}`)
	d := decide(t, p, runs...)
	if !slices.Contains(d.Keep, "dead") {
		t.Fatalf("an empty-handed run was dropped with count_incomplete false: %+v", d)
	}
	p = newPolicy(t, `{"count": 1, "min_keep": 1, "count_incomplete": true}`)
	d = decide(t, p, runs...)
	if slices.Contains(d.Keep, "dead") {
		t.Fatalf("an empty-handed run outside the newest count was kept: %+v", d)
	}
	runs[1] = holder("dead-first", 0, nil, nil)
	runs[0] = holder("new", 1, []string{"db/a"}, []string{"db/a"})
	d = decide(t, p, runs...)
	if !slices.Contains(d.Keep, "dead-first") {
		t.Fatalf("the newest empty-handed run was dropped with count_incomplete true: %+v", d)
	}
}

// TestFloorNamesTheNewestCopies: the floor is the newest min_keep successful
// copies of every live identity, so a byte budget cannot cut below it, and the
// floor only ever names kept runs.
func TestFloorNamesTheNewestCopies(t *testing.T) {
	p := newPolicy(t, `{"count": 3, "min_keep": 2, "count_incomplete": true}`)
	all := []string{"db/a", "db/b"}
	d := decide(t, p,
		holder("r1", 0, all, all),
		holder("r2", 1, all, all),
		holder("r3", 2, all, all),
	)
	if !slices.Equal(sorted(d.KeepFloor), []string{"r1", "r2"}) {
		t.Fatalf("floor %v, want the two newest holders", d.KeepFloor)
	}
	for _, id := range d.KeepFloor {
		if !slices.Contains(d.Keep, id) {
			t.Fatalf("floor run %s is not kept: %+v", id, d)
		}
	}
}

// TestWholeRunHistoryIsNotACopyOfEverything: a run holding one identity's copy
// never stands in for another identity whose copies are all gone.
func TestWholeRunHistoryIsNotACopyOfEverything(t *testing.T) {
	p := newPolicy(t, `{"count": 2, "min_keep": 1, "count_incomplete": true}`)
	d := decide(t, p,
		holder("holds-b", 0, []string{"db/b"}, []string{"db/a", "db/b"}),
		holder("holds-a-old", 1, []string{"db/a"}, []string{"db/a", "db/b"}),
		holder("holds-a-oldest", 2, []string{"db/a"}, []string{"db/a"}),
	)
	// db/a is live (named by the newest run) and db/b is live; each keeps its
	// newest two copies: db/b has one holder, db/a has two.
	if !slices.Equal(sorted(d.Keep), []string{"holds-a-old", "holds-a-oldest", "holds-b"}) {
		t.Fatalf("kept %v", d.Keep)
	}
}

func TestBuildReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := instances.Build(ctx, testRegistry(t), retention.Kind, "keep_versions",
		jsontext.Value(`{"count": 7, "min_keep": 1, "count_incomplete": true}`),
		instances.Deps{Instance: "keep_versions", Log: slog.New(slog.DiscardHandler)})
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
		if _, err := r.BuildAny(context.Background(), "retention", "keep_versions", jsontext.Value(raw), instances.Deps{Instance: "keep_versions"}); err == nil {
			t.Errorf("config %s accepted", raw)
		}
	}
}
