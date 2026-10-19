package gfs_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/gfs"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention/retentiontest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := gfs.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func newPolicy(t *testing.T, cfg string) retention.Retention {
	t.Helper()
	p, err := plugin.Build(context.Background(), testRegistry(t), retention.Kind, "gfs", jsontext.Value(cfg),
		plugin.Deps{Instance: "gfs", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func decide(t *testing.T, p retention.Retention, now time.Time, runs ...model.StoredRun) retention.Decision {
	t.Helper()
	d, err := p.Decide(context.Background(), retention.DecideRequest{Now: now, Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func run(id string, at time.Time) model.StoredRun {
	return model.StoredRun{RunID: id, At: at, Bytes: 10, Verified: true}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestRetentionConformance(t *testing.T) {
	retentiontest.Run(t, func() retention.Retention {
		return newPolicy(t, `{"daily": 7, "weekly": 4, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`)
	})
}

func TestDailyBuckets(t *testing.T) {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	p := newPolicy(t, `{"daily": 7, "weekly": 4, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`)
	var runs []model.StoredRun
	for day := range 10 {
		at := base.AddDate(0, 0, -day)
		runs = append(runs, run(at.Format("20060102T150405Z")+"A", at))
		runs = append(runs, run(at.Format("20060102T150405Z")+"B", at.Add(-time.Hour))) // older run, same day
	}
	d := decide(t, p, base, runs...)
	keep := slices.Contains(d.Keep, base.Format("20060102T150405Z")+"A")
	if !keep {
		t.Fatalf("newest run of today not kept: %v", d.Keep)
	}
	if slices.Contains(d.Keep, base.Format("20060102T150405Z")+"B") {
		t.Fatalf("a second run of the same day was kept by the daily rule: %v", d.Keep)
	}
	if slices.Contains(d.Keep, base.AddDate(0, 0, -9).Format("20060102T150405Z")+"A") {
		t.Fatalf("a run beyond the daily window was kept: %v", d.Keep)
	}
}

func TestWeeklyCatchesWhatDailyMisses(t *testing.T) {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC) // a Monday
	p := newPolicy(t, `{"daily": 2, "weekly": 4, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`)
	var runs []model.StoredRun
	// One run every week for 6 weeks: daily=2 keeps the newest two, weekly=4
	// must reach two weeks further back.
	for week := range 6 {
		at := base.AddDate(0, 0, -7*week)
		runs = append(runs, run(at.Format("20060102T150405Z"), at))
	}
	d := decide(t, p, base, runs...)
	keptWeeks := map[int]bool{}
	for week := range 6 {
		at := base.AddDate(0, 0, -7*week)
		if slices.Contains(d.Keep, at.Format("20060102T150405Z")) {
			keptWeeks[week] = true
		}
	}
	for week := range 4 {
		if !keptWeeks[week] {
			t.Fatalf("week %d not kept: %v", week, d.Keep)
		}
	}
	if keptWeeks[4] || keptWeeks[5] {
		t.Fatalf("a week beyond the weekly window was kept: %v", d.Keep)
	}
}

func TestIncompleteRuns(t *testing.T) {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	unfinished := model.StoredRun{RunID: "unfinished", At: base, Bytes: 10, Verified: false}
	older := run("older", base.AddDate(0, 0, -1))

	p := newPolicy(t, `{"daily": 1, "weekly": 0, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`)
	d := decide(t, p, base, unfinished, older)
	if !slices.Contains(d.Keep, "unfinished") {
		t.Fatalf("an unfinished run was dropped: %+v", d)
	}
	if !slices.Contains(d.Keep, "older") {
		t.Fatalf("an unfinished run occupied a slot: %+v", d)
	}

	p = newPolicy(t, `{"daily": 1, "weekly": 0, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`)
	d = decide(t, p, base, unfinished, older)
	if !slices.Contains(d.Keep, "unfinished") || slices.Contains(d.Keep, "older") {
		t.Fatalf("counted-incomplete did not occupy a slot: %+v", d)
	}
}

func TestMinKeepFloor(t *testing.T) {
	base := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	p := newPolicy(t, `{"daily": 1, "weekly": 0, "monthly": 0, "timezone": "UTC", "min_keep": 3, "count_incomplete": false}`)
	runs := []model.StoredRun{
		run("a", base),
		run("b", base.AddDate(0, 0, -1)),
		run("c", base.AddDate(0, 0, -2)),
		run("d", base.AddDate(0, 0, -3)),
	}
	d := decide(t, p, base, runs...)
	for _, id := range []string{"a", "b", "c"} {
		if !slices.Contains(d.Keep, id) {
			t.Fatalf("floor did not keep %s: %+v", id, d)
		}
	}
	if slices.Contains(d.Keep, "d") {
		t.Fatalf("a run beyond floor and buckets was kept: %+v", d)
	}
}

func TestTimezoneShiftsTheCalendarDay(t *testing.T) {
	// 23:30 UTC on the 11th is already the 12th in Berlin (UTC+1 in October);
	// 20:00 UTC is still the 11th there. With daily=2 UTC sees the two runs as one
	// day (and keeps only the newest), Berlin sees two days (and keeps both).
	at := time.Date(2026, 10, 11, 23, 30, 0, 0, time.UTC)
	other := time.Date(2026, 10, 11, 20, 0, 0, 0, time.UTC)
	p := newPolicy(t, `{"daily": 2, "weekly": 0, "monthly": 0, "timezone": "Europe/Berlin", "min_keep": 1, "count_incomplete": false}`)
	d := decide(t, p, at, run("late", at), run("early", other))
	if !slices.Contains(d.Keep, "late") || !slices.Contains(d.Keep, "early") {
		t.Fatalf("shifted day not applied: %+v", d)
	}
	p = newPolicy(t, `{"daily": 2, "weekly": 0, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`)
	d = decide(t, p, at, run("late", at), run("early", other))
	if !slices.Contains(d.Keep, "late") || slices.Contains(d.Keep, "early") {
		t.Fatalf("UTC day not applied: %+v", d)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	r := testRegistry(t)
	for _, raw := range []string{
		`{"daily": 0, "weekly": 1, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`,
		`{"daily": 1, "weekly": -1, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`,
		`{"daily": 1, "weekly": 1, "monthly": -1, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`,
		`{"daily": 1, "weekly": 1, "timezone": "UTC", "min_keep": 1, "count_incomplete": false}`,
		`{"daily": 1, "weekly": 1, "monthly": 0, "timezone": "Not/AZone", "min_keep": 1, "count_incomplete": false}`,
		`{"daily": 1, "weekly": 1, "monthly": 0, "timezone": "UTC", "min_keep": 0, "count_incomplete": false}`,
	} {
		if _, err := r.BuildAny(context.Background(), "retention", "gfs", jsontext.Value(raw), plugin.Deps{Instance: "gfs"}); err == nil {
			t.Errorf("config %s accepted", raw)
		}
	}
}

// TestBuildReturnsOnCancelledContext: construction owes a cancelled context the
// same prompt return as every other call, before any calendar is read.
func TestBuildReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := plugin.Build(ctx, testRegistry(t), retention.Kind, "gfs",
		jsontext.Value(`{"daily": 7, "weekly": 4, "monthly": 0, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`),
		plugin.Deps{Instance: "gfs", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("Build with a cancelled context succeeded")
	}
}

// keepSet decides and returns the kept ids, sorted, for compact assertions.
func keepSet(t *testing.T, p retention.Retention, now time.Time, runs ...model.StoredRun) []string {
	t.Helper()
	d := decide(t, p, now, runs...)
	slices.Sort(d.Keep)
	slices.Sort(d.Drop)
	if len(d.Keep)+len(d.Drop) != len(runs) {
		t.Fatalf("keep %v + drop %v do not cover %d runs", d.Keep, d.Drop, len(runs))
	}
	return d.Keep
}

// gfsCfg renders a complete gfs config: every test states the members it
// cares about, and a strict decoder never sees one repeated. The byte cap is not
// here — it belongs to the engine, on the storage envelope.
func gfsCfg(daily, weekly, monthly int, tz string, minKeep int, countIncomplete bool) string {
	return fmt.Sprintf(
		`{"daily": %d, "weekly": %d, "monthly": %d, "timezone": %q, "min_keep": %d, "count_incomplete": %t}`,
		daily, weekly, monthly, tz, minKeep, countIncomplete)
}

// TestMonthlyTier: the monthly tier keeps the newest run of each of the newest
// K calendar months; flipping its comparison or forgetting the bucket drops or
// keeps the wrong month points.
func TestMonthlyTier(t *testing.T) {
	t0 := time.Date(2026, 5, 31, 4, 0, 0, 0, time.UTC)
	may := run("may", t0)
	jun := run("jun", time.Date(2026, 6, 15, 4, 0, 0, 0, time.UTC))
	jul := run("jul", time.Date(2026, 7, 1, 4, 0, 0, 0, time.UTC))

	p := newPolicy(t, gfsCfg(1, 0, 2, "UTC", 1, false))
	if got := keepSet(t, p, jul.At, jul, jun, may); !slices.Equal(got, []string{"jul", "jun"}) {
		t.Fatalf("monthly=2 keeps %v, want the newest two month points", got)
	}

	p = newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, false))
	if got := keepSet(t, p, jul.At, jul, jun, may); !slices.Equal(got, []string{"jul"}) {
		t.Fatalf("monthly=0 keeps %v, want the day point only", got)
	}
}

// TestWeekAndMonthPointShareOneRun: the last day of a month (and of an ISO
// week) satisfies several tiers at once; the run is kept once, and it is the
// newest run that takes the slot.
func TestWeekAndMonthPointShareOneRun(t *testing.T) {
	// 2026-10-29 is a Thursday of ISO week 2026-W44; 2026-10-31 is the Saturday
	// of the same week and the last day of the month.
	within := run("within", time.Date(2026, 10, 29, 4, 0, 0, 0, time.UTC))
	point := run("point", time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC))
	p := newPolicy(t, gfsCfg(1, 1, 1, "UTC", 1, false))
	d := decide(t, p, point.At, point, within)
	if !slices.Equal(d.Keep, []string{"point"}) || !slices.Equal(d.Drop, []string{"within"}) {
		t.Fatalf("week and month share one point: keep %v drop %v", d.Keep, d.Drop)
	}
}

// TestMonthBoundaryFollowsTheConfiguredZone: the month bucket is the local
// calendar's, not UTC's — a run late on 31 March is an April point in Berlin,
// and two runs spanning the DST jump share one local day.
func TestMonthBoundaryFollowsTheConfiguredZone(t *testing.T) {
	// 2026-03-31T23:30Z is 2026-04-01 01:30 in Berlin (CEST, +2).
	late := run("late", time.Date(2026, 3, 31, 23, 30, 0, 0, time.UTC))
	// 2026-04-02T10:00Z is April in both calendars, and newer.
	april := run("april", time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC))
	p := newPolicy(t, gfsCfg(1, 0, 1, "Europe/Berlin", 1, false))
	if got := keepSet(t, p, april.At, april, late); !slices.Equal(got, []string{"april"}) {
		t.Fatalf("both runs are April points in Berlin; monthly=1 keeps %v, want the newer", got)
	}

	// The DST jump: 2026-03-29 02:00 CET becomes 03:00 CEST; both instants are
	// the same local day, so one day bucket holds them.
	before := run("before-jump", time.Date(2026, 3, 29, 0, 30, 0, 0, time.UTC))
	after := run("after-jump", time.Date(2026, 3, 29, 1, 30, 0, 0, time.UTC))
	p = newPolicy(t, gfsCfg(2, 0, 2, "Europe/Berlin", 1, false))
	if got := keepSet(t, p, after.At, after, before); !slices.Equal(got, []string{"after-jump"}) {
		t.Fatalf("the DST jump split one local day: kept %v, want only the newer run", got)
	}
}

// TestTwoRunsInOneDayCountOnce: a retry evening after a failed morning is the
// same calendar day — the day bucket is the day's newest run, not both.
func TestTwoRunsInOneDayCountOnce(t *testing.T) {
	morning := run("morning", time.Date(2026, 7, 10, 4, 0, 0, 0, time.UTC))
	evening := run("evening", time.Date(2026, 7, 10, 22, 0, 0, 0, time.UTC))
	p := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, false))
	if got := keepSet(t, p, evening.At, evening, morning); !slices.Equal(got, []string{"evening"}) {
		t.Fatalf("one day holds one slot: kept %v", got)
	}
}

// TestAVerifiedRunTakesThePeriodSlot: with count_incomplete=true a period's
// slot goes to its newest verified run — an unverified run takes it only when
// the period has no verified one. A killed run at the end of a week or month
// would otherwise be the only representative and the complete run beside it
// would age out with it.
func TestAVerifiedRunTakesThePeriodSlot(t *testing.T) {
	// Same ISO week, complete first, killed retry after it.
	complete := run("complete", time.Date(2026, 10, 19, 4, 0, 0, 0, time.UTC)) // Monday, W43
	killed := run("killed", time.Date(2026, 10, 25, 4, 0, 0, 0, time.UTC))     // Sunday, W43
	killed.Verified = false

	// daily=1 covers only the newest day; the week must hold the complete run.
	weekly := newPolicy(t, gfsCfg(1, 1, 0, "UTC", 1, true))
	if got := keepSet(t, weekly, killed.At, killed, complete); !slices.Contains(got, "complete") {
		t.Fatalf("the week's slot went to the killed run: kept %v, want the verified complete run", got)
	}

	// And the month: the complete run three days earlier, killed at month end.
	monthComplete := run("month-complete", time.Date(2026, 10, 27, 4, 0, 0, 0, time.UTC))
	monthKilled := run("month-killed", time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC))
	monthKilled.Verified = false
	monthly := newPolicy(t, gfsCfg(1, 0, 1, "UTC", 1, true))
	if got := keepSet(t, monthly, monthKilled.At, monthKilled, monthComplete); !slices.Contains(got, "month-complete") {
		t.Fatalf("the month's slot went to the killed run: kept %v, want the verified complete run", got)
	}
}

// TestAnIncompletePeriodStillTakesItsSlot: when a period has no verified run
// at all, its newest unverified run keeps the slot (count_incomplete=true);
// with count_incomplete=false the unverified run is force-kept instead and the
// verified run of the same day takes the day's slot.
func TestAnIncompletePeriodStillTakesItsSlot(t *testing.T) {
	complete := run("complete", time.Date(2026, 7, 10, 4, 0, 0, 0, time.UTC))
	incomplete := run("incomplete", time.Date(2026, 7, 10, 22, 0, 0, 0, time.UTC))
	incomplete.Verified = false

	// Only the unverified run on its day: it takes the day's slot.
	lonely := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, true))
	if got := keepSet(t, lonely, incomplete.At, incomplete); !slices.Equal(got, []string{"incomplete"}) {
		t.Fatalf("a period of one unverified run kept %v", got)
	}

	// The verified run of the same day wins the slot; the unverified one stays
	// only through the floor.
	counting := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, true))
	if got := keepSet(t, counting, incomplete.At, incomplete, complete); !slices.Equal(got, []string{"complete", "incomplete"}) {
		t.Fatalf("count_incomplete=true kept %v, want the verified run in the slot and the newest run on the floor", got)
	}

	lenient := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, false))
	if got := keepSet(t, lenient, incomplete.At, incomplete, complete); !slices.Equal(got, []string{"complete", "incomplete"}) {
		t.Fatalf("count_incomplete=false kept %v, want the incomplete force-kept and the complete in the slot", got)
	}
}

// TestAReadableManifestBeatsBareObjectsInThePeriod: under a standing failure
// every run is unverified, and the period's slot still goes to the run this
// build could read — one that reported an outcome — rather than to the objects
// of a run killed before it wrote a manifest, even when those are newer. A
// period with nothing readable keeps its newest objects: the walk cannot judge
// them, and they are all the period has.
func TestAReadableManifestBeatsBareObjectsInThePeriod(t *testing.T) {
	reported := run("reported", time.Date(2026, 5, 4, 4, 0, 0, 0, time.UTC))
	reported.Verified, reported.Manifest = false, true
	killed := run("killed", time.Date(2026, 5, 4, 22, 0, 0, 0, time.UTC))
	killed.Verified, killed.Manifest = false, false

	p := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, true))
	if got := keepSet(t, p, killed.At, killed, reported); !slices.Contains(got, "reported") {
		t.Fatalf("the day's slot went to the manifest-less run: kept %v, want the run with a readable manifest", got)
	}

	bareOld := run("bare-old", time.Date(2026, 5, 11, 4, 0, 0, 0, time.UTC))
	bareOld.Verified, bareOld.Manifest = false, false
	bareNew := run("bare-new", time.Date(2026, 5, 11, 22, 0, 0, 0, time.UTC))
	bareNew.Verified, bareNew.Manifest = false, false

	q := newPolicy(t, gfsCfg(1, 0, 0, "UTC", 1, true))
	if got := keepSet(t, q, bareNew.At, bareNew, bareOld); !slices.Contains(got, "bare-new") {
		t.Fatalf("a period of manifest-less runs kept %v, want its newest", got)
	}
}
