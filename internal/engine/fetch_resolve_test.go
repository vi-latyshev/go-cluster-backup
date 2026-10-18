package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func TestCheckFetchRequestArtifactRequired(t *testing.T) {
	if err := checkFetchRequest(FetchRequest{RunID: "r"}); err == nil ||
		!strings.Contains(err.Error(), "Artifact") {
		t.Fatalf("RunID without Artifact accepted: %v", err)
	}
}

func TestJoinSkipsEmpty(t *testing.T) {
	if got := joinSkips(nil); got != "no storage was tried" {
		t.Fatalf("joinSkips(nil) = %q", got)
	}
	if got := joinSkips([]StorageSkip{{Storage: "a", Reason: "b"}}); got != "a: b" {
		t.Fatalf("joinSkips = %q", got)
	}
}

func TestFindArtifactMissing(t *testing.T) {
	rep := covManifest("r", "wireguard", "k", []byte("x"))
	if ar := findArtifact(rep, "absent"); ar != nil {
		t.Fatalf("findArtifact found %+v", ar)
	}
	if ar := findArtifact(rep, "wireguard"); ar == nil || ar.Key != "k" {
		t.Fatalf("findArtifact = %+v", ar)
	}
}

func TestInspectRunRefusals(t *testing.T) {
	ok := covManifest("r", "wireguard", "k", []byte("x"))
	noKey := covManifest("r", "wireguard", "", []byte("x"))
	noKey.Artifacts[0].SHA256 = ""
	otherSchema := covManifest("r", "wireguard", "k", []byte("x"))
	otherSchema.Schema = 7

	cases := []struct {
		name string
		sur  survey
		want string
	}{
		{"run absent", survey{ok: true, runs: map[string]*surveyRun{}}, "not present"},
		{"no manifest", covSurvey("r", nil, nil), "no manifest"},
		{"bad schema", covSurvey("r", otherSchema, nil), "schema"},
		{"artifact absent", covSurvey("r", covManifest("r", "other", "k", []byte("x")), nil), "absent from the run"},
		{"artifact failed", covSurvey("r", failedManifest("r"), nil), "failed in run"},
		{"no key", covSurvey("r", noKey, nil), "key and sha256"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ar, reason := inspectRun(tc.sur, "r", "wireguard")
			if ar != nil {
				t.Fatalf("inspectRun returned %+v", ar)
			}
			if !strings.Contains(reason, tc.want) {
				t.Fatalf("reason %q, want %q", reason, tc.want)
			}
		})
	}
	if ar, reason := inspectRun(covSurvey("r", ok, nil), "r", "wireguard"); ar == nil || reason != "" {
		t.Fatalf("ok run refused: %+v %q", ar, reason)
	}
}

func failedManifest(runID string) *model.RunReport {
	rep := covManifest(runID, "wireguard", "k", []byte("x"))
	rep.Artifacts[0].Outcome = model.ArtifactFailed
	return rep
}

func TestNearestOKNoOther(t *testing.T) {
	only := covSurvey("r", covManifest("r", "wireguard", "k", []byte("x")), nil)
	if got := nearestOK(only, "wireguard", "r"); !strings.Contains(got, "no other run") {
		t.Fatalf("nearestOK = %q", got)
	}
	// A second run where the artifact is ok is named as the nearest.
	other := covManifest("other", "wireguard", "k2", []byte("x"))
	other.StartedAt = time.Unix(1800000000, 0).UTC()
	two := only
	two.runs["other"] = &surveyRun{manifest: other}
	// A third run whose artifact is not ok is passed over entirely.
	two.runs["failed"] = &surveyRun{manifest: failedManifest("failed")}
	if got := nearestOK(two, "wireguard", "r"); !strings.Contains(got, "other") {
		t.Fatalf("nearestOK = %q", got)
	}
}

func TestStorageByNameAndOrder(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if sb := e.storageByName("ghost"); sb != nil {
		t.Fatalf("storageByName found %+v", sb)
	}
	if _, err := e.StorageOrder([]string{"ghost"}); err == nil {
		t.Fatal("StorageOrder accepted an unknown storage")
	}
	got, err := e.StorageOrder([]string{"primary"})
	if err != nil || len(got) != 1 || got[0] != "primary" {
		t.Fatalf("StorageOrder = %v, %v", got, err)
	}
	// No restore section and no explicit list.
	if _, err := e.StorageOrder(nil); err == nil {
		t.Fatal("StorageOrder without a restore section and without names succeeded")
	}
}

// --- decodeStages / decodeTo -------------------------------------------------

func TestDecodeStagesRefusals(t *testing.T) {
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})

	cases := []struct {
		name  string
		chain []model.ProcessorStep
		want  string
	}{
		{"no instance name", []model.ProcessorStep{{Type: "fake"}}, "no instance name"},
		{"unknown instance", []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}, "does not have"},
		{"type mismatch", []model.ProcessorStep{{Instance: "zstd3", Type: "other"}}, "in the manifest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := e.decodeStages(context.Background(), tc.chain); err == nil ||
				!strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	// A processor that cannot decode says so here.
	e.processors["zstd3"] = &covProcessor{checkDecodeErr: errors.New("no key mounted")}
	if _, err := e.decodeStages(context.Background(), []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}); err == nil ||
		!strings.Contains(err.Error(), "cannot decode") {
		t.Fatalf("CheckDecode failure not refused: %v", err)
	}
}

func TestResolveFetchRefusals(t *testing.T) {
	ctx := context.Background()

	// A request shape ResolveFetch refuses.
	w := newWorld(t)
	e := w.newEngine(t, withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary"))
	if _, err := e.ResolveFetch(ctx, FetchRequest{Artifact: "a"}); err == nil {
		t.Fatal("ResolveFetch accepted a request with neither RunID nor Newest")
	}

	// No restore section.
	_, bare := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	if _, err := bare.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}

	// Unknown storage in the walk order.
	if _, err := e.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}); err == nil {
		t.Fatal("ResolveFetch accepted an unknown storage")
	}

	// No layout section.
	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	el := w.newEngine(t, noLayout)
	if _, err := el.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}

	// A run prefix that is not a literal plus {{ .Run.ID }}.
	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	ep := w.newEngine(t, badPrefix)
	if _, err := ep.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("bad run prefix: %v", err)
	}

	// No run holds the artifact.
	if _, err := e.ResolveFetch(ctx, FetchRequest{Newest: true, Artifact: "nope"}); err == nil ||
		!strings.Contains(err.Error(), "no run on any storage") {
		t.Fatalf("no candidate run: %v", err)
	}

	// Raw by-key with an unknown storage.
	if _, err := e.ResolveFetch(ctx, FetchRequest{RawStorage: "ghost", RawKey: "k"}); err == nil {
		t.Fatal("ResolveFetch accepted an unknown raw storage")
	}
}

// TestResolveFetchFailedSurvey: one storage answers, another cannot be
// surveyed at all — the plan reports the failed storage as a skip.
func TestResolveFetchFailedSurvey(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, twoStorages()), "primary", "offsite")
	e := runOnce(t, w, cfg)
	runs, err := e.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) == 0 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	w.storage(t, "offsite").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return plugin.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	plan, err := e.ResolveFetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.RunID != runs[0].RunID {
		t.Fatalf("plan run %q", plan.RunID)
	}
	var failed bool
	for _, h := range plan.Storages {
		if h.Storage == "offsite" && !h.Holds && strings.Contains(h.Reason, "survey failed") {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("the failed survey is not reported: %+v", plan.Storages)
	}
}

// --- fetchCandidates ---------------------------------------------------------

func TestFetchCandidatesSkipAndTieBreak(t *testing.T) {
	ctx := context.Background()
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})

	sameAt := time.Unix(1700000000, 0).UTC()
	repA := covManifest("run-a", "wireguard", "ka", []byte("x"))
	repB := covManifest("run-b", "wireguard", "kb", []byte("x"))
	repA.StartedAt, repB.StartedAt = sameAt, sameAt
	sur := survey{ok: true, runs: map[string]*surveyRun{
		"run-a": {manifest: repA},
		"run-b": {manifest: repB},
	}}
	sb := &storageBinding{name: "primary"}
	// One storage cannot be surveyed: it is skipped, not fatal.
	calls := 0
	getSurvey := func(*storageBinding) (survey, bool) {
		calls++
		if calls == 1 {
			return survey{}, false
		}
		return sur, true
	}
	got, err := e.fetchCandidates(ctx, FetchRequest{Newest: true, Artifact: "wireguard"},
		[]*storageBinding{sb, sb}, getSurvey)
	if err != nil {
		t.Fatal(err)
	}
	// Equal StartedAt: the tie breaks on the run id, newest first.
	if len(got) != 2 || got[0] != "run-b" || got[1] != "run-a" {
		t.Fatalf("candidates %v", got)
	}
}

// --- StoredRuns --------------------------------------------------------------

func TestStoredRunsRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	e := w.newEngine(t, withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary"))

	if _, err := e.StoredRuns(ctx, StoredRunsRequest{Storages: []string{"ghost"}}); err == nil {
		t.Fatal("StoredRuns accepted an unknown storage")
	}

	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	el := w.newEngine(t, noLayout)
	if _, err := el.StoredRuns(ctx, StoredRunsRequest{}); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}

	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	ep := w.newEngine(t, badPrefix)
	if _, err := ep.StoredRuns(ctx, StoredRunsRequest{}); err == nil ||
		!strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("bad run prefix: %v", err)
	}

	// A storage that cannot be surveyed fails the listing.
	w.storage(t, "primary").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return plugin.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	if _, err := e.StoredRuns(ctx, StoredRunsRequest{}); err == nil ||
		!strings.Contains(err.Error(), "stored runs") {
		t.Fatalf("a failed survey did not fail the listing: %v", err)
	}
}

func TestStoredRunsTieBreak(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)

	// Two runs whose manifests carry the same StartedAt: the listing breaks the
	// tie on the run id.
	at := time.Unix(1700000000, 0).UTC()
	f := w.storage(t, "primary")
	repA := covManifest("20990101T000000Z", "wireguard", "runs/20990101T000000Z/a", []byte("x"))
	repA.StartedAt = at
	repB := covManifest("20990101T000001Z", "wireguard", "runs/20990101T000001Z/a", []byte("x"))
	repB.StartedAt = at
	f.PutObject("runs/20990101T000000Z/manifest.json", mustMarshal(t, repA), at)
	f.PutObject("runs/20990101T000001Z/manifest.json", mustMarshal(t, repB), at)

	runs, err := e.StoredRuns(ctx, StoredRunsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// The two equal-timestamp runs come back with the higher id first.
	idx := map[string]int{}
	for i, r := range runs {
		idx[r.RunID] = i
	}
	hi, okHi := idx["20990101T000001Z"]
	lo, okLo := idx["20990101T000000Z"]
	if !okHi || !okLo || hi > lo {
		t.Fatalf("tie not broken on the id: %v", runs)
	}
}

// --- Apply -------------------------------------------------------------------
