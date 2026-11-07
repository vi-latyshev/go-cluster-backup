package restore

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

func TestRequestAndArtifactHelpers(t *testing.T) {
	if err := checkFetchRequest(FetchRequest{RunID: "r"}); err == nil || !strings.Contains(err.Error(), "Artifact") {
		t.Fatalf("RunID without Artifact accepted: %v", err)
	}
	if got := joinSkips(nil); got != "no storage was tried" {
		t.Fatalf("joinSkips(nil) = %q", got)
	}
	if got := joinSkips([]StorageSkip{{Storage: "a", Reason: "b"}}); got != "a: b" {
		t.Fatalf("joinSkips = %q", got)
	}
	rep := covManifest("r", "wireguard", "k", []byte("x"))
	if ar := findArtifact(rep, "absent"); ar != nil {
		t.Fatalf("findArtifact found %+v", ar)
	}
	if ar := findArtifact(rep, "wireguard"); ar == nil || ar.Key != "k" {
		t.Fatalf("findArtifact = %+v", ar)
	}
	if artifactOK(rep, "absent") {
		t.Fatal("artifactOK found an absent artifact")
	}
}

func TestInspectRunRefusals(t *testing.T) {
	ok := covManifest("r", "wireguard", "k", []byte("x"))
	noKey := covManifest("r", "wireguard", "", []byte("x"))
	noKey.Artifacts[0].SHA256 = ""
	otherSchema := covManifest("r", "wireguard", "k", []byte("x"))
	otherSchema.Schema = 7
	noKind := covManifest("r", "wireguard", "k", []byte("x"))
	noKind.Kind = ""

	cases := []struct {
		name string
		sur  inventory.Survey
		want string
	}{
		{"run absent", inventory.Survey{OK: true, Runs: map[string]inventory.Run{}}, "not present"},
		{"no manifest", covSurvey(map[string]*model.RunReport{"r": nil}), "no manifest"},
		{"bad schema", covSurvey(map[string]*model.RunReport{"r": otherSchema}), "schema"},
		{"no kind", covSurvey(map[string]*model.RunReport{"r": noKind}), "kind"},
		{"artifact absent", covSurvey(map[string]*model.RunReport{"r": covManifest("r", "other", "k", []byte("x"))}), "absent from the run"},
		{"artifact failed", covSurvey(map[string]*model.RunReport{"r": failedManifest("r")}), "failed in run"},
		{"no key", covSurvey(map[string]*model.RunReport{"r": noKey}), "key and sha256"},
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
	if ar, reason := inspectRun(covSurvey(map[string]*model.RunReport{"r": ok}), "r", "wireguard"); ar == nil || reason != "" {
		t.Fatalf("ok run refused: %+v %q", ar, reason)
	}
}

func TestNearestOK(t *testing.T) {
	only := covSurvey(map[string]*model.RunReport{"r": covManifest("r", "wireguard", "k", []byte("x"))})
	if got := nearestOK(only, "wireguard", "r"); !strings.Contains(got, "no other run") {
		t.Fatalf("nearestOK = %q", got)
	}
	two := only
	other := covManifest("other", "wireguard", "k2", []byte("x"))
	other.StartedAt = other.StartedAt.Add(time.Hour)
	two.Runs["other"] = inventory.Run{Manifest: other}
	two.Runs["failed"] = inventory.Run{Manifest: failedManifest("failed")}
	if got := nearestOK(two, "wireguard", "r"); !strings.Contains(got, "other") {
		t.Fatalf("nearestOK = %q", got)
	}
}

func TestStorageByNameAndOrder(t *testing.T) {
	w := newWorld(t)
	w.addStorage("primary")
	flow := w.flow()
	if sb := flow.storageByName("ghost"); sb != nil {
		t.Fatalf("storageByName found %+v", sb)
	}
	if _, err := flow.StorageOrder([]string{"ghost"}); err == nil {
		t.Fatal("StorageOrder accepted an unknown storage")
	}
	got, err := flow.StorageOrder([]string{"primary"})
	if err != nil || len(got) != 1 || got[0] != "primary" {
		t.Fatalf("StorageOrder = %v, %v", got, err)
	}
	// The default walk is restore.storage_order.
	if got, err := flow.StorageOrder(nil); err != nil || len(got) != 1 {
		t.Fatalf("default StorageOrder = %v, %v", got, err)
	}
	// No restore section and no explicit list.
	w.configured = false
	if _, err := w.flow().StorageOrder(nil); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}
}

func TestPrefixRefusesAMissingLayout(t *testing.T) {
	w := newWorld(t)
	w.addStorage("primary")
	w.layoutCfg.RunPrefix = mustTemplate(t, "")
	if _, err := w.flow().prefix("fetch"); err == nil || !strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("empty run prefix accepted: %v", err)
	}
	w.layoutCfg = testLayoutConfig(t)
	w.layoutCfg.RunPrefix = mustTemplate(t, "{{ .Run.ID }}")
	if _, err := w.flow().prefix("fetch"); err == nil || !strings.Contains(err.Error(), "literal") {
		t.Fatalf("action-only run prefix accepted: %v", err)
	}
}

func TestDecodeStagesRefusals(t *testing.T) {
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()
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
	w.processor("zstd3").checkDecodeErr = errors.New("no key mounted")
	if _, err := e.decodeStages(context.Background(), []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}); err == nil ||
		!strings.Contains(err.Error(), "cannot decode") {
		t.Fatalf("CheckDecode failure not refused: %v", err)
	}
}

func TestEventWithoutALoggerIsSilent(t *testing.T) {
	w := newWorld(t)
	w.addStorage("primary")
	New(Options{Layout: layout.New(w.layoutCfg)}).event(closeFailure())
}

func TestEventReachesTheLogger(t *testing.T) {
	var buf strings.Builder
	w := newWorld(t)
	w.addStorage("primary")
	w.flow(func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&buf, nil)) }).event(closeFailure())
	if !strings.Contains(buf.String(), "target.close.failed") {
		t.Fatalf("event was not logged: %s", buf.String())
	}
}

func closeFailure() human.Event {
	return human.Failure("target.close.failed", human.Text("reason", "closed"))
}

func TestStoredRunsRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()

	if _, err := e.StoredRuns(ctx, []string{"ghost"}); err == nil {
		t.Fatal("StoredRuns accepted an unknown storage")
	}

	w.layoutCfg.RunPrefix = mustTemplate(t, "")
	if _, err := w.flow().StoredRuns(ctx, nil); err == nil || !strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}
	w.layoutCfg = testLayoutConfig(t)

	// A storage that cannot be surveyed fails the listing.
	w.storage("primary").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	if _, err := w.flow().StoredRuns(ctx, nil); err == nil || !strings.Contains(err.Error(), "stored runs") {
		t.Fatalf("a failed survey did not fail the listing: %v", err)
	}
}

func TestStoredRunsTieBreak(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	e := w.flow()

	at := time.Unix(1700000000, 0).UTC()
	repA := covManifest("20990101T000000Z", "wireguard", "runs/20990101T000000Z/a", []byte("x"))
	repA.StartedAt = at
	repB := covManifest("20990101T000001Z", "wireguard", "runs/20990101T000001Z/a", []byte("x"))
	repB.StartedAt = at
	f.PutObject("runs/20990101T000000Z/manifest.json", mustMarshal(t, repA), at)
	f.PutObject("runs/20990101T000001Z/manifest.json", mustMarshal(t, repB), at)

	runs, err := e.StoredRuns(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
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
