package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// TestResolveFetchAgreesWithFetchOnFallback: ResolveFetch picks the same run and
// reports which storage holds a copy, in the same three fallback cases a fetch
// tests — the plan and the download must not disagree on either.
func TestResolveFetchAgreesWithFetchOnFallback(t *testing.T) {
	cases := []struct {
		name      string
		corrupt   func(t *testing.T, f *storagetest.Fake, key string, body []byte)
		holdsAll  bool // the corruption leaves the listing indistinguishable from a good copy
		reasonSub string
	}{
		{"not_found", func(_ *testing.T, f *storagetest.Fake, key string, _ []byte) {
			_ = f.Delete(context.Background(), key)
		}, false, "not in the storage listing"},
		{"wrong size", func(_ *testing.T, f *storagetest.Fake, key string, _ []byte) {
			f.PutObject(key, []byte("short"), testTime())
		}, false, "in the listing, the manifest says"},
		{"wrong hash", func(_ *testing.T, f *storagetest.Fake, key string, body []byte) {
			f.PutObject(key, bytes.Repeat([]byte{'z'}, len(body)), testTime())
		}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := withRestore(t, baseConfig(t, twoStorages()), "primary", "offsite")
			e := runOnce(t, w, cfg)
			runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
			runID := runs[0].RunID
			key := "runs/" + runID + "/vpn/wireguard.dat.zst"
			body := w.sources["vpn"].data
			f := w.storage(t, "primary")
			tc.corrupt(t, f, key, body)

			req := FetchRequest{RunID: runID, Artifact: "wireguard"}
			plan, err := e.ResolveFetch(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if plan.RunID != runID || plan.Key != key || plan.Bytes != int64(len(body)) || plan.SHA256 == "" {
				t.Fatalf("plan %+v", plan)
			}
			if len(plan.Chain) != 1 || len(plan.Storages) != 2 {
				t.Fatalf("plan does not describe the chain and the walk: %+v", plan)
			}
			if tc.holdsAll {
				// The listing cannot see a hash mismatch — only the bytes can —
				// so the plan says both storages hold a copy and Fetch still
				// falls through to the second one.
				if !plan.Storages[0].Holds {
					t.Fatalf("plan denies a copy the listing holds: %+v", plan.Storages)
				}
			} else if plan.Storages[0].Holds || !strings.Contains(plan.Storages[0].Reason, tc.reasonSub) {
				t.Fatalf("the first storage is not reported as holding nothing: %+v", plan.Storages[0])
			}
			if !plan.Storages[1].Holds {
				t.Fatalf("the second storage is not holding the copy: %+v", plan.Storages[1])
			}
			if f.CallCount(storagetest.OpGet, key) != 0 {
				t.Fatal("resolving downloaded the object")
			}

			var out bytes.Buffer
			rep, err := e.Fetch(context.Background(), req, &out)
			if err != nil {
				t.Fatal(err)
			}
			if rep.RunID != plan.RunID || rep.Storage != "offsite" {
				t.Fatalf("fetch served %s from %q, plan said %s", rep.RunID, rep.Storage, plan.RunID)
			}
			if !bytes.Equal(out.Bytes(), body) {
				t.Fatal("the second storage returned wrong bytes")
			}
		})
	}
}

// TestResolveFetchNewestAndSensitivity: the newest-ok selection is the one Fetch
// uses, and the plan carries the sensitivity the plaintext rule needs.
func TestResolveFetchNewestAndSensitivity(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	oldRun := runs[0].RunID
	w.storage(t, "primary").PutObject("runs/20990101T000000Z/manifest.json", mustMarshal(t, model.RunReport{
		Schema: model.ManifestSchema, RunID: "20990101T000000Z", StartedAt: testTime().AddDate(1, 0, 0),
		Outcome: model.OutcomeFailed,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "wireguard"}, Outcome: model.ArtifactFailed, Error: "boom",
		}},
	}), testTime().AddDate(1, 0, 0))

	plan, err := e.ResolveFetch(context.Background(), FetchRequest{Newest: true, Artifact: "wireguard"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.RunID != oldRun {
		t.Fatalf("newest-ok resolved to %q, want %q", plan.RunID, oldRun)
	}
	if plan.Sensitivity != model.SensitivityPlain {
		t.Fatalf("plan sensitivity %q", plan.Sensitivity)
	}

	_, err = e.ResolveFetch(context.Background(), FetchRequest{RunID: "20990101T000000Z", Artifact: "wireguard"})
	if err == nil {
		t.Fatal("a failed artifact was planned")
	}
	if !strings.Contains(err.Error(), oldRun) {
		t.Fatalf("the refusal does not name the nearest ok run %q: %v", oldRun, err)
	}
}

func TestResolveFetchRawByKey(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	key := "runs/" + runs[0].RunID + "/vpn/wireguard.dat.zst"

	plan, err := e.ResolveFetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Key != key || len(plan.Storages) != 1 || !plan.Storages[0].Holds {
		t.Fatalf("raw plan %+v", plan)
	}
	if plan.Sensitivity != "" || plan.SHA256 != "" || plan.RunID != "" {
		t.Fatalf("a raw plan claims a manifest: %+v", plan)
	}

	_ = w.storage(t, "primary").Delete(context.Background(), key)
	plan, err = e.ResolveFetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Storages[0].Holds || plan.Storages[0].Reason == "" {
		t.Fatalf("an absent raw object reported as held: %+v", plan.Storages[0])
	}
}

// TestFetchReportsSensitivity: the report of a manifest-described artifact
// carries its sensitivity — the CLI's plaintext rule reads it — and a raw
// by-key fetch reports none, because it never decodes.
func TestFetchReportsSensitivity(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "seal"}, "required", "secret")}
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil || rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("fixture run: %+v, %v", rep, err)
	}
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID

	var stored bytes.Buffer
	frep, err := e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard"}, &stored)
	if err != nil {
		t.Fatal(err)
	}
	if frep.Sensitivity != model.SensitivitySecret || !frep.Verified {
		t.Fatalf("fetch report %+v", frep)
	}

	var raw bytes.Buffer
	rrep, err := e.Fetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: frep.Key}, &raw)
	if err != nil {
		t.Fatal(err)
	}
	if rrep.Sensitivity != "" || rrep.Verified {
		t.Fatalf("raw fetch report %+v", rrep)
	}

	// A raw fetch never decodes, so asking it to is a refusal, not a silence.
	if _, err := e.Fetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: frep.Key, Decode: true}, &raw); err == nil ||
		!strings.Contains(err.Error(), "never decodes") {
		t.Fatalf("raw fetch with Decode accepted: %v", err)
	}
}

// TestVerifyStoredNewest: a stored check resolves the newest-ok selector from
// the manifests — it downloads nothing — and reports per storage what it found.
func TestVerifyStoredNewest(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, twoStorages()), "primary", "offsite")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	oldRun := runs[0].RunID
	body := w.sources["vpn"].data

	newRun := "20990101T000000Z"
	newKey := "runs/" + newRun + "/vpn/wireguard.dat.zst"
	f := w.storage(t, "primary")
	f.PutObject("runs/"+newRun+"/manifest.json", mustMarshal(t, model.RunReport{
		Schema: model.ManifestSchema, RunID: newRun, StartedAt: testTime().AddDate(1, 0, 0),
		Outcome: model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "wireguard", Group: "vpn", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired},
			Outcome:  model.ArtifactOK, Key: newKey, Bytes: int64(len(body)), SHA256: model.SHA256Hex(body),
		}},
	}), testTime().AddDate(1, 0, 0))
	f.PutObject(newKey, body, testTime().AddDate(1, 0, 0))

	// The newest run exists on one storage only; the check verifies the copy
	// that is there instead of answering FAILED for a healthy backup.
	res, err := e.Verify(context.Background(), VerifyRequest{
		Fetch: FetchRequest{Newest: true, Artifact: "wireguard"}, Depth: "stored"})
	if err != nil || len(res) != 1 || !res[0].OK {
		t.Fatalf("stored newest-ok: %+v, %v", res, err)
	}
	if res[0].RunID != newRun || res[0].Storage != "primary" {
		t.Fatalf("stored newest-ok checked %s on %s", res[0].RunID, res[0].Storage)
	}

	// The newest run's object is gone: the same selector lands on the older run,
	// and the storages that cannot serve it say why.
	if err := f.Delete(context.Background(), newKey); err != nil {
		t.Fatal(err)
	}
	res, err = e.Verify(context.Background(), VerifyRequest{
		Fetch: FetchRequest{Newest: true, Artifact: "wireguard"}, Depth: "stored"})
	if err != nil || len(res) != 1 || !res[0].OK {
		t.Fatalf("stored newest-ok fallback: %+v, %v", res, err)
	}
	if res[0].RunID != oldRun || res[0].Storage != "primary" {
		t.Fatalf("stored newest-ok fell to %s on %s, want %s", res[0].RunID, res[0].Storage, oldRun)
	}

	// Nothing holds the artifact anywhere: the result names that, it does not
	// pretend to have checked.
	res, err = e.Verify(context.Background(), VerifyRequest{
		Fetch: FetchRequest{Newest: true, Artifact: "ghost"}, Depth: "stored"})
	if err != nil || len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "no run on any storage holds") {
		t.Fatalf("stored newest-ok for an unknown artifact: %+v, %v", res, err)
	}
}

// TestFetchStreamsThePlaintextWithoutADecodedFile: a decoded fetch reverses the
// chain from the verified spool file straight into the destination, so the
// plaintext of a secret artifact never sits on the spool — the README's promise
// about a laptop's disk holds because the file never exists.
func TestFetchStreamsThePlaintextWithoutADecodedFile(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "seal"}, "required", "secret")}
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil || rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("fixture run: %+v, %v", rep, err)
	}
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	// A fetch spools into restore.spool_dir, not the run's.
	spoolDir := cfg["restore"].(map[string]any)["spool_dir"].(string)

	var seen []string
	w.processors["seal"].decodeHook = func() {
		names, err := readDirNames(spoolDir)
		if err != nil {
			t.Errorf("listing the spool: %v", err)
			return
		}
		seen = append(seen, names...)
	}
	var out bytes.Buffer
	frep, err := e.Fetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard", Decode: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !frep.Verified || !bytes.Equal(out.Bytes(), w.sources["vpn"].data) {
		t.Fatalf("decoded fetch: %+v", frep)
	}
	if len(seen) == 0 {
		t.Fatal("the decode ran without listing the spool: the probe never saw the verified copy")
	}
	for _, name := range seen {
		if strings.Contains(name, "decoded") {
			t.Fatalf("a decoded file existed in the spool during a stdout fetch: %q (whole spool: %v)", name, seen)
		}
	}
}

// TestApplyRefusesBeforeDownloading: the target, its type, the confirmation and
// the origin are checked against the manifest before a byte moves.
func TestApplyRefusesBeforeDownloading(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID
	key := "runs/" + runID + "/vpn/wireguard.dat.zst"
	f := w.storage(t, "primary")

	_, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{RunID: runID, Artifact: "wireguard"},
		Target: "target", Mode: source.ApplyInPlace, Confirm: "somewhere-else"})
	if err == nil || !strings.Contains(err.Error(), "Confirm") {
		t.Fatalf("wrong confirm accepted: %v", err)
	}
	if got := f.CallCount(storagetest.OpGet, key); got != 0 {
		t.Fatalf("the artifact was downloaded %d times before the refusal", got)
	}
	if target.applied != 0 || target.probed != 0 {
		t.Fatal("a refused apply ran")
	}
}
