package inventory

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

const c3RunID = "20261001T040000Z"

// validManifestDoc is the current format's complete document as a mutable map:
// the corpus deletes and rewrites one member at a time.
func validManifestDoc(t *testing.T) map[string]any {
	t.Helper()
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: c3RunID, Kind: model.RunKindScheduled,
		StartedAt: time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC), Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
	}
	var doc map[string]any
	if err := json.Unmarshal(mustMarshal(t, rep), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// TestC3_StrictManifestDecodeRefusesIncompleteOrContradictoryDocuments: the one
// reader of the current manifest format refuses unknown members, every missing
// required member, and a schema, run id, kind, outcome or start it cannot
// accept. No compatibility document is read.
func TestC3_StrictManifestDecodeRefusesIncompleteOrContradictoryDocuments(t *testing.T) {
	kind := model.RunKindScheduled
	if _, err := decodeManifest(mustMarshal(t, validManifestDoc(t)), c3RunID, kind); err != nil {
		t.Fatalf("the complete document was refused: %v", err)
	}

	for _, member := range manifestMembers {
		t.Run("missing "+member, func(t *testing.T) {
			doc := validManifestDoc(t)
			delete(doc, member)
			_, err := decodeManifest(mustMarshal(t, doc), c3RunID, kind)
			if err == nil || !strings.Contains(err.Error(), "missing field /"+member) {
				t.Fatalf("decode = %v, want the missing-%s refusal", err, member)
			}
		})
	}

	cases := map[string]struct {
		mutate func(map[string]any)
		body   string
		want   string
	}{
		"unknown member":  {mutate: func(d map[string]any) { d["bogus"] = true }, want: "parsing the manifest"},
		"foreign schema":  {mutate: func(d map[string]any) { d["schema"] = model.ManifestSchema + 1 }, want: "manifest schema"},
		"foreign run id":  {mutate: func(d map[string]any) { d["run_id"] = "20990101T000000Z" }, want: "the manifest says run"},
		"empty kind":      {mutate: func(d map[string]any) { d["kind"] = "" }, want: "the run id says"},
		"unknown kind":    {mutate: func(d map[string]any) { d["kind"] = "weekly" }, want: "the run id says"},
		"manual kind":     {mutate: func(d map[string]any) { d["kind"] = model.RunKindManual }, want: "the run id says"},
		"empty outcome":   {mutate: func(d map[string]any) { d["outcome"] = "" }, want: "outcome"},
		"running outcome": {mutate: func(d map[string]any) { d["outcome"] = model.OutcomeRunning }, want: "outcome"},
		"zero start":      {mutate: func(d map[string]any) { d["started_at"] = time.Time{}.Format(time.RFC3339) }, want: "no started_at"},
		"not an object":   {body: "[]", want: "parsing the manifest"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			body := []byte(tc.body)
			if tc.mutate != nil {
				doc := validManifestDoc(t)
				tc.mutate(doc)
				body = mustMarshal(t, doc)
			}
			_, err := decodeManifest(body, c3RunID, kind)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestC3_IncompleteManifestIsAProblemRunWithoutCredit: the strict refusal
// reaches the survey as a problem run — no manifest credit, no verified copy —
// while the listed bytes still count against occupancy and the manifest key
// still orders deletion.
func TestC3_IncompleteManifestIsAProblemRunWithoutCredit(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	doc := validManifestDoc(t)
	delete(doc, "outcome")
	body := mustMarshal(t, doc)
	key := "runs/" + c3RunID + "/manifest.json"
	f.PutObject("runs/"+c3RunID+"/a.bin", []byte("artifact"), now)
	f.PutObject(key, body, now)

	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	r := sur.Runs[c3RunID]
	if !strings.Contains(r.ManifestProblem, "missing field /outcome") {
		t.Fatalf("manifest problem = %q, want the incomplete-document refusal", r.ManifestProblem)
	}
	if r.Manifest != nil || r.Stored.Manifest || r.Stored.Verified {
		t.Fatalf("an incomplete manifest was credited: manifest %v stored %+v", r.Manifest, r.Stored)
	}
	if want := int64(len("artifact") + len(body)); r.Stored.Bytes != want {
		t.Fatalf("listed bytes = %d, want %d: occupancy survives the refusal", r.Stored.Bytes, want)
	}
	if r.ManifestKey != key {
		t.Fatalf("manifest key = %q, want %q: deletion still removes the record first", r.ManifestKey, key)
	}
}

// completeManifestDoc is the current format with every nested shape filled: the
// recursive corpus mutates one member of it at a time.
func completeManifestDoc(t *testing.T) map[string]any {
	t.Helper()
	at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	body := []byte("sealed!")
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: c3RunID, Kind: model.RunKindScheduled, StartedAt: at,
		Duration: model.Duration(time.Minute), Outcome: model.OutcomeDelivered, ExitCode: 0,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "prod", Group: "databases", Ext: ".dump.age",
				Sensitivity: model.SensitivitySecret, Importance: model.ImportanceRequired, Sealed: true},
			Outcome: model.ArtifactOK, Key: "runs/" + c3RunID + "/databases/prod.dump.age",
			Chain:    []model.ProcessorStep{{Instance: "seal", Type: "age", Ext: ".age", Seals: true}},
			Source:   model.SourceRef{Instance: "prod", Type: "postgres", Origin: "postgres://example.com/prod"},
			Pipeline: "sealed", RawBytes: 10, Bytes: int64(len(body)),
			Stats:  model.DumpStats{SkippedEntries: 1, PaddedFiles: 0},
			SHA256: model.SHA256Hex(body), Duration: model.Duration(time.Second), Hint: "restore with the key",
		}},
		Storages: []model.StorageResult{{Storage: "primary", Type: "fake", Outcome: model.StorageVerified,
			Objects: 2, Bytes: int64(len(body)), Duration: model.Duration(time.Second), Attempts: 1,
			CapExceeded: true, CapLimitBytes: 100, CapUsedBytes: 90,
			Kept: 1, Deleted: []string{"old-run"}, Freed: 3}},
		Excluded: []model.Exclusion{{Name: "volumes/cache", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "marker"}},
		Previous: model.PreviousRun{RunID: "20260930T040000Z", Unfinished: false, LastGoodAt: at.Add(-24 * time.Hour),
			Unavailable: "previous run status unavailable",
			Failure: &model.PreviousFailure{Job: "nightly-1", Reason: "BackoffLimitExceeded", Message: "boom",
				Containers: []model.ContainerFailure{
					{Pod: "nightly-1-abc", Container: "backup", Init: true,
						Terminated: &model.ContainerTermination{Reason: "Error", ExitCode: 1, Message: "last lines"}},
					{Pod: "nightly-1-abc", Container: "restore", Waiting: "ImagePullBackOff"},
				},
				Events: []model.EventFailure{{Reason: "FailedCreate", Message: "psa"}}}},
		Lease: &model.LeaseState{Refused: true, Holder: "pod-a", HolderKind: model.RunKindScheduled},
	}
	var doc map[string]any
	if err := json.Unmarshal(mustMarshal(t, rep), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// manifestPath walks a slash-separated path through the mutable document.
func manifestPath(t *testing.T, doc map[string]any, path string) any {
	t.Helper()
	var cur any = doc
	for _, part := range strings.Split(path, "/") {
		switch node := cur.(type) {
		case map[string]any:
			cur = node[part]
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil {
				t.Fatalf("path %s: %q is not an index", path, part)
			}
			cur = node[i]
		default:
			t.Fatalf("path %s: %T has no members", path, cur)
		}
	}
	return cur
}

func deleteManifestPath(t *testing.T, doc map[string]any, path string) {
	t.Helper()
	parts := strings.Split(path, "/")
	if len(parts) == 1 {
		if _, present := doc[path]; !present {
			t.Fatalf("path %s: member is not there to delete", path)
		}
		delete(doc, path)
		return
	}
	obj, ok := manifestPath(t, doc, strings.Join(parts[:len(parts)-1], "/")).(map[string]any)
	if !ok {
		t.Fatalf("path %s: parent is not an object", path)
	}
	last := parts[len(parts)-1]
	if _, present := obj[last]; !present {
		t.Fatalf("path %s: member %q is not there to delete", path, last)
	}
	delete(obj, last)
}

func setManifestPath(t *testing.T, doc map[string]any, path string, value any) {
	t.Helper()
	parts := strings.Split(path, "/")
	if len(parts) == 1 {
		doc[path] = value
		return
	}
	obj, ok := manifestPath(t, doc, strings.Join(parts[:len(parts)-1], "/")).(map[string]any)
	if !ok {
		t.Fatalf("path %s: parent is not an object", path)
	}
	obj[parts[len(parts)-1]] = value
}

// TestC3_RecursiveCorpusRefusesEveryMissingNestedMember: one recursive reader
// covers the whole current format — an artifact, its chain step, source, stats,
// a storage, an exclusion and the previous-run record each lose one required
// member, and every mutation is a problem run.
func TestC3_RecursiveCorpusRefusesEveryMissingNestedMember(t *testing.T) {
	kind := model.RunKindScheduled
	if _, err := decodeManifest(mustMarshal(t, completeManifestDoc(t)), c3RunID, kind); err != nil {
		t.Fatalf("the complete document was refused: %v", err)
	}
	for _, path := range []string{
		"artifacts/0/artifact", "artifacts/0/artifact/name", "artifacts/0/artifact/group",
		"artifacts/0/artifact/ext", "artifacts/0/artifact/sensitivity", "artifacts/0/artifact/importance",
		"artifacts/0/artifact/sealed", "artifacts/0/outcome", "artifacts/0/key", "artifacts/0/chain",
		"artifacts/0/chain/0/instance", "artifacts/0/chain/0/type", "artifacts/0/chain/0/ext",
		"artifacts/0/chain/0/seals", "artifacts/0/source", "artifacts/0/source/instance",
		"artifacts/0/source/type", "artifacts/0/source/origin", "artifacts/0/pipeline",
		"artifacts/0/raw_bytes", "artifacts/0/bytes", "artifacts/0/stats",
		"artifacts/0/stats/skipped_entries", "artifacts/0/stats/padded_files", "artifacts/0/sha256",
		"artifacts/0/duration", "artifacts/0/error",
		"storages/0/storage", "storages/0/type", "storages/0/outcome", "storages/0/objects",
		"storages/0/bytes", "storages/0/duration", "storages/0/attempts", "storages/0/kept",
		"storages/0/deleted", "storages/0/freed", "storages/0/error",
		"excluded/0/name", "excluded/0/kind", "excluded/0/reason",
		"previous/run_id", "previous/unfinished", "previous/last_good_at",
		"previous/failure/job",
		"previous/failure/containers/0/pod", "previous/failure/containers/0/container",
		"previous/failure/containers/0/terminated/reason",
		"previous/failure/containers/0/terminated/exit_code",
		"previous/failure/events/0/reason", "previous/failure/events/0/message",
	} {
		t.Run("missing "+path, func(t *testing.T) {
			doc := completeManifestDoc(t)
			deleteManifestPath(t, doc, path)
			_, err := decodeManifest(mustMarshal(t, doc), c3RunID, kind)
			if err == nil || !strings.Contains(err.Error(), "missing field /"+path) {
				t.Fatalf("decode = %v, want the missing-%s refusal", err, path)
			}
		})
	}
}

// TestC3_RecursiveCorpusRefusesContradictoryNestedValues: a well-formed member
// with a value this build cannot act on — an unreplayable chain step, an ok
// artifact without its key or digest, an outcome outside its closed set — is a
// problem run too.
func TestC3_RecursiveCorpusRefusesContradictoryNestedValues(t *testing.T) {
	kind := model.RunKindScheduled
	cases := map[string]struct {
		path  string
		value any
		want  string
	}{
		"artifact outcome":       {"artifacts/0/outcome", "running", "outcome"},
		"ok without sha256":      {"artifacts/0/sha256", "", "must record its object key and a canonical sha256"},
		"ok without key":         {"artifacts/0/key", "", "must record its object key and a canonical sha256"},
		"chain without type":     {"artifacts/0/chain/0/type", "", "must name its instance and type"},
		"chain without instance": {"artifacts/0/chain/0/instance", "", "must name its instance and type"},
		"storage outcome":        {"storages/0/outcome", "", "not one this build knows"},
		"exclusion kind":         {"excluded/0/kind", "weekly", "not one this build knows"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			doc := completeManifestDoc(t)
			setManifestPath(t, doc, tc.path, tc.value)
			_, err := decodeManifest(mustMarshal(t, doc), c3RunID, kind)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestC3_RecursiveCorpusRespectsTheRealOptionalMembers: every omitempty member
// of the current format is optional — its absence is not an incomplete
// document.
func TestC3_RecursiveCorpusRespectsTheRealOptionalMembers(t *testing.T) {
	kind := model.RunKindScheduled
	for _, path := range []string{
		"lease", "artifacts/0/hint", "excluded/0/group",
		"storages/0/cap_exceeded", "storages/0/cap_limit_bytes", "storages/0/cap_used_bytes",
		"previous/unavailable", "previous/failure/reason", "previous/failure/message",
		"previous/failure/containers", "previous/failure/events",
		"previous/failure/containers/0/init", "previous/failure/containers/0/terminated/message",
		"previous/failure/containers/1/waiting",
	} {
		t.Run("without "+path, func(t *testing.T) {
			doc := completeManifestDoc(t)
			deleteManifestPath(t, doc, path)
			if _, err := decodeManifest(mustMarshal(t, doc), c3RunID, kind); err != nil {
				t.Fatalf("an absent optional member was refused: %v", err)
			}
		})
	}
}

// TestC3_ManifestShapeRefusesNullsAndWrongShapes: a required member present as
// null, or with a shape the current format does not have, is a problem run.
func TestC3_ManifestShapeRefusesNullsAndWrongShapes(t *testing.T) {
	kind := model.RunKindScheduled
	cases := map[string]struct {
		path  string
		value any
		want  string
	}{
		"null scalar":    {"outcome", nil, "is null"},
		"null optional":  {"artifacts/0/hint", nil, "is null"},
		"struct shape":   {"previous", "x", "parsing the manifest"},
		"slice shape":    {"artifacts", map[string]any{}, "parsing the manifest"},
		"artifact shape": {"artifacts/0/source", []any{}, "parsing the manifest"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			doc := completeManifestDoc(t)
			setManifestPath(t, doc, tc.path, tc.value)
			_, err := decodeManifest(mustMarshal(t, doc), c3RunID, kind)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode = %v, want %q", err, tc.want)
			}
		})
	}
}

// shapeFixture is a model-shaped struct with the fields the walker must skip:
// no JSON tag at all, an explicitly skipped one and an unexported one.
type shapeFixture struct {
	Name    string `json:"name"`
	Skipped string `json:"-"`
}

// TestC3_ManifestShapeSkipsUntaggedFields: only tagged exported fields are part
// of the stored format; the walker ignores the rest.
func TestC3_ManifestShapeSkipsUntaggedFields(t *testing.T) {
	if errs := manifestShape(jsontext.Value(`{"name":"x"}`), reflect.TypeFor[shapeFixture](), ""); len(errs) != 0 {
		t.Fatalf("the walker demanded an untagged field: %v", errs)
	}
}

// TestC3_ManifestShapeAcceptsANullList: the writer emits an empty list as null,
// so a required list member present as null is empty, never missing.
func TestC3_ManifestShapeAcceptsANullList(t *testing.T) {
	if errs := manifestShape(jsontext.Value("null"), reflect.TypeFor[[]string](), "/list"); len(errs) != 0 {
		t.Fatalf("a null list was refused: %v", errs)
	}
}

// TestR3_C3_MalformedDigestKeepsOccupancy: a manifest whose ok artifact carries
// a digest no restore could ever match is a problem run — no credit, and its
// listed bytes still count against occupancy.
func TestR3_C3_MalformedDigestKeepsOccupancy(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	doc := completeManifestDoc(t)
	setManifestPath(t, doc, "artifacts/0/sha256", "not-a-sha256")
	body := mustMarshal(t, doc)
	key := "runs/" + c3RunID + "/manifest.json"
	f.PutObject("runs/"+c3RunID+"/databases/prod.dump.age", []byte("sealed!"), now)
	f.PutObject(key, body, now)

	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	r := sur.Runs[c3RunID]
	if !strings.Contains(r.ManifestProblem, "canonical sha256") {
		t.Fatalf("manifest problem = %q, want the malformed-digest refusal", r.ManifestProblem)
	}
	if r.Manifest != nil || r.Stored.Manifest || r.Stored.Verified {
		t.Fatalf("an unrestorable digest was credited: manifest %v stored %+v", r.Manifest, r.Stored)
	}
	if want := int64(len("sealed!") + len(body)); r.Stored.Bytes != want {
		t.Fatalf("listed bytes = %d, want %d: occupancy survives the refusal", r.Stored.Bytes, want)
	}
}
