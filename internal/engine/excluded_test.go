package engine

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// excludedFind is one object a discovery saw and deliberately does not back up.
// Its type is one no registry knows: a build of it would be a failure, which is
// how a test proves nothing was built.
const excludedType = "a-type-no-registry-knows"

func excludedDiscovery(w *world, found ...discovery.Found) {
	w.discoveries["workloads"] = &fakeDiscovery{found: found}
}

func ignoredFind(artifact string) discovery.Found {
	return discovery.Found{
		Name:     "volumes/" + artifact,
		Type:     excludedType,
		Artifact: model.Artifact{Group: "volumes", Name: artifact, Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		// The recorded group is deliberately not the envelope's: the record
		// names the rule that would have produced the object, and this test is
		// where the engine's use of that value instead of the envelope's shows.
		Excluded: model.ExclusionIgnored, ExcludedGroup: "pod-dumps",
		ExcludedReason: "carries the exclusion marker backup.example/ignore=true",
	}
}

// excludedRun is a run config whose discovery reports the given objects, with
// one healthy static source beside them.
func excludedRun(t *testing.T, w *world, found ...discovery.Found) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	cfg["notifiers"] = map[string]any{"chat": instance("fake")}
	excludedDiscovery(w, found...)
	return cfg
}

// TestExcludedObjectsAreRecordedNotBuilt: every deliberate exclusion is one
// record — in the report, in the manifest, in the catalog's report and in the
// finished event — and nothing of it is built. The excluded object's source type
// is one the registry does not know, so a build would show up as a failed
// artifact.
func TestExcludedObjectsAreRecordedNotBuilt(t *testing.T) {
	w := newWorld(t)
	cfg := excludedRun(t, w,
		ignoredFind("ns/media"),
		// A decision the accounting reached: no rule would have produced an
		// artifact for it, so the record carries no group — and the stored
		// record must not carry the key at all.
		discovery.Found{
			Name:           "undeclared/ns/orphan",
			Type:           excludedType,
			Artifact:       model.Artifact{Group: "undeclared", Name: "ns/orphan", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Excluded:       model.ExclusionIgnored,
			ExcludedReason: "carries the exclusion marker backup.example/ignore=true",
		},
		discovery.Found{
			Name:     "volumes/ns/data-db-0",
			Type:     excludedType,
			Artifact: model.Artifact{Group: "volumes", Name: "ns/data-db-0", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Excluded: model.ExclusionCovered, ExcludedGroup: "pod-dumps",
			ExcludedReason: `covered by the dump of pod ns/db-0 (rule "databases")`,
		})
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Artifact.Name != "wireguard" {
		t.Fatalf("artifacts %+v, want only the static source — the excluded objects build nothing", rep.Artifacts)
	}
	if len(rep.Excluded) != 3 {
		t.Fatalf("excluded %+v, want one record per decision", rep.Excluded)
	}
	want := []model.Exclusion{
		{Name: "ns/media", Group: "pod-dumps", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker backup.example/ignore=true"},
		{Name: "ns/orphan", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker backup.example/ignore=true"},
		{Name: "ns/data-db-0", Group: "pod-dumps", Kind: model.ExclusionCovered, Reason: `covered by the dump of pod ns/db-0 (rule "databases")`},
	}
	for i, x := range want {
		if rep.Excluded[i] != x {
			t.Fatalf("excluded[%d] = %+v, want %+v", i, rep.Excluded[i], x)
		}
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q, want delivered: a decision is not a failure", rep.Outcome)
	}

	// The manifest carries the decisions, and so does the finished event: the
	// notification data is the report itself.
	body, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/manifest.json")
	if !ok {
		t.Fatal("manifest missing")
	}
	var stored model.RunReport
	if err := jsonUnmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Excluded) != 3 || stored.Excluded[0] != want[0] || stored.Excluded[1] != want[1] {
		t.Fatalf("the manifest lost the exclusions: %+v", stored.Excluded)
	}
	// The empty group is left out of the stored bytes, not written as "": the
	// manifest is a format other readers see, and a group key is a claim about
	// the rule that would have produced the object.
	var rawDoc struct {
		Excluded []map[string]jsontext.Value `json:"excluded"`
	}
	if err := jsonUnmarshal(body, &rawDoc); err != nil {
		t.Fatal(err)
	}
	for _, entry := range rawDoc.Excluded {
		if _, present := entry["group"]; !present && string(entry["name"]) == `"ns/orphan"` {
			continue
		}
		if _, present := entry["group"]; !present {
			t.Errorf("the record %s lost its group", entry["name"])
			continue
		}
		if string(entry["group"]) == `""` && string(entry["name"]) == `"ns/orphan"` {
			t.Errorf("the stored record of a claim no rule would have produced carries an empty group key: %s", entry["group"])
		}
	}
	ev, ok := w.notifiers["chat"].last()
	if !ok || ev.Name != model.EventFinished || len(ev.Run.Excluded) != 3 {
		t.Fatalf("the finished event lost the exclusions: %+v", ev)
	}
}

// TestExcludedLinesAreDebugOnly: a covered or ignored object is machine detail.
// The run log names each one only at --log-level debug — at info a clean night
// must not read as a problem, and an undeclared claim is a failed artifact
// named among the failures instead.
func TestExcludedLinesAreDebugOnly(t *testing.T) {
	render := func(level slog.Level) string {
		w := newWorld(t)
		cfg := excludedRun(t, w, ignoredFind("ns/media"))
		raw, err := jsonMarshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		e, err := New(context.Background(), raw, nil, w.registry, slog.New(human.NewHandler(&buf, level)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	if got := render(slog.LevelInfo); strings.Contains(got, "excluded ns/media") {
		t.Fatalf("the info log carries a per-object exclusion line:\n%s", got)
	}
	if got := render(slog.LevelDebug); !strings.Contains(got, "excluded ns/media") {
		t.Fatalf("the debug log does not carry the exclusion line, so the info check proves nothing:\n%s", got)
	}
}

// TestExclusionsReachTheDryRun: a dry run produces nothing and still reports
// every decision, so the shape of the night can be read before it happens.
func TestExclusionsReachTheDryRun(t *testing.T) {
	w := newWorld(t)
	cfg := excludedRun(t, w, ignoredFind("ns/media"))
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Excluded) != 1 || rep.Excluded[0].Kind != model.ExclusionIgnored {
		t.Fatalf("dry run excluded %+v", rep.Excluded)
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatalf("a dry run wrote %v", w.storage(t, "primary").Keys())
	}
}

// TestPreflightReportsAnExclusion: an exclusion is a row of its own that carries
// the decision and no error, and — its type being one the registry does not know
// — proves no source was built for it.
func TestPreflightReportsAnExclusion(t *testing.T) {
	w := newWorld(t)
	cfg := excludedRun(t, w, ignoredFind("ns/media"))
	rows := w.newEngine(t, cfg).PreflightResults(context.Background(), PreflightRequest{})
	var found bool
	for _, r := range rows {
		if r.Instance != "volumes/ns/media" {
			continue
		}
		found = true
		if !r.OK || r.Error != "" || r.Excluded != model.ExclusionIgnored ||
			!strings.Contains(r.ExcludedReason, "exclusion marker") {
			t.Fatalf("row = %+v, want an OK row carrying the decision", r)
		}
	}
	if !found {
		t.Fatalf("no row for the excluded object: %+v", rows)
	}
}

// TestResolveTargetRefusesAnExcludedObject: a restore that names an object the
// run decided not to back up is told so, instead of "no source has it".
func TestResolveTargetRefusesAnExcludedObject(t *testing.T) {
	w := newWorld(t)
	cfg := excludedRun(t, w, ignoredFind("ns/media"))
	e := w.newEngine(t, cfg)
	scope := &closerScope{}
	defer func() { _ = scope.close() }()

	_, _, err := e.resolveTarget(context.Background(), "discovered", "ns/media", scope)
	if err == nil || !strings.Contains(err.Error(), "is excluded") || !strings.Contains(err.Error(), "exclusion marker") {
		t.Fatalf("resolveTarget = %v, want the exclusion named", err)
	}
}

// TestResolveTargetFindsAUnit: a restore names the artifact of one unit, and the
// engine expands the instance whose envelope holds it — no other found object is
// built on the way.
func TestResolveTargetFindsAUnit(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	// The second object must be left alone: its type is one no registry knows,
	// so building it would turn this resolution into a failure.
	excludedDiscovery(w,
		discovery.Found{
			Name: "databases/pg-0", Type: "fake", Config: jsontext.Value(`{}`),
			Artifact: model.Artifact{Group: "databases", Name: "ns/pg-0", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Pipeline: []string{"zstd3"},
		},
		discovery.Found{
			Name: "elsewhere", Type: excludedType, Config: jsontext.Value(`{}`),
			Artifact: model.Artifact{Group: "other", Name: "ns/other", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Pipeline: []string{"zstd3"},
		})
	twoUnits(t, w, "databases/pg-0")

	e := w.newEngine(t, cfg)
	scope := &closerScope{}
	defer func() { _ = scope.close() }()

	inst, typ, err := e.resolveTarget(context.Background(), "discovered", "ns/pg-0/db2", scope)
	if err != nil {
		t.Fatal(err)
	}
	if typ != "fake" || inst != w.extraSources["databases/pg-0/db2"] {
		t.Fatalf("resolved %T %q, want the second unit's own source", inst, typ)
	}
	// A unit the server no longer has — a database dropped by hand, an instance
	// recreated without it — is answered with what the instance does hold and
	// with the one step a restore cannot take for itself.
	_, _, err = e.resolveTarget(context.Background(), "discovered", "ns/pg-0/ghost", scope)
	for _, want := range []string{"ns/pg-0/ghost", "databases/pg-0", "must exist empty", "ns/pg-0/db1"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("a vanished unit = %v, want it to mention %q", err, want)
		}
	}
}
