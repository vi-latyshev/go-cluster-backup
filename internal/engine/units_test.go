package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// twoUnits is a parent source holding two artifacts, with one source per unit
// standing behind the names the engine will build.
func twoUnits(t *testing.T, w *world, parent string) {
	t.Helper()
	w.extraSources[parent] = &fakeSource{data: []byte("parent data"), units: []source.Unit{
		{Name: "db1", Config: jsontext.Value(`{}`)},
		{Name: "db2", Config: jsontext.Value(`{}`)},
	}}
	w.extraSources[parent+"/db1"] = &fakeSource{data: []byte("unit one data")}
	w.extraSources[parent+"/db2"] = &fakeSource{data: []byte("unit two data")}
}

// TestStaticSourceExpandsIntoItsUnits: an instance whose domain holds several
// artifacts becomes one artifact per unit, named by the engine from the unit's
// namespace and name — the artifact "<envelope>/<unit>", the instance
// "<instance>/<unit>" — and the parent itself produces nothing.
func TestStaticSourceExpandsIntoItsUnits(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
	twoUnits(t, w, "vpn")

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("artifacts %+v, want one per unit", rep.Artifacts)
	}
	for i, want := range []struct{ artifact, instance string }{
		{"cluster/db1", "vpn/db1"},
		{"cluster/db2", "vpn/db2"},
	} {
		ar := rep.Artifacts[i]
		if ar.Artifact.Name != want.artifact || ar.Source.Instance != want.instance {
			t.Fatalf("artifact %d = %s from %s, want %s from %s", i, ar.Artifact.Name, ar.Source.Instance, want.artifact, want.instance)
		}
		if ar.Outcome != model.ArtifactOK || ar.Artifact.Group != "databases" || ar.Artifact.Importance != model.ImportanceRequired {
			t.Fatalf("artifact %d = %+v, want the unit under the rule's envelope", i, ar)
		}
	}
	s := w.storage(t, "primary")
	for _, key := range []string{"databases/cluster/db1.dat.zst", "databases/cluster/db2.dat.zst"} {
		if _, ok := s.Bytes("runs/" + rep.RunID + "/" + key); !ok {
			t.Fatalf("%s misses %s (has %v)", "primary", key, s.Keys())
		}
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q, want delivered: %+v", rep.Outcome, rep.Artifacts)
	}
}

// TestDiscoveredSourceExpandsIntoItsUnits: the same expansion for a source a
// discovery found, whose instance name is the one the rule rendered.
func TestDiscoveredSourceExpandsIntoItsUnits(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{}
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name: "databases/pg-0", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/pg-0", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	twoUnits(t, w, "databases/pg-0")

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 2 || rep.Artifacts[0].Artifact.Name != "ns/pg-0/db1" || rep.Artifacts[1].Artifact.Name != "ns/pg-0/db2" {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if rep.Artifacts[0].Source.Instance != "databases/pg-0/db1" {
		t.Fatalf("instance = %q, want the unit under the discovered instance", rep.Artifacts[0].Source.Instance)
	}
}

// TestOneBrokenUnitCostsOneArtifact: a unit whose source cannot be built or
// checked fails alone; every other unit of the same instance is delivered.
func TestOneBrokenUnitCostsOneArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
	twoUnits(t, w, "vpn")
	w.extraSources["vpn/db2"] = &fakeSource{data: []byte("x"), checkErr: errors.New("unit two is down")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the healthy unit did not deliver: %+v", rep.Artifacts[0])
	}
	if rep.Artifacts[1].Outcome != model.ArtifactFailed || !strings.Contains(rep.Artifacts[1].Error, "unit two is down") {
		t.Fatalf("the broken unit = %+v, want its own failure", rep.Artifacts[1])
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q, want failed for a required unit", rep.Outcome)
	}
}

// TestFailedEnumerationIsOneArtifact: a domain that cannot be read is one failed
// artifact with the parent's envelope — never a discovery that fails and takes
// every other rule's results with it — and an empty answer is a plugin fault,
// because a run that silently produces nothing is worse than one failed artifact.
func TestFailedEnumerationIsOneArtifact(t *testing.T) {
	cases := []struct {
		name  string
		units []source.Unit
		err   error
		want  string
	}{
		{"the enumeration fails", nil, errors.New("the catalog is unreadable"), "catalog is unreadable"},
		{"the enumeration answers nothing", []source.Unit{}, nil, "answers no artifact"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
			cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
			w.extraSources["vpn"] = &fakeSource{data: []byte("x"), units: tc.units, unitsErr: tc.err}

			rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if len(rep.Artifacts) != 1 {
				t.Fatalf("artifacts %+v, want the instance's one failed artifact", rep.Artifacts)
			}
			ar := rep.Artifacts[0]
			if ar.Outcome != model.ArtifactFailed || !strings.Contains(ar.Error, tc.want) {
				t.Fatalf("artifact = %+v, want a failure mentioning %q", ar, tc.want)
			}
			if ar.Artifact.Name != "cluster" || ar.Pipeline != "zstd3" {
				t.Fatalf("artifact = %+v, want the parent's envelope and pipeline", ar)
			}
			if w.storage(t, "primary").CallCount(storagetest.OpPut, "") != 0 {
				t.Fatalf("a failed instance still uploaded something: %v", w.storage(t, "primary").Keys())
			}
		})
	}
}

// TestUnitConfigBuildsTheSameType: the unit is built from the config the plugin
// handed back, by the engine, under the unit's own instance name — a plugin
// never constructs its own child, and the parent's config is not reused.
func TestUnitConfigBuildsTheSameType(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["config"] = map[string]any{"role": "parent"}
	w.extraSources["vpn"] = &fakeSource{data: []byte("x"), units: []source.Unit{
		{Name: "only", Config: jsontext.Value(`{"role": "unit"}`)},
	}}
	w.extraSources["vpn/only"] = &fakeSource{data: []byte("the unit")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Artifact.Name != "cluster/only" {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if got := string(w.configs["vpn/only"]["role"]); got != `"unit"` {
		t.Fatalf("the unit was built from the parent's config: %q", got)
	}
}

// TestEnumerationRetries: "this instance is one artifact" is an answer, not a
// fault — it must cost neither the retry budget nor the run's deadline (the
// cluster's unclassified_error is transient with a 5s-to-40s backoff, so an
// unclassified ErrUnsupported would spend a minute of every run on every
// single-artifact instance) — while a transient enumeration failure is worth
// the attempts the policy allows.
func TestEnumerationRetries(t *testing.T) {
	cases := []struct {
		name      string
		unitsErr  error
		wantCalls int
		wantError string
	}{
		{"one artifact is an answer", errors.ErrUnsupported, 1, ""},
		{"a transient failure is worth the attempts", plugin.Transient(errors.New("the catalog is busy")), 3, "the catalog is busy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
			parent := &fakeSource{data: []byte("parent"), unitsErr: tc.unitsErr}
			w.extraSources["vpn"] = parent

			rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if got := parent.unitsCount(); got != tc.wantCalls {
				t.Fatalf("the enumeration ran %d times, want %d", got, tc.wantCalls)
			}
			if tc.wantError == "" {
				if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactOK {
					t.Fatalf("artifacts %+v, want the parent as the one artifact", rep.Artifacts)
				}
				return
			}
			if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, tc.wantError) {
				t.Fatalf("artifacts %+v, want the failed instance", rep.Artifacts)
			}
		})
	}
}

// TestAUnitNameTheEngineCannotUseFailsThatUnitAlone: a name a server hands back
// — a database whose name carries a separator or a control character — is not
// the plugin's to invent, so the engine refuses it as THAT unit's failed
// artifact and every other unit of the instance is still dumped.
func TestAUnitNameTheEngineCannotUseFailsThatUnitAlone(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
	w.extraSources["vpn"] = &fakeSource{data: []byte("parent"), units: []source.Unit{
		{Name: "good", Config: jsontext.Value(`{}`)},
		{Name: "odd/name", Config: jsontext.Value(`{}`)},
		{Name: "bad\tname", Config: jsontext.Value(`{}`)},
		{Name: "", Config: jsontext.Value(`{}`)},
	}}
	w.extraSources["vpn/good"] = &fakeSource{data: []byte("the good one")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 4 {
		t.Fatalf("artifacts %+v, want one per unit", rep.Artifacts)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK || rep.Artifacts[0].Artifact.Name != "cluster/good" {
		t.Fatalf("the usable unit = %+v, want it dumped", rep.Artifacts[0])
	}
	for i, want := range []struct{ artifact, reason string }{
		{"cluster/odd/name", "carries a path separator"},
		{"cluster/bad\tname", "carries a control character"},
		{"cluster/", "it is empty"},
	} {
		ar := rep.Artifacts[i+1]
		if ar.Outcome != model.ArtifactFailed || ar.Artifact.Name != want.artifact ||
			!strings.Contains(ar.Error, want.reason) {
			t.Fatalf("artifact %d = %+v, want the unusable name named (%s)", i+1, ar, want.reason)
		}
	}
}

// TestUnitWhoseConfigCannotBuildFailsThatUnitAlone: a unit whose config the
// engine cannot build into a source is that unit's own failed artifact.
func TestUnitWhoseConfigCannotBuildFailsThatUnitAlone(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "databases", "cluster", []string{"zstd3"}, "required", "plain")}
	w.extraSources["vpn"] = &fakeSource{data: []byte("parent"), units: []source.Unit{
		{Name: "bad", Config: jsontext.Value(`{not json`)},
	}}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 {
		t.Fatalf("artifacts %+v, want the one failed unit", rep.Artifacts)
	}
	ar := rep.Artifacts[0]
	if ar.Outcome != model.ArtifactFailed || ar.Artifact.Name != "cluster/bad" {
		t.Fatalf("artifact = %+v, want the unit's own failure", ar)
	}
}

// --- key.go -----------------------------------------------------------------
