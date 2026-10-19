package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// foundSource builds one well-formed Found of the fake source type.
func foundSource(name, artifactName string, importance model.Importance) discovery.Found {
	return discovery.Found{
		Name: name, Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: artifactName,
			Importance: importance, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
}

// TestRunRetriesATransientDiscoverFailure: one 5xx on Discover is retried like
// every other plugin call — the rule the module is built on, and the one that
// decides whether nine discovered artifacts exist tonight.
func TestRunRetriesATransientDiscoverFailure(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	d := w.discoveries["workloads"]
	d.found = []discovery.Found{foundSource("workloads/ns/db", "ns/db", model.ImportanceRequired)}
	d.errOnce = plugin.Transient(errors.New("api server returned 500"))

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s, want delivered: one 5xx cost the discovery", rep.Outcome)
	}
	if d.discovers < 2 {
		t.Fatalf("Discover ran %d times, want a retry", d.discovers)
	}
	if a := artifactByName(rep, "ns/db"); a.Outcome != model.ArtifactOK {
		t.Fatalf("discovered artifact %+v", a)
	}
}

// TestOneBadFoundCostsOnlyItsOwnArtifact: an object the discovery could not
// render is one failed artifact with the rule's importance — every other
// artifact of the rule, and of the run, still runs.
func TestOneBadFoundCostsOnlyItsOwnArtifact(t *testing.T) {
	cases := []struct {
		name       string
		importance model.Importance
		outcome    model.Outcome
		exit       int
	}{
		{"required", model.ImportanceRequired, model.OutcomeFailed, 7},
		{"best_effort", model.ImportanceBestEffort, model.OutcomeDegraded, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
			cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
			e := serviceCheck(t, w, cfg)
			bad := foundSource("workloads/ns/broken", "ns/broken", tc.importance)
			bad.Problem = `pod ns/db: container 0 has no environment variable "PGPASSWORD"`
			bad.Type, bad.Config = "", nil
			w.discoveries["workloads"].found = []discovery.Found{bad, foundSource("workloads/ns/db", "ns/db", model.ImportanceRequired)}

			rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != tc.outcome || rep.ExitCode != tc.exit {
				t.Fatalf("outcome %s exit %d, want %s/%d", rep.Outcome, rep.ExitCode, tc.outcome, tc.exit)
			}
			if a := artifactByName(rep, "ns/broken"); a.Outcome != model.ArtifactFailed || !strings.Contains(a.Error, "PGPASSWORD") {
				t.Fatalf("the broken object %+v", a)
			}
			if a := artifactByName(rep, "ns/db"); a.Outcome != model.ArtifactOK {
				t.Fatalf("the healthy object of the same rule %+v", a)
			}
			if len(w.storage(t, "primary").Keys()) == 0 {
				t.Fatal("nothing was delivered")
			}
			if bad.Type != "" {
				t.Fatal("fixture drift: the broken Found should carry no type")
			}
		})
	}
}

// TestPreflightBuildsAndChecksDiscoveredSources: `validate --preflight` builds
// every found source exactly as a run would and runs its Check, so a literal
// only the plugin rejects fails here, not at 04:20 — and a Found that carries a
// problem gets its own row.
func TestPreflightBuildsAndChecksDiscoveredSources(t *testing.T) {
	w := newWorld(t)
	registerShaped(t, w)
	cfg := rulesConfig(t, w)
	w.discoveries["workloads"] = &fakeDiscovery{}
	e := w.newEngine(t, cfg)

	good := discovery.Found{
		Name: "workloads/ns/good", Type: "shaped",
		Config: []byte(`{"address":"a:1","database":"db","password":{"file":"/f"},"retries":1,"labels":{}}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/good",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
	bad := good
	bad.Name, bad.Artifact.Name = "workloads/ns/bad", "ns/bad"
	bad.Config = []byte(`{"address":"a:1","database":"db","password":{"file":"/f"},"retries":"6","labels":{}}`)
	problem := good
	problem.Name, problem.Artifact.Name = "workloads/ns/problem", "ns/problem"
	problem.Problem = "port 0 is not declared on this pod"
	w.discoveries["workloads"].found = []discovery.Found{good, bad, problem}

	rows := e.PreflightResults(context.Background(), PreflightRequest{})
	byKey := map[string]PreflightResult{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Instance] = r
	}
	if r := byKey["source/workloads/ns/good"]; !r.OK {
		t.Fatalf("a valid found source failed preflight: %+v", r)
	}
	if r := byKey["source/workloads/ns/bad"]; r.OK || r.Error == "" {
		t.Fatalf("a literal its own plugin rejects passed preflight: %+v", r)
	}
	if r := byKey["source/workloads/ns/problem"]; r.OK || !strings.Contains(r.Error, "port 0") {
		t.Fatalf("a found carrying a problem: %+v", r)
	}
}
