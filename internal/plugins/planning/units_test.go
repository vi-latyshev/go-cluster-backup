package planning

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"encoding/json/jsontext"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// unitSource is a source that enumerates units; a plain source answers
// errors.ErrUnsupported to Units, and plainSource is a source without the
// capability at all.
type unitSource struct {
	units    []source.Unit
	unitsErr error
	unitsRun int
	closed   int
}

func (s *unitSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, nil
}

func (s *unitSource) Dump(context.Context, io.Writer) error { return nil }

func (s *unitSource) Units(context.Context) ([]source.Unit, error) {
	s.unitsRun++
	return s.units, s.unitsErr
}

func (s *unitSource) Close() error {
	s.closed++
	return nil
}

// plainSource is a source without the Enumerator capability: exactly one artifact.
type plainSource struct{}

func (plainSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, nil
}

func (plainSource) Dump(context.Context, io.Writer) error { return nil }

// bareSource is a source that is not an io.Closer either.
type bareSource struct{}

func (bareSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, nil
}

func (bareSource) Dump(context.Context, io.Writer) error { return nil }

// fleet builds children by instance name, records the config each was built
// from, and fails the test on a build of an unknown name.
type fleet struct {
	children   map[string]source.Source
	buildErrs  map[string]error
	checkErrs  map[string]error
	configs    map[string]jsontext.Value
	own        []io.Closer
	checkedAs  []string
	buildCalls int
}

func (f *fleet) build(_ context.Context, typ, instance string, raw jsontext.Value) (source.Source, error) {
	f.buildCalls++
	if err := f.buildErrs[instance]; err != nil {
		return nil, err
	}
	if f.configs == nil {
		f.configs = map[string]jsontext.Value{}
	}
	f.configs[instance] = raw
	s, ok := f.children[instance]
	if !ok {
		return nil, errors.New("no child " + instance)
	}
	return s, nil
}

func (f *fleet) check(_ context.Context, _ source.Source, instance string) error {
	f.checkedAs = append(f.checkedAs, instance)
	return f.checkErrs[instance]
}

func (f *fleet) ownCloser(c io.Closer) { f.own = append(f.own, c) }

func (f *fleet) expansion() Expansion {
	return Expansion{Build: f.build, Check: f.check, Own: f.ownCloser}
}

func parentUnit(en source.Source) Unit {
	return Unit{
		Instance: "vpn", Type: "fake", Source: en,
		Artifact: model.Artifact{Group: "databases", Name: "cluster", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
	}
}

// TestExpandWithoutEnumeratorIsOneUnit: a source that is not a multi-artifact
// instance is exactly one unit: itself.
func TestExpandWithoutEnumeratorIsOneUnit(t *testing.T) {
	parent := parentUnit(plainSource{})
	got := testPlanner(1).Expand(context.Background(), parent, Expansion{})
	if len(got) != 1 || got[0].Instance != "vpn" || got[0].Artifact.Name != "cluster" || got[0].Problem != nil {
		t.Fatalf("units = %+v, want the parent alone", got)
	}
}

// TestExpandTreatsUnsupportedAsOneArtifact: errors.ErrUnsupported is the
// answer "this instance is one artifact" — a decision, never a fault, and it
// costs exactly one call.
func TestExpandTreatsUnsupportedAsOneArtifact(t *testing.T) {
	en := &unitSource{unitsErr: errors.ErrUnsupported}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), Expansion{})
	if len(got) != 1 || got[0].Problem != nil || got[0].Instance != "vpn" {
		t.Fatalf("units = %+v, want the parent alone", got)
	}
	if en.unitsRun != 1 {
		t.Fatalf("Units ran %d times, want 1", en.unitsRun)
	}
}

// TestExpandFailedEnumerationIsOneArtifact: a domain that cannot be read is one
// failed artifact with the parent's envelope, never a discovery that fails and
// takes every other rule's results with it.
func TestExpandFailedEnumerationIsOneArtifact(t *testing.T) {
	failure := errors.New("the catalog is unreadable")
	got := testPlanner(1).Expand(context.Background(), parentUnit(&unitSource{unitsErr: failure}), Expansion{})
	if len(got) != 1 || got[0].Artifact.Name != "cluster" || got[0].Instance != "vpn" {
		t.Fatalf("units = %+v, want the parent's one failed artifact", got)
	}
	if !errors.Is(got[0].Problem, failure) || !strings.Contains(got[0].Problem.Error(), "enumerating the artifacts of vpn") {
		t.Fatalf("problem = %v", got[0].Problem)
	}
}

// TestExpandEmptyEnumerationIsAPluginFault: a plugin that enumerates nothing has
// broken its own contract; a run that silently produces nothing would be worse
// than one failed artifact.
func TestExpandEmptyEnumerationIsAPluginFault(t *testing.T) {
	got := testPlanner(1).Expand(context.Background(), parentUnit(&unitSource{units: []source.Unit{}}), Expansion{})
	if len(got) != 1 || got[0].Problem == nil || !strings.Contains(got[0].Problem.Error(), "answers no artifact") {
		t.Fatalf("units = %+v", got)
	}
}

// TestExpandNamesUnits: a parent becomes one unit per name — the artifact
// "<envelope>/<unit>", the instance "<instance>/<unit>", the parent's plugin
// type — and each child is built from the config the plugin handed back.
func TestExpandNamesUnits(t *testing.T) {
	f := &fleet{children: map[string]source.Source{
		"vpn/db1": &unitSource{},
		"vpn/db2": &unitSource{},
	}}
	en := &unitSource{units: []source.Unit{
		{Name: "db1", Config: jsontext.Value(`{"role":"one"}`)},
		{Name: "db2", Config: jsontext.Value(`{"role":"two"}`)},
	}}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), f.expansion())
	if len(got) != 2 {
		t.Fatalf("units = %+v, want one per name", got)
	}
	for i, want := range []struct{ artifact, instance string }{
		{"cluster/db1", "vpn/db1"},
		{"cluster/db2", "vpn/db2"},
	} {
		u := got[i]
		if u.Artifact.Name != want.artifact || u.Instance != want.instance || u.Type != "fake" {
			t.Fatalf("unit %d = %+v, want %s from %s", i, u, want.artifact, want.instance)
		}
		if u.Problem != nil || u.Source == nil {
			t.Fatalf("unit %d = %+v, want a built source", i, u)
		}
		if u.Artifact.Group != "databases" || u.Artifact.Importance != model.ImportanceRequired {
			t.Fatalf("unit %d lost the rule's envelope: %+v", i, u.Artifact)
		}
	}
	if string(f.configs["vpn/db1"]) != `{"role":"one"}` || string(f.configs["vpn/db2"]) != `{"role":"two"}` {
		t.Fatalf("build configs = %v", f.configs)
	}
	if len(f.own) != 2 {
		t.Fatalf("owned %d built sources, want 2", len(f.own))
	}
	if f.checkedAs[0] != "vpn/db1" || f.checkedAs[1] != "vpn/db2" {
		t.Fatalf("checked as %v", f.checkedAs)
	}
}

// TestExpandRefusesUnusableNames: a name the planner cannot build a name from —
// a server's own database name — is that unit's own failed artifact, and the
// units beside it are still checked.
func TestExpandRefusesUnusableNames(t *testing.T) {
	f := &fleet{children: map[string]source.Source{"vpn/good": &unitSource{}}}
	en := &unitSource{units: []source.Unit{
		{Name: "good", Config: jsontext.Value(`{}`)},
		{Name: "odd/name", Config: jsontext.Value(`{}`)},
		{Name: "bad\tname", Config: jsontext.Value(`{}`)},
		{Name: "", Config: jsontext.Value(`{}`)},
	}}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), f.expansion())
	if len(got) != 4 || got[0].Problem != nil {
		t.Fatalf("units = %+v", got)
	}
	for i, want := range []struct{ artifact, reason string }{
		{"cluster/odd/name", "carries a path separator"},
		{"cluster/bad\tname", "carries a control character"},
		{"cluster/", "it is empty"},
	} {
		u := got[i+1]
		if u.Artifact.Name != want.artifact || u.Problem == nil || !strings.Contains(u.Problem.Error(), want.reason) {
			t.Fatalf("unit %d = %+v, want the unusable name named (%s)", i+1, u, want.reason)
		}
		if class, _ := pluginerrors.ClassOf(u.Problem); class != pluginerrors.ClassPermanent {
			t.Fatalf("unit %d class = %q, want permanent", i+1, class)
		}
	}
	if f.buildCalls != 1 {
		t.Fatalf("built %d children, want only the usable unit", f.buildCalls)
	}
}

// TestExpandOneBrokenUnitCostsOneArtifact: a unit whose source cannot be built
// or checked fails alone; every other unit still runs.
func TestExpandOneBrokenUnitCostsOneArtifact(t *testing.T) {
	f := &fleet{
		children:  map[string]source.Source{"vpn/db1": &unitSource{}, "vpn/db2": &unitSource{}},
		checkErrs: map[string]error{"vpn/db2": errors.New("unit two is down")},
	}
	en := &unitSource{units: []source.Unit{
		{Name: "db1", Config: jsontext.Value(`{}`)},
		{Name: "db2", Config: jsontext.Value(`{}`)},
	}}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), f.expansion())
	if len(got) != 2 {
		t.Fatalf("units = %+v", got)
	}
	if got[0].Problem != nil || got[0].Source == nil {
		t.Fatalf("the healthy unit did not build: %+v", got[0])
	}
	if got[1].Problem == nil || !strings.Contains(got[1].Problem.Error(), "unit two is down") {
		t.Fatalf("the broken unit = %+v, want its own failure", got[1])
	}
	if got[1].Source != nil {
		t.Fatalf("a failed check left a source: %+v", got[1])
	}
}

// TestExpandBuildFailureIsThatUnitsFailure: a config the registry refuses is the
// unit's own failed artifact, and the sibling still builds.
func TestExpandBuildFailureIsThatUnitsFailure(t *testing.T) {
	f := &fleet{
		children:  map[string]source.Source{"vpn/good": &unitSource{}},
		buildErrs: map[string]error{"vpn/bad": errors.New("bad config")},
	}
	en := &unitSource{units: []source.Unit{
		{Name: "bad", Config: jsontext.Value(`{not json`)},
		{Name: "good", Config: jsontext.Value(`{}`)},
	}}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), f.expansion())
	if len(got) != 2 || got[0].Problem == nil || got[0].Source != nil {
		t.Fatalf("units = %+v, want the bad unit failed alone", got)
	}
	if got[1].Problem != nil || got[1].Source == nil {
		t.Fatalf("the good unit did not build: %+v", got[1])
	}
}

// TestExpandWithoutCheckOrOwnership: a caller that passes no Check gets the
// built source as the unit's own; a source that is not a closer is not owned.
func TestExpandWithoutCheckOrOwnership(t *testing.T) {
	f := &fleet{children: map[string]source.Source{"vpn/a": bareSource{}, "vpn/b": plainSource{}}}
	en := &unitSource{units: []source.Unit{
		{Name: "a", Config: jsontext.Value(`{}`)},
		{Name: "b", Config: jsontext.Value(`{}`)},
	}}
	got := testPlanner(1).Expand(context.Background(), parentUnit(en), Expansion{Build: f.build})
	if len(got) != 2 || got[0].Source == nil || got[1].Source == nil {
		t.Fatalf("units = %+v, want checked-free sources", got)
	}
	if f.buildCalls != 2 {
		t.Fatalf("built %d children", f.buildCalls)
	}
}

// TestExpandRetriesEnumeration: a transient enumeration failure is worth the
// attempts the policy allows; the answer "one artifact" is not retried.
func TestExpandRetriesEnumeration(t *testing.T) {
	en := &unitSource{unitsErr: pluginerrors.Transient(errors.New("the catalog is busy"))}
	got := testPlanner(3).Expand(context.Background(), parentUnit(en), Expansion{})
	if en.unitsRun != 3 {
		t.Fatalf("Units ran %d times, want 3", en.unitsRun)
	}
	if len(got) != 1 || !strings.Contains(got[0].Problem.Error(), "the catalog is busy") {
		t.Fatalf("units = %+v", got)
	}
}

// TestExpandRecoversAPanic: a crashing Units call is that instance's permanent
// failure, never the process's.
func TestExpandRecoversAPanic(t *testing.T) {
	got := testPlanner(1).Expand(context.Background(), parentUnit(panicEnumerator{}), Expansion{})
	if len(got) != 1 || got[0].Problem == nil || !strings.Contains(got[0].Problem.Error(), "plugin panic") {
		t.Fatalf("units = %+v", got)
	}
}

type panicEnumerator struct{}

func (panicEnumerator) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, nil
}

func (panicEnumerator) Dump(context.Context, io.Writer) error { return nil }

func (panicEnumerator) Units(context.Context) ([]source.Unit, error) { panic("boom") }

// TestUnitNameRefusal pins the rule a plugin must not invent.
func TestUnitNameRefusal(t *testing.T) {
	cases := map[string]string{
		"db1":       "",
		"":          "it is empty",
		"a/b":       "it carries a path separator",
		"a\tb":      "it carries a control character",
		"fine.name": "",
	}
	for name, want := range cases {
		if got := unitNameRefusal(name); got != want {
			t.Errorf("unitNameRefusal(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestExpandAppliesTheCredentialDecisionToUnits: every built unit is judged by
// the run's credential rule before its Check; a refusal fails that unit alone,
// a warning rides beside it.
func TestExpandAppliesTheCredentialDecisionToUnits(t *testing.T) {
	parent := &unitSource{units: []source.Unit{
		{Name: "warned", Config: jsontext.Value(`{}`)},
		{Name: "refused", Config: jsontext.Value(`{}`)},
	}}
	child := &unitSource{unitsErr: errors.ErrUnsupported}
	f := &fleet{children: map[string]source.Source{"vpn/warned": child, "vpn/refused": child}}
	credential := func(_ context.Context, _ source.Source, artifact model.Artifact, pipeline []string) (model.Warning, error) {
		if len(pipeline) != 1 || pipeline[0] != "zstd3" {
			t.Fatalf("the unit's pipeline = %v, want the parent's", pipeline)
		}
		if strings.HasSuffix(artifact.Name, "/refused") {
			return model.Warning{}, errors.New("carries credentials")
		}
		return model.Warning{Code: "source.credentials.unsealed", Message: "plaintext"}, nil
	}
	exp := f.expansion()
	exp.Credential = credential
	units := testPlanner(1).Expand(context.Background(), Unit{
		Instance: "vpn", Type: "fake", Source: parent,
		Artifact: model.Artifact{Group: "databases", Name: "cluster", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}, exp)
	byName := map[string]Unit{}
	for _, u := range units {
		byName[u.Instance] = u
	}
	if u := byName["vpn/warned"]; u.Problem != nil || len(u.Warnings) != 1 || u.Warnings[0].Code != "source.credentials.unsealed" {
		t.Fatalf("the warned unit = %+v", u)
	}
	if u := byName["vpn/refused"]; u.Problem == nil || !strings.Contains(u.Problem.Error(), "carries credentials") {
		t.Fatalf("the refused unit = %+v", u)
	}
}
