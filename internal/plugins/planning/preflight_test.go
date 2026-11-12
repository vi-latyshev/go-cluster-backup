package planning

import (
	"context"
	"errors"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// checker is a plugin's own Check with scripted failures.
type checker struct {
	errs  []error
	calls int
}

func (c *checker) Check(context.Context) error {
	c.calls++
	if n := len(c.errs); n > 0 {
		err := c.errs[0]
		c.errs = c.errs[1:]
		return err
	}
	return nil
}

// decodeChecker is a processor's restore-side check with scripted failures.
type decodeChecker struct {
	errs  []error
	calls int
}

func (c *decodeChecker) CheckDecode(context.Context) error {
	c.calls++
	if n := len(c.errs); n > 0 {
		err := c.errs[0]
		c.errs = c.errs[1:]
		return err
	}
	return nil
}

func instance(kind, name string, c *checker) Instance {
	inst := Instance{Kind: kind, Name: name}
	inst.Check = c
	return inst
}

// TestTargetsListsInstancesInAStableOrder: what the CLI validates a selection
// against, sorted, and nothing at all when the caller's context already ended.
func TestTargetsListsInstancesInAStableOrder(t *testing.T) {
	got := Targets(context.Background(), []Instance{
		{Kind: "source", Name: "vpn"},
		{Kind: "discovery", Name: "workloads"},
		{Kind: "source", Name: "app"},
	})
	want := []Target{{"discovery", "workloads"}, {"source", "app"}, {"source", "vpn"}}
	if len(got) != len(want) {
		t.Fatalf("targets = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets = %+v, want %+v", got, want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := Targets(ctx, []Instance{{Kind: "source", Name: "app"}}); got != nil {
		t.Fatalf("cancelled targets = %+v, want nil", got)
	}
}

// TestResultsCoversEveryInstanceAndDiscoveredSource: `validate --preflight`
// checks everything, decode side included, and reports one row per instance.
func TestResultsCoversEveryInstanceAndDiscoveredSource(t *testing.T) {
	decode := &decodeChecker{errs: []error{errors.New("the key is not mounted")}}
	inst := Instance{Kind: "processor", Name: "seal", Decode: decode}
	d := &scriptedDiscovery{passes: [][]discovery.Found{{
		{
			Name:     "volumes/ns/media",
			Type:     "a-type-no-registry-knows",
			Artifact: model.Artifact{Group: "volumes", Name: "ns/media", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
			Excluded: model.ExclusionIgnored, ExcludedReason: "carries the exclusion marker",
		},
	}}}
	rows := testPlanner(1).Results(context.Background(), Request{}, Options{
		Instances:   []Instance{inst, instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
	})
	byKey := map[string]Result{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Instance] = r
	}
	if r := byKey["processor/seal"]; r.OK || !strings.Contains(r.Error, "the key is not mounted") {
		t.Fatalf("the decode side row = %+v", r)
	}
	if r := byKey["discovery/workloads"]; !r.OK {
		t.Fatalf("the discovery row = %+v", r)
	}
	if r := byKey["source/volumes/ns/media"]; !r.OK || r.Excluded != model.ExclusionIgnored || r.ExcludedReason != "carries the exclusion marker" {
		t.Fatalf("the excluded row = %+v", r)
	}
}

// TestCheckSelectsAndRefusesUnknownTargets: the selection is applied before any
// Check, and a name that is not a listed instance is a failed row of its own,
// never silence.
func TestCheckSelectsAndRefusesUnknownTargets(t *testing.T) {
	checked := &checker{}
	picked := &checker{}
	rows := testPlanner(1).Check(context.Background(), []Target{
		{"source", "picked"},
		{"source", "ghost"},
	}, Options{Instances: []Instance{
		instance("source", "checked", checked),
		instance("source", "picked", picked),
	}})
	if checked.calls != 0 {
		t.Fatal("an unselected instance was checked")
	}
	if picked.calls != 1 {
		t.Fatalf("the selected instance ran %d checks", picked.calls)
	}
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Name] = r
	}
	if r := byKey["source/ghost"]; r.Err == nil || !strings.Contains(r.Err.Error(), `unknown instance source/ghost (known: source/checked, source/picked)`) {
		t.Fatalf("the unknown target row = %+v", r)
	}
}

// TestCheckMissingBinaryFailsBeforeAnyCheck: a binary a plugin type requires and
// PATH does not have is a startup failure, and the plugin's Check never runs.
func TestCheckMissingBinaryFailsBeforeAnyCheck(t *testing.T) {
	c := &checker{}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances: []Instance{instance("source", "vpn", c)},
		Binaries: func(kind, name string) []string {
			if kind != "source" || name != "vpn" {
				t.Fatalf("binaries asked for %s %s", kind, name)
			}
			return []string{"definitely-not-on-path-gcb"}
		},
	})
	if len(rows) != 1 || rows[0].Err == nil || !strings.Contains(rows[0].Err.Error(), "required binaries are not on PATH: definitely-not-on-path-gcb") {
		t.Fatalf("rows = %+v", rows)
	}
	if c.calls != 0 {
		t.Fatal("a check ran despite the missing binary")
	}
	// No lookup means no missing binaries.
	rows = testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{instance("source", "vpn", c)}})
	if len(rows) != 1 || rows[0].Err != nil || c.calls != 1 {
		t.Fatalf("rows = %+v, checks = %d", rows, c.calls)
	}
}

// TestCheckScopesEveryFailure: every instance is checked regardless of a
// sibling's failure, rows come back sorted, and the decode side is not asked
// unless the caller does.
func TestCheckScopesEveryFailure(t *testing.T) {
	broken := &checker{errs: []error{errors.New("credentials rejected")}}
	decode := &decodeChecker{}
	rows := testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{
		{Kind: "storage", Name: "offsite", Check: broken},
		{Kind: "processor", Name: "seal", Decode: decode},
		{Kind: "source", Name: "vpn"},
	}})
	if len(rows) != 3 {
		t.Fatalf("rows = %+v", rows)
	}
	want := []struct{ kind, name string }{{"processor", "seal"}, {"source", "vpn"}, {"storage", "offsite"}}
	for i, w := range want {
		if rows[i].Kind != w.kind || rows[i].Name != w.name {
			t.Fatalf("row %d = %+v, want %s/%s", i, rows[i], w.kind, w.name)
		}
	}
	if rows[2].Err == nil || !strings.Contains(rows[2].Err.Error(), "credentials rejected") {
		t.Fatalf("the failed storage = %+v", rows[2])
	}
	if decode.calls != 0 {
		t.Fatal("the decode side was checked without being asked")
	}
}

// TestCheckAsksForDecodeOnlyWhenRequested: `validate --preflight` asks whether
// the key material is at hand; a run or a stage, which only encode, must not.
// A failed Check skips the decode side: the instance has already failed.
func TestCheckAsksForDecodeOnlyWhenRequested(t *testing.T) {
	decode := &decodeChecker{errs: []error{errors.New("the key is not mounted")}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:  []Instance{{Kind: "processor", Name: "seal", Decode: decode}},
		WantDecode: true,
	})
	if len(rows) != 1 || rows[0].Err == nil || !strings.Contains(rows[0].Err.Error(), "the key is not mounted") {
		t.Fatalf("rows = %+v", rows)
	}
	if decode.calls != 1 {
		t.Fatalf("decode ran %d times", decode.calls)
	}

	failing := &checker{errs: []error{errors.New("encode side down")}}
	decode = &decodeChecker{}
	rows = testPlanner(1).Check(context.Background(), nil, Options{
		Instances:  []Instance{{Kind: "processor", Name: "seal", Check: failing, Decode: decode}},
		WantDecode: true,
	})
	if len(rows) != 1 || rows[0].Err == nil {
		t.Fatalf("rows = %+v", rows)
	}
	if decode.calls != 0 {
		t.Fatal("the decode side ran after the check failed")
	}
}

// TestCheckRecoversAPanic: a crashing Check fails its instance permanently,
// never the process.
func TestCheckRecoversAPanic(t *testing.T) {
	rows := testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{
		{Kind: "source", Name: "boom", Check: panicChecker{}},
	}})
	if len(rows) != 1 || rows[0].Err == nil || !strings.Contains(rows[0].Err.Error(), "plugin panic") {
		t.Fatalf("rows = %+v", rows)
	}
	if class, _ := pluginerrors.ClassOf(rows[0].Err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

type panicChecker struct{}

func (panicChecker) Check(context.Context) error { panic("boom") }

// TestCheckRunsDiscoveriesAndChecksFoundSources: a preflight runs every selected
// discovery, builds each found source exactly as a run would and checks it,
// expanding its units; an unselected discovery contributes no row at all.
func TestCheckRunsDiscoveriesAndChecksFoundSources(t *testing.T) {
	f := &fleet{children: map[string]source.Source{
		"workloads/ns/db": &unitSource{unitsErr: errors.ErrUnsupported},
	}}
	found := discovery.Found{
		Name: "workloads/ns/db", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/db", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
	d := &scriptedDiscovery{passes: [][]discovery.Found{{found}}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Build:       f.build,
		Check:       f.check,
		Discovered:  true,
	})
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Name] = r
	}
	if r, ok := byKey["discovery/workloads"]; !ok || r.Err != nil {
		t.Fatalf("the discovery row = %+v (%v)", r, ok)
	}
	if r, ok := byKey["source/workloads/ns/db"]; !ok || r.Err != nil {
		t.Fatalf("the found source row = %+v (%v)", r, ok)
	}

	// An unselected discovery is not even discovered.
	d.calls = 0
	rows = testPlanner(1).Check(context.Background(), []Target{{"source", "vpn"}}, Options{
		Instances: []Instance{
			instance("discovery", "workloads", &checker{}),
			instance("source", "vpn", &checker{}),
		},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Discovered:  true,
	})
	for _, r := range rows {
		if r.Kind == "discovery" {
			t.Fatalf("an unselected discovery contributed a row: %+v", rows)
		}
	}
	if d.calls != 0 {
		t.Fatal("an unselected discovery was run")
	}
}

// TestCheckAnnotatesAFailedDiscovery: a discovery whose own Check failed cannot
// build anything — its one row is annotated, Discover never runs, and no second
// row appears.
func TestCheckAnnotatesAFailedDiscovery(t *testing.T) {
	d := &scriptedDiscovery{passes: [][]discovery.Found{{plainFound("workloads/a", "ns/a")}}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{errs: []error{errors.New("the API is refused")}})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Discovered:  true,
	})
	if len(rows) != 1 || rows[0].Err == nil ||
		!strings.Contains(rows[0].Err.Error(), "the API is refused; its sources were not checked") {
		t.Fatalf("rows = %+v", rows)
	}
	if d.calls != 0 {
		t.Fatal("a discovery whose Check failed was run")
	}
}

// TestCheckDiscoveryFailureIsItsOwnRow: a failed Discover turns its own row into
// the failed one instead of adding another.
func TestCheckDiscoveryFailureIsItsOwnRow(t *testing.T) {
	d := &scriptedDiscovery{
		passes: [][]discovery.Found{{}},
		errs:   []error{pluginerrors.Permanent(errors.New("the API is unreachable"))},
	}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Discovered:  true,
	})
	if len(rows) != 1 || rows[0].Err == nil || !strings.Contains(rows[0].Err.Error(), "the API is unreachable") {
		t.Fatalf("rows = %+v", rows)
	}
}

// TestCheckSkipsADiscoveryWithNoRow: a discovery that is not part of the checked
// scope is not run by the preflight either.
func TestCheckSkipsADiscoveryWithNoRow(t *testing.T) {
	d := &scriptedDiscovery{passes: [][]discovery.Found{{}}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("source", "vpn", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Discovered:  true,
	})
	if len(rows) != 1 || d.calls != 0 {
		t.Fatalf("rows = %+v, discovery calls = %d", rows, d.calls)
	}
}

// TestCheckFoundRefusals: an excluded object is a decision with no build at all,
// a found carrying a problem is its row's error, and a source that cannot be
// built or checked fails alone.
func TestCheckFoundRefusals(t *testing.T) {
	f := &fleet{children: map[string]source.Source{}}
	f.buildErrs = map[string]error{"workloads/ns/badconfig": errors.New("bad config")}
	f.checkErrs = map[string]error{"workloads/ns/badcheck": errors.New("check refused")}
	excluded := discovery.Found{
		Name: "volumes/ns/media", Type: "a-type-no-registry-knows",
		Artifact: model.Artifact{Group: "volumes", Name: "ns/media", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Excluded: model.ExclusionIgnored, ExcludedReason: "carries the exclusion marker",
	}
	problem := plainFound("workloads/ns/problem", "ns/problem")
	problem.Problem = "port 0 is not declared on this pod"
	badConfig := plainFound("workloads/ns/badconfig", "ns/badconfig")
	badConfig.Type = "fake"
	badCheck := plainFound("workloads/ns/badcheck", "ns/badcheck")
	badCheck.Type = "fake"
	good := plainFound("workloads/ns/good", "ns/good")
	good.Type = "fake"
	f.children["workloads/ns/badcheck"] = &unitSource{unitsErr: errors.ErrUnsupported}
	f.children["workloads/ns/good"] = &unitSource{unitsErr: errors.ErrUnsupported}

	d := &scriptedDiscovery{passes: [][]discovery.Found{{excluded, problem, badConfig, badCheck, good}}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Build:       f.build,
		Check:       f.check,
		Discovered:  true,
	})
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Name] = r
	}
	if r := byKey["source/volumes/ns/media"]; r.Err != nil || r.Excluded != model.ExclusionIgnored || r.Reason != "carries the exclusion marker" {
		t.Fatalf("the excluded row = %+v", r)
	}
	if r := byKey["source/workloads/ns/problem"]; r.Err == nil || !strings.Contains(r.Err.Error(), "port 0") {
		t.Fatalf("the problem row = %+v", r)
	}
	if r := byKey["source/workloads/ns/badconfig"]; r.Err == nil || !strings.Contains(r.Err.Error(), "bad config") {
		t.Fatalf("the unbuildable row = %+v", r)
	}
	if r := byKey["source/workloads/ns/badcheck"]; r.Err == nil || !strings.Contains(r.Err.Error(), "check refused") {
		t.Fatalf("the failed-check row = %+v", r)
	}
	if r := byKey["source/workloads/ns/good"]; r.Err != nil {
		t.Fatalf("the healthy found source = %+v", r)
	}
}

// TestCheckFoundExpandsUnitsAndClosesThem: a found instance whose domain holds
// several artifacts reports one row per unit, and every source the preflight
// built is closed before it returns.
func TestCheckFoundExpandsUnitsAndClosesThem(t *testing.T) {
	parent := &unitSource{units: []source.Unit{
		{Name: "db1", Config: jsontext.Value(`{}`)},
		{Name: "odd/name", Config: jsontext.Value(`{}`)},
	}}
	child := &unitSource{unitsErr: errors.ErrUnsupported}
	f := &fleet{children: map[string]source.Source{
		"workloads/pg":     parent,
		"workloads/pg/db1": child,
	}}
	found := discovery.Found{
		Name: "workloads/pg", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/pg", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}
	d := &scriptedDiscovery{passes: [][]discovery.Found{{found}}}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Build:       f.build,
		Check:       f.check,
		Discovered:  true,
	})
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Name] = r
	}
	if r, ok := byKey["source/workloads/pg/db1"]; !ok || r.Err != nil {
		t.Fatalf("the unit row = %+v (%v)", r, ok)
	}
	if r, ok := byKey["source/workloads/pg/odd/name"]; !ok || r.Err == nil {
		t.Fatalf("the unusable unit row = %+v (%v)", r, ok)
	}
	if parent.closed == 0 || child.closed == 0 {
		t.Fatalf("the preflight left instances behind: parent closed %d, child closed %d", parent.closed, child.closed)
	}
}

// TestBinaryErrorsNamesEveryInstance: a run or a stage refuses to start before
// any plugin work, naming the instance and the program.
func TestBinaryErrorsNamesEveryInstance(t *testing.T) {
	instances := []Instance{{Kind: "source", Name: "vpn"}, {Kind: "source", Name: "app"}}
	errs := BinaryErrors(instances, func(_, name string) []string {
		if name == "vpn" {
			return []string{"definitely-not-on-path-gcb"}
		}
		return nil
	})
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "source vpn: required binaries are not on PATH: definitely-not-on-path-gcb") {
		t.Fatalf("errs = %v", errs)
	}
	if errs := BinaryErrors(instances, nil); len(errs) != 0 {
		t.Fatalf("errs without a lookup = %v", errs)
	}
}

// binariesChecker scripts the optional local suite proof.
type binariesChecker struct {
	err   error
	calls int
}

func (b *binariesChecker) CheckBinaries(context.Context) error {
	b.calls++
	return b.err
}

// TestCheckProvesTheBinarySuiteBeforeTheRemoteCheck: a configured client suite
// is proved locally first — a wrong-major or incomplete directory is the
// deployment's fault, and the remote Check must not hide it behind a connection
// error. A suite that holds up lets the remote Check run.
func TestCheckProvesTheBinarySuiteBeforeTheRemoteCheck(t *testing.T) {
	suite := &binariesChecker{err: errors.New("client suite /usr/libexec/postgresql15 names major 15 but its programs report 16")}
	remote := &checker{}
	inst := Instance{Kind: "source", Name: "pg", Check: remote, BinariesCheck: suite}
	rows := testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{inst}})
	if len(rows) != 1 || rows[0].Err == nil || !strings.Contains(rows[0].Err.Error(), "names major 15") {
		t.Fatalf("rows = %+v", rows)
	}
	if remote.calls != 0 {
		t.Fatal("the remote check ran on an unusable client suite")
	}

	suite.err = nil
	rows = testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{inst}})
	if len(rows) != 1 || rows[0].Err != nil {
		t.Fatalf("rows = %+v", rows)
	}
	if suite.calls != 2 || remote.calls != 1 {
		t.Fatalf("suite calls = %d, remote calls = %d", suite.calls, remote.calls)
	}

	// An instance without the capability is never asked for a suite.
	noSuite := Instance{Kind: "source", Name: "plain", Check: &checker{}}
	rows = testPlanner(1).Check(context.Background(), nil, Options{Instances: []Instance{noSuite}})
	if len(rows) != 1 || rows[0].Err != nil {
		t.Fatalf("rows = %+v", rows)
	}
}

// TestCheckFoundAppliesTheCredentialDecision: a discovered source answers the
// run's credential rule before its operational Check — a refusal is its failed
// row, a warning rides beside an ok row.
func TestCheckFoundAppliesTheCredentialDecision(t *testing.T) {
	f := &fleet{children: map[string]source.Source{
		"workloads/refused": &unitSource{unitsErr: errors.ErrUnsupported},
		"workloads/warned":  &unitSource{unitsErr: errors.ErrUnsupported},
	}}
	refused := plainFound("workloads/refused", "databases/refused")
	refused.Type = "fake"
	warned := plainFound("workloads/warned", "databases/warned")
	warned.Type = "fake"
	d := &scriptedDiscovery{passes: [][]discovery.Found{{refused, warned}}}
	credential := func(_ context.Context, _ source.Source, artifact model.Artifact, _ []string) (model.Warning, error) {
		if artifact.Name == "databases/refused" {
			return model.Warning{}, errors.New("carries credentials and its pipeline has no sealing processor")
		}
		return model.Warning{Code: "source.credentials.unsealed", Message: "plaintext credentials"}, nil
	}
	rows := testPlanner(1).Check(context.Background(), nil, Options{
		Instances:   []Instance{instance("discovery", "workloads", &checker{})},
		Discoveries: []Discoverer{{Name: "workloads", Instance: d}},
		Build:       f.build,
		Check:       f.check,
		Credential:  credential,
		Discovered:  true,
	})
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Kind+"/"+r.Name] = r
	}
	if r := byKey["source/workloads/refused"]; r.Err == nil || !strings.Contains(r.Err.Error(), "carries credentials") {
		t.Fatalf("the refused row = %+v", r)
	}
	if r := byKey["source/workloads/warned"]; r.Err != nil || len(r.Warnings) != 1 || r.Warnings[0].Code != "source.credentials.unsealed" {
		t.Fatalf("the warned row = %+v", r)
	}
}
