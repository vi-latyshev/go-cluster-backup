package assembly

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// decodingProcessor answers the restore-side preflight: the only shape that
// projects a planning.Instance.Decode, which `validate --preflight` asks about
// and a run does not.
type decodingProcessor struct {
	fakeProcessor
	decodeErr error
}

func (p *decodingProcessor) CheckDecode(context.Context) error { return p.decodeErr }

// sourceOnly is a discovered source whose plugin offers neither a Check nor an
// Enumerator: the preflight checks nothing for it and reports it healthy.
type sourceOnly struct{}

func (sourceOnly) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "test://source-only"}, nil
}

func (sourceOnly) Dump(context.Context, io.Writer) error { return nil }

// preflightStageConfig is the smallest configuration with a stage section: the
// stage's own source is what a distribution-wide preflight sees and a run does not.
func preflightStageConfig(t *testing.T) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["stage"] = map[string]any{
		"dir": t.TempDir(), "degraded_exit_code": 5, "failed_exit_code": 9,
		"sources": map[string]any{
			"snapshot": sourceInstance("fake", "control-plane", "k3s-state", []string{}, "required", "plain"),
		},
	}
	return cfg
}

// TestPreflightTargetsListsEveryDeclaredInstance: a distribution-wide preflight
// selects from every declared instance — processors, storages, run sources,
// discoveries, notifiers, the catalog and the stage's own sources, each under
// its own kind — in the stable order the CLI reports. A cancelled context lists
// nothing, because there is no work left to select for.
func TestPreflightTargetsListsEveryDeclaredInstance(t *testing.T) {
	w := newWorld(t)
	cfg := preflightStageConfig(t)
	cfg["discoveries"] = map[string]any{"d": instance("fake")}
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	e := w.newAssembly(t, cfg)

	got := e.PreflightTargets(context.Background())
	want := []planning.Target{
		{Kind: "catalog", Name: "catalog"},
		{Kind: "discovery", Name: "d"},
		{Kind: "notifier", Name: "ops"},
		{Kind: "processor", Name: "zstd3"},
		{Kind: "source", Name: "vpn"},
		{Kind: "stage_source", Name: "snapshot"},
		{Kind: "storage", Name: "primary"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := e.PreflightTargets(ctx); got != nil {
		t.Fatalf("a cancelled preflight listed %v, want nothing", got)
	}
}

// TestPreflightResultsAsksTheDecodeSideAndSkipsChecklessSources: the preflight
// checks a processor's restore side — key material a run must not demand — and
// a discovered source whose plugin implements no Check is healthy, not skipped
// from the report.
func TestPreflightResultsAsksTheDecodeSideAndSkipsChecklessSources(t *testing.T) {
	w := newWorld(t)
	registerProcessor(t, w, "decodefail", &decodingProcessor{decodeErr: errors.New("no key")})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"].(map[string]any)["dec"] = instance("decodefail")
	cfg["discoveries"] = map[string]any{"d": instance("fake")}
	w.Discoveries["d"] = &fakeDiscovery{Found: []discovery.Found{{
		Name: "plain", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Name: "plain"},
	}}}
	w.ExtraSources["plain"] = sourceOnly{}
	e := w.newAssembly(t, cfg)

	byKey := map[string]planning.Result{}
	for _, r := range e.PreflightResults(context.Background(), planning.Request{}) {
		byKey[r.Kind+"/"+r.Instance] = r
	}
	if r := byKey["processor/dec"]; r.OK || !strings.Contains(r.Error, "no key") {
		t.Fatalf("the processor's decode side = %+v, want the missing key reported", r)
	}
	if r := byKey["source/plain"]; !r.OK || r.Error != "" {
		t.Fatalf("a discovered source without a Check = %+v, want it healthy", r)
	}
}
