package assembly

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// trackedSource is a source that is also an io.Closer: a restore's own scope
// owns every instance it built and closes it when the call returns.
type trackedSource struct {
	fakeSource
	closed bool
}

func (s *trackedSource) Close() error {
	s.closed = true
	return nil
}

// restoreSection is the restore half of a config: the spool, the walk order and
// the read-only retry policy every restore path needs.
func restoreSection(t *testing.T, order ...string) map[string]any {
	t.Helper()
	return map[string]any{
		"spool_dir": t.TempDir(), "spool_reserve_bytes": 0, "storage_order": order,
		"unclassified_error": "transient",
		"retry":              map[string]any{"attempts": 3, "initial_delay": "1ms", "max_delay": "5ms", "jitter": 0},
	}
}

// discoveredRig is a run config whose one discovery answers with the given
// objects: what a restore resolves its target from.
func discoveredRig(t *testing.T, found ...discovery.Found) (*world, *Assembly) {
	t.Helper()
	w := newWorld(t)
	w.Discoveries["d"] = &fakeDiscovery{Found: found}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"d": instance("fake")}
	return w, w.newAssembly(t, cfg)
}

// TestStorageOrderNamesTheWalk: the order a fetch walks is the request's own
// list, else restore.storage_order — the same derivation the walk itself uses —
// and an unknown storage or a config without a restore section is refused.
func TestStorageOrderNamesTheWalk(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{
		"primary": storageInstance("fake", "keep_last"),
		"offsite": storageInstance("fake", "keep_last"),
	})
	cfg["restore"] = restoreSection(t, "offsite", "primary")
	e := w.newAssembly(t, cfg)

	got, err := e.StorageOrder(nil)
	if err != nil || !slices.Equal(got, []string{"offsite", "primary"}) {
		t.Fatalf("the configured walk = %v, %v", got, err)
	}
	got, err = e.StorageOrder([]string{"primary"})
	if err != nil || !slices.Equal(got, []string{"primary"}) {
		t.Fatalf("the requested walk = %v, %v", got, err)
	}
	if _, err := e.StorageOrder([]string{"ghost"}); err == nil || !strings.Contains(err.Error(), "unknown storage") {
		t.Fatalf("an unknown storage = %v, want it refused", err)
	}

	plain := newWorld(t)
	e2 := plain.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if _, err := e2.StorageOrder(nil); err == nil || !strings.Contains(err.Error(), "no restore section") {
		t.Fatalf("a walk without a restore section = %v, want it refused", err)
	}
}

// TestResolveTargetRefusesWhatItCannotBuild: every refusal a restore makes about
// its target before writing a byte — no target, a discovery that cannot answer,
// an excluded or unavailable object, an unknown type, an envelope that holds no
// matching unit and a discovery that holds nothing — names what is wrong, and
// the scope still closes the instances it built.
func TestResolveTargetRefusesWhatItCannotBuild(t *testing.T) {
	ctx := context.Background()
	var tracked *trackedSource
	cases := []struct {
		name     string
		target   string
		artifact string
		Found    []discovery.Found
		prepare  func(w *world)
		after    func(*testing.T)
		errSub   string
	}{
		{name: "target is required", target: "", artifact: "vpn", errSub: "Target is required"},
		{
			name: "discovery fails", target: "discovered", artifact: "vpn",
			prepare: func(w *world) { w.Discoveries["d"].Err = errors.New("api down") },
			errSub:  "apply: discovery d: api down",
		},
		{
			name: "excluded", target: "discovered", artifact: "vpn",
			Found: []discovery.Found{{
				Name: "vpn", Artifact: model.Artifact{Name: "vpn"},
				Excluded: model.ExclusionIgnored, ExcludedReason: "carries the exclusion marker",
			}},
			errSub: "is excluded: carries the exclusion marker",
		},
		{
			name: "unavailable", target: "discovered", artifact: "vpn",
			Found: []discovery.Found{{
				Name: "vpn", Artifact: model.Artifact{Name: "vpn"}, Problem: "cannot render the source",
			}},
			errSub: "is unavailable: cannot render the source",
		},
		{
			name: "unknown type", target: "discovered", artifact: "vpn",
			Found: []discovery.Found{{
				Name: "vpn", Type: "nosuch", Artifact: model.Artifact{Name: "vpn"},
			}},
			errSub: `unknown source type "nosuch"`,
		},
		{
			name: "under another envelope", target: "discovered", artifact: "vpn",
			Found:  []discovery.Found{{Name: "other", Artifact: model.Artifact{Name: "other"}}},
			errSub: `no discovered source has artifact "vpn"`,
		},
		{
			name: "no discovered source", target: "discovered", artifact: "vpn",
			errSub: `no discovered source has artifact "vpn"`,
		},
		{
			name: "holds no artifact", target: "discovered", artifact: "vpn/db",
			Found: []discovery.Found{{Name: "vpn", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Name: "vpn"}}},
			prepare: func(w *world) {
				tracked = &trackedSource{fakeSource: fakeSource{UnitList: []source.Unit{{Name: "cache", Config: jsontext.Value(`{}`)}}}}
				w.ExtraSources["vpn"] = tracked
			},
			after: func(t *testing.T) {
				t.Helper()
				if !tracked.closed {
					t.Fatal("the scope did not close the instance it built before the refusal")
				}
			},
			errSub: `holds no artifact "vpn/db"`,
		},
		{
			name: "unit is unavailable", target: "discovered", artifact: "vpn/db",
			Found: []discovery.Found{{Name: "vpn", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Name: "vpn"}}},
			prepare: func(w *world) {
				w.ExtraSources["vpn"] = &fakeSource{UnitList: []source.Unit{{Name: "db", Config: jsontext.Value(`{}`)}, {Name: "cache", Config: jsontext.Value(`{}`)}}}
				w.ExtraSources["vpn/db"] = &fakeSource{CheckErr: errors.New("no key")}
			},
			errSub: "is unavailable: no key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tracked = nil
			w, e := discoveredRig(t, tc.Found...)
			if tc.prepare != nil {
				tc.prepare(w)
			}
			tgt, closer, err := e.ResolveTarget(ctx, tc.target, tc.artifact)
			if err == nil || !strings.Contains(err.Error(), tc.errSub) {
				t.Fatalf("ResolveTarget = %+v, %v, want an error naming %q", tgt, err, tc.errSub)
			}
			if closer != nil {
				t.Fatal("a refused resolution returned a closer")
			}
			if tc.after != nil {
				tc.after(t)
			}
		})
	}
}

// TestResolveTargetUsesAStaticSourceByName: a target that names a declared source
// writes into that instance, and the call's closer is the scope, empty of
// discovered instances.
func TestResolveTargetUsesAStaticSourceByName(t *testing.T) {
	w := newWorld(t)
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	tgt, closer, err := e.ResolveTarget(context.Background(), "vpn", "wireguard")
	if err != nil {
		t.Fatal(err)
	}
	if tgt.Type != "fake" || tgt.Source != source.Source(w.Sources["vpn"]) {
		t.Fatalf("the static target = %+v, want the declared vpn instance", tgt)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("closing the empty scope: %v", err)
	}
}

// TestResolveTargetRefusesAnUnknownTargetName: a target that is neither a
// declared source nor "discovered" is refused with every name that would work.
func TestResolveTargetRefusesAnUnknownTargetName(t *testing.T) {
	w := newWorld(t)
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	_, _, err := e.ResolveTarget(context.Background(), "ghost", "wireguard")
	if err == nil || !strings.Contains(err.Error(), `unknown target "ghost"`) ||
		!strings.Contains(err.Error(), "vpn") || !strings.Contains(err.Error(), `"discovered"`) {
		t.Fatalf("an unknown target = %v, want it refused with the known names", err)
	}
}

// TestResolveTargetPicksTheUnitUnderTheEnvelope: a discovered instance whose
// domain holds several artifacts resolves the unit the caller named, leaves the
// units beside it unbuilt and closes every instance the resolution built.
func TestResolveTargetPicksTheUnitUnderTheEnvelope(t *testing.T) {
	w, e := discoveredRig(t, discovery.Found{
		Name: "vpn", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Name: "vpn"},
	})
	parent := &trackedSource{fakeSource: fakeSource{UnitList: []source.Unit{{Name: "db", Config: jsontext.Value(`{}`)}, {Name: "cache", Config: jsontext.Value(`{}`)}}}}
	w.ExtraSources["vpn"] = parent
	unpicked := &trackedSource{}
	w.ExtraSources["vpn/db"] = unpicked

	tgt, closer, err := e.ResolveTarget(context.Background(), "discovered", "vpn/cache")
	if err != nil {
		t.Fatal(err)
	}
	if tgt.Type != "fake" || tgt.Source == nil {
		t.Fatalf("the resolved unit = %+v, want the cache source", tgt)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("closing the scope: %v", err)
	}
	if !unpicked.closed || !parent.closed {
		t.Fatalf("the scope left instances behind: unpicked closed=%v parent closed=%v", unpicked.closed, parent.closed)
	}
}
