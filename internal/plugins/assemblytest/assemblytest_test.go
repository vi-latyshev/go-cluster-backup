package assemblytest

// The kit's own tests: every fake's branch, every builder and the registration
// mechanics, so the shared package keeps the repository's 100% coverage.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// newTestWorld builds one kit world with no distribution plugins.
func newTestWorld(t *testing.T) *World {
	t.Helper()
	w, err := NewWorld(nil)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("the call did not panic")
		}
	}()
	fn()
}

// TestNewWorldRegistersTheKit: the six fake factories build through the kit's
// registry, and the world's construction instances answer by name.
func TestNewWorldRegistersTheKit(t *testing.T) {
	w := newTestWorld(t)
	if w.Loader == nil || w.Registry == nil {
		t.Fatal("the kit world carries no registries")
	}
	ctx := context.Background()
	deps := instances.Deps{Instance: "a"}
	src := must.Do(instances.Build(ctx, w.Registry, source.Kind, "fake", jsontext.Value(`{}`), deps))
	if _, ok := src.(*Source); !ok {
		t.Fatalf("the source factory answered %T", src)
	}
	// The same instance name answers the same fake twice.
	again := must.Do(instances.Build(ctx, w.Registry, source.Kind, "fake", jsontext.Value(`{}`), deps))
	if again != src {
		t.Fatal("the source factory built a second fake for one instance")
	}
	if _, ok := w.Configs["a"]; !ok {
		t.Fatal("the source factory did not record the instance config")
	}
	// An explicitly provided source wins over the one the factory would build.
	extra := &Source{Data: []byte("preset")}
	w.ExtraSources["b"] = extra
	if got := must.Do(instances.Build(ctx, w.Registry, source.Kind, "fake", jsontext.Value(`{}`), instances.Deps{Instance: "b"})); got != extra {
		t.Fatalf("the extra source was not handed back: %T", got)
	}
	p := must.Do(instances.Build(ctx, w.Registry, processor.Kind, "fake", jsontext.Value(`{}`), deps))
	if _, ok := p.(*Processor); !ok {
		t.Fatalf("the processor factory answered %T", p)
	}
	if again := must.Do(instances.Build(ctx, w.Registry, processor.Kind, "fake", jsontext.Value(`{}`), deps)); again != p {
		t.Fatal("the processor factory built a second fake for one instance")
	}
	if got := must.Do(instances.Build(ctx, w.Registry, discovery.Kind, "fake", jsontext.Value(`{}`), deps)); got == nil {
		t.Fatal("the discovery factory answered nothing")
	}
	if got := must.Do(instances.Build(ctx, w.Registry, discovery.Kind, "fake", jsontext.Value(`{}`), deps)); got == nil {
		t.Fatal("the discovery factory answered nothing on the second call")
	}
	if got := must.Do(instances.Build(ctx, w.Registry, notifier.Kind, "fake", jsontext.Value(`{}`), deps)); got == nil {
		t.Fatal("the notifier factory answered nothing")
	}
	if got := must.Do(instances.Build(ctx, w.Registry, notifier.Kind, "fake", jsontext.Value(`{}`), deps)); got == nil {
		t.Fatal("the notifier factory answered nothing on the second call")
	}
	if got := must.Do(instances.Build(ctx, w.Registry, catalog.Kind, "fake", jsontext.Value(`{}`), instances.Deps{Instance: "catalog"})); got == nil {
		t.Fatal("the catalog factory answered nothing")
	}
	if w.Catalog == nil {
		t.Fatal("the catalog factory did not bind the world's catalog")
	}
	if got := must.Do(storage.Build(ctx, w.Registry, "fake", jsontext.Value(`{}`), storage.BuildInput{CallParallelism: 1}, deps)); got == nil {
		t.Fatal("the storage factory answered nothing")
	}
}

func TestNewWorldReturnsTheDistributionError(t *testing.T) {
	_, err := NewWorld(func(*loader.Registry, *instances.Registry) error {
		return errors.New("distribution refused")
	})
	if err == nil || !strings.Contains(err.Error(), "distribution refused") {
		t.Fatalf("NewWorld = %v, want the distribution error", err)
	}
}

// TestNewWorldAppliesTheDistribution: a non-nil registration runs before the
// kit's own plugins, and a nil one leaves the compiled set to the kit alone.
func TestNewWorldAppliesTheDistribution(t *testing.T) {
	called := false
	w, err := NewWorld(func(lr *loader.Registry, reg *instances.Registry) error {
		called = true
		if lr == nil || reg == nil {
			return errors.New("the kit handed no registries")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("the distribution registration never ran")
	}
	if len(loader.Plugins(w.Loader)) == 0 {
		t.Fatal("the kit compiled no plugins")
	}
}

// TestSyncLoaderDeclaresALateRegistration: a factory registered after NewWorld
// is not compiled yet; SyncLoader declares it and resets the selection.
func TestSyncLoaderDeclaresALateRegistration(t *testing.T) {
	w := newTestWorld(t)
	if err := instances.Register(w.Registry, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
		Type: "late", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (source.Source, error) {
			return &Source{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	w.SyncLoader()
	if !loader.Active(w.Loader, "source/late") {
		t.Fatal("the late factory is not a compiled active plugin")
	}
	// A second sync sees it compiled and changes nothing.
	w.SyncLoader()
}

// TestStorageLookup: the world answers a built storage, and the factory creates
// one on first use; an unknown name is reported.
func TestStorageLookup(t *testing.T) {
	w := newTestWorld(t)
	created := w.StorageFor("made")
	if w.StorageFor("made") != created {
		t.Fatal("StorageFor did not remember the fake it created")
	}
	if got, ok := w.Storage("made"); !ok || got != created {
		t.Fatalf("Storage = %v, %v", got, ok)
	}
	if _, ok := w.Storage("absent"); ok {
		t.Fatal("an unknown storage answered a fake")
	}
	extra := w.StorageFor("extra")
	w.ExtraStorages["extra"] = extra
	if w.StorageFor("extra") != extra {
		t.Fatal("the extra storage did not win")
	}
}

func TestConfigBuilders(t *testing.T) {
	cfg := BaseConfig(t, map[string]any{"alpha": StorageInstance("fake", "keep_last")})
	if cfg["run"] == nil || cfg["layout"] == nil || cfg["sources"] == nil {
		t.Fatalf("the base config is incomplete: %v", cfg)
	}
	if cfg["storages"].(map[string]any)["alpha"] == nil {
		t.Fatal("the base config lost its storage")
	}
	WithRetention(cfg, "alpha", KeepLastCfg(2))
	if cfg["storages"].(map[string]any)["alpha"].(map[string]any)["retention"].(map[string]any)["scheduled"] == nil {
		t.Fatal("WithRetention did not replace the policy")
	}
	WithManualRetention(cfg, "alpha", KeepLastCfg(1))
	if cfg["storages"].(map[string]any)["alpha"].(map[string]any)["retention"].(map[string]any)["manual"] == nil {
		t.Fatal("WithManualRetention did not add the policy")
	}
	if ConfigRun(t.TempDir())["id_layout"] == "" {
		t.Fatal("ConfigRun has no id layout")
	}
	if ConfigLayout()["run_prefix"] == "" {
		t.Fatal("ConfigLayout has no run prefix")
	}
	if Instance("fake")["type"] != "fake" {
		t.Fatal("Instance lost its type")
	}
	if SourceInstance("fake", "g", "n", nil, "required", "plain")["artifact"] == nil {
		t.Fatal("SourceInstance lost its artifact")
	}
}

func TestSourceBranches(t *testing.T) {
	ctx := context.Background()
	if _, err := (&Source{}).Units(canceled()); err == nil {
		t.Fatal("Units ignored the cancelled context")
	}
	if _, err := (&Source{UnitsErr: errors.New("no units")}).Units(ctx); err == nil {
		t.Fatal("Units ignored UnitsErr")
	}
	if _, err := (&Source{}).Units(ctx); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("a single-artifact source = %v, want ErrUnsupported", err)
	}
	units := []source.Unit{{Name: "one"}}
	if got, err := (&Source{UnitList: units}).Units(ctx); err != nil || len(got) != 1 {
		t.Fatalf("Units = %v, %v", got, err)
	}
	if _, err := (&Source{}).DumpStats(canceled()); err == nil {
		t.Fatal("DumpStats ignored the cancelled context")
	}
	if _, err := (&Source{StatsErr: errors.New("no stats")}).DumpStats(ctx); err == nil {
		t.Fatal("DumpStats ignored StatsErr")
	}
	if _, err := (&Source{Stats: model.DumpStats{SkippedEntries: 1}}).DumpStats(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Source{}).Describe(canceled()); err == nil {
		t.Fatal("Describe ignored the cancelled context")
	}
	if _, err := (&Source{DescribeErr: errors.New("no traits")}).Describe(ctx); err == nil {
		t.Fatal("Describe ignored DescribeErr")
	}
	if tr, err := (&Source{}).Describe(ctx); err != nil || tr.Ext != ".dat" || tr.Origin == "" {
		t.Fatalf("default traits = %+v, %v", tr, err)
	}
	if tr, err := (&Source{Traits: source.SourceTraits{Ext: ".bin", Origin: "x", Sealed: true}}).Describe(ctx); err != nil || tr.Ext != ".bin" {
		t.Fatalf("preset traits = %+v, %v", tr, err)
	}
	// Dump: panic, cancellation, a configured error, the first-call error, the
	// delay that the context cuts short, and the successful write.
	mustPanic(t, func() { _ = (&Source{DumpPanic: true}).Dump(ctx, &bytes.Buffer{}) })
	p := &Source{Data: []byte("x")}
	if err := p.Dump(canceled(), &bytes.Buffer{}); err == nil {
		t.Fatal("Dump ignored the cancelled context")
	}
	if err := (&Source{DumpErr: errors.New("dump refused")}).Dump(ctx, &bytes.Buffer{}); err == nil {
		t.Fatal("Dump ignored DumpErr")
	}
	once := &Source{Data: []byte("x"), DumpOnce: errors.New("first refused")}
	if err := once.Dump(ctx, &bytes.Buffer{}); err == nil {
		t.Fatal("Dump ignored DumpOnce")
	}
	if err := once.Dump(ctx, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	delayed := &Source{Data: []byte("x"), DumpDelay: time.Hour}
	dctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	if err := delayed.Dump(dctx, &bytes.Buffer{}); err == nil {
		t.Fatal("a delayed Dump ignored the context")
	}
	var out bytes.Buffer
	if err := (&Source{Data: []byte("payload")}).Dump(ctx, &out); err != nil || out.String() != "payload" {
		t.Fatalf("Dump wrote %q, %v", out.String(), err)
	}
	// Check: cancellation, the first-call error, the permanent error, success.
	if err := (&Source{}).Check(canceled()); err == nil {
		t.Fatal("Check ignored the cancelled context")
	}
	once = &Source{CheckOnce: errors.New("first"), CheckErr: errors.New("always")}
	if err := once.Check(ctx); err == nil {
		t.Fatal("Check ignored CheckOnce")
	}
	if err := once.Check(ctx); err == nil {
		t.Fatal("Check ignored CheckErr")
	}
	good := &Source{}
	if err := good.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err := good.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if good.CheckCount() != 1 || good.DumpCount() != 0 {
		t.Fatalf("counts = %d, %d", good.CheckCount(), good.DumpCount())
	}
}

func TestChainSourceBranches(t *testing.T) {
	ctx := context.Background()
	c := &ChainSource{Data: []byte("staged"), Ext: ".tar", Chain: []model.ProcessorStep{{Instance: "p"}}}
	if _, err := c.Describe(canceled()); err == nil {
		t.Fatal("Describe ignored the cancelled context")
	}
	if tr, err := c.Describe(ctx); err != nil || tr.Ext != ".tar" {
		t.Fatalf("Describe = %+v, %v", tr, err)
	}
	if err := c.Dump(canceled(), &bytes.Buffer{}); err == nil {
		t.Fatal("Dump ignored the cancelled context")
	}
	var out bytes.Buffer
	if err := c.Dump(ctx, &out); err != nil || out.String() != "staged" {
		t.Fatalf("Dump wrote %q, %v", out.String(), err)
	}
	if _, err := c.RecordedChain(canceled()); err == nil {
		t.Fatal("RecordedChain ignored the cancelled context")
	}
	if chain, err := c.RecordedChain(ctx); err != nil || len(chain) != 1 {
		t.Fatalf("RecordedChain = %v, %v", chain, err)
	}
}

func TestProcessorBranches(t *testing.T) {
	ctx := context.Background()
	once := &Processor{CheckOnce: errors.New("first"), CheckErr: errors.New("always")}
	if err := once.Check(ctx); err == nil {
		t.Fatal("Check ignored CheckOnce")
	}
	if err := once.Check(ctx); err == nil {
		t.Fatal("Check ignored CheckErr")
	}
	if err := (&Processor{}).Check(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Processor{}).Describe(canceled()); err == nil {
		t.Fatal("Describe ignored the cancelled context")
	}
	if tr, err := (&Processor{}).Describe(ctx); err != nil || tr.Ext != ".zst" {
		t.Fatalf("default traits = %+v, %v", tr, err)
	}
	if tr, err := (&Processor{Ext: ".age", Seals: true}).Describe(ctx); err != nil || !tr.Seals {
		t.Fatalf("preset traits = %+v, %v", tr, err)
	}
	mustPanic(t, func() { _ = (&Processor{PanicNow: true}).Encode(ctx, &bytes.Buffer{}, strings.NewReader("x")) })
	if err := (&Processor{EncodeErr: errors.New("encode refused")}).Encode(ctx, &bytes.Buffer{}, strings.NewReader("x")); err == nil {
		t.Fatal("Encode ignored EncodeErr")
	}
	var enc bytes.Buffer
	if err := (&Processor{}).Encode(ctx, &enc, strings.NewReader("payload")); err != nil || enc.String() != "payload" {
		t.Fatalf("Encode wrote %q, %v", enc.String(), err)
	}
	hook := false
	dec := &Processor{DecodeHook: func() { hook = true }}
	var out bytes.Buffer
	if err := dec.Decode(ctx, &out, strings.NewReader("payload")); err != nil || out.String() != "payload" || !hook {
		t.Fatalf("Decode wrote %q hook %v, %v", out.String(), hook, err)
	}
	mustPanic(t, func() { _ = (&Processor{PanicNow: true}).Decode(ctx, &bytes.Buffer{}, strings.NewReader("x")) })
}

func TestDiscoveryBranches(t *testing.T) {
	ctx := context.Background()
	if err := (&Discovery{CheckErr: errors.New("check")}).Check(ctx); err == nil {
		t.Fatal("Check ignored CheckErr")
	}
	mustPanic(t, func() { _, _ = (&Discovery{PanicNow: true}).Discover(ctx) })
	if _, err := (&Discovery{}).Discover(canceled()); err == nil {
		t.Fatal("Discover ignored the cancelled context")
	}
	once := &Discovery{ErrOnce: errors.New("once"), Err: errors.New("always")}
	if _, err := once.Discover(ctx); err == nil {
		t.Fatal("Discover ignored ErrOnce")
	}
	if _, err := once.Discover(ctx); err == nil {
		t.Fatal("Discover ignored Err")
	}
	found := []discovery.Found{{Name: "x"}}
	if got, err := (&Discovery{Found: found}).Discover(ctx); err != nil || len(got) != 1 {
		t.Fatalf("Discover = %v, %v", got, err)
	}
}

func TestNotifierBranches(t *testing.T) {
	ctx := context.Background()
	if err := (&Notifier{CheckErr: errors.New("check")}).Check(ctx); err == nil {
		t.Fatal("Check ignored CheckErr")
	}
	if err := (&Notifier{}).Notify(canceled(), model.Event{Name: "x"}); err == nil {
		t.Fatal("Notify ignored the cancelled context")
	}
	if err := (&Notifier{FailOn: "bad"}).Notify(ctx, model.Event{Name: "bad"}); err == nil {
		t.Fatal("Notify ignored FailOn")
	}
	failing := &Notifier{Fails: 1}
	if err := failing.Notify(ctx, model.Event{Name: "x"}); err == nil {
		t.Fatal("Notify ignored Fails")
	}
	if err := failing.Notify(ctx, model.Event{Name: model.EventStarted}); err != nil {
		t.Fatal(err)
	}
	if err := failing.Notify(ctx, model.Event{Name: model.EventFinished}); err != nil {
		t.Fatal(err)
	}
	if names := failing.EventNames(); len(names) != 2 || names[0] != model.EventStarted {
		t.Fatalf("EventNames = %v", names)
	}
	if ev, ok := failing.Last(); !ok || ev.Name != model.EventFinished {
		t.Fatalf("Last = %+v, %v", ev, ok)
	}
	if ev, ok := failing.Finished(); !ok || ev.Name != model.EventFinished {
		t.Fatalf("Finished = %+v, %v", ev, ok)
	}
	empty := &Notifier{}
	if _, ok := empty.Last(); ok {
		t.Fatal("Last answered an empty notifier")
	}
	if _, ok := empty.Finished(); ok {
		t.Fatal("Finished answered an empty notifier")
	}
	if names := empty.EventNames(); len(names) != 0 {
		t.Fatalf("EventNames = %v", names)
	}

	// The optional notifier.Bounded capability and the per-event counter.
	bounded := &Notifier{Bound: 7 * time.Second}
	if got := bounded.AttemptTimeout(); got != 7*time.Second {
		t.Fatalf("AttemptTimeout() = %s, want the configured 7s", got)
	}
	// A hold has no socket behind it: both waits run on virtual time.
	synctest.Test(t, func(t *testing.T) {
		held := &Notifier{Hold: time.Millisecond}
		if err := held.Notify(context.Background(), model.Event{Name: "held"}); err != nil {
			t.Fatalf("a held call = %v", err)
		}
		if got := held.Calls("held"); got != 1 {
			t.Fatalf("Calls(held) = %d, want 1", got)
		}
		if got := held.Calls("never"); got != 0 {
			t.Fatalf("Calls(never) = %d, want 0", got)
		}
		// A call whose context ends while the hold is in flight answers at once.
		waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancelWait()
		if err := (&Notifier{Hold: time.Hour}).Notify(waitCtx, model.Event{Name: "x"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a held call under a deadline = %v, want the deadline", err)
		}
	})
}

// TestAddTestRolesRefusesADuplicate: the compiled set already owns a role or
// the plugin; both registrations fail loudly instead of silently replacing.
func TestAddTestRolesRefusesADuplicate(t *testing.T) {
	lr := loader.New()
	if err := loader.Add(lr, loader.Spec{ID: "other"}, func(lr *loader.Registry) error {
		return loader.Role(lr, loader.RoleSpec{
			Name:     string(configuration.RoleBackup),
			Run:      func(context.Context, loader.Session) int { return 0 },
			Disabled: func(loader.Session) int { return 1 },
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := AddTestRoles(lr); err == nil {
		t.Fatal("AddTestRoles accepted a duplicate role")
	}
	if testRoleRun(context.Background(), loader.Session{}) != 0 {
		t.Fatal("the kit role ran with a non-zero code")
	}
	if testRoleDisabled(loader.Session{}) != 1 {
		t.Fatal("the kit role's disabled outcome is not a refusal")
	}
	lr = loader.New()
	if err := AddTestRoles(lr); err != nil {
		t.Fatal(err)
	}
	if err := AddTestRoles(lr); err == nil {
		t.Fatal("AddTestRoles accepted a duplicate plugin id")
	}
}

func TestCatalogAndHelpers(t *testing.T) {
	if err := (&Catalog{CheckErr: errors.New("check")}).Check(context.Background()); err == nil {
		t.Fatal("Check ignored CheckErr")
	}
	if Logger() == nil {
		t.Fatal("Logger answered nil")
	}
	if v, err := JSONMarshal(map[string]int{"a": 1}); err != nil || len(v) == 0 {
		t.Fatalf("JSONMarshal = %q, %v", v, err)
	}
	var decoded map[string]int
	if err := JSONUnmarshal([]byte(`{"a":1}`), &decoded); err != nil || decoded["a"] != 1 {
		t.Fatalf("JSONUnmarshal = %v, %v", decoded, err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/one", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if names, err := ReadDirNames(dir); err != nil || len(names) != 1 {
		t.Fatalf("ReadDirNames = %v, %v", names, err)
	}
	if _, err := ReadDirNames(dir + "/missing"); err == nil {
		t.Fatal("ReadDirNames accepted a missing directory")
	}
	if body := MustMarshal(map[string]int{"a": 1}); len(body) == 0 {
		t.Fatal("MustMarshal wrote nothing")
	}
}

// TestNewWorldKeepsDistributionRoles: a distribution that already carries the
// CLI host registers the operational roles itself; the kit must not add its
// stand-in roles a second time.
func TestNewWorldKeepsDistributionRoles(t *testing.T) {
	w, err := NewWorld(func(lr *loader.Registry, _ *instances.Registry) error {
		return loader.Add(lr, loader.Spec{ID: "test-host"}, func(lr *loader.Registry) error {
			return loader.Role(lr, loader.RoleSpec{
				Name:     string(configuration.RoleBackup),
				Run:      func(context.Context, loader.Session) int { return 0 },
				Disabled: func(loader.Session) int { return 1 },
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	roles := loader.Roles(w.Loader)["test-host"]
	if len(roles) != 1 || roles[0] != string(configuration.RoleBackup) {
		t.Fatalf("distribution roles = %v", roles)
	}
	if _, stacked := loader.Roles(w.Loader)["test-roles"]; stacked {
		t.Fatal("the kit added stand-in roles beside the distribution's")
	}
}
