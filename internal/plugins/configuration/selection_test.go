package configuration

import (
	"context"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// c6Loader is the compiled graph the resolver tests run against: a floor
// workflow, the two commands' roles, a library one plugin depends on, and the
// three storages the local-only bundle names.
func c6Loader(t *testing.T) *loader.Registry {
	t.Helper()
	lr := loader.New()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	role := func(name string, def bool) func(*loader.Registry) error {
		return func(r *loader.Registry) error {
			return loader.Role(r, loader.RoleSpec{
				Name:     name,
				Default:  def,
				Run:      func(context.Context, loader.Session) int { return 0 },
				Disabled: func(loader.Session) int { return 1 },
			})
		}
	}
	// A contributing plugin owns one entry of a seam kind; a library plugin
	// contributes nothing and the loader keeps it active whatever the deployment
	// lists.
	seam := loader.NewKind[string]("test.seam", loader.KindOptions[string]{})
	contributing := func(id string, requires ...string) func(*loader.Registry) error {
		return func(r *loader.Registry) error {
			return loader.Register(r, seam, id)
		}
	}
	must(loader.Add(lr, loader.Spec{ID: "workflow-backup", Floor: true}, role("backup-job", true)))
	must(loader.Add(lr, loader.Spec{ID: "workflow-restore", Floor: true}, role("restore-job", false)))
	must(loader.Add(lr, loader.Spec{ID: "model"}, nil))
	must(loader.Add(lr, loader.Spec{ID: "source/postgres", Requires: []string{"model"}}, contributing("source/postgres")))
	must(loader.Add(lr, loader.Spec{ID: "storage/local"}, contributing("storage/local")))
	must(loader.Add(lr, loader.Spec{ID: "storage/s3"}, contributing("storage/s3")))
	must(loader.Add(lr, loader.Spec{ID: "storage/yandex_disk"}, contributing("storage/yandex_disk")))
	must(loader.Add(lr, loader.Spec{ID: "configuration"}, Apply))
	return lr
}

func pluginsWith(bundles []string, disabled []string) *Plugins {
	return &Plugins{Bundles: bundles, Disabled: disabled}
}

// TestC6_ResolveAppliesTheLocalOnlyBundle: the one resolver turns the shipped
// bundle into the effective set, records it on the loader and reports nothing.
func TestC6_ResolveAppliesTheLocalOnlyBundle(t *testing.T) {
	lr := c6Loader(t)
	active, errs := Resolve(&Config{Plugins: pluginsWith([]string{"local-only"}, nil)}, lr)
	if len(errs) != 0 {
		t.Fatalf("Resolve = %v, want no problems", errs)
	}
	for id, want := range map[string]bool{
		"storage/local":       true,
		"storage/s3":          false,
		"storage/yandex_disk": false,
		"source/postgres":     true,
		"model":               true,
		"workflow-backup":     true,
	} {
		if got := active.Enabled(id); got != want {
			t.Errorf("Resolve left %s active=%v, want %v", id, got, want)
		}
	}
	// The effective set is the loader's own: a second reader agrees.
	for _, id := range loader.ActiveIDs(lr) {
		if !active.Enabled(id) {
			t.Errorf("the resolved Active lacks the loader's active plugin %s", id)
		}
	}
}

// TestC6_ResolveRefusesUnknownSelections: a bundle or an ID the build does not
// compile is an error, and the refused entry never narrows the set.
func TestC6_ResolveRefusesUnknownSelections(t *testing.T) {
	lr := c6Loader(t)
	active, errs := Resolve(&Config{Plugins: pluginsWith([]string{"nope"}, []string{"storage/ghost"})}, lr)
	for _, want := range []string{"unknown bundle \"nope\"", "unknown plugin \"storage/ghost\""} {
		if !anyErrHas(errs, want) {
			t.Errorf("Resolve does not report %q: %v", want, errs)
		}
	}
	if !active.Enabled("storage/s3") {
		t.Error("a refused selection narrowed the active set")
	}
}

// TestC6_ResolveRefusesFloorAndLibraryDisables: Floor holds authority and the
// generated import graph makes a library a dependency of its consumer, so the
// loader refuses both before any command runs.
func TestC6_ResolveRefusesFloorAndLibraryDisables(t *testing.T) {
	lr := c6Loader(t)
	active, errs := Resolve(&Config{Plugins: pluginsWith(nil, []string{"workflow-backup", "model"})}, lr)
	if !anyErrHas(errs, "floor plugin \"workflow-backup\" cannot be disabled") {
		t.Errorf("Resolve does not refuse the floor disable: %v", errs)
	}
	if !anyErrHas(errs, "library plugin \"model\" is always active and cannot be disabled") {
		t.Errorf("Resolve does not refuse the library disable: %v", errs)
	}
	for _, id := range []string{"workflow-backup", "model"} {
		if !active.Enabled(id) {
			t.Errorf("the refused disable left %s off", id)
		}
	}
}

// TestC6_ResolveWithoutALoader: no compiled graph means no selection, never an
// implied "everything active".
func TestC6_ResolveWithoutALoader(t *testing.T) {
	if _, errs := Resolve(&Config{}, nil); !anyErrHas(errs, "loader registry is required") {
		t.Fatalf("Resolve(nil) = %v, want the missing registry refused", errs)
	}
	// A configuration without a plugins root enables everything the build
	// compiled, once a registry is present.
	active, errs := Resolve(&Config{}, c6Loader(t))
	if len(errs) != 0 || !active.Enabled("storage/s3") {
		t.Fatalf("Resolve of a plugins-less config = (%v, %v)", active, errs)
	}
}

// TestC6_RolesFromAndCheckRoles: the loader's owner→roles table is inverted into
// the role→providers table, and a present section whose role has no active
// provider refuses.
func TestC6_RolesFromAndCheckRoles(t *testing.T) {
	lr := c6Loader(t)
	roles := RolesFrom(lr)
	if got := strings.Join(roles[RoleBackup], ","); got != "workflow-backup" {
		t.Fatalf("RolesFrom(backup-job) = %q", got)
	}
	if got := strings.Join(roles[RoleRestore], ","); got != "workflow-restore" {
		t.Fatalf("RolesFrom(restore-job) = %q", got)
	}
	if RolesFrom(nil) != nil {
		t.Error("RolesFrom(nil) is not nil")
	}
	if errs := (&Config{Run: &RunConfig{}}).CheckRoles(ActiveFrom(nil), roles); !anyErrHas(errs, `the "backup-job" role`) {
		t.Fatalf("CheckRoles with no active provider = %v, want the role refused", errs)
	}
	if errs := (&Config{Run: &RunConfig{}}).CheckRoles(ActiveFrom(loader.ActiveIDs(lr)), roles); len(errs) != 0 {
		t.Fatalf("CheckRoles with the compiled set active = %v", errs)
	}
}

// TestC6_AddBundleRegistersTheCompiledTable: AddBundle records one bundle, the
// table refuses a second bundle of the same name through the loader's health,
// and the shipped bundle disables exactly the two remote storages.
func TestC6_AddBundleRegistersTheCompiledTable(t *testing.T) {
	lr := c6Loader(t)
	bundles := loader.All(lr, Bundles)
	if len(bundles) != 1 || bundles[0].Name != "local-only" {
		t.Fatalf("the compiled bundle table = %+v", bundles)
	}
	if got := strings.Join(LocalOnly().Disables, ","); got != "storage/s3,storage/yandex_disk" {
		t.Fatalf("LocalOnly disables %q", got)
	}
	// A second registration of the same name is a health problem, not a silent
	// second table entry the deployment might pick between.
	if err := loader.Add(lr, loader.Spec{ID: "second-distribution"}, func(r *loader.Registry) error {
		return AddBundle(r, LocalOnly())
	}); err != nil {
		t.Fatalf("AddBundle: %v", err)
	}
	if problems := loader.Health(lr); !containsProblem(problems, "declared twice") {
		t.Fatalf("loader.Health = %v, want the duplicate bundle named", problems)
	}
}

func containsProblem(problems []string, sub string) bool {
	for _, p := range problems {
		if strings.Contains(p, sub) {
			return true
		}
	}
	return false
}

// TestC6_ValidateBundles: an empty name, a repeated name and an empty or
// repeated disable are all refused.
func TestC6_ValidateBundles(t *testing.T) {
	cases := []struct {
		name    string
		bundles []Bundle
		want    string
	}{
		{"a nameless bundle", []Bundle{{Disables: []string{"a"}}}, "has no name"},
		{"a repeated name", []Bundle{{Name: "x"}, {Name: "x"}}, "declared twice"},
		{"an empty disable", []Bundle{{Name: "x", Disables: []string{""}}}, "disables an empty plugin ID"},
		{"a repeated disable", []Bundle{{Name: "x", Disables: []string{"a", "a"}}}, "disables \"a\" twice"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := validateBundles(c.bundles); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("validateBundles = %v, want %q", err, c.want)
			}
		})
	}
	if err := validateBundles([]Bundle{{Name: "x", Disables: []string{"a", "b"}}}); err != nil {
		t.Fatalf("validateBundles refused a valid table: %v", err)
	}
}
