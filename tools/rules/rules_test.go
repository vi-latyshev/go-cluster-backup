package main

import (
	"strings"
	"testing"
)

// TestImportCoverageRule proves the generated graph is judged against the
// production imports rather than trusted as an opaque file.
func TestImportCoverageRule(t *testing.T) {
	t.Run("missing composition root", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": "",
		}))
		wantFinding(t, findings, generatedFile+" is missing")
	})
	t.Run("stale graph misses a production import", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": generatedRoot(`loader.Imports{}`),
		}))
		wantFinding(t, findings, "production import of internal/plugins/thing is in neither")
	})
	t.Run("runtime Requires covers the import", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": generatedRoot(`loader.Imports{}`),
			"internal/plugins/thing/backend/plugin.go": `package backend

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/thing"
)

var Spec = loader.Spec{
	ID:       instances.ID(thing.Kind.Name(), "backend"),
	Summary:  "a backend",
	Requires: []string{"thing"},
}

func Apply(_ *loader.Registry, _ *instances.Registry) error { return nil }
`,
		}))
		wantNone(t, findings, "production import")
	})
	t.Run("a compiled plugin must be imported", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": strings.Replace(generatedRoot(`loader.Imports{"thing/backend": {"thing"}}`),
				"\t\"example.test/app/internal/plugins/library\"\n", "", 1),
		}))
		wantFinding(t, findings, "internal/plugins/library is a compiled plugin but the generated composition root does not import it")
	})
	t.Run("an unknown generated plugin", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": generatedRoot(`loader.Imports{"ghost": {"thing"}, "thing/backend": {"thing"}}`),
		}))
		wantFinding(t, findings, `names plugin "ghost"`)
	})
	t.Run("an unknown generated dependency", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/all/components/zz_generated.go": generatedRoot(`loader.Imports{"thing/backend": {"ghost"}}`),
		}))
		wantFinding(t, findings, `depend on "ghost"`)
	})
	t.Run("a plugin inside a Go internal boundary", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/thing/internal/plugin.go": `package internal

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "hidden", Summary: "unreachable"}
`,
		}))
		wantFinding(t, findings, "Go internal boundary the composition root cannot import")
	})
}

// TestRequiresRule proves the runtime-only edge list: ids exist once, no edge
// repeats an import, and the effective graph is acyclic.
func TestRequiresRule(t *testing.T) {
	cases := []struct {
		name  string
		spec  string
		want  string
		extra map[string]string
	}{
		{
			name: "empty id",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{""}}`,
			want: "Spec.Requires has an empty plugin id",
		},
		{
			name: "self dependency",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"library"}}`,
			want: "Spec.Requires names the plugin itself",
		},
		{
			name: "unknown plugin",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"ghost"}}`,
			want: `Spec.Requires names "ghost", which is not a compiled plugin`,
		},
		{
			name: "duplicate id",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing", "thing"}}`,
			want: `Spec.Requires names "thing" twice`,
		},
		{
			name: "repeated import edge",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing"}}`,
			want: `Spec.Requires names "thing", already a generated import edge`,
			extra: map[string]string{
				"internal/plugins/library/plugin.go": libraryWithThingImport,
				"internal/plugins/all/components/zz_generated.go": generatedRoot(
					`loader.Imports{"thing/backend": {"thing"}, "library": {"thing"}}`),
			},
		},
		{
			name: "cycle",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing"}}`,
			want: "dependency cycle:",
			extra: map[string]string{
				"internal/plugins/thing/plugin.go": `package thing

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "thing", Summary: "the thing kind", Requires: []string{"library"}}
`,
			},
		},
		{
			name: "valid runtime-only edge",
			spec: `var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing"}}`,
			want: "Spec.Requires",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			overrides := map[string]string{"internal/plugins/library/plugin.go": "package library\n\nimport \"example.test/app/internal/loader\"\n\n" + test.spec + "\n"}
			for name, content := range test.extra {
				overrides[name] = content
			}
			findings := checkTree(t, checkedTree(overrides))
			if test.name == "valid runtime-only edge" {
				wantNone(t, findings, test.want)
				return
			}
			wantFinding(t, findings, test.want)
		})
	}
	t.Run("duplicate ids", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "thing/backend", Summary: "a library"}
`,
		}))
		wantFinding(t, findings, `both declare plugin id "thing/backend"`)
	})
}

// TestFactoryIDRule proves a factory id has one spelling: the derivation helper
// or nothing.
func TestFactoryIDRule(t *testing.T) {
	t.Run("a literal factory id", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/thing/thing.go": `package thing

import "example.test/app/internal/plugins/instances"

type Thing interface{ Do() }

var Kind = instances.NewKind[Thing]("thing")
`,
			"internal/plugins/library/stray.go": `package library

const stray = "thing/backend"
`,
		})
		findings := ruleFactoryIDs(scanned, nil)
		if len(findings) != 1 || !strings.Contains(findings[0], `literal factory id "thing/backend"`) {
			t.Fatalf("findings = %v", findings)
		}
		if !strings.Contains(findings[0], "stray.go:3") {
			t.Fatalf("the finding must carry the position: %q", findings[0])
		}
	})
	t.Run("derived ids and lookalikes", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/library/stray.go": `package library

import "example.test/app/internal/plugins/instances"

var derived = instances.ID("thing", "backend")
var otherKind = "other/type"
var deep = "thing/backend/extra"
var empty = "thing/"
var bare = "thing"
`,
			"internal/plugins/library/librarytest/kit.go": `package librarytest

const stray = "thing/backend"
`,
			"internal/plugins/all/zz_generated.go": `package all

const stray = "thing/backend"
`,
		})
		if findings := ruleFactoryIDs(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
}

// TestLoaderPurityRule proves the kernel names no feature: ids, kinds, roles and
// commands stay in their owning plugins, and an import path is not a name.
func TestLoaderPurityRule(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/loader/loader.go": `package loader

import _ "example.test/app/internal/plugins/thing"

const (
	id      = "thing/backend"
	kind    = "thing"
	role    = "cli"
	command = "run"
	own     = "loader.selection"
)
`,
		"internal/plugins/thing/thing.go": `package thing

import "example.test/app/internal/plugins/instances"

type Thing interface{ Do() }

var Kind = instances.NewKind[Thing]("thing")
`,
		"internal/plugins/thing/plugin.go": `package thing

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "thing", Summary: "the thing kind"}
`,
		"internal/plugins/thing/backend/plugin.go": `package backend

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/thing"
)

var Spec = loader.Spec{ID: instances.ID(thing.Kind.Name(), "backend"), Summary: "a backend"}
`,
		"internal/plugins/library/plugin.go": librarySpec("library"),
		"internal/plugins/cli/cli.go": `package cli

import "example.test/app/internal/loader"

const RoleCLI = "cli"

func Roles() []loader.RoleSpec { return []loader.RoleSpec{{Name: RoleCLI}} }

func Main(command string) int {
	switch command {
	case "run":
		return 0
	}
	return 2
}
`,
	})
	findings := ruleLoaderPurity(scanned, nil)
	wants := []string{
		`the loader names plugin id "thing/backend"`,
		`the loader names plugin id "thing"`,
		`the loader names role name "cli"`,
		`the loader names command name "run"`,
	}
	for _, want := range wants {
		wantFinding(t, findings, want)
	}
	for _, finding := range findings {
		if strings.Contains(finding, "example.test/app/internal/plugins/thing") {
			t.Fatalf("an import path is not a feature name: %q", finding)
		}
	}
}

// TestRoleCommandRule proves the role token namespace stays disjoint from the
// command namespace.
func TestRoleCommandRule(t *testing.T) {
	t.Run("no command layer", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/library/plugin.go": librarySpec("library"),
		})
		findings := ruleRoleCommands(scanned, nil)
		wantFinding(t, findings, "the role/command conflict rule cannot run")
	})
	t.Run("conflict", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/library/plugin.go": librarySpec("library"),
			"internal/plugins/cli/cli.go": `package cli

import "example.test/app/internal/loader"

const (
	RoleCLI = "cli"
	RoleRun = "run"
)

func Roles() []loader.RoleSpec {
	_ = loader.RoleSpec{"positional"}
	return []loader.RoleSpec{
		{Name: RoleCLI},
		{Name: RoleRun},
		{Name: unknownName},
		{Name: library.RoleExtra},
		{Name: other.Pkg.Name},
		{Name: 1},
		{Default: true},
	}
}

func Main(cmd string) int {
	switch cmd {
	case "run":
		return 0
	}
	return 2
}
`,
		})
		findings := ruleRoleCommands(scanned, nil)
		wantFinding(t, findings, `role "run" has the same name as a CLI command`)
	})
}

// TestRoleNamesResolution proves the static forms the rules can read: a Role*
// constant in any package and a RoleSpec.Name that resolves to one.
func TestRoleNamesResolution(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

const RoleExtra = "extra"

var Spec = loader.Spec{ID: "library", Summary: "a library"}
`,
		"internal/plugins/cli/cli.go": `package cli

import "example.test/app/internal/loader"

const (
	RoleCLI = "cli"
	Alias   = RoleCLI
)

var HostSpec = loader.Spec{ID: "cli", Summary: "host"}

func Roles() []loader.RoleSpec {
	return []loader.RoleSpec{
		{Name: RoleCLI},
		{Name: library.RoleExtra},
	}
}
`,
	})
	roles := scanned.roleNames()
	for _, want := range []string{"cli", "extra"} {
		if _, ok := roles[want]; !ok {
			t.Fatalf("roles = %v; want %q", roles, want)
		}
	}
	if _, ok := roles[""]; ok {
		t.Fatal("a constant without a literal value must not resolve")
	}
}

// TestCommandNamesTags proves the dispatch discovery and its tag filter.
func TestCommandNamesTags(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/cli/cli.go": `package cli

func Main(command string) int { return 0 }

func dispatch(command string) int {
	switch command {
	case "plugin-setup", "inner":
		return 0
	}
	return 2
}

func ignore(mode string) int {
	switch mode {
	case "not-a-command":
		return 0
	}
	return 2
}

func legacy(cmd string) int {
	switch cmd {
	case "legacy":
		return 0
	}
	return 2
}
`,
		"internal/plugins/thing/thing.go": `package thing

func ignored(command string) int {
	switch command {
	case "not-a-command":
		return 0
	}
	return 1
}
`,
	})
	commands := scanned.commandNames()
	for _, want := range []string{"plugin-setup", "inner", "legacy"} {
		if _, ok := commands[want]; !ok {
			t.Fatalf("commands = %v; want %q", commands, want)
		}
	}
	if _, ok := commands["not-a-command"]; ok {
		t.Fatalf("commands = %v; the tag filter must refuse it", commands)
	}
}

// TestRawStorageRule proves the raw boundary is confined to its owner and guard.
func TestRawStorageRule(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/storage/internal/raw.go": "package raw\n",
		"internal/plugins/storage/storage.go":      "package storage\n\nimport _ \"example.test/app/internal/plugins/storage/internal\"\n",
		"internal/plugins/storage/guard/guard.go":  "package guard\n\nimport _ \"example.test/app/internal/plugins/storage/internal\"\n",
		"internal/plugins/library/plugin.go":       "package library\n\nimport _ \"example.test/app/internal/plugins/storage/internal\"\n",
	})
	findings := ruleRawStorage(scanned, nil)
	if len(findings) != 1 || !strings.Contains(findings[0], "only the storage owner and its guard may import the raw boundary") {
		t.Fatalf("findings = %v", findings)
	}
	if !strings.Contains(findings[0], "internal/plugins/library") {
		t.Fatalf("the finding must name the importer: %q", findings[0])
	}
}

// TestEagerInitRule proves a package initializer does no work, while literals,
// function values, interface assertions, conversions and test seams stay.
func TestEagerInitRule(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/thing/thing.go": `package thing

type Thing interface{ Do() }

var Kind = "thing"
`,
		"internal/plugins/library/extra.go": `package library

import (
	"errors"
	"reflect"
	"regexp"
	"sync"
	"time"

	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/thing"
)

type local int

func helper() int { return 1 }

var (
	okError       = errors.New("x")
	okKind        = instances.NewKind[thing.Thing]("local")
	okOnce        = sync.OnceValue(func() int { return 1 })
	okOnceValues  = sync.OnceValues(func() (int, error) { return 1, nil })
	okTypeFor     = reflect.TypeFor[int]()
	okPredeclared = int64(0)
	okLocal       = local(0)
	okRepoType    = thing.Thing(nil)
	okPointer     = (*local)(nil)
	okSlice       = []byte("x")
	okMap         = map[string]int(nil)
	okChan        = chan int(nil)
	okFunc        = func(int) int(nil)
	okInterface   = interface{}(nil)
	okStruct      = struct{}(nil)
	okMake        = make(map[string]int)
	okNew         = new(int)
	okFuncLiteral = func() int { return 1 }

	badRegexp   = regexp.MustCompile("x")
	badCall     = helper()
	badMethod   = thing.Kind.Name()
	badIIFE     = func() int { return 1 }()
	badStdlib   = time.Duration(0)
	badDeep     = helper().Method()
	badParen    = (helper)()
)
`,
		"internal/plugins/library/plugin.go":  librarySpec("library"),
		"internal/plugins/library/plugin2.go": "package library\n\nvar seam = helper\n",
	})
	findings := ruleEagerInit(scanned, nil)
	wants := []string{"regexp.MustCompile", "calls helper", "calls Name", "a function literal", "time.Duration", "calls Method", "an expression"}
	for _, want := range wants {
		wantFinding(t, findings, want)
	}
	if len(findings) != 8 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestEagerInitSkipsKitsAndGenerated proves the rule describes shipped code.
func TestEagerInitSkipsKitsAndGenerated(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/librarytest/kit.go": "package librarytest\n\nimport \"regexp\"\n\nvar re = regexp.MustCompile(\"x\")\n",
		"internal/plugins/all/zz_generated.go":        "package all\n\nimport \"regexp\"\n\nvar re = regexp.MustCompile(\"x\")\n",
	})
	if findings := ruleEagerInit(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestPluginFilesRule proves every plugin package declares its identity in
// plugin.go, while the generated root, kits, a Go internal boundary and the
// data-only defaults package are not plugins.
func TestPluginFilesRule(t *testing.T) {
	t.Run("shipped plugins", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/noidentity/other.go": "package noidentity\n",
			"internal/plugins/misplaced/spec.go": `package misplaced

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "misplaced", Summary: "here"}
`,
			"internal/plugins/library/plugin.go": librarySpec("library"),
		})
		findings := rulePluginFiles(scanned, nil)
		wantFinding(t, findings, "internal/plugins/noidentity: plugin package has no plugin.go with a literal loader.Spec")
		wantFinding(t, findings, "the loader.Spec literal must be declared in plugin.go, not internal/plugins/misplaced/spec.go")
		wantNone(t, findings, "internal/plugins/library")
	})
	t.Run("non-plugin directories", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/all/zz_generated.go":                "package all\n",
			"internal/plugins/library/librarytest/kit.go":         "package librarytest\n",
			"internal/plugins/storage/internal/raw.go":            "package raw\n",
			"internal/plugins/configuration/defaults/defaults.go": "package defaults\n",
			"internal/loader/loader.go":                           "package loader\n",
		})
		if findings := rulePluginFiles(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
}

// generatedRoot renders a composition root whose graph literal is the argument.
// compositionRoot renders the root half: the hostless components half plus the
// host registrations.
func compositionRoot(hosts string) string {
	return `package all

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/all/components"
	"example.test/app/internal/plugins/cli"
	"example.test/app/internal/plugins/instances"
)

// Compose installs the hostless half, then each host.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := components.Compose(r, factories); err != nil {
		return err
	}
` + hosts + `
}
`
}

// generatedRoot renders the hostless components half whose graph literal is the
// argument.
func generatedRoot(graph string) string {
	return `package components

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/library"
	"example.test/app/internal/plugins/thing"
	"example.test/app/internal/plugins/thing/backend"
)

// Compose installs every hostless plugin.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := loader.SetImports(r, ` + graph + `); err != nil {
		return err
	}
	_ = library.Spec
	_ = thing.Spec
	return loader.Add(r, backend.Spec, func(lr *loader.Registry) error { return backend.Apply(lr, factories) })
}
`
}

const hostRegistration = `	return loader.Add(r, cli.Spec, func(lr *loader.Registry) error { return cli.Apply(lr, factories) })`

const libraryWithThingImport = `package library

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/thing"
)

var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing"}}

var _ = thing.Kind
`

// librarySpec is a plain library plugin declaration.
func librarySpec(id string) string {
	return `package library

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "` + id + `", Summary: "a library"}
`
}

// mustScan scans a fixture or fails the test.
func mustScan(t *testing.T, files map[string]string) *repo {
	t.Helper()
	scanned, err := scan(moduleTree(t, files))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return scanned
}
