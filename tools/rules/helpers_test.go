package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

const fixtureModule = "example.test/app"

// healthyTree is one module that satisfies every rule: a kind package, a factory
// backend that imports it, a library plugin, the CLI command layer and the
// generated composition root that ties them together.
func healthyTree() map[string]string {
	return map[string]string{
		"go.mod": "module " + fixtureModule + "\n\ngo 1.27.3\n",
		"internal/loader/loader.go": `package loader

import "errors"

// ErrSomething is a loader-owned error.
var ErrSomething = errors.New("loader: something")

const separator = "/"
`,
		"internal/plugins/thing/thing.go": `package thing

import "example.test/app/internal/plugins/instances"

// Thing is the seam this package owns.
type Thing interface{ Do() }

// Kind is the thing seam.
var Kind = instances.NewKind[Thing]("thing")
`,
		"internal/plugins/thing/plugin.go": `package thing

import "example.test/app/internal/loader"

// Spec is the compiled identity of the thing kinds.
var Spec = loader.Spec{ID: "thing", Summary: "the thing kind"}
`,
		"internal/plugins/thing/backend/plugin.go": `package backend

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/thing"
)

// Spec is the compiled identity of the backend.
var Spec = loader.Spec{ID: instances.ID(thing.Kind.Name(), "backend"), Summary: "a backend"}

// Apply contributes the backend.
func Apply(_ *loader.Registry, _ *instances.Registry) error { return nil }
`,
		"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

// Spec is the compiled identity of the library.
var Spec = loader.Spec{ID: "library", Summary: "a library"}
`,
		"internal/plugins/cli/plugin.go": `package cli

import "example.test/app/internal/loader"

// Spec is the compiled identity of the CLI host.
var Spec = loader.Spec{ID: "cli", Summary: "the command host", Floor: true}
`,
		"internal/plugins/cli/cli.go": `package cli

import "example.test/app/internal/loader"

// Role names are the process entrypoints.
const (
	RoleCLI = "cli"
	RoleJob = "backup-job"
)

// Roles is the compiled role table.
func Roles() []loader.RoleSpec {
	return []loader.RoleSpec{{
		Name:     RoleCLI,
		Default:  true,
		Run:      func(loader.Session) int { return 0 },
		Disabled: func(loader.Session) int { return 1 },
	}}
}

// Main dispatches one command.
func Main(command string) int {
	switch command {
	case "run", "help":
		return 0
	default:
		return 2
	}
}
`,
		"internal/plugins/all/zz_generated.go": `package all

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/all/components"
	"example.test/app/internal/plugins/cli"
	"example.test/app/internal/plugins/instances"
)

// Register installs every compiled plugin over one fresh factory registry.
func Register(r *loader.Registry) error {
	return Compose(r, instances.NewRegistry())
}

// Compose installs the hostless half, then each host.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := components.Compose(r, factories); err != nil {
		return err
	}
	return loader.Add(r, cli.Spec, func(lr *loader.Registry) error { return cli.Apply(lr, factories) })
}
`,
		"internal/plugins/all/components/zz_generated.go": `package components

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/library"
	"example.test/app/internal/plugins/thing"
	"example.test/app/internal/plugins/thing/backend"
)

// Compose installs every hostless plugin.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := loader.SetImports(r, loader.Imports{"thing/backend": {"thing"}}); err != nil {
		return err
	}
	_ = library.Spec
	_ = thing.Spec
	return loader.Add(r, backend.Spec, func(lr *loader.Registry) error { return backend.Apply(lr, factories) })
}
`,
	}
}

// writeTree materialises one fixture module in a temp directory.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("create %s: %v", filepath.Dir(target), err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return root
}

// moduleTree is writeTree with the module file every fixture needs.
func moduleTree(t *testing.T, files map[string]string) string {
	t.Helper()
	all := make(map[string]string, len(files)+1)
	for name, content := range files {
		all[name] = content
	}
	if _, ok := all["go.mod"]; !ok {
		all["go.mod"] = "module " + fixtureModule + "\n\ngo 1.27.3\n"
	}
	return writeTree(t, all)
}

// checkedTree is healthyTree with files replaced, added or removed, so a
// negative case names only what it changes.
func checkedTree(overrides map[string]string) map[string]string {
	files := healthyTree()
	for name, content := range overrides {
		if content == "" {
			delete(files, name)
			continue
		}
		files[name] = content
	}
	return files
}

// checkTree runs every rule over a fixture and returns its findings.
func checkTree(t *testing.T, files map[string]string) []string {
	t.Helper()
	findings, err := check(writeTree(t, files))
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	return findings
}

// wantFinding fails the test when no finding contains the wanted text.
func wantFinding(t *testing.T, findings []string, want string) {
	t.Helper()
	for _, finding := range findings {
		if strings.Contains(finding, want) {
			return
		}
	}
	t.Fatalf("want a finding containing %q, got %v", want, findings)
}

// wantNone fails the test when any finding contains the unwanted text.
func wantNone(t *testing.T, findings []string, unwanted string) {
	t.Helper()
	for _, finding := range findings {
		if strings.Contains(finding, unwanted) {
			t.Fatalf("unexpected finding %q in %v", finding, findings)
		}
	}
}

// wantError fails the test when err is nil or does not name the cause.
func wantError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want an error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("want an error containing %q, got %q", want, err)
	}
}

// parseExpr parses one Go expression for a unit test.
func parseExpr(t *testing.T, source string) ast.Expr {
	t.Helper()
	expr, err := parser.ParseExpr(source)
	if err != nil {
		t.Fatalf("parse %s: %v", source, err)
	}
	return expr
}

// testRepo builds the rules' policy view for a generated-root unit test.
func testRepo(root string) *repo {
	return &repo{Tree: &pluginmeta.Tree{Root: root}}
}

// parseFile parses one Go file for a unit test.
func parseFile(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return file
}
