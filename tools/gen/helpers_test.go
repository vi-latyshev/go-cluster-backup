package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	fixtureModule = "../internal/pluginmeta/testdata/corpus"

	moduleGoMod = "module example.test/app\n\ngo 1.27.4\n"

	instancesSource = `package instances

// Kind is a typed seam token.
type Kind[T any] struct{ name string }

// NewKind declares a kind.
func NewKind[T any](name string) Kind[T] { return Kind[T]{name: name} }

// ID is the stable identity of one factory contribution.
func ID(kind, typ string) string { return kind + "/" + typ }
`

	validPlugin = `package p

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: "p", Summary: "one plugin"}

func Apply(lr *loader.Registry, factories *instances.Registry) error { return nil }
`
)

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
			t.Fatalf("write %s: %v", target, err)
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
		all["go.mod"] = moduleGoMod
	}
	return writeTree(t, all)
}

// copyTree copies a checked-in fixture into a temp directory.
func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, name)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
	return dst
}

// mustRead fails the test when a generated file cannot be read.
func mustRead(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// wantSubstring fails the test when the error does not name the cause.
func wantSubstring(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("want error containing %q, got %q", want, err)
	}
}
