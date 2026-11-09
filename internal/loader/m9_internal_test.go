package loader

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestM9_HealthSortsThroughTheStandardLibrary: the health report orders kind
// and service names with slices.Sorted(maps.Keys(...)). A package-local
// key-sorting helper is the duplication this finding removed, so the package
// may declare none.
func TestM9_HealthSortsThroughTheStandardLibrary(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no loader source was found")
	}
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == "sortedKeys" {
				t.Fatalf("%s: a local sortedKeys helper duplicates slices.Sorted(maps.Keys(...))",
					fset.Position(fn.Pos()))
			}
		}
	}
}
