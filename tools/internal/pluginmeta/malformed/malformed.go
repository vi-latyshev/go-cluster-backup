// Package malformed is the one malformed-metadata corpus both metadata
// consumers run: tools/gen through its generation error rendering and
// tools/internal/pluginmeta through Package.Problems. A case is data — the
// source that breaks one rule and the diagnostic that must name it — so the
// parser cases are maintained once and every consumer keeps its own policy
// assertions beside it.
package malformed

// Case is one malformed plugin source and the diagnostic it must produce.
type Case struct {
	Name  string            // the rule the case breaks
	Files map[string]string // module-relative source files; the consumer adds go.mod
	Want  string            // the diagnostic substring that must name the rule
}

// imports is the header every single-file case shares.
const imports = `import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

`

// spec is the one-file shape: package p, the protocol imports, then a Spec.
func spec(body string) map[string]string {
	return map[string]string{"internal/plugins/p/plugin.go": "package p\n\n" + imports + body}
}

// Cases returns every malformed-metadata case, in a fixed order.
func Cases() []Case {
	return []Case{
		{Name: "Spec is not a composite literal", Files: spec("var Spec = loader.Spec\n"),
			Want: "Spec must be a loader.Spec composite literal"},
		{Name: "Spec is declared twice", Files: map[string]string{
			"internal/plugins/p/one.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"p\", Summary: \"p\"}\n",
			"internal/plugins/p/two.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"q\", Summary: \"q\"}\n",
		}, Want: "declares Spec twice"},
		{Name: "Spec type is not a selector", Files: spec("var Spec = SpecT{}\n"),
			Want: "Spec must be loader.Spec"},
		{Name: "Spec type is a nested selector", Files: map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport pkg \"example.test/app/internal/loader\"\n\nvar Spec = pkg.Loader.Spec{}\n",
		}, Want: "Spec must be loader.Spec"},
		{Name: "Spec comes from another package", Files: map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport other \"example.test/app/internal/other\"\n\nvar Spec = other.Spec{}\n",
		}, Want: "Spec must be loader.Spec"},
		{Name: "Spec uses positional fields", Files: spec("var Spec = loader.Spec{\"p\"}\n"),
			Want: "Spec must use field names"},
		{Name: "Spec field name is not an identifier", Files: spec("var Spec = loader.Spec{\"ID\": \"p\"}\n"),
			Want: "Spec field name is not an identifier"},
		{Name: "Spec field is set twice", Files: spec("var Spec = loader.Spec{ID: \"p\", ID: \"q\"}\n"),
			Want: "Spec field ID is set twice"},
		{Name: "Spec has an unknown field", Files: spec("var Spec = loader.Spec{Bogus: \"p\"}\n"),
			Want: "unknown Spec field Bogus"},
		{Name: "ID is missing", Files: spec("var Spec = loader.Spec{Summary: \"p\"}\n"),
			Want: "Spec.ID is required"},
		{Name: "ID is empty", Files: spec("var Spec = loader.Spec{ID: \"\", Summary: \"p\"}\n"),
			Want: "Spec.ID is required"},
		{Name: "Summary is not a string", Files: spec("var Spec = loader.Spec{ID: \"p\", Summary: 42}\n"),
			Want: "Spec.Summary: must be a string literal"},
		{Name: "Floor is not a bool", Files: spec("var Spec = loader.Spec{ID: \"p\", Summary: \"p\", Floor: \"yes\"}\n"),
			Want: "Spec.Floor: must be true or false"},
		{Name: "Requires is not a list", Files: spec("var Spec = loader.Spec{ID: \"p\", Summary: \"p\", Requires: \"a\"}\n"),
			Want: "Spec.Requires: must be a []string literal"},
		{Name: "Requires element is not a string", Files: spec("var Spec = loader.Spec{ID: \"p\", Summary: \"p\", Requires: []string{1}}\n"),
			Want: "Spec.Requires: must be a string literal"},
		{Name: "After is not a list", Files: spec("var Spec = loader.Spec{ID: \"p\", Summary: \"p\", After: \"a\"}\n"),
			Want: "Spec.After: must be a []string literal"},
		{Name: "ID is not a literal or a derivation", Files: spec("var Spec = loader.Spec{ID: 42, Summary: \"p\"}\n"),
			Want: "Spec.ID: must be a string literal or instances.ID(...)"},
		{Name: "ID call is not a selector", Files: spec("var Spec = loader.Spec{ID: makeID(), Summary: \"p\"}\n"),
			Want: "Spec.ID: must be a string literal or instances.ID(...)"},
		{Name: "ID selector is not instances.ID", Files: spec("var Spec = loader.Spec{ID: instances.Ident(\"a\", \"b\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: must be a string literal or instances.ID(...)"},
		{Name: "ID comes from another package", Files: map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport (\n\t\"fmt\"\n\n\t\"example.test/app/internal/loader\"\n)\n\nvar Spec = loader.Spec{ID: fmt.ID(\"a\", \"b\"), Summary: \"p\"}\n",
		}, Want: "Spec.ID: ID must come from the instances package"},
		{Name: "ID has one argument", Files: spec("var Spec = loader.Spec{ID: instances.ID(\"a\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: instances.ID takes a kind and a type"},
		{Name: "ID type is not a string", Files: spec("var Spec = loader.Spec{ID: instances.ID(\"a\", typ), Summary: \"p\"}\n"),
			Want: "Spec.ID: type: must be a string literal"},
		{Name: "ID kind is not a literal or a derivation", Files: spec("var Spec = loader.Spec{ID: instances.ID(42, \"t\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: kind: must be a string literal or <pkg>.Kind.Name()"},
		{Name: "Kind.Name is called with an argument", Files: spec("var Spec = loader.Spec{ID: instances.ID(instances.Kind.Name(\"x\"), \"t\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: kind: must be a string literal or <pkg>.Kind.Name()"},
		{Name: "ID kind selector is not Name", Files: spec("var Spec = loader.Spec{ID: instances.ID(instances.Kind.Other(), \"t\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: kind: must be a string literal or <pkg>.Kind.Name()"},
		{Name: "ID kind selector is not Kind", Files: spec("var Spec = loader.Spec{ID: instances.ID(instances.Other.Name(), \"t\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: kind: must be a string literal or <pkg>.Kind.Name()"},
		{Name: "ID kind is a nested call", Files: spec("var Spec = loader.Spec{ID: instances.ID(get().Kind.Name(), \"t\"), Summary: \"p\"}\n"),
			Want: "Spec.ID: kind: must be a string literal or <pkg>.Kind.Name()"},
		{Name: "ID kind names a package outside the plugin tree", Files: map[string]string{
			"internal/plugins/p/plugin.go": `package p

import (
	"fmt"

	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: instances.ID(fmt.Kind.Name(), "t"), Summary: "p"}
`,
		}, Want: "does not name a package under internal/plugins"},
		{Name: "ID kind package declares no Kind", Files: map[string]string{
			"internal/plugins/p/plugin.go": `package p

import (
	plain "example.test/app/internal/plugins/plain"

	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: instances.ID(plain.Kind.Name(), "t"), Summary: "p"}
`,
			"internal/plugins/plain/plain.go": "package plain\n",
		}, Want: "declares no Kind"},
	}
}
