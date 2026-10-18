package cli

import (
	"reflect"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// enumMarker stands for "the schema carries an enum": a type switch cannot take
// a string literal as a case.
type enumMarker struct{}

// TestSchemaForTypeName: every Go type a descriptor names maps to the closest
// JSON kind, a named type with no case accepts any value, and a nested struct
// recurses.
func TestSchemaForTypeName(t *testing.T) {
	cases := []struct {
		name string
		want any
	}{
		{"string", "string"},
		{"bool", "boolean"},
		{"int64", "integer"},
		{"uint32", "integer"},
		{"float64", "number"},
		{"model.Duration", "string"},
		{"plugin.Template", "string"},
		{"plugin.Secret", map[string]any{"file": "string"}},
		{"plugin.Class", enumMarker{}},
		{"model.Importance", enumMarker{}},
		{"model.Sensitivity", enumMarker{}},
		{"unknown.Named", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := schemaForTypeName(c.name, nil, nil)
			switch want := c.want.(type) {
			case nil:
				if len(got) != 0 {
					t.Errorf("schemaForTypeName(%s) = %v, want an empty schema", c.name, got)
				}
			case map[string]any:
				if got["x-secret"] != true {
					t.Errorf("schemaForTypeName(%s) = %v, want a secret object", c.name, got)
				}
			case enumMarker:
				if _, ok := got["enum"]; !ok {
					t.Errorf("schemaForTypeName(%s) = %v, want an enum", c.name, got)
				}
			default:
				if got["type"] != want {
					t.Errorf("schemaForTypeName(%s) = %v, want type %v", c.name, got, want)
				}
			}
		})
	}
	for _, name := range []string{"[]string", "map[string]int"} {
		if _, ok := schemaForTypeName(name, nil, nil)["type"]; !ok {
			t.Errorf("schemaForTypeName(%s) = %v, want a collection", name, schemaForTypeName(name, nil, nil))
		}
	}
	nested := schemaForTypeName("probeNested", []plugin.Field{{Name: "inner", Type: "string", Required: true}}, nil)
	if _, ok := nested["properties"]; !ok {
		t.Errorf("a nested struct did not recurse: %v", nested)
	}
}

// schemaProbe is the struct typeObject is exercised against: a member JSON never
// names, an untagged member, a pointer, an opaque value and a kind no JSON
// schema has.
type schemaProbe struct {
	Hidden string           `json:"-"`
	NoName string           // no json name: not a member
	Config jsontext.Value   `json:"config"`
	Ptr    *schemaProbeLeaf `json:"ptr"`
	Secret plugin.Secret    `json:"secret"`
	Odd    chan int         `json:"odd"`
}

type schemaProbeLeaf struct {
	Leaf string `json:"leaf" required:"true"`
}

// TestTypeObjectSkipsWhatIsNotAMember: only an exported member with a JSON name
// is a member, a pointer type is the type it points at, and a member the document
// fills is marked as defaulted unless another layer decodes it.
func TestTypeObjectSkipsWhatIsNotAMember(t *testing.T) {
	block := jsontext.Value(`{"config":{"any":true},"ptr":{"leaf":"v"},"secret":{"file":"/k"},"odd":{}}`)
	got := typeObject(reflect.TypeFor[schemaProbe](), block)
	props, _ := got["properties"].(map[string]any)
	for _, absent := range []string{"Hidden", "NoName"} {
		if _, ok := props[absent]; ok {
			t.Errorf("typeObject kept the member %q: %v", absent, props)
		}
	}
	for _, present := range []string{"config", "ptr", "secret", "odd"} {
		if _, ok := props[present]; !ok {
			t.Errorf("typeObject dropped the member %q: %v", present, props)
		}
	}
	ptr, _ := props["ptr"].(map[string]any)
	if ptr["type"] != "object" {
		t.Errorf("a pointer member is not the object it points at: %v", ptr)
	}
	raw, _ := props["config"].(map[string]any)
	if _, described := raw["type"]; described {
		t.Errorf("an opaque value was described as a JSON type: %v", raw)
	}
	secret, _ := props["secret"].(map[string]any)
	if secret["x-secret"] != true {
		t.Errorf("a secret member is not described as one: %v", secret)
	}
	odd, _ := props["odd"].(map[string]any)
	if _, described := odd["type"]; described {
		t.Errorf("a kind no schema has was described as a JSON type: %v", odd)
	}
}

// TestSchemaForKindOfAnUndeclaredType: a named type the descriptor does not
// describe accepts any JSON value, and a pointer is the type it points at.
func TestSchemaForKindOfAnUndeclaredType(t *testing.T) {
	if got := schemaForKind(reflect.TypeFor[chan int](), nil); len(got) != 0 {
		t.Errorf("schemaForKind(chan int) = %v, want an empty schema", got)
	}
	if got := schemaForKind(reflect.TypeFor[*schemaProbeLeaf](), nil); got["type"] != "object" {
		t.Errorf("schemaForKind(*leaf) = %v, want an object", got)
	}
	if got := schemaForKind(reflect.TypeFor[plugin.Secret](), nil); got["x-secret"] != true {
		t.Errorf("schemaForKind(plugin.Secret) = %v, want a secret object", got)
	}
	if got := typeObject(reflect.TypeFor[*schemaProbeLeaf](), nil); got["type"] != "object" {
		t.Errorf("typeObject(*leaf) = %v, want the object it points at", got)
	}
}

// TestObjectFromFieldsSkipsAnOpaqueMember: bytes another layer decodes are never
// demanded of the deployment, even though the document carries them.
func TestObjectFromFieldsSkipsAnOpaqueMember(t *testing.T) {
	fields := []plugin.Field{
		{Name: "raw", Type: "jsontext.Value", Doc: "opaque bytes"},
		{Name: "name", Type: "string", Required: true},
	}
	got := objectFromFields(fields, jsontext.Value(`{"raw":{"a":1},"name":"n"}`))
	required, _ := got["required"].([]string)
	if len(required) != 1 || required[0] != "name" {
		t.Errorf("required = %v, want [name]", required)
	}
}

// TestInstanceAlternativesOfAKindWithNoType: a section with no registered type
// is still an object the decoder reads — the envelope, nothing more.
func TestInstanceAlternativesOfAKindWithNoType(t *testing.T) {
	got := instanceAlternatives(reflect.TypeFor[engine.Instance](), nil, nil)
	if got["type"] != "object" {
		t.Fatalf("instanceAlternatives = %v, want an object", got)
	}
	if _, ok := got["oneOf"]; ok {
		t.Errorf("a kind with no type offered alternatives: %v", got)
	}
}
