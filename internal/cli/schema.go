package cli

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// schemaCommand prints the JSON Schema of one configuration file. The engine's
// own sections are reflected from the config types the decoder reads, every
// plugin config from the descriptor of its type, and the classification of
// every field from the same two sources the check at startup uses: a
// `required:"true"` tag, or a value in the distribution's defaults document
// (printed as `default`). It is weaker in one direction on purpose: a
// constraint a descriptor does not carry — an enum living in plugin code, a
// range checked in a constructor — appears in the field's doc, not as a keyword.
func schemaCommand(program string, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("schema", stderr, func() {
		writef(stderr, "usage: %s schema\n", program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	doc := configSchema(reg.Descriptors(), defs)
	// The document is built from Go types and strings: there is nothing in it
	// JSON cannot carry.
	b := must.Do(json.Marshal(doc, jsontext.WithIndent("  "), json.Deterministic(true)))
	if _, err := fmt.Fprintf(stdout, "%s\n", b); err != nil {
		return failure(stderr, program, "schema", err)
	}
	return exitOK
}

// configSchema builds the schema of the whole file: the engine envelope from
// reflection, each instance section as one alternative per registered type of
// its kind, every default read from the distribution's documents.
func configSchema(descs []plugin.Descriptor, defs *engine.Defaults) map[string]any {
	byKind := map[string][]plugin.Descriptor{}
	for _, d := range descs {
		byKind[d.Kind] = append(byKind[d.Kind], d)
	}
	root := typeObject(reflect.TypeFor[engine.Config](), defs.Get(""))
	props, _ := root["properties"].(map[string]any)
	props["version"] = map[string]any{
		"const":       1,
		"description": "configuration format version; this build reads version 1 only",
	}
	instance := reflect.TypeFor[engine.Instance]()
	props["discoveries"] = instanceMapSchema(instance, byKind["discovery"], defs)
	props["sources"] = instanceMapSchema(reflect.TypeFor[engine.SourceInstance](), byKind["source"], defs)
	props["processors"] = instanceMapSchema(instance, byKind["processor"], defs)
	props["storages"] = instanceMapSchema(reflect.TypeFor[engine.StorageInstance](), byKind["storage"], defs)
	props["notifiers"] = instanceMapSchema(instance, byKind["notifier"], defs)
	if catalog := byKind["catalog"]; len(catalog) > 0 {
		props["catalog"] = instanceAlternatives(instance, catalog, defs)
	}
	root["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	root["title"] = "go-cluster-backup configuration"
	root["description"] = "one configuration file as the strict decoder reads it after the distribution's defaults are merged under it: a member the schema does not list in `required` is filled from a defaults document when the file omits it"
	return root
}

// instanceMapSchema is the schema of one named-instance section: a map whose
// every value is one of the registered types of that kind.
func instanceMapSchema(inst reflect.Type, descs []plugin.Descriptor, defs *engine.Defaults) map[string]any {
	return map[string]any{"type": "object", "additionalProperties": instanceAlternatives(inst, descs, defs)}
}

// instanceAlternatives is one JSON Schema alternative per registered type: the
// instance's own fields, its type pinned by const, and its config described by
// that type's descriptor — the classification of the envelope fields coming
// from the kind's `instance` block, the config's from the type's plugin block.
func instanceAlternatives(inst reflect.Type, descs []plugin.Descriptor, defs *engine.Defaults) map[string]any {
	if len(descs) == 0 {
		return typeObject(inst, nil)
	}
	base := typeObject(inst, defs.InstanceBlock(descs[0].Kind))
	baseProps, _ := base["properties"].(map[string]any)
	required, _ := base["required"].([]string)
	alternatives := make([]any, 0, len(descs))
	for _, d := range descs {
		props := map[string]any{
			"type":   map[string]any{"const": d.Type},
			"config": pluginConfigSchema(d, defs.PluginBlock(d.Kind, d.Type)),
		}
		for name, prop := range baseProps {
			if name == "type" || name == "config" {
				continue
			}
			props[name] = prop
		}
		altRequired := required
		block, _ := plugin.ObjectOf(defs.PluginBlock(d.Kind, d.Type))
		if len(block) == 0 {
			// The config key is the one field a plugin block creates whole: with
			// no block at all, the strict decoder demands it like any other.
			altRequired = append(append([]string{}, required...), "config")
		}
		alternatives = append(alternatives, map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties":           props,
			"required":             altRequired,
		})
	}
	return map[string]any{"oneOf": alternatives}
}

func pluginConfigSchema(d plugin.Descriptor, block jsontext.Value) map[string]any {
	if len(d.Fields) == 0 {
		return map[string]any{}
	}
	return objectFromFields(d.Fields, block)
}

// objectFromFields renders one descriptor's fields as a JSON Schema object: a
// field the deployment states is required, a field the document fills carries its
// value as `default`.
func objectFromFields(fields []plugin.Field, block jsontext.Value) map[string]any {
	obj, _ := plugin.ObjectOf(block)
	props := map[string]any{}
	required := make([]string, 0, len(fields))
	for _, f := range fields {
		val, has := obj[f.Name]
		props[f.Name] = fieldSchema(f, val)
		if has {
			if len(f.Fields) == 0 {
				s, _ := props[f.Name].(map[string]any)
				s["default"] = jsontext.Value(val).Clone()
			}
			if f.Type == "jsontext.Value" {
				// Opaque bytes a plugin decodes itself; present in the document,
				// so never demanded of the deployment.
				continue
			}
		}
		if f.Required {
			required = append(required, f.Name)
		}
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           props,
		"required":             required,
	}
}

// fieldSchema maps one descriptor field. The descriptor carries a Go type name,
// so named scalars whose constraints live in plugin code describe as the closest
// JSON kind and name their values in the doc.
func fieldSchema(f plugin.Field, block jsontext.Value) map[string]any {
	s := schemaForTypeName(f.Type, f.Fields, block)
	if f.Doc != "" {
		s["description"] = f.Doc
	}
	if f.Secret {
		s["x-secret"] = true
	}
	return s
}

func schemaForTypeName(name string, fields []plugin.Field, block jsontext.Value) map[string]any {
	switch name {
	case "string":
		return map[string]any{"type": "string"}
	case "bool":
		return map[string]any{"type": "boolean"}
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return map[string]any{"type": "integer"}
	case "float32", "float64":
		return map[string]any{"type": "number"}
	case "model.Duration", "plugin.Template":
		// Both cross the wire as strings: "30s" and a template source.
		return map[string]any{"type": "string"}
	case "model.ByteSize":
		return map[string]any{"type": []string{"integer", "string"}}
	case "jsontext.Value":
		return map[string]any{}
	case "plugin.Secret":
		return secretSchema()
	case "plugin.Class":
		return map[string]any{"type": "string", "enum": []string{string(plugin.ClassTransient), string(plugin.ClassPermanent)}}
	case "model.Importance":
		return map[string]any{"type": "string", "enum": []string{string(model.ImportanceRequired), string(model.ImportanceBestEffort)}}
	case "model.Sensitivity":
		return map[string]any{"type": "string", "enum": []string{string(model.SensitivityPlain), string(model.SensitivitySecret)}}
	}
	switch {
	case strings.HasPrefix(name, "[]"):
		return map[string]any{"type": "array", "items": schemaForTypeName(strings.TrimPrefix(name, "[]"), fields, nil)}
	case strings.HasPrefix(name, "map[") && strings.Contains(name, "]"):
		value := name[strings.Index(name, "]")+1:]
		return map[string]any{"type": "object", "additionalProperties": schemaForTypeName(value, fields, nil)}
	case len(fields) > 0:
		return objectFromFields(fields, block)
	default:
		// A named type the descriptor does not describe: its values are named
		// in the doc, the schema accepts any JSON value.
		return map[string]any{}
	}
}

func secretSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"file": map[string]any{"type": "string", "description": "path of a mounted secret file"},
		},
		"required": []string{"file"},
		"x-secret": true,
	}
}

// typeObject renders one Go struct type as a JSON Schema object exactly the way
// the strict decoder reads it after the merge: every exported member with a
// JSON name is required when the deployment states it, carries `default` when the
// defaults document fills it, and the whole object is refused for unknown
// members.
func typeObject(t reflect.Type, block jsontext.Value) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	obj, _ := plugin.ObjectOf(block)
	props := map[string]any{}
	required := []string{}
	if t.Kind() == reflect.Struct {
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "" || name == "-" {
				continue
			}
			val, has := obj[name]
			props[name] = schemaForType(f.Type, f.Tag.Get("doc"), val)
			if has {
				if len(fieldsOf(f.Type)) == 0 {
					s, _ := props[name].(map[string]any)
					s["default"] = jsontext.Value(val).Clone()
				}
				if deref(f.Type) == reflect.TypeFor[jsontext.Value]() {
					// Opaque bytes another layer decodes; the document fills them.
					continue
				}
			}
			if plugin.RequiredField(f) {
				required = append(required, name)
			}
		}
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           props,
		"required":             required,
	}
}

// schemaForType is typeObject's per-field half: the reflection path of the
// engine's own config sections, where the real Go type is available.
func schemaForType(t reflect.Type, doc string, block jsontext.Value) map[string]any {
	s := schemaForKind(t, block)
	if doc != "" {
		s["description"] = doc
	}
	return s
}

func schemaForKind(t reflect.Type, block jsontext.Value) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case reflect.TypeFor[model.Duration]():
		return map[string]any{"type": "string", "description": "a duration string, e.g. \"30s\""}
	case reflect.TypeFor[model.ByteSize]():
		return map[string]any{"type": []string{"integer", "string"},
			"description": "a whole number of bytes, or digits with a unit: KB, MB, GB, TB (1000^n) or KiB, MiB, GiB, TiB (1024^n)"}
	case reflect.TypeFor[plugin.Template]():
		return map[string]any{"type": "string"}
	case reflect.TypeFor[plugin.Secret]():
		return secretSchema()
	case reflect.TypeFor[jsontext.Value]():
		return map[string]any{}
	case reflect.TypeFor[plugin.Class]():
		return map[string]any{"type": "string", "enum": []string{string(plugin.ClassTransient), string(plugin.ClassPermanent)}}
	case reflect.TypeFor[model.Importance]():
		return map[string]any{"type": "string", "enum": []string{string(model.ImportanceRequired), string(model.ImportanceBestEffort)}}
	case reflect.TypeFor[model.Sensitivity]():
		return map[string]any{"type": "string", "enum": []string{string(model.SensitivityPlain), string(model.SensitivitySecret)}}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaForKind(t.Elem(), nil)}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": schemaForKind(t.Elem(), nil)}
	case reflect.Struct:
		return typeObject(t, block)
	default:
		return map[string]any{}
	}
}

// fieldsOf is fields() for the reflection path: the members of a struct type,
// none for anything else — what tells a leaf from a structure.
func fieldsOf(t reflect.Type) []reflect.StructField {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	out := make([]reflect.StructField, 0, t.NumField())
	for i := range t.NumField() {
		if f := t.Field(i); f.IsExported() {
			out = append(out, f)
		}
	}
	return out
}

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}
