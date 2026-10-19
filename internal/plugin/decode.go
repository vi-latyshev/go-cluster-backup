package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"text/template"

	"encoding/json/jsontext"
	json "encoding/json/v2"
)

// optionalTag marks an ENGINE config section that may be absent from a given config
// file (a run-shaped file has no `stage`, a stage-shaped one no `storages`),
// and which a
// defaults document therefore never creates: the section is the deployment's to
// declare. Plugin configs never use it.
const optionalTag = "optional"

// requiredTag marks a config field the deployment states itself, because no one value
// suits every deployment or a wrong silent one would lose data. A field
// carrying it may never appear in a defaults document — the check at startup
// refuses that by name. Every config field is either required or present in a
// defaults document; the check proves that too, field by field.
const requiredTag = "required"

// RequiredField reports whether one struct field must be stated by the deployment: it
// carries required:"true", or its type is a secret reference — a document never
// supplies a credential, and a default for one would be a literal where a
// mounted file belongs. The value is read as exactly "true"; any other value is
// not required, which leaves the field to the completeness rule.
func RequiredField(f reflect.StructField) bool {
	return f.Tag.Get(requiredTag) == "true" || isSecretType(f.Type)
}

// OptionalField reports whether one struct field is an ENGINE section a given
// config file may leave out (a run-shaped file has no `stage`). A defaults
// document
// never creates one: the section is the deployment's to declare, and the document
// only fills it once it is there.
func OptionalField(f reflect.StructField) bool { return f.Tag.Get(optionalTag) != "" }

// Decode strictly decodes raw config bytes into cfg (a pointer to a struct):
// every field must be present — the walk is recursive and reports each gap with
// its JSON-pointer path — unknown members are rejected, a Validator runs when
// decoding was clean, and every Template field is parsed with the core function
// set plus, when cfg implements TemplateFuncs, the functions that adds.
// All errors are collected, not just the first.
func Decode(raw jsontext.Value, cfg any) error {
	rv := reflect.ValueOf(cfg)
	t := rv.Type()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var errs []error
	errs = append(errs, missing(t, raw, "")...)
	errs = append(errs, nulls(t, raw, "")...)
	err := json.Unmarshal(raw, cfg, json.RejectUnknownMembers(true))
	if err != nil {
		errs = append(errs, err)
	}
	funcs, ferr := funcsFor(cfg)
	if ferr != nil {
		errs = append(errs, ferr)
	}
	if err == nil {
		errs = append(errs, parseTemplates(rv.Elem(), funcs, "")...)
	}
	if len(errs) == 0 {
		if v, ok := cfg.(Validator); ok {
			errs = append(errs, v.Validate())
		}
	}
	return errors.Join(errs...)
}

// AbsolutePath validates a config field that names a path: it must be present
// and absolute, because a plugin that runs in a container has no useful
// working directory. The returned errors are what a Validate aggregates; field
// is the JSON path for the message.
func AbsolutePath(path, field string) []error {
	if path == "" {
		return []error{fmt.Errorf("%s is empty", field)}
	}
	if !filepath.IsAbs(path) {
		return []error{fmt.Errorf("%s %q must be absolute", field, path)}
	}
	return nil
}

// DecodeOptions strictly decodes one instance's option block — the JSON bytes
// a kind hands an instance itself, restore.apply.<type> being the one shipped:
// an absent value is an error (options are never defaulted, nothing
// destructive ever happens by omission) and a bad member is refused with the
// block's name. key names the config location in both messages.
func DecodeOptions[T any](raw jsontext.Value, key string) (T, error) {
	var opts T
	if len(raw) == 0 {
		return opts, fmt.Errorf("%s is not configured; there is nothing to decode", key)
	}
	if err := Decode(raw, &opts); err != nil {
		return opts, fmt.Errorf("%s: %w", key, err)
	}
	return opts, nil
}

// TemplateFuncs is implemented by a config whose Template fields need functions
// beyond CoreFuncs; the returned map is merged in at decode time.
type TemplateFuncs interface{ TemplateFuncs() template.FuncMap }

func funcsFor(cfg any) (template.FuncMap, error) {
	funcs := CoreFuncs()
	tf, ok := cfg.(TemplateFuncs)
	if !ok {
		return funcs, nil
	}
	var errs []error
	for name, fn := range tf.TemplateFuncs() {
		if _, dup := funcs[name]; dup {
			errs = append(errs, fmt.Errorf("template function %q already exists in the core set", name))
			continue
		}
		funcs[name] = fn
	}
	return funcs, errors.Join(errs...)
}

// missing reports every struct field absent from the JSON value, recursively, with
// its path. Opaque byte fields (jsontext.Value) parse as leaves; a field tagged
// optional:"true" is skipped when absent.
func missing(t reflect.Type, raw jsontext.Value, path string) []error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil // wrong shape: the strict decode reports it
		}
		var errs []error
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "" || name == "-" {
				continue
			}
			val, ok := obj[name]
			if !ok {
				if f.Tag.Get(optionalTag) != "" {
					continue
				}
				errs = append(errs, fmt.Errorf("missing field %s/%s", path, name))
				continue
			}
			errs = append(errs, missing(f.Type, val, path+"/"+name)...)
		}
		return errs
	case reflect.Slice:
		var items []jsontext.Value
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil
		}
		var errs []error
		for i, item := range items {
			errs = append(errs, missing(t.Elem(), item, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	case reflect.Map:
		if t.Elem().Kind() == reflect.Uint8 || t.Elem().Kind() != reflect.Struct {
			return nil // opaque bytes or a scalar value: nothing to require
		}
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var errs []error
		for _, k := range keys {
			errs = append(errs, missing(t.Elem(), obj[k], path+"/"+k)...)
		}
		return errs
	}
	return nil
}

// nullError is the one wording a null is reported with, wherever it is found.
func nullError(path string) error {
	return fmt.Errorf("%s is null; a member that is present carries a value, and a section that is out is written by removing it", path)
}

// nulls reports every JSON null the config carries, with its pointer — a struct
// field, a map value of any element type, a list element. A member that is
// present carries a value, and null is not one: an optional section is taken
// out by removing it, and everywhere else null would silently mean "the zero
// value" to the decoder's own rules, which is how a YAML file reading
//
//	exclude_databases:
//
// reaches a plugin as an empty list where a default belongs. Opaque byte fields
// are leaves here as they are in missing: what a plugin's own config carries is
// the plugin's to decode, and an opaque config is walked by CheckNulls.
func nulls(t reflect.Type, raw jsontext.Value, path string) []error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if IsNull(raw) {
		return []error{nullError(path)}
	}
	switch t.Kind() {
	case reflect.Struct:
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil // wrong shape: the strict decode reports it
		}
		var errs []error
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "" || name == "-" {
				continue
			}
			val, ok := obj[name]
			if !ok {
				continue
			}
			errs = append(errs, nulls(f.Type, val, path+"/"+name)...)
		}
		return errs
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return nil // raw bytes
		}
		var items []jsontext.Value
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil
		}
		var errs []error
		for i, item := range items {
			errs = append(errs, nulls(t.Elem(), item, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	case reflect.Map:
		var obj map[string]jsontext.Value
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil
		}
		var errs []error
		for _, k := range sortedNames(obj) {
			errs = append(errs, nulls(t.Elem(), obj[k], path+"/"+k)...)
		}
		return errs
	}
	return nil
}

// ObjectOf reads one raw JSON value as an object: what a defaults block is,
// and what every walk over one looks a field up in. It answers false for a
// value that is not an object, or does not parse.
func ObjectOf(raw jsontext.Value) (map[string]jsontext.Value, bool) {
	var obj map[string]jsontext.Value
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}
	return obj, true
}

// IsNull reports whether a raw JSON value is a null, whitespace included. The
// defaults mechanism refuses one in both layers: the document and the user's file.
func IsNull(raw jsontext.Value) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

var templateType = reflect.TypeFor[Template]()

// parseTemplates finalizes every Template reachable from v, reporting parse errors
// with the field's path.
func parseTemplates(v reflect.Value, funcs template.FuncMap, path string) []error {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return parseTemplates(v.Elem(), funcs, path)
	case reflect.Struct:
		if v.Type() == templateType {
			if !v.CanAddr() {
				return nil
			}
			if err := v.Addr().Interface().(*Template).finalize(path, funcs); err != nil {
				return []error{err}
			}
			return nil
		}
		var errs []error
		t := v.Type()
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "" || name == "-" {
				name = f.Name
			}
			errs = append(errs, parseTemplates(v.Field(i), funcs, path+"/"+name)...)
		}
		return errs
	case reflect.Slice, reflect.Array:
		var errs []error
		for i := range v.Len() {
			errs = append(errs, parseTemplates(v.Index(i), funcs, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	case reflect.Map:
		var errs []error
		for _, k := range v.MapKeys() {
			// map values are not addressable: finalize a copy and store it back.
			nv := reflect.New(v.Type().Elem()).Elem()
			nv.Set(v.MapIndex(k))
			es := parseTemplates(nv, funcs, path+"/"+fmt.Sprint(k.Interface()))
			errs = append(errs, es...)
			if len(es) == 0 {
				v.SetMapIndex(k, nv)
			}
		}
		return errs
	}
	return nil
}

// fields describes a config type for Descriptors: names, types, docs, secrecy
// and whether the deployment must state the field.
func fields(t reflect.Type) []Field {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	var out []Field
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if !f.IsExported() || name == "" || name == "-" {
			continue
		}
		out = append(out, Field{Name: name, Type: f.Type.String(), Doc: f.Tag.Get("doc"),
			Secret: isSecretType(f.Type), Required: RequiredField(f), Fields: fields(f.Type)})
	}
	return out
}

func isSecretType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	return t == reflect.TypeFor[Secret]()
}
