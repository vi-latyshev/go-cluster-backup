package plugin

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// CheckShape validates the structure of one instance config without decoding a
// single value: every missing member, every unknown member and every value
// whose JSON kind the field cannot decode is reported with its JSON-pointer
// path under path, and no string is parsed as a template. It is the offline
// check a nested config gets, where the strict decoder sees only opaque bytes —
// a discovery rule's source config, whose strings the discovery renders per
// matched object.
//
// A key is never templated, so a key-level answer is always safe. A leaf's kind
// is judged because a template renders to a JSON string and to nothing else: a
// template in a field that needs a number, a boolean, an object or an array
// cannot decode at run time, and a literal of the wrong kind is wrong already.
// What cannot be classified with certainty stays unconstrained — a false
// refusal blocks a correct production config, which is worse than a miss.
func (r *Registry) CheckShape(kind, typ string, raw jsontext.Value, path string) error {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return fmt.Errorf("%s: %w", path, r.unknown(kind, typ))
	}
	errs := missing(e.cfg, raw, path)
	errs = append(errs, shape(e.cfg, raw, path)...)
	return errors.Join(errs...)
}

// jsonKinds is a set of JSON kinds; the five a check judges, never null.
type jsonKinds uint8

const (
	kindString jsonKinds = 1 << iota
	kindNumber
	kindBool
	kindObject
	kindArray
	// kindAny is what an unclassifiable field gets: no kind is refused.
	kindAny jsonKinds = kindString | kindNumber | kindBool | kindObject | kindArray
)

var (
	durationType    = reflect.TypeFor[model.Duration]()
	byteSizeType    = reflect.TypeFor[model.ByteSize]()
	valueType       = reflect.TypeFor[jsontext.Value]()
	unmarshalerType = reflect.TypeFor[json.Unmarshaler]()
)

// decodableKinds answers which JSON kinds a field of this Go type decodes from.
// The framework's types that unmarshal themselves are classified explicitly;
// every other self-unmarshalling type is left open, because only the type
// itself knows what it takes; a plain kind follows reflect.
func decodableKinds(t reflect.Type) jsonKinds {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case templateType: // a template field is a JSON string, whatever it renders to
		return kindString
	case durationType: // "30s"
		return kindString
	case byteSizeType: // 5368709120 or "5GiB", and a template may render either
		return kindString | kindNumber
	case valueType: // jsontext.Value holds any document, kept opaque
		return kindAny
	}
	if t.Implements(unmarshalerType) || reflect.PointerTo(t).Implements(unmarshalerType) {
		return kindAny
	}
	switch t.Kind() {
	case reflect.String:
		return kindString
	case reflect.Bool:
		return kindBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return kindNumber
	case reflect.Struct:
		return kindObject
	case reflect.Map:
		return kindObject
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return kindAny // raw bytes
		}
		return kindArray
	}
	return kindAny // interface, channel, function: not something this check judges
}

// shape walks one raw value against the type that must decode it: the kind of
// every leaf, and the members of every structure. A type left unconstrained is
// not walked into — nothing of it is checked, which is the point.
func shape(t reflect.Type, raw jsontext.Value, path string) []error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	kinds := decodableKinds(t)
	if kinds == kindAny {
		return nil
	}
	kind := rawKind(raw)
	switch {
	case kind == 0:
		return nil // an unjudged value: null, or nothing at all
	case kind&kinds == 0:
		return []error{fmt.Errorf("%s must be a JSON %s, got a JSON %s", path, kindName(kinds), kindName(kind))}
	}
	var errs []error
	switch {
	case kind == kindObject && t.Kind() == reflect.Struct:
		obj, ok := ObjectOf(raw)
		if !ok {
			return nil // a malformed object: the decoder reports its own
		}
		for _, name := range sortedNames(obj) {
			ft, known := fieldType(t, name)
			if !known {
				errs = append(errs, fmt.Errorf("unknown field %s/%s", path, name))
				continue
			}
			errs = append(errs, shape(ft, obj[name], path+"/"+name)...)
		}
	case kind == kindObject && t.Kind() == reflect.Map:
		obj, ok := ObjectOf(raw)
		if !ok {
			return nil
		}
		for _, name := range sortedNames(obj) {
			errs = append(errs, shape(t.Elem(), obj[name], path+"/"+name)...)
		}
	case kind == kindArray && (t.Kind() == reflect.Slice || t.Kind() == reflect.Array):
		items, ok := arrayOf(raw)
		if !ok {
			return nil
		}
		for i, item := range items {
			errs = append(errs, shape(t.Elem(), item, fmt.Sprintf("%s/%d", path, i))...)
		}
	}
	return errs
}

// CheckNulls reports every JSON null one value carries, at any depth, with its
// pointer path. It is what a configuration the strict decoder only sees as
// opaque bytes owes the offline check: a rule's source config is decoded by its
// plugin at run time, and by then a null has already been read as "absent" —
// the list the deployment commented out becomes an empty list where a default
// belongs. A null at the root is reported too: there is no value to check.
func CheckNulls(raw jsontext.Value, path string) []error {
	if IsNull(raw) {
		return []error{nullError(path)}
	}
	if obj, ok := ObjectOf(raw); ok {
		var errs []error
		for _, name := range sortedNames(obj) {
			errs = append(errs, CheckNulls(obj[name], path+"/"+name)...)
		}
		return errs
	}
	if arr, ok := arrayOf(raw); ok {
		var errs []error
		for i, item := range arr {
			errs = append(errs, CheckNulls(item, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	}
	return nil
}

// rawKind classifies one raw JSON value by its first byte. It answers 0 for
// null, which no template produces and whose decoding rules are the decoder's
// own, and for a value it cannot classify — both stay unjudged.
func rawKind(raw jsontext.Value) jsonKinds {
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\n', '\r':
		case '"':
			return kindString
		case '{':
			return kindObject
		case '[':
			return kindArray
		case 't', 'f':
			return kindBool
		case 'n':
			return 0
		default:
			return kindNumber
		}
	}
	return 0
}

func kindName(kind jsonKinds) string {
	switch kind {
	case kindString:
		return "string"
	case kindNumber:
		return "number"
	case kindBool:
		return "boolean"
	case kindObject:
		return "object"
	case kindArray:
		return "array"
	}
	return "value"
}

func arrayOf(raw jsontext.Value) ([]jsontext.Value, bool) {
	var items []jsontext.Value
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, false
	}
	return items, true
}

// fieldType finds the struct field one JSON member name belongs to, promoted
// embedded fields included, the way encoding/json resolves a member.
func fieldType(t reflect.Type, name string) (reflect.Type, bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		switch {
		case tag == "-":
		case tag != "":
			if tag == name {
				return f.Type, true
			}
		case f.Anonymous:
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if inner, ok := fieldType(ft, name); ok {
				return inner, true
			}
		case f.Name == name:
			return f.Type, true
		}
	}
	return nil, false
}

func sortedNames(obj map[string]jsontext.Value) []string {
	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
