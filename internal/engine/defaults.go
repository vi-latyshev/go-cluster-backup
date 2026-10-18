package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// The defaults mechanism. The service's defaults live in one document
// (internal/defaults/defaults.yaml, embedded beside this package's sibling), a
// build may add a document of its own, and the engine merges
// them under the user's configuration file before a single byte crosses a
// plugin boundary. A plugin still receives complete bytes and still treats
// every field as required; what it never sees is a field the file omitted,
// because by then the defaults document filled it.
//
// The rules, all enforced at startup by LoadDefaults and applied by Merge:
//
//   - objects merge key by key, recursively; a scalar or a list the user's file
//     carries replaces the default whole;
//   - an element of a list the user declared merges over the document's
//     `name[]` block, element by element — the block never creates the list or
//     an element of it;
//   - a key whose field is tagged optional:"true", an instance and a list
//     element are never created: the document only fills what the file
//     declares. A nested object of a present parent is created;
//   - null is refused in both layers;
//   - a default never exists for a field the deployment states (required:"true", or a
//     secret reference), and no field may be left with neither a required tag
//     nor a default.
//
// Two documents must not set the same key, and a distribution's own document
// may only carry `plugins` blocks of its own plugins.

// instanceKind is one place a configuration holds plugin instances: the kind
// each one holds, the Go type of one entry, and whether the section is a single
// instance (catalog) rather than a map of named ones.
type instanceKind struct {
	json   string
	kind   string
	elem   reflect.Type
	single bool
}

// instanceKinds is every such place. A kind may have no section of its own and
// still be named here: the retention policy lives inside its storage, and the
// walk and the merge reach it through this entry.
var instanceKinds = []instanceKind{
	{"discoveries", "discovery", reflect.TypeFor[Instance](), false},
	{"sources", "source", reflect.TypeFor[SourceInstance](), false},
	{"processors", "processor", reflect.TypeFor[Instance](), false},
	{"storages", "storage", reflect.TypeFor[StorageInstance](), false},
	// A retention is inside its storage, one per run kind, so it has no section
	// key of its own: the path names where the walk finds one.
	{"storages/*/retention/*", "retention", reflect.TypeFor[Instance](), false},
	{"notifiers", "notifier", reflect.TypeFor[Instance](), false},
	{"catalog", "catalog", reflect.TypeFor[Instance](), true},
}

// Defaults is the service's defaults for one distribution: its documents
// unioned into one. The zero value holds nothing, so a distribution without a
// document is a Defaults whose every block is absent — and its plugins must
// then state every field themselves, because the check demands it.
type Defaults struct {
	doc jsontext.Value
	// documents is how many documents were unioned into this one: `doctor`
	// prints it as the proof that every document of the distribution was read.
	documents int
}

// Documents is how many documents these defaults were loaded from.
func (d *Defaults) Documents() int {
	if d == nil {
		return 0
	}
	return d.documents
}

// LoadDefaults parses and checks the defaults documents of one distribution.
// docs[0] is the module's own document; every following one belongs to the
// distribution and may only carry `plugins` blocks of its own plugins.
//
// Every rule it enforces is a rule about the document alone, so a broken
// document stops every command at startup rather than one run at night:
//
//  1. a block naming an unregistered kind/type;
//  2. a key no field of the target type has — a typo;
//  3. a value that does not decode into its field's own type: a bad duration, a
//     bad size unit, a malformed secret reference, a template that does not parse;
//  4. a default for a field the deployment states (required:"true", or a secret);
//  5. a field that is neither required nor defaulted;
//  6. one key set by two documents;
//  7. a null, at any depth of any document.
func LoadDefaults(reg *plugin.Registry, docs []jsontext.Value) (*Defaults, error) {
	if reg == nil {
		return nil, errors.New("defaults: the registry is nil")
	}
	var errs []error
	union := map[string]jsontext.Value{}
	for i, doc := range docs {
		obj, ok := plugin.ObjectOf(doc)
		if !ok {
			errs = append(errs, fmt.Errorf("defaults document %d: must be a JSON object", i+1))
			continue
		}
		if i > 0 {
			for _, name := range sortedKeys(obj) {
				if name != "plugins" {
					errs = append(errs, fmt.Errorf("defaults document %d: /%s — a distribution's own document may only carry `plugins` blocks of its own plugins", i+1, name))
				}
			}
		}
		errs = append(errs, docNulls(doc, "")...)
		var unionErrs []error
		union, unionErrs = unionDocs(union, "", doc)
		errs = append(errs, unionErrs...)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("defaults: %w", errors.Join(errs...))
	}
	doc := encode(union)
	if errs := checkDefaults(reg, doc); len(errs) > 0 {
		return nil, fmt.Errorf("defaults: %w", errors.Join(errs...))
	}
	return &Defaults{doc: doc, documents: len(docs)}, nil
}

// Get returns the block at one JSON pointer in the unioned document, or nil
// when the document has no value there. It is how `plugins describe` and
// `schema` read a default beside a descriptor.
func (d *Defaults) Get(pointer string) jsontext.Value {
	if d == nil {
		return nil
	}
	cur := d.doc
	for _, seg := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		if seg == "" {
			continue
		}
		obj, ok := plugin.ObjectOf(cur)
		if !ok {
			return nil
		}
		next, present := obj[seg]
		if !present {
			return nil
		}
		cur = next
	}
	return cur
}

// PluginBlock returns the block of one plugin type — the config defaults of
// every instance of it, wherever the instance is declared. The block's key is
// kind/type, one key of the plugins object and not two pointer segments, so it
// is read as a key of the object Get returns.
func (d *Defaults) PluginBlock(kind, typ string) jsontext.Value {
	plugins, ok := plugin.ObjectOf(d.Get("/plugins"))
	if !ok {
		return nil
	}
	return plugins[kind+"/"+typ]
}

// DefaultsConfig builds the instance config a plugin's block states: the block
// merged under the plugin's own complete example, with every value the block
// states removed from the example first so that what the plugin judges is the
// document's value and not the example's.
//
// The examples are complete by contract, so merging a block *under* one — the
// direction a deployment's file merges in — would leave every value of the
// block out of the result. Removing the block's own paths first is what makes
// the merge the block speaking, and `doctor` is the reader: a bad enum, range
// or combination in a defaults document stops the image build here, because the
// check that reads a document judges a value by its Go type and an enum is a
// plain string there.
func (d *Defaults) DefaultsConfig(reg *plugin.Registry, kind, typ string, example jsontext.Value) (jsontext.Value, error) {
	block := d.PluginBlock(kind, typ)
	cfgType, ok := reg.ConfigType(kind, typ)
	if !ok {
		return example, fmt.Errorf("%s/%s is not a registered plugin", kind, typ)
	}
	if len(block) == 0 {
		return example, nil
	}
	trimmed := withoutValues(cfgType, example, block)
	merged, _ := mergeValue(cfgType, block, trimmed, "")
	return merged, nil
}

// withoutValues removes from one example every value a block states: a struct
// member the block sets, and — for a `name[]` key — the block's members inside
// every element of the list the example carries, which is where the merge would
// have applied them.
func withoutValues(t reflect.Type, example, block jsontext.Value) jsontext.Value {
	t = derefType(t)
	exObj, exOK := plugin.ObjectOf(example)
	blkObj, blkOK := plugin.ObjectOf(block)
	if !exOK || !blkOK {
		return example
	}
	for _, key := range sortedKeys(blkObj) {
		base, isElement := strings.CutSuffix(key, "[]")
		f, known := fieldByName(t, base)
		if !known {
			continue
		}
		ft := derefType(f.Type)
		if isElement {
			items, ok := jsonArray(exObj[base])
			if !ok || ft.Kind() != reflect.Slice {
				continue
			}
			out := make([]jsontext.Value, len(items))
			for i, item := range items {
				out[i] = withoutValues(ft.Elem(), item, blkObj[key])
			}
			exObj[base] = encode(out)
			continue
		}
		val, present := exObj[base]
		if !present {
			continue
		}
		if ft.Kind() == reflect.Struct {
			exObj[base] = withoutValues(ft, val, blkObj[key])
			continue
		}
		delete(exObj, base)
	}
	return encode(exObj)
}

// InstanceBlock returns the `instance.<kind>` envelope — what the engine reads
// on every instance of that kind.
func (d *Defaults) InstanceBlock(kind string) jsontext.Value {
	return d.Get("/instance/" + kind)
}

// Merge merges the defaults under one configuration document and returns the
// merged bytes with the JSON pointers whose value came from a default. It fills
// the engine's own sections, every declared instance — its kind's envelope over
// the whole entry, its type's plugin block under the entry's config — and every
// rule source a discovery declares, which is where the nested source config
// lives that the strict decoder only ever sees as opaque bytes.
func (d *Defaults) Merge(ctx context.Context, reg *plugin.Registry, log *slog.Logger, raw jsontext.Value) (jsontext.Value, []string, error) {
	if d == nil || len(d.doc) == 0 {
		return raw, nil, nil
	}
	user, ok := plugin.ObjectOf(raw)
	if !ok {
		return raw, nil, nil // not an object: the strict decoder reports the shape
	}
	def, _ := plugin.ObjectOf(d.doc)
	engineDef := map[string]jsontext.Value{}
	for name, val := range def {
		if name == "plugins" || name == "instance" {
			continue
		}
		engineDef[name] = val
	}
	merged, pointers := mergeValue(reflect.TypeFor[Config](), encode(engineDef), encode(user), "")
	merged, instancePointers := d.mergeInstances(reg, merged)
	merged, rulePointers, err := d.mergeRuleSources(ctx, reg, log, merged)
	if err != nil {
		return nil, nil, err
	}
	pointers = append(append(pointers, instancePointers...), rulePointers...)
	return merged, pointers, nil
}

// mergeInstances merges every instance the configuration declares: the
// envelope of its kind over the whole entry and the plugin block of its type
// under the entry's config. An instance the file does not declare never
// appears — a default creates no instance.
func (d *Defaults) mergeInstances(reg *plugin.Registry, doc jsontext.Value) (jsontext.Value, []string) {
	def, _ := plugin.ObjectOf(d.doc)
	merged, ok := plugin.ObjectOf(doc)
	if !ok {
		return doc, nil
	}
	var pointers []string
	for _, sec := range instanceKinds {
		if sec.single {
			entry, ok := plugin.ObjectOf(merged[sec.json])
			if !ok {
				continue
			}
			out, ptrs := mergeInstance(def, reg, sec, entry, "/"+sec.json)
			merged[sec.json] = out
			pointers = append(pointers, ptrs...)
			continue
		}
		entries, ok := plugin.ObjectOf(merged[sec.json])
		if !ok {
			continue
		}
		out, ptrs := d.mergeEntries(reg, def, sec, entries, "/"+sec.json)
		merged[sec.json] = out
		pointers = append(pointers, ptrs...)
	}
	// Two places hold an instance that is not an entry of a top-level section:
	// the retention inside a storage — the policy belongs to the storage, so it
	// has no name of its own — and the sources of the stage section, which the
	// stage command builds and the run never does.
	if entries, ok := plugin.ObjectOf(merged["storages"]); ok {
		for _, name := range sortedKeys(entries) {
			entry, ok := plugin.ObjectOf(entries[name])
			if !ok {
				continue
			}
			ret, ok := plugin.ObjectOf(entry["retention"])
			if !ok {
				continue
			}
			sec, _ := sectionForKind("retention")
			for _, kind := range sortedKeys(ret) {
				inst, ok := plugin.ObjectOf(ret[kind])
				if !ok {
					continue
				}
				out, ptrs := mergeInstance(def, reg, sec, inst, "/storages/"+name+"/retention/"+kind)
				ret[kind] = out
				pointers = append(pointers, ptrs...)
			}
			entry["retention"] = encode(ret)
			entries[name] = encode(entry)
		}
		merged["storages"] = encode(entries)
	}
	if stage, ok := plugin.ObjectOf(merged["stage"]); ok {
		if sources, ok := plugin.ObjectOf(stage["sources"]); ok {
			sec, _ := sectionForKind("source")
			out, ptrs := d.mergeEntries(reg, def, sec, sources, "/stage/sources")
			stage["sources"] = out
			merged["stage"] = encode(stage)
			pointers = append(pointers, ptrs...)
		}
	}
	return encode(merged), pointers
}

// mergeEntries merges every declared entry of one section, path being the
// section's own path: each entry takes the envelope of its kind and the plugin
// block of its own type, and an entry the file does not declare is never
// created.
func (d *Defaults) mergeEntries(reg *plugin.Registry, def map[string]jsontext.Value, sec instanceKind, entries map[string]jsontext.Value, path string) (jsontext.Value, []string) {
	var pointers []string
	for _, name := range sortedKeys(entries) {
		entry, ok := plugin.ObjectOf(entries[name])
		if !ok {
			continue
		}
		out, ptrs := mergeInstance(def, reg, sec, entry, path+"/"+name)
		entries[name] = out
		pointers = append(pointers, ptrs...)
	}
	return encode(entries), pointers
}

// mergeInstance merges one entry: first the envelope of its kind — the fields
// the engine reads on every instance of that kind — then the plugin block of
// the entry's own type under its config. The second step walks with the
// plugin's own Config type: the bytes an envelope holds are opaque, and only
// the type says what a field of them is.
func mergeInstance(def map[string]jsontext.Value, reg *plugin.Registry, sec instanceKind, entry map[string]jsontext.Value, path string) (jsontext.Value, []string) {
	envelopes, _ := plugin.ObjectOf(def["instance"])
	envelope, _ := plugin.ObjectOf(envelopes[sec.kind])
	out, pointers := mergeValue(sec.elem, encode(envelope), encode(entry), path)
	typ, ok := jsonString(entry["type"])
	if !ok {
		return out, pointers
	}
	blocks, _ := plugin.ObjectOf(def["plugins"])
	block, ok := plugin.ObjectOf(blocks[sec.kind+"/"+typ])
	if !ok || len(block) == 0 {
		return out, pointers
	}
	cfgType, ok := reg.ConfigType(sec.kind, typ)
	if !ok {
		return out, pointers // the check refused an unregistered type at load
	}
	merged, _ := plugin.ObjectOf(out)
	at := path + "/config"
	if cfg, present := merged["config"]; present {
		cfgJSON, ptrs := mergeValue(cfgType, encode(block), cfg, at)
		merged["config"] = cfgJSON
		pointers = append(pointers, ptrs...)
	} else {
		// A config the file does not declare at all: the block creates it, and
		// every leaf of it is defaulted.
		merged["config"] = encode(block)
		pointers = append(pointers, leafPointers(encode(block), at)...)
	}
	return encode(merged), pointers
}

// mergeRuleSources merges each rule's source block under the rule's own source
// config, in place in the document: a discovery declares where its rules live
// (discovery.RuleSourceReporter), and the nested config is opaque bytes to the
// strict decoder, so this is the one place the engine learns the path. A
// discovery that cannot be built is left alone — the decode and the build
// report it with the whole error list — and so is a rule whose source type has
// no block.
func (d *Defaults) mergeRuleSources(ctx context.Context, reg *plugin.Registry, log *slog.Logger, doc jsontext.Value) (jsontext.Value, []string, error) {
	def, _ := plugin.ObjectOf(d.doc)
	blocks, _ := plugin.ObjectOf(def["plugins"])
	if len(blocks) == 0 {
		return doc, nil, nil
	}
	merged, ok := plugin.ObjectOf(doc)
	if !ok {
		return doc, nil, nil
	}
	entries, ok := plugin.ObjectOf(merged["discoveries"])
	if !ok {
		return doc, nil, nil
	}
	out := doc
	var pointers []string
	for _, name := range sortedKeys(entries) {
		entry, ok := plugin.ObjectOf(entries[name])
		if !ok {
			continue
		}
		typ, ok := jsonString(entry["type"])
		if !ok || typ == "" {
			continue
		}
		deps := plugin.Deps{Instance: name, Log: log.With("kind", "discovery", "type", typ, "instance", name)}
		inst, err := reg.BuildAny(ctx, "discovery", typ, entry["config"], deps)
		if err != nil {
			continue
		}
		if c, ok := inst.(interface{ Close() error }); ok {
			defer func() { _ = c.Close() }()
		}
		reporter, ok := inst.(discovery.RuleSourceReporter)
		if !ok {
			continue
		}
		rules, err := reporter.RuleSources(ctx)
		if err != nil {
			continue
		}
		for _, rs := range rules {
			if rs.Type == "" {
				continue
			}
			block, ok := plugin.ObjectOf(blocks["source/"+rs.Type])
			if !ok || len(block) == 0 {
				continue
			}
			cfgType, ok := reg.ConfigType("source", rs.Type)
			if !ok {
				continue
			}
			ruleConfig, ptrs := mergeValue(cfgType, encode(block), rs.Config, "")
			if len(ptrs) == 0 {
				continue
			}
			// A rule's paths are pointers inside the discovery's own config,
			// which is the entry's config key.
			at := "/discoveries/" + name + "/config" + rs.ConfigPath
			out, err = setAt(out, at, ruleConfig)
			if err != nil {
				return nil, nil, err
			}
			for i := range ptrs {
				ptrs[i] = at + ptrs[i]
			}
			pointers = append(pointers, ptrs...)
		}
	}
	return out, pointers, nil
}

// mergeValue merges one defaults value under one user value, guided by the type
// the result decodes into, and returns the value and the pointers whose value
// came from the defaults.
//
// A key the user's file lacks is created unless its field is optional:"true" —
// an optional section is the deployment's to declare. A `name[]` key merges the
// elements of a list the user's file declares and never creates the list or an
// element of it. Anything else the user's file carries replaces the default
// whole, a list included.
func mergeValue(t reflect.Type, def, user jsontext.Value, path string) (jsontext.Value, []string) {
	t = derefType(t)
	if t == valueType || plugin.IsNull(user) {
		return user, nil // opaque bytes, or no value to merge under
	}
	defObj, defIsObj := plugin.ObjectOf(def)
	userObj, userIsObj := plugin.ObjectOf(user)
	if !defIsObj || len(defObj) == 0 || !userIsObj {
		return user, nil // nothing to merge, or the user's value wins whole
	}
	var pointers []string
	switch t.Kind() {
	case reflect.Struct:
		out := map[string]jsontext.Value{}
		for name, val := range userObj {
			out[name] = val
		}
		for _, key := range sortedKeys(defObj) {
			if base, isElem := strings.CutSuffix(key, "[]"); isElem {
				f, known := fieldByName(t, base)
				if !known {
					continue // the check refused it when the document was loaded
				}
				ft := derefType(f.Type)
				list, present := userObj[base]
				if !present || ft.Kind() != reflect.Slice {
					continue // a default never creates a list or an element of one
				}
				merged, ptrs := mergeElements(ft.Elem(), defObj[key], list, path+"/"+base)
				out[base] = merged
				pointers = append(pointers, ptrs...)
				continue
			}
			f, known := fieldByName(t, key)
			if !known {
				continue
			}
			if userVal, present := userObj[key]; present {
				merged, ptrs := mergeValue(f.Type, defObj[key], userVal, path+"/"+key)
				out[key] = merged
				pointers = append(pointers, ptrs...)
				continue
			}
			if plugin.OptionalField(f) {
				continue
			}
			out[key] = defObj[key]
			pointers = append(pointers, leafPointers(defObj[key], path+"/"+key)...)
		}
		return encode(out), pointers
	case reflect.Map:
		// A map merges key by key like an object: an entry the user's file has
		// replaces the default, one it lacks is added.
		out := map[string]jsontext.Value{}
		for name, val := range userObj {
			out[name] = val
		}
		for _, key := range sortedKeys(defObj) {
			if userVal, present := userObj[key]; present {
				merged, ptrs := mergeValue(t.Elem(), defObj[key], userVal, path+"/"+key)
				out[key] = merged
				pointers = append(pointers, ptrs...)
				continue
			}
			out[key] = defObj[key]
			pointers = append(pointers, leafPointers(defObj[key], path+"/"+key)...)
		}
		return encode(out), pointers
	}
	return user, nil
}

// mergeElements merges one element block over every element of a list the
// user's file declared. The block is the defaults of each element — for nested
// objects, ordinary nesting — and a list the file does not declare, or an
// element the defaults might carry beyond it, is never created.
func mergeElements(elem reflect.Type, block, user jsontext.Value, path string) (jsontext.Value, []string) {
	userItems, ok := jsonArray(user)
	if !ok {
		return user, nil
	}
	out := make([]jsontext.Value, len(userItems))
	var pointers []string
	for i := range userItems {
		merged, ptrs := mergeValue(elem, block, userItems[i], fmt.Sprintf("%s/%d", path, i))
		out[i] = merged
		pointers = append(pointers, ptrs...)
	}
	return encode(out), pointers
}

// leafPointers lists the leaves of a value that was created whole from a
// defaults document: an object at its keys, everything else at its own path. A
// `name[]` key carries none — it fills elements the user declared.
func leafPointers(val jsontext.Value, path string) []string {
	obj, ok := plugin.ObjectOf(val)
	if !ok {
		return []string{path}
	}
	if len(obj) == 0 {
		return []string{path}
	}
	var out []string
	for _, name := range sortedKeys(obj) {
		if strings.HasSuffix(name, "[]") {
			continue
		}
		out = append(out, leafPointers(obj[name], path+"/"+name)...)
	}
	return out
}

// checkDefaults walks the unioned document against the registry and the
// engine's own types. It is LoadDefaults' rules 1 to 5; the null and the
// two-documents checks run before the union, at the only place they can.
func checkDefaults(reg *plugin.Registry, doc jsontext.Value) []error {
	obj, _ := plugin.ObjectOf(doc)
	var errs []error

	// The engine's own sections, and its version: the document minus the two
	// sections that are not fields of the configuration.
	engineDoc := map[string]jsontext.Value{}
	for name, val := range obj {
		if name == "plugins" || name == "instance" {
			continue
		}
		engineDoc[name] = val
	}
	errs = append(errs, walkValue(reflect.TypeFor[Config](), encode(engineDoc), "")...)

	// The envelope of every kind: what the engine reads on every instance of
	// that kind, wherever the instance is declared.
	instances, _ := plugin.ObjectOf(obj["instance"])
	for _, name := range sortedKeys(instances) {
		sec, known := sectionForKind(name)
		if !known {
			errs = append(errs, fmt.Errorf("/instance/%s: the configuration has no such section (kinds: %s)", name, strings.Join(kindNames(), ", ")))
			continue
		}
		if block, ok := plugin.ObjectOf(instances[name]); ok {
			if _, has := block["config"]; has {
				errs = append(errs, fmt.Errorf("/instance/%s/config: a plugin config default belongs under plugins/<kind>/<type>; an envelope carries the fields the engine reads on every instance", name))
				continue
			}
		}
		at := "/" + sec.json
		if !sec.single {
			at += "/*"
		}
		errs = append(errs, walkValue(sec.elem, instances[name], at)...)
	}

	// The plugin blocks: the config of every instance of that kind and type,
	// wherever it is declared.
	blocks, _ := plugin.ObjectOf(obj["plugins"])
	for _, name := range sortedKeys(blocks) {
		kind, typ, ok := strings.Cut(name, "/")
		if !ok || kind == "" || typ == "" || strings.Contains(typ, "/") {
			errs = append(errs, fmt.Errorf("/plugins/%s: a block is named kind/type, e.g. source/postgres", name))
			continue
		}
		t, ok := reg.ConfigType(kind, typ)
		if !ok {
			errs = append(errs, fmt.Errorf("/plugins/%s: unknown plugin (known: %s)", name, knownPlugins(reg)))
			continue
		}
		errs = append(errs, walkValue(t, blocks[name], "/plugins/"+name)...)
	}
	return errs
}

// walkValue checks one defaults value against the type that decodes it, and is
// where every structural rule of a document lives. An object walks field by
// field — a `name[]` key walks the elements of the list `name` — a map walks
// key by key, and everything else decodes whole into the field's own type.
func walkValue(t reflect.Type, val jsontext.Value, path string) []error {
	t = derefType(t)
	if t == valueType {
		return nil // opaque bytes: a plugin's own config, checked against its block
	}
	if len(val) == 0 {
		// No block at all: there is no value to judge, only the classification
		// of the type's own fields — every one of them is required or defaulted
		// somewhere, and this is where the walk proves it. A list or a map is
		// entered through its element type, path and all.
		switch {
		case t.Kind() == reflect.Struct:
			return walkStruct(t, nil, path)
		case (t.Kind() == reflect.Slice || t.Kind() == reflect.Map) && !instanceEnvelope(t.Elem()):
			if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
				return nil // raw bytes
			}
			return walkValue(t.Elem(), nil, path+"/*")
		}
		return nil
	}
	obj, isObj := plugin.ObjectOf(val)
	switch {
	case isObj && t.Kind() == reflect.Struct:
		return walkStruct(t, obj, path)
	case isObj && t.Kind() == reflect.Map:
		var errs []error
		for _, name := range sortedKeys(obj) {
			errs = append(errs, walkValue(t.Elem(), obj[name], path+"/"+name)...)
		}
		return errs
	default:
		return decodeDefault(t, val, path)
	}
}

// walkStruct checks one object against one struct type: the classification of
// every field (required, defaulted, or — refused — neither), the value of every
// key, the elements of every `name[]` list block, and every key that names no
// field at all.
func walkStruct(t reflect.Type, obj map[string]jsontext.Value, path string) []error {
	var errs []error
	matched := map[string]bool{}
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if !f.IsExported() || name == "" || name == "-" {
			continue
		}
		required := plugin.RequiredField(f)
		val, has := obj[name]
		if has {
			matched[name] = true
		}
		elem, hasElem := obj[name+"[]"]
		switch {
		case has && required:
			errs = append(errs, fmt.Errorf("%s/%s: a default for a field the deployment states (required:\"true\", or a secret reference); remove the key from the document", path, name))
		case has:
			errs = append(errs, walkValue(f.Type, val, path+"/"+name)...)
		case required, plugin.OptionalField(f), derefType(f.Type) == valueType:
			// The deployment states it; nothing in the document may. An optional
			// section — the deployment declares it or leaves it out — and an opaque
			// field, whose own config is checked against its plugin block, are
			// classified below their own field. An instance envelope is the one
			// type walked by the section that knows its kind's envelope block,
			// and a list with a `name[]` block is classified element by element
			// there instead of here.
			if !hasElem && !instanceEnvelope(f.Type) {
				errs = append(errs, walkValue(f.Type, nil, path+"/"+name)...)
			}
		default:
			errs = append(errs, fmt.Errorf("%s/%s: neither required:\"true\" nor defaulted; every config field is one of the two — a default goes in the defaults document, a tag on the field", path, name))
		}
		if !hasElem {
			continue
		}
		matched[name+"[]"] = true
		ft := derefType(f.Type)
		if ft.Kind() != reflect.Slice {
			errs = append(errs, fmt.Errorf("%s/%s[]: %s is not a list; the block fills the elements of one list field", path, name, name))
			continue
		}
		errs = append(errs, walkValue(ft.Elem(), elem, path+"/"+name+"[]")...)
	}
	for _, name := range sortedKeys(obj) {
		if !matched[name] {
			errs = append(errs, fmt.Errorf("%s/%s: unknown key; the type has no such field", path, name))
		}
	}
	return errs
}

// decodeDefault checks one leaf default the way the strict decoder will read it
// at run time: through the field's own type, so a bad duration, a bad size, a
// malformed secret reference or a template that does not parse is refused here,
// at startup, with its pointer.
func decodeDefault(t reflect.Type, val jsontext.Value, path string) []error {
	if err := plugin.Decode(val, reflect.New(t).Interface()); err != nil {
		return []error{fmt.Errorf("%s: %w", path, err)}
	}
	return nil
}

// unionDocs merges one document into the union of the earlier ones. Two
// documents must not set the same key: two objects merge key by key, so a
// document adding a key to a block another one declares conflicts only where
// both set it, and everything else is a conflict at its own pointer.
func unionDocs(acc map[string]jsontext.Value, path string, doc jsontext.Value) (map[string]jsontext.Value, []error) {
	obj, _ := plugin.ObjectOf(doc)
	var errs []error
	for _, name := range sortedKeys(obj) {
		val := obj[name]
		prev, present := acc[name]
		if !present {
			acc[name] = val
			continue
		}
		prevObj, prevIsObj := plugin.ObjectOf(prev)
		valObj, valIsObj := plugin.ObjectOf(val)
		if prevIsObj && valIsObj {
			merged, inner := unionDocs(prevObj, path+"/"+name, encode(valObj))
			errs = append(errs, inner...)
			acc[name] = encode(merged)
			continue
		}
		errs = append(errs, fmt.Errorf("%s/%s: set by two defaults documents; a key has exactly one source", path, name))
	}
	return acc, errs
}

// docNulls reports every null a defaults document carries, at any depth: a
// document sets values, and switching something off has off-values of its own
// (false, 0, an empty list, an empty template).
func docNulls(raw jsontext.Value, path string) []error {
	if plugin.IsNull(raw) {
		return []error{fmt.Errorf("%s is null; a defaults document carries values, and switching something off is written as an off-value", path)}
	}
	if obj, ok := plugin.ObjectOf(raw); ok {
		var errs []error
		for _, name := range sortedKeys(obj) {
			errs = append(errs, docNulls(obj[name], path+"/"+name)...)
		}
		return errs
	}
	if arr, ok := jsonArray(raw); ok {
		var errs []error
		for i, item := range arr {
			errs = append(errs, docNulls(item, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	}
	return nil
}

// setAt replaces the value at one JSON pointer inside a document.
func setAt(doc jsontext.Value, pointer string, val jsontext.Value) (jsontext.Value, error) {
	head, rest, _ := strings.Cut(strings.TrimPrefix(pointer, "/"), "/")
	next := func(child jsontext.Value) (jsontext.Value, error) {
		if rest == "" {
			return val, nil // the last segment: the value replaces this one
		}
		return setAt(child, "/"+rest, val)
	}
	if arr, ok := jsonArray(doc); ok {
		i, err := strconv.Atoi(head)
		if err != nil || i < 0 || i >= len(arr) {
			return nil, fmt.Errorf("%s: no such element", pointer)
		}
		child, err := next(arr[i])
		if err != nil {
			return nil, err
		}
		arr[i] = child
		return encode(arr), nil
	}
	obj, ok := plugin.ObjectOf(doc)
	if !ok {
		return nil, fmt.Errorf("%s: no such member", pointer)
	}
	child, present := obj[head]
	if !present {
		return nil, fmt.Errorf("%s: no such member", pointer)
	}
	child, err := next(child)
	if err != nil {
		return nil, err
	}
	obj[head] = child
	return encode(obj), nil
}

// instanceEnvelope reports whether one type is the envelope of a plugin
// instance — the {type, config} shape whose config belongs to a plugin block
// and whose other fields to the kind's envelope. It is classified by the walk
// that has that envelope at hand, never as a plain struct of the configuration.
func instanceEnvelope(t reflect.Type) bool {
	t = derefType(t)
	for _, sec := range instanceKinds {
		if derefType(sec.elem) == t {
			return true
		}
	}
	return false
}

// sectionForKind finds the config section that holds instances of one kind.
func sectionForKind(kind string) (instanceKind, bool) {
	for _, sec := range instanceKinds {
		if sec.kind == kind {
			return sec, true
		}
	}
	return instanceKinds[0], false
}

func kindNames() []string {
	names := make([]string, 0, len(instanceKinds))
	for _, sec := range instanceKinds {
		names = append(names, sec.kind)
	}
	return names
}

func knownPlugins(reg *plugin.Registry) string {
	var names []string
	for _, d := range reg.Descriptors() {
		names = append(names, d.Kind+"/"+d.Type)
	}
	return strings.Join(names, ", ")
}

// derefType is the type behind every pointer; a nil type stays nil.
func derefType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// fieldByName finds one struct field by its JSON name — what a defaults block
// key has to match.
func fieldByName(t reflect.Type, name string) (reflect.StructField, bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if !f.IsExported() || tag == "-" {
			continue
		}
		if tag == name || (tag == "" && f.Name == name) {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

var valueType = reflect.TypeFor[jsontext.Value]()

// encode re-encodes a map or a slice of raw JSON values, deterministically. The
// values come from a document that already parsed; a failure here would be a
// bug in this package, not input.
func encode(v any) jsontext.Value {
	b, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		panic(fmt.Sprintf("engine: re-encoding a defaults document: %v", err))
	}
	return b
}

func jsonArray(raw jsontext.Value) ([]jsontext.Value, bool) {
	var arr []jsontext.Value
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, false
	}
	return arr, true
}

func jsonString(raw jsontext.Value) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}
