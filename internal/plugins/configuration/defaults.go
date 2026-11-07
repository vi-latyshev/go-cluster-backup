package configuration

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"
)

// The defaults mechanism. The service's defaults live in one document
// (embedded by the distribution), a build may add a document of its own, and
// the configuration layer merges them under the deployment's file before a
// single byte crosses a plugin boundary. A plugin still receives complete bytes
// and still treats every field as required; what it never sees is a field the
// file omitted, because by then the defaults document filled it.
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
// The root `plugins` object is the one plugin control shared by both layers:
// `plugins.config.<ID>` is the middle settings layer, above the defaults
// document's block of the same ID and below an instance's own `config`.

// instanceKind is one place a configuration holds plugin instances: the kind
// each one holds, the Go type of one entry, and whether the section is a single
// instance (catalog) rather than a map of named ones.
type instanceKind struct {
	json   string
	kind   Kind
	elem   reflect.Type
	single bool
}

// instanceKinds is every such place. A kind may have no section of its own and
// still be named here: the retention policy lives inside its storage, and the
// walk and the merge reach it through this entry.
var instanceKinds = []instanceKind{
	{"discoveries", KindDiscovery, reflect.TypeFor[Instance](), false},
	{"sources", KindSource, reflect.TypeFor[SourceInstance](), false},
	{"processors", KindProcessor, reflect.TypeFor[Instance](), false},
	{"storages", KindStorage, reflect.TypeFor[StorageInstance](), false},
	// A retention is inside its storage, one per run kind, so it has no section
	// key of its own: the path names where the walk finds one.
	{"storages/*/retention/*", KindRetention, reflect.TypeFor[Instance](), false},
	{"notifiers", KindNotifier, reflect.TypeFor[Instance](), false},
	{"catalog", KindCatalog, reflect.TypeFor[Instance](), true},
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
// distribution and may only carry `plugins.config` blocks of its own plugins.
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
func LoadDefaults(schema Schema, docs []jsontext.Value) (*Defaults, error) {
	if schema == nil {
		return nil, errors.New("defaults: the schema is nil")
	}
	var errs []error
	union := map[string]jsontext.Value{}
	for i, doc := range docs {
		obj, ok := ObjectOf(doc)
		if !ok {
			errs = append(errs, fmt.Errorf("defaults document %d: must be a JSON object", i+1))
			continue
		}
		if i > 0 {
			errs = append(errs, distributionDocErrors(i+1, obj)...)
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
	if errs := checkDefaults(schema, doc); len(errs) > 0 {
		return nil, fmt.Errorf("defaults: %w", errors.Join(errs...))
	}
	return &Defaults{doc: doc, documents: len(docs)}, nil
}

// distributionDocErrors checks the shape of one distribution-supplied document:
// it may only set per-type settings under plugins.config, never the selection
// or the service's own sections.
func distributionDocErrors(n int, obj map[string]jsontext.Value) []error {
	var errs []error
	for _, name := range sortedKeys(obj) {
		if name != "plugins" {
			errs = append(errs, fmt.Errorf("defaults document %d: /%s — a distribution's own document may only carry `plugins.config` blocks of its own plugins", n, name))
			continue
		}
		plugins, ok := ObjectOf(obj[name])
		if !ok {
			errs = append(errs, fmt.Errorf("defaults document %d: /plugins must be an object", n))
			continue
		}
		for _, key := range sortedKeys(plugins) {
			if key != "config" {
				errs = append(errs, fmt.Errorf("defaults document %d: /plugins/%s — a distribution's own document may only carry `plugins.config` blocks of its own plugins", n, key))
			}
		}
	}
	return errs
}

// Get returns the block at one JSON pointer in the unioned document, or nil
// when the document has no value there. It is how `plugins describe` and
// `schema` read a default beside a descriptor.
func (d *Defaults) Get(pointer string) jsontext.Value {
	if d == nil {
		return nil
	}
	cur := d.doc
	for seg := range jsontext.Pointer(pointer).Tokens() {
		if seg == "" {
			continue
		}
		obj, ok := ObjectOf(cur)
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

// ConfigBlock returns the defaults of one plugin type's settings — the config
// of every instance of it, wherever the instance is declared. The block lives
// under /plugins/config, keyed by the plugin ID: one key, not two pointer
// segments.
func (d *Defaults) ConfigBlock(id string) jsontext.Value {
	blocks, ok := ObjectOf(d.Get("/plugins/config"))
	if !ok {
		return nil
	}
	return blocks[id]
}

// PluginBlock returns the block of one plugin type by kind and type; it is
// ConfigBlock for a factory ID.
func (d *Defaults) PluginBlock(kind, typ string) jsontext.Value {
	return d.ConfigBlock(ID(Kind(kind), typ))
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
func (d *Defaults) DefaultsConfig(schema Schema, kind, typ string, example jsontext.Value) (jsontext.Value, error) {
	block := d.PluginBlock(kind, typ)
	cfgType, ok := schema.ConfigType(kind, typ)
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
	exObj, exOK := ObjectOf(example)
	blkObj, blkOK := ObjectOf(block)
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
			items, ok := arrayOf(exObj[base])
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

// InstanceBlock returns the `instance.<kind>` envelope — the fields every
// instance of that kind carries.
func (d *Defaults) InstanceBlock(kind string) jsontext.Value {
	return d.Get("/instance/" + kind)
}

// Merge merges the defaults under one configuration document and returns the
// merged bytes with the JSON pointers whose value came from a default. It fills
// the configuration's own sections, the selection controls, every declared instance —
// its kind's envelope over the whole entry, the per-type settings of its own
// ID under the entry's config, then the entry's own config on top — and every
// rule source a discovery declares, which is where the nested source config
// lives that the strict decoder only ever sees as opaque bytes.
//
// The merge performs no I/O and builds no plugin — rule sources come from the
// compiled pure hook, so a disabled discovery is merged and checked exactly
// like an enabled one.
//
// A mergeable document always yields a candidate, even when a settings block
// does not match the compiled schema: the candidate is what a diagnostic
// command prints, and the problems are what refuses operational work. Input
// that cannot be merged at all — anything but a JSON object — comes back
// untouched for the strict decoder to report.
func (d *Defaults) Merge(schema Schema, raw jsontext.Value) (jsontext.Value, []string, []error) {
	if d == nil || len(d.doc) == 0 {
		return raw, nil, nil
	}
	user, ok := ObjectOf(raw)
	if !ok {
		return raw, nil, nil // not an object: the strict decoder reports the shape
	}
	def, _ := ObjectOf(d.doc)
	configDef := map[string]jsontext.Value{}
	for name, val := range def {
		if name == "plugins" || name == "instance" {
			continue
		}
		configDef[name] = val
	}
	merged, pointers := mergeValue(reflect.TypeFor[Config](), encode(configDef), encode(user), "")
	// mergeValue over the configuration struct always answers an object.
	mergedObj, _ := ObjectOf(merged)

	var problems []error
	blocks := map[string]jsontext.Value{}
	defBlocks := pluginsConfigFrom(def["plugins"])
	userBlocks := pluginsConfigFrom(mergedObj["plugins"])
	// A plugins root the deployment wrote in a shape this build cannot merge —
	// a null, a scalar, an unknown member — is left exactly as written: the
	// strict decoder reports it with its pointer, and the merge never turns a
	// broken file into a valid one.
	if _, present := mergedObj["plugins"]; !present || mergeablePlugins(mergedObj["plugins"]) {
		plugins, pluginPointers := mergePlugins(schema, def["plugins"], mergedObj["plugins"])
		if len(plugins) > 0 {
			mergedObj["plugins"] = encode(plugins)
			merged = encode(mergedObj)
		}
		pointers = append(pointers, pluginPointers...)
		blocks = pluginsConfig(plugins)
	}
	// Every settings block is judged against the compiled schema, declared
	// instance or not; a problem keeps the candidate and refuses operations.
	problems = append(problems, checkPluginBlocks(schema, blocks)...)
	merged, instancePointers := mergeInstances(schema, def, merged, userBlocks, defBlocks)
	merged, rulePointers, ruleProblems := d.mergeRuleSources(schema, merged, userBlocks, defBlocks)
	problems = append(problems, ruleProblems...)
	pointers = append(append(pointers, instancePointers...), rulePointers...)
	return merged, pointers, problems
}

// mergeablePlugins reports whether the deployment's plugins root is one the
// merge can fill: an object whose members are the three controls this build
// knows and whose settings member, when present, is an object. Anything else is
// the strict decoder's to report, not the merge's to rewrite.
func mergeablePlugins(raw jsontext.Value) bool {
	if IsNull(raw) {
		return false
	}
	obj, ok := ObjectOf(raw)
	if !ok {
		return false
	}
	for name := range obj {
		switch name {
		case "bundles", "disabled", "config":
		default:
			return false
		}
	}
	if cfg, present := obj["config"]; present {
		if IsNull(cfg) {
			return false
		}
		if _, ok := ObjectOf(cfg); !ok {
			return false
		}
	}
	return true
}

// pluginsConfigFrom reads the per-type settings of one raw plugins root; an
// absent or non-object root has none.
func pluginsConfigFrom(plugins jsontext.Value) map[string]jsontext.Value {
	obj, _ := ObjectOf(plugins)
	return pluginsConfig(obj)
}

// checkPluginBlocks judges every per-type settings block against the compiled
// schema, whether an instance uses it or not: an unknown factory or provider
// and a misspelled member are errors before any command runs. A block is
// partial by design, so a missing required field is not judged here — assembly
// checks that after the three layers merge.
func checkPluginBlocks(schema Schema, blocks map[string]jsontext.Value) []error {
	var errs []error
	for _, id := range sortedKeys(blocks) {
		at := ptr("/plugins/config", id)
		if !ValidID(id) {
			errs = append(errs, fmt.Errorf("%s: not a plugin ID (a factory kind/type of a known kind, or a stable name)", at))
			continue
		}
		t, known := configTypeOf(schema, id)
		if !known {
			if _, _, factory := ParseID(id); factory {
				errs = append(errs, fmt.Errorf("%s: unknown plugin (known: %s)", at, knownPlugins(schema)))
			} else if _, providers := schema.(ProviderSchema); providers {
				errs = append(errs, fmt.Errorf("%s: unknown plugin %q; this build compiles no such provider", at, id))
			}
			continue
		}
		errs = append(errs, shape(t, blocks[id], at)...)
	}
	return errs
}

// pluginsConfig reads the per-type settings map out of a merged plugins
// object; a document without the section has none.
func pluginsConfig(plugins map[string]jsontext.Value) map[string]jsontext.Value {
	blocks, _ := ObjectOf(plugins["config"])
	return blocks
}

// mergePlugins merges the defaults document's plugins root under the
// deployment's: the deployment's lists replace the document's whole, and the
// per-type settings of both layers merge through the compiled schema, so a
// deployment states only what it changes and every value it leaves out stays
// the default. Two lists the deployment does not state come from the document
// ({bundles: [], disabled: []} in the service's own); a settings block the
// document does not carry is the deployment's alone.
func mergePlugins(schema Schema, def, user jsontext.Value) (map[string]jsontext.Value, []string) {
	defObj, _ := ObjectOf(def)
	userObj, _ := ObjectOf(user)
	out := map[string]jsontext.Value{}
	var pointers []string
	for _, key := range []string{"bundles", "disabled"} {
		if val, present := userObj[key]; present {
			out[key] = val
			continue
		}
		if val, present := defObj[key]; present {
			out[key] = val
			pointers = append(pointers, leafPointers(val, "/plugins/"+key)...)
		}
	}
	defBlocks, _ := ObjectOf(defObj["config"])
	userBlocks, _ := ObjectOf(userObj["config"])
	ids := map[string]bool{}
	for id := range defBlocks {
		ids[id] = true
	}
	for id := range userBlocks {
		ids[id] = true
	}
	if len(ids) > 0 {
		blocks := map[string]jsontext.Value{}
		for _, id := range sortedKeys(ids) {
			at := ptr("/plugins/config", id)
			defBlock, hasDef := defBlocks[id]
			userBlock, hasUser := userBlocks[id]
			switch {
			case hasDef && hasUser:
				if cfgType, ok := configTypeOf(schema, id); ok {
					merged, ptrs := mergeValue(cfgType, defBlock, userBlock, at)
					blocks[id] = merged
					pointers = append(pointers, ptrs...)
				} else {
					blocks[id] = userBlock
				}
			case hasUser:
				blocks[id] = userBlock
			default:
				blocks[id] = defBlock
				pointers = append(pointers, leafPointers(defBlock, at)...)
			}
		}
		out["config"] = encode(blocks)
	}
	return out, pointers
}

// configTypeOf resolves the compiled config type behind one plugin ID: a
// factory ID through the factory schema, a stable provider ID through the
// provider schema. An ID neither layer knows has no type here, and its settings
// merge by replacement.
func configTypeOf(schema Schema, id string) (reflect.Type, bool) {
	if kind, typ, ok := ParseID(id); ok {
		return schema.ConfigType(string(kind), typ)
	}
	if providers, ok := schema.(ProviderSchema); ok {
		return providers.ProviderConfigType(id)
	}
	return nil, false
}

// mergeInstances merges every instance the configuration declares: the
// envelope of its kind over the whole entry, the per-type settings of its ID
// under the entry's config, and the entry's own config on top. An instance the
// file does not declare never appears — a default creates no instance.
func mergeInstances(schema Schema, def map[string]jsontext.Value, doc jsontext.Value, userBlocks, defBlocks map[string]jsontext.Value) (jsontext.Value, []string) {
	merged, ok := ObjectOf(doc)
	if !ok {
		return doc, nil
	}
	var pointers []string
	for _, sec := range instanceKinds {
		if sec.single {
			entry, ok := ObjectOf(merged[sec.json])
			if !ok {
				continue
			}
			out, ptrs := mergeInstance(def, schema, userBlocks, defBlocks, sec, entry, "/"+sec.json)
			merged[sec.json] = out
			pointers = append(pointers, ptrs...)
			continue
		}
		entries, ok := ObjectOf(merged[sec.json])
		if !ok {
			continue
		}
		out, ptrs := mergeEntries(schema, def, userBlocks, defBlocks, sec, entries, "/"+sec.json)
		merged[sec.json] = out
		pointers = append(pointers, ptrs...)
	}
	// Two places hold an instance that is not an entry of a top-level section:
	// the retention inside a storage — the policy belongs to the storage, so it
	// has no name of its own — and the sources of the stage section, which the
	// stage command builds and the run never does.
	if entries, ok := ObjectOf(merged["storages"]); ok {
		for _, name := range sortedKeys(entries) {
			entry, ok := ObjectOf(entries[name])
			if !ok {
				continue
			}
			ret, ok := ObjectOf(entry["retention"])
			if !ok {
				continue
			}
			sec, _ := sectionForKind(KindRetention)
			for _, kind := range sortedKeys(ret) {
				inst, ok := ObjectOf(ret[kind])
				if !ok {
					continue
				}
				out, ptrs := mergeInstance(def, schema, userBlocks, defBlocks, sec, inst, ptr(ptr(ptr("/storages", name), "retention"), kind))
				ret[kind] = out
				pointers = append(pointers, ptrs...)
			}
			entry["retention"] = encode(ret)
			entries[name] = encode(entry)
		}
		merged["storages"] = encode(entries)
	}
	if stage, ok := ObjectOf(merged["stage"]); ok {
		if sources, ok := ObjectOf(stage["sources"]); ok {
			sec, _ := sectionForKind(KindSource)
			out, ptrs := mergeEntries(schema, def, userBlocks, defBlocks, sec, sources, "/stage/sources")
			stage["sources"] = out
			merged["stage"] = encode(stage)
			pointers = append(pointers, ptrs...)
		}
	}
	return encode(merged), pointers
}

// mergeEntries merges every declared entry of one section, path being the
// section's own path: each entry takes the envelope of its kind and the
// per-type settings of its own ID, and an entry the file does not declare is
// never created.
func mergeEntries(schema Schema, def map[string]jsontext.Value, userBlocks, defBlocks map[string]jsontext.Value, sec instanceKind, entries map[string]jsontext.Value, path string) (jsontext.Value, []string) {
	var pointers []string
	for _, name := range sortedKeys(entries) {
		entry, ok := ObjectOf(entries[name])
		if !ok {
			continue
		}
		out, ptrs := mergeInstance(def, schema, userBlocks, defBlocks, sec, entry, ptr(path, name))
		entries[name] = out
		pointers = append(pointers, ptrs...)
	}
	return encode(entries), pointers
}

// mergeInstance merges one entry: first the envelope of its kind — the fields
// every instance of that kind carries — then the per-type settings
// of the entry's own ID under its config, and finally the entry's own config on
// top. The second step walks with the plugin's own Config type: the bytes an
// envelope holds are opaque, and only the type says what a field of them is.
//
// The three layers apply in order — the embedded defaults under the
// deployment's per-type settings under the entry's own config — and the
// defaulted pointers are the leaves the embedded layer alone filled, so a
// deployment override is never reported as a default even when its value
// equals the embedded one.
func mergeInstance(def map[string]jsontext.Value, schema Schema, userBlocks, defBlocks map[string]jsontext.Value, sec instanceKind, entry map[string]jsontext.Value, path string) (jsontext.Value, []string) {
	envelopes, _ := ObjectOf(def["instance"])
	envelope, _ := ObjectOf(envelopes[string(sec.kind)])
	out, pointers := mergeValue(sec.elem, encode(envelope), encode(entry), path)
	typ, ok := jsonString(entry["type"])
	if !ok {
		return out, pointers
	}
	id := ID(sec.kind, typ)
	userBlock := userBlocks[id]
	defBlock := defBlocks[id]
	cfgType, ok := schema.ConfigType(string(sec.kind), typ)
	if !ok {
		return out, pointers // the check refused an unregistered type at load
	}
	merged, _ := ObjectOf(out)
	at := path + "/config"
	cfg, present := merged["config"]
	// The deployment's own settings merge under the entry's config first: an
	// instance value outranks them, and both outrank the embedded defaults. An
	// entry without a config starts from an empty object, so the deployment
	// layer still applies.
	userCfg := cfg
	if !present {
		userCfg = encode(map[string]jsontext.Value{})
	}
	if len(userBlock) > 0 {
		userCfg, _ = mergeValue(cfgType, encode(userBlock), userCfg, at)
	}
	effective, ptrs := mergeValue(cfgType, encode(defBlock), userCfg, at)
	pointers = append(pointers, ptrs...)
	if present || len(userBlock) > 0 || len(defBlock) > 0 {
		merged["config"] = effective
	}
	return encode(merged), pointers
}

// mergeRuleSources merges each rule's source settings under the rule's own
// source config, in place in the document: a discovery declares where its rules
// live through the pure compiled hook, and the nested config is opaque bytes to
// the strict decoder, so this is the one place the configuration layer learns
// the path. A discovery whose type registers no hook is left alone, and so is a
// rule whose source type has no settings.
func (d *Defaults) mergeRuleSources(schema Schema, doc jsontext.Value, userBlocks, defBlocks map[string]jsontext.Value) (jsontext.Value, []string, []error) {
	hook, ok := schema.(RuleSchema)
	if !ok {
		return doc, nil, nil
	}
	merged, ok := ObjectOf(doc)
	if !ok {
		return doc, nil, nil
	}
	entries, ok := ObjectOf(merged["discoveries"])
	if !ok {
		return doc, nil, nil
	}
	out := doc
	var pointers []string
	var problems []error
	for _, name := range sortedKeys(entries) {
		entry, ok := ObjectOf(entries[name])
		if !ok {
			continue
		}
		typ, ok := jsonString(entry["type"])
		if !ok || typ == "" {
			continue
		}
		rules, err := hook.RuleSources(string(KindDiscovery), typ, entry["config"])
		if err != nil {
			continue // the offline check reports it with the whole error list
		}
		for _, rs := range rules {
			if rs.Type == "" {
				continue
			}
			id := ID(KindSource, rs.Type)
			userBlock := userBlocks[id]
			defBlock := defBlocks[id]
			if len(userBlock) == 0 && len(defBlock) == 0 {
				continue
			}
			cfgType, ok := schema.ConfigType(string(KindSource), rs.Type)
			if !ok {
				continue
			}
			// The deployment's settings under the rule's own config, then the
			// embedded defaults under that; the pointers are what the embedded
			// layer alone filled. A rule without a config starts from an empty
			// object, so the deployment layer still applies.
			base := rs.Config
			if len(base) == 0 {
				base = encode(map[string]jsontext.Value{})
			}
			userCfg, _ := mergeValue(cfgType, encode(userBlock), base, "")
			ruleConfig, ptrs := mergeValue(cfgType, encode(defBlock), userCfg, "")
			if len(ruleConfig) == 0 || bytes.Equal(ruleConfig, rs.Config) {
				continue // the three layers changed nothing
			}
			// A rule's paths are pointers inside the discovery's own config,
			// which is the entry's config key.
			at := ptr(ptr("/discoveries", name), "config") + rs.ConfigPath
			candidate, err := setAtCreate(out, at, ruleConfig)
			if err != nil {
				// The hook named a path the document does not hold: the
				// candidate stays printable and the problem refuses operations.
				problems = append(problems, err)
				continue
			}
			out = candidate
			for i := range ptrs {
				ptrs[i] = at + ptrs[i]
			}
			pointers = append(pointers, ptrs...)
		}
	}
	return out, pointers, problems
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
	if t == valueType || IsNull(user) {
		return user, nil // opaque bytes, or no value to merge under
	}
	defObj, defIsObj := ObjectOf(def)
	userObj, userIsObj := ObjectOf(user)
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
			if OptionalField(f) {
				continue
			}
			out[key] = defObj[key]
			pointers = append(pointers, leafPointers(defObj[key], ptr(path, key))...)
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
				merged, ptrs := mergeValue(t.Elem(), defObj[key], userVal, ptr(path, key))
				out[key] = merged
				pointers = append(pointers, ptrs...)
				continue
			}
			out[key] = defObj[key]
			pointers = append(pointers, leafPointers(defObj[key], ptr(path, key))...)
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
	userItems, ok := arrayOf(user)
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
	obj, ok := ObjectOf(val)
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
		out = append(out, leafPointers(obj[name], ptr(path, name))...)
	}
	return out
}

// checkDefaults walks the unioned document against the schema and the
// configuration's own types. It is LoadDefaults' rules 1 to 5; the null and the
// two-documents checks run before the union, at the only place they can.
func checkDefaults(schema Schema, doc jsontext.Value) []error {
	obj, _ := ObjectOf(doc)
	var errs []error

	// The configuration's own sections, minus the two sections that are not
	// fields of the configuration: the instance envelopes and the plugin
	// controls.
	configDoc := map[string]jsontext.Value{}
	for name, val := range obj {
		if name == "plugins" || name == "instance" {
			continue
		}
		configDoc[name] = val
	}
	errs = append(errs, walkValue(reflect.TypeFor[Config](), encode(configDoc), "")...)

	// The plugin controls: bundles, disabled and the per-type settings map's
	// shape. The settings themselves are walked against the compiled schema
	// below, one block per ID.
	if plugins, ok := obj["plugins"]; ok {
		errs = append(errs, walkValue(reflect.TypeFor[Plugins](), plugins, "/plugins")...)
	}

	// The envelope of every kind: what every instance of
	// that kind carries, wherever the instance is declared.
	instances, _ := ObjectOf(obj["instance"])
	for _, name := range sortedKeys(instances) {
		sec, known := sectionForKind(Kind(name))
		if !known {
			errs = append(errs, fmt.Errorf("/instance/%s: the configuration has no such section (kinds: %s)", name, strings.Join(kindNames(), ", ")))
			continue
		}
		if block, ok := ObjectOf(instances[name]); ok {
			if _, has := block["config"]; has {
				errs = append(errs, fmt.Errorf("/instance/%s/config: a plugin config default belongs under plugins.config/<ID>; an envelope carries the fields every instance of that kind carries", name))
				continue
			}
		}
		at := "/" + sec.json
		if !sec.single {
			at += "/*"
		}
		errs = append(errs, walkValue(sec.elem, instances[name], at)...)
	}

	// The per-type settings: the config of every instance of that plugin,
	// wherever it is declared. A factory ID is checked against its own config
	// type even when the selection leaves the plugin off — compiled knowledge is
	// not active availability — and a provider's settings against the type it
	// registered.
	plugins, _ := ObjectOf(obj["plugins"])
	blocks, _ := ObjectOf(plugins["config"])
	for _, id := range sortedKeys(blocks) {
		if !ValidID(id) {
			errs = append(errs, fmt.Errorf("/plugins/config/%s: not a plugin ID (a factory kind/type of a known kind, or a stable name)", id))
			continue
		}
		at := ptr("/plugins/config", id)
		kind, typ, factory := ParseID(id)
		if factory {
			t, ok := schema.ConfigType(string(kind), typ)
			if !ok {
				errs = append(errs, fmt.Errorf("%s: unknown plugin (known: %s)", at, knownPlugins(schema)))
				continue
			}
			errs = append(errs, walkValue(t, blocks[id], at)...)
			continue
		}
		if providers, ok := schema.(ProviderSchema); ok {
			if t, known := providers.ProviderConfigType(id); known {
				errs = append(errs, walkValue(t, blocks[id], at)...)
			}
		}
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
	obj, isObj := ObjectOf(val)
	switch {
	case isObj && t.Kind() == reflect.Struct:
		return walkStruct(t, obj, path)
	case isObj && t.Kind() == reflect.Map:
		var errs []error
		for _, name := range sortedKeys(obj) {
			errs = append(errs, walkValue(t.Elem(), obj[name], ptr(path, name))...)
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
		required := RequiredField(f)
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
		case required, OptionalField(f), derefType(f.Type) == valueType:
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
	if err := Decode(val, reflect.New(t).Interface()); err != nil {
		return []error{fmt.Errorf("%s: %w", path, err)}
	}
	return nil
}

// unionDocs merges one document into the union of the earlier ones. Two
// documents must not set the same key: two objects merge key by key, so a
// document adding a key to a block another one declares conflicts only where
// both set it, and everything else is a conflict at its own pointer.
func unionDocs(acc map[string]jsontext.Value, path string, doc jsontext.Value) (map[string]jsontext.Value, []error) {
	obj, _ := ObjectOf(doc)
	var errs []error
	for _, name := range sortedKeys(obj) {
		val := obj[name]
		prev, present := acc[name]
		if !present {
			acc[name] = val
			continue
		}
		prevObj, prevIsObj := ObjectOf(prev)
		valObj, valIsObj := ObjectOf(val)
		if prevIsObj && valIsObj {
			merged, inner := unionDocs(prevObj, ptr(path, name), encode(valObj))
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
	if IsNull(raw) {
		return []error{fmt.Errorf("%s is null; a defaults document carries values, and switching something off is written as an off-value", path)}
	}
	if obj, ok := ObjectOf(raw); ok {
		var errs []error
		for _, name := range sortedKeys(obj) {
			errs = append(errs, docNulls(obj[name], ptr(path, name))...)
		}
		return errs
	}
	if arr, ok := arrayOf(raw); ok {
		var errs []error
		for i, item := range arr {
			errs = append(errs, docNulls(item, fmt.Sprintf("%s/%d", path, i))...)
		}
		return errs
	}
	return nil
}

// setAtCreate places one merged value at a JSON pointer: a missing object member
// on the path is created as an empty object, while an array index must exist —
// a rule the document does not hold is a problem, a config it omits is not.
func setAtCreate(doc jsontext.Value, pointer string, val jsontext.Value) (jsontext.Value, error) {
	return setTokensCreate(doc, slices.Collect(jsontext.Pointer(pointer).Tokens()), val, pointer)
}

func setTokensCreate(doc jsontext.Value, tokens []string, val jsontext.Value, pointer string) (jsontext.Value, error) {
	if len(tokens) == 0 {
		return val, nil
	}
	head, rest := tokens[0], tokens[1:]
	if arr, ok := arrayOf(doc); ok {
		i, err := strconv.Atoi(head)
		if err != nil || i < 0 || i >= len(arr) {
			return nil, fmt.Errorf("%s: no such element", pointer)
		}
		child, err := setTokensCreate(arr[i], rest, val, pointer)
		if err != nil {
			return nil, err
		}
		arr[i] = child
		return encode(arr), nil
	}
	obj, ok := ObjectOf(doc)
	if !ok {
		return nil, fmt.Errorf("%s: no such member", pointer)
	}
	child, present := obj[head]
	if !present {
		if len(rest) == 0 {
			obj[head] = val
			return encode(obj), nil
		}
		child = encode(map[string]jsontext.Value{})
	}
	child, err := setTokensCreate(child, rest, val, pointer)
	if err != nil {
		return nil, err
	}
	obj[head] = child
	return encode(obj), nil
}

// ptr appends one reference token to a JSON pointer, escaped the one way RFC
// 6901 spells it: a factory ID such as source/postgres is a single member name.
func ptr(path, token string) string {
	return string(jsontext.Pointer(path).AppendToken(token))
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
func sectionForKind(kind Kind) (instanceKind, bool) {
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
		names = append(names, string(sec.kind))
	}
	return names
}

func knownPlugins(schema Schema) string {
	var names []string
	for _, kind := range kindOrder {
		for _, typ := range schema.Types(string(kind)) {
			names = append(names, ID(kind, typ))
		}
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

// encode re-encodes a map or a slice of raw JSON values, deterministically. The
// values come from a document that already parsed; a failure here would be a
// bug in this package, not input.
func encode(v any) jsontext.Value {
	b, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		panic(fmt.Sprintf("configuration: re-encoding a defaults document: %v", err))
	}
	return b
}

func jsonString(raw jsontext.Value) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}
