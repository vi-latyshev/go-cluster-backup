package configuration

import (
	"fmt"
	"reflect"
	"slices"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// RuleSource is one nested source a plugin's own config declares, exactly as
// the config file spells it: what an offline check and the defaults merge need
// to see a rule without a cluster. Paths are JSON pointers inside the discovery
// instance's own config, each starting with a "/".
type RuleSource struct {
	// Path is the rule's own path, e.g. "/pods/0"; problems with the rule as a
	// whole are reported under it.
	Path string
	// ConfigPath is the path of the nested source config, e.g.
	// "/pods/0/source/config": its shape errors are reported under it.
	ConfigPath string
	// Type is the source plugin type the rule builds; empty for the one rule
	// that renders no source and only accounts for unclaimed objects.
	Type string
	// Config is the nested source config exactly as written: its strings are
	// templates rendered per matched object, so the values are not evaluated
	// and the config is not decoded into the source's config type.
	Config jsontext.Value
	// Pipeline and the artifact envelope are the rule's routing.
	Pipeline    []string
	Importance  model.Importance
	Sensitivity model.Sensitivity
	// RateLimit is the rule's own raw-bytes-per-second envelope, applied before
	// the first processor; 0 is unlimited. It is extracted, never evaluated:
	// the check only refuses a negative value.
	RateLimit model.ByteSize
}

// RuleExtractor is the pure compiled hook a plugin type registers beside its
// factory: it reads the nested rule sources out of one raw config value. It
// constructs nothing, performs no I/O, renders no template and knows nothing
// about the selection — merge and diagnostics call it while the plugin itself
// stays unbuilt, and they call it for a disabled plugin too.
type RuleExtractor func(raw jsontext.Value) ([]RuleSource, error)

// RuleSchema is the compiled hook set the configuration layer reads rule
// sources from. It is discovered on the schema the caller already holds; a
// distribution whose plugin types register no hook simply has no rule sources
// the configuration layer can see.
type RuleSchema interface {
	RuleSources(kind, typ string, raw jsontext.Value) ([]RuleSource, error)
}

// Schema is the slice of the compiled plugin registry the configuration layer
// reads: the config type behind a factory ID, the type names of a kind, and the
// shape check of one nested config. The registry keeps a disabled plugin's
// entries, so merge and shape never depend on the selection.
type Schema interface {
	ConfigType(kind, typ string) (reflect.Type, bool)
	Types(kind string) []string
	CheckShape(kind, typ string, raw jsontext.Value, path string) error
}

// ProviderSchema is the compiled schema of the plugins that declare a stable
// name instead of a factory — the application providers. A Schema without it
// carries no provider settings, so their blocks are not judged. ProviderConfigType
// answers the Go type one provider's settings decode into.
type ProviderSchema interface {
	ProviderConfigType(id string) (reflect.Type, bool)
}

// ExtractRules returns every declared discovery's nested rule sources, by
// discovery name, and every extraction failure with its path. A discovery whose
// type registers no hook contributes nothing; the caller reports the failures
// and checks the rules it did get, so one broken discovery never hides the rest.
func ExtractRules(schema Schema, discoveries map[string]Instance) (map[string][]RuleSource, []error) {
	hook, ok := schema.(RuleSchema)
	if !ok {
		return nil, nil
	}
	out := make(map[string][]RuleSource, len(discoveries))
	var errs []error
	for _, name := range sortedKeys(discoveries) {
		entry := discoveries[name]
		if entry.Type == "" {
			continue
		}
		rules, err := hook.RuleSources(string(KindDiscovery), entry.Type, entry.Config)
		if err != nil {
			errs = append(errs, fmt.Errorf("/discoveries/%s/config: reading the rules: %w", name, err))
			continue
		}
		out[name] = rules
	}
	return out, errs
}

// CheckRules validates what every declared discovery's rules nest, offline: the
// source type must be compiled and survive the selection, the nested config's
// shape must be the type's own, a null is refused, the pipeline must name
// declared processor instances, and the artifact envelope is an enum. Values
// are not evaluated and templates are not rendered; shapes, names and routing
// are. Every failure of every rule is returned, so one validate pass reports
// them all.
func CheckRules(schema Schema, active Active, cfg *Config) []error {
	byDiscovery, errs := ExtractRules(schema, cfg.Discoveries)
	for _, name := range sortedKeys(byDiscovery) {
		prefix := ptr("/discoveries", name)
		for _, rs := range byDiscovery[name] {
			errs = append(errs, checkRule(schema, active, cfg, prefix, rs)...)
		}
	}
	return errs
}

// checkRule is one rule's offline check against the compiled schema, the active
// selection and the declared processors.
func checkRule(schema Schema, active Active, cfg *Config, prefix string, rs RuleSource) []error {
	var errs []error
	rulePath := prefix + rs.Path
	if rs.Type != "" {
		known := schema.Types(string(KindSource))
		switch {
		case !slices.Contains(known, rs.Type):
			errs = append(errs, fmt.Errorf("%s: unknown source type %q (known: %v)", rulePath, rs.Type, known))
		case !active.Enabled(ID(KindSource, rs.Type)):
			errs = append(errs, fmt.Errorf("%s: source type %q is disabled by the plugin selection; this rule can never produce a source", rulePath, rs.Type))
		default:
			if err := schema.CheckShape(string(KindSource), rs.Type, rs.Config, prefix+rs.ConfigPath); err != nil {
				errs = append(errs, err)
			}
			errs = append(errs, CheckNulls(rs.Config, prefix+rs.ConfigPath)...)
		}
	}
	if rs.Importance != model.ImportanceRequired && rs.Importance != model.ImportanceBestEffort {
		errs = append(errs, fmt.Errorf("%s: importance must be \"required\" or \"best_effort\", got %q", rulePath, rs.Importance))
	}
	if rs.Sensitivity != model.SensitivityPlain && rs.Sensitivity != model.SensitivitySecret {
		errs = append(errs, fmt.Errorf("%s: sensitivity must be \"plain\" or \"secret\", got %q", rulePath, rs.Sensitivity))
	}
	if rs.RateLimit < 0 {
		errs = append(errs, fmt.Errorf("%s: rate_limit must not be negative: %d", rulePath, rs.RateLimit))
	}
	for i, ref := range rs.Pipeline {
		if ref == "" {
			errs = append(errs, fmt.Errorf("%s/pipeline/%d is empty; an empty pipeline is written as []", rulePath, i))
			continue
		}
		if _, declared := cfg.Processors[ref]; !declared {
			errs = append(errs, fmt.Errorf("%s/pipeline/%d: unknown processor %q (declared: %v)", rulePath, i, ref, sortedKeys(cfg.Processors)))
		}
	}
	return errs
}
