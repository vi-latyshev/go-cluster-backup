// Package instances is the registration mechanics of the framework. It knows no kind
// and holds no defaults: what a config file omits is filled from the
// distribution's defaults document by the assembly, before any byte of an
// instance config reaches a plugin — a plugin still receives complete bytes and
// still treats every field as required. Packaged here is what the defaults
// check reads: the config type of every registered type, and the Field.Required
// of every descriptor.
//
// Six rules shape every kind interface (and this package obeys them for its own API):
//  1. every method takes ctx first and returns error last;
//  2. arguments and results are flat data or io.Reader/io.Writer — no callbacks,
//     channels, shared pointers or host types;
//  3. a config crosses the boundary as JSON bytes; the plugin decodes it itself;
//  4. an error class is a serialisable marker (errors.go), never a private Go type;
//  5. a capability is discoverable by call (errors.Is(err, errors.ErrUnsupported)),
//     not only by type assertion;
//  6. a Descriptor is plain data.
package instances

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
)

// Kind is a typed seam token; T is the interface the consumer of the seam calls.
type Kind[T any] struct{ name string }

func NewKind[T any](name string) Kind[T] { return Kind[T]{name: name} }

// Name returns the kind's name as it appears in error messages and descriptors.
func (k Kind[T]) Name() string { return k.name }

// ID is the stable identity of one factory contribution: "<kind>/<type>". A
// factory never declares an ID of its own — its kind and its type are the whole
// of it, and this function is the only spelling of that derivation, so a
// descriptor, a selection line and a loader registration cannot disagree. A
// plugin that contributes no factory declares its one stable ID itself.
func ID(kind, typ string) string { return kind + "/" + typ }

// ID returns the stable identity of this kind's contribution under typ.
func (k Kind[T]) ID(typ string) string { return ID(k.name, typ) }

type Stability string

const (
	Experimental Stability = "experimental"
	Beta         Stability = "beta"
	Stable       Stability = "stable"
)

// Deps is a struct (not a parameter list) so a future field never breaks a plugin.
// Log already carries the kind, type and instance attributes of this instance.
type Deps struct {
	Instance string
	Log      *slog.Logger
}

// FactorySpec describes one plugin type. There is no Default here: what the config file
// omits is filled from the distribution's defaults document, one field at a
// time, by the assembly. Example is a complete instance config — documentation and
// conformance input for `plugins describe`, never applied to anything.
type FactorySpec[C any, T any] struct {
	Type      string
	Summary   string
	Stability Stability
	// Binaries lists the external programs this type invokes, verified by
	// `doctor` at image build time and before a run starts. It is not a plugin
	// dependency: a loader Requires edge names another plugin, never a binary.
	Binaries []string
	// Rules is the pure compiled hook this type registers beside its factory: the
	// nested rule sources one raw config value declares. It constructs nothing and
	// performs no I/O, so the defaults merge and offline validation read a
	// disabled plugin exactly like an active one. Nil when the type nests no rules.
	Rules   configuration.RuleExtractor
	Example []byte
	New     func(ctx context.Context, cfg C, deps Deps) (T, error)
}

// Field describes one config member. Required is the classification the
// defaults check enforces: a required field is stated by the deployment and a
// defaults document may never carry it, while every other field is present in
// a defaults document. The default value itself is not here — it lives in the
// document, which `plugins describe` and `schema` read beside the descriptor.
type Field struct {
	Name     string
	Type     string
	Doc      string
	Secret   bool
	Required bool
	Fields   []Field
}

type Descriptor struct {
	ID        string
	Kind      string
	Type      string
	Summary   string
	Stability Stability
	Binaries  []string
	Fields    []Field
	Example   []byte
}

// Optional behaviour of an instance, discovered by type assertion.
type (
	// Checker runs at preflight: credentials, network, an external binary's version.
	// A failed Check is scoped to the instance: the run fails that instance's
	// own work and every other instance proceeds — one broken dependency must not
	// cost a night's backup.
	Checker interface {
		Check(ctx context.Context) error
	}
	// A config may also implement io.Closer; Close is idempotent and safe after a
	// failed New. The instance may implement io.Closer too.
)

// CheckBinaries is an optional capability of an instance whose external
// programs are a configured suite, not one PATH lookup — the PostgreSQL sources
// resolve an exact-major client directory. It proves, locally and before any
// run, that the suite this instance's config names is complete and internally
// consistent (every binary present, one major), and answers
// errors.ErrUnsupported when the instance carries no such suite. `doctor` and
// planning call it when an instance implements it; schema shape and defaults
// checks never do, because they run without the deployment's files. A failure
// is scoped to the instance like Checker's.
type CheckBinaries interface {
	CheckBinaries(ctx context.Context) error
}

type entry struct {
	desc  Descriptor
	cfg   reflect.Type // C, for CheckShape: the type the config decodes into
	check func(raw jsontext.Value) error
	rules configuration.RuleExtractor
	build func(ctx context.Context, raw jsontext.Value, d Deps) (any, error)
}

// Registry is the compiled factory and provider schema of one process: every
// registered plugin type, whatever the deployment's selection.
type Registry struct {
	kinds     map[string]map[string]entry
	providers map[string]reflect.Type // stable plugin ID -> the config type its settings decode into
}

func NewRegistry() *Registry {
	return &Registry{kinds: map[string]map[string]entry{}, providers: map[string]reflect.Type{}}
}

// typeNameRe is a lazily-built singleton: a package initializer does no work.
var typeNameRe = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
})

// Register adds one plugin type to the registry. A duplicate type, a malformed
// type name or an Example that does not decode is an error at startup, not at run time.
func Register[C, T any](r *Registry, k Kind[T], s FactorySpec[C, T]) error {
	_, err := register(r, k, s)
	return err
}

// register is Register with the descriptor a schema contribution needs: the
// registration mechanics exist once, and AddFactory contributes exactly the
// descriptor the entry holds.
func register[C, T any](r *Registry, k Kind[T], s FactorySpec[C, T]) (Descriptor, error) {
	if k.name == "" {
		return Descriptor{}, errors.New("plugin: kind name is empty")
	}
	if !typeNameRe().MatchString(s.Type) {
		return Descriptor{}, fmt.Errorf("plugin %s: type %q must match %s", k.name, s.Type, typeNameRe())
	}
	if s.New == nil {
		return Descriptor{}, fmt.Errorf("plugin %s: New is nil", k.ID(s.Type))
	}
	if _, dup := r.kinds[k.name][s.Type]; dup {
		return Descriptor{}, fmt.Errorf("plugin %s registered twice", k.ID(s.Type))
	}
	decode := func(raw jsontext.Value) (C, error) {
		var cfg C
		return cfg, configuration.Decode(raw, &cfg)
	}
	if s.Example != nil {
		if _, err := decode(s.Example); err != nil {
			return Descriptor{}, fmt.Errorf("plugin %s: Example does not decode: %w", k.ID(s.Type), err)
		}
	}
	desc := Descriptor{ID: k.ID(s.Type), Kind: k.name, Type: s.Type, Summary: s.Summary, Stability: s.Stability,
		Binaries: s.Binaries, Fields: fields(reflect.TypeFor[C]()), Example: s.Example}
	if r.kinds[k.name] == nil {
		r.kinds[k.name] = map[string]entry{}
	}
	r.kinds[k.name][s.Type] = entry{
		desc:  desc,
		cfg:   reflect.TypeFor[C](),
		check: func(raw jsontext.Value) error { _, err := decode(raw); return err },
		rules: s.Rules,
		build: func(ctx context.Context, raw jsontext.Value, d Deps) (any, error) {
			cfg, err := decode(raw)
			if err != nil {
				return nil, err
			}
			return s.New(ctx, cfg, d)
		},
	}
	return desc, nil
}

// Build decodes one instance config and constructs its plugin.
func Build[T any](ctx context.Context, r *Registry, k Kind[T], typ string, raw jsontext.Value, d Deps) (T, error) {
	var zero T
	v, err := r.BuildAny(ctx, k.name, typ, raw, d)
	if err != nil {
		return zero, err
	}
	t, ok := v.(T)
	if !ok { // a Kind carries only its name: another token of the same name reaches this entry
		return zero, fmt.Errorf("%s %q: constructed %T, want %T", k.name, typ, v, zero)
	}
	return t, nil
}

// BuildAny is Build without the seam token: it serves generic consumers such as
// the conformance kit, which walk every registered kind alike.
func (r *Registry) BuildAny(ctx context.Context, kind, typ string, raw jsontext.Value, d Deps) (any, error) {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return nil, r.unknown(kind, typ)
	}
	v, err := e.build(ctx, raw, d)
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", kind, d.Instance, err)
	}
	return v, nil
}

// Check decodes and validates a config without constructing the plugin: the whole
// of `validate`, offline.
func (r *Registry) Check(kind, typ string, raw jsontext.Value) error {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return r.unknown(kind, typ)
	}
	return e.check(raw)
}

// RuleSources reads the nested rule sources one plugin type declares in one raw
// config value, through the pure hook the type registered. It is the
// configuration layer's RuleSchema: the hook runs while the plugin stays
// unbuilt, and for a disabled plugin too. A registered type that declares no
// hook contributes no rules; an unregistered type is an error naming the known
// ones.
func (r *Registry) RuleSources(kind, typ string, raw jsontext.Value) ([]configuration.RuleSource, error) {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return nil, r.unknown(kind, typ)
	}
	if e.rules == nil {
		return nil, nil
	}
	return e.rules(raw)
}

// RegisterProvider records the config type one plugin's settings decode into
// for a plugin that declares a stable ID instead of a factory — an application
// provider such as reporting. The ID is the plugin's own loader.Spec.ID; this
// call publishes the type the defaults merge and the offline walk judge its
// `plugins.config` block against, so provider settings are checked exactly like
// a factory's. It registers no instance and creates nothing.
func RegisterProvider[C any](r *Registry, id string) error {
	if strings.Contains(id, "/") {
		return fmt.Errorf("plugin %q: a stable ID has no kind/type form; a factory registers with Register", id)
	}
	if !typeNameRe().MatchString(id) {
		return fmt.Errorf("plugin %q: a stable ID must match %s", id, typeNameRe())
	}
	if _, dup := r.providers[id]; dup {
		return fmt.Errorf("plugin %s registered twice", id)
	}
	r.providers[id] = reflect.TypeFor[C]()
	return nil
}

// ProviderConfigType returns the Go type one stable-ID plugin's settings decode
// into — the configuration layer's ProviderSchema. It answers false for an ID
// no provider declared.
func (r *Registry) ProviderConfigType(id string) (reflect.Type, bool) {
	t, ok := r.providers[id]
	return t, ok
}

func (r *Registry) unknown(kind, typ string) error {
	return fmt.Errorf("unknown %s type %q (known: %s)", kind, typ, strings.Join(r.types(kind), ", "))
}

// Descriptors is the single source of documentation, JSON Schema and `plugins describe`.
func (r *Registry) Descriptors() []Descriptor {
	var out []Descriptor
	for _, byType := range r.kinds {
		for _, e := range byType {
			out = append(out, e.desc)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Registry) types(kind string) []string {
	var names []string
	for name := range r.kinds[kind] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Types lists the type names registered under one kind, sorted — what a caller
// names in an error about an unregistered type.
func (r *Registry) Types(kind string) []string { return r.types(kind) }

// ConfigType returns the Go type one registered plugin's config decodes into —
// the type the defaults check walks and the merge reads. It answers false for
// an unregistered kind/type, which is how a defaults block naming one is caught.
func (r *Registry) ConfigType(kind, typ string) (reflect.Type, bool) {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return nil, false
	}
	return e.cfg, true
}

// Binaries lists the external programs one plugin type declares, the list
// `doctor` verifies at image build time and a run verifies before any work.
func (r *Registry) Binaries(kind, typ string) []string {
	if e, ok := r.kinds[kind][typ]; ok {
		return e.desc.Binaries
	}
	return nil
}

// fields describes a config type for Descriptors: names, types, docs, secrecy
// and whether the deployment must state the field. Type is the Go type exactly
// as reflect prints it — a deployment reads the same name in `plugins describe`
// and `schema`, and there is no second spelling to drift from it.
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
			Secret: configuration.IsSecretType(f.Type), Required: configuration.RequiredField(f), Fields: fields(f.Type)})
	}
	return out
}
