// Package plugin is the registration mechanics of the framework. It knows no kind
// and holds no defaults: what a config file omits is filled from the
// distribution's defaults document by the engine, before any byte of an
// instance config reaches a plugin — a plugin still receives complete bytes and
// still treats every field as required. Packaged here is what the defaults
// check reads: RequiredField, the config type of every registered type, and the
// Field.Required of every descriptor.
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
package plugin

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
)

// Kind is a typed seam token; T is the interface the consumer of the seam calls.
type Kind[T any] struct{ name string }

func NewKind[T any](name string) Kind[T] { return Kind[T]{name: name} }

// Name returns the kind's name as it appears in error messages and descriptors.
func (k Kind[T]) Name() string { return k.name }

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

// Spec describes one plugin type. There is no Default here: what the config file
// omits is filled from the distribution's defaults document, one field at a
// time, by the engine. Example is a complete instance config — documentation and
// conformance input for `plugins describe`, never applied to anything.
type Spec[C any, T any] struct {
	Type      string
	Summary   string
	Stability Stability
	Requires  []string // external binaries, verified by `doctor`
	Example   []byte
	New       func(ctx context.Context, cfg C, deps Deps) (T, error)
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
	Kind      string
	Type      string
	Summary   string
	Stability Stability
	Requires  []string
	Fields    []Field
	Example   []byte
}

// Optional behaviour of a config or an instance, discovered by type assertion.
type (
	// Validator runs on a decoded config, without I/O.
	Validator interface{ Validate() error }
	// Checker runs at preflight: credentials, network, an external binary's version.
	// A failed Check is scoped to the instance: the engine fails that instance's
	// own work and every other instance proceeds — one broken dependency must not
	// cost a night's backup.
	Checker interface {
		Check(ctx context.Context) error
	}
	// A config may also implement io.Closer; Close is idempotent and safe after a
	// failed New. The instance may implement io.Closer too.
)

type entry struct {
	desc  Descriptor
	cfg   reflect.Type // C, for CheckShape: the type the config decodes into
	check func(raw jsontext.Value) error
	build func(ctx context.Context, raw jsontext.Value, d Deps) (any, error)
}

type Registry struct {
	kinds map[string]map[string]entry
}

func NewRegistry() *Registry { return &Registry{kinds: map[string]map[string]entry{}} }

var typeNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Register adds one plugin type to the registry. A duplicate type, a malformed
// type name or an Example that does not decode is an error at startup, not at run time.
func Register[C, T any](r *Registry, k Kind[T], s Spec[C, T]) error {
	if k.name == "" {
		return errors.New("plugin: kind name is empty")
	}
	if !typeNameRe.MatchString(s.Type) {
		return fmt.Errorf("plugin %s: type %q must match %s", k.name, s.Type, typeNameRe)
	}
	if s.New == nil {
		return fmt.Errorf("plugin %s/%s: New is nil", k.name, s.Type)
	}
	if _, dup := r.kinds[k.name][s.Type]; dup {
		return fmt.Errorf("plugin %s/%s registered twice", k.name, s.Type)
	}
	decode := func(raw jsontext.Value) (C, error) {
		var cfg C
		return cfg, Decode(raw, &cfg)
	}
	if s.Example != nil {
		if _, err := decode(s.Example); err != nil {
			return fmt.Errorf("plugin %s/%s: Example does not decode: %w", k.name, s.Type, err)
		}
	}
	if r.kinds[k.name] == nil {
		r.kinds[k.name] = map[string]entry{}
	}
	r.kinds[k.name][s.Type] = entry{
		desc: Descriptor{Kind: k.name, Type: s.Type, Summary: s.Summary, Stability: s.Stability,
			Requires: s.Requires, Fields: fields(reflect.TypeFor[C]()), Example: s.Example},
		cfg:   reflect.TypeFor[C](),
		check: func(raw jsontext.Value) error { _, err := decode(raw); return err },
		build: func(ctx context.Context, raw jsontext.Value, d Deps) (any, error) {
			cfg, err := decode(raw)
			if err != nil {
				return nil, err
			}
			return s.New(ctx, cfg, d)
		},
	}
	return nil
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
	sort.Slice(out, func(i, j int) bool { return out[i].Kind+"/"+out[i].Type < out[j].Kind+"/"+out[j].Type })
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

// Requires lists the external binaries one plugin type declares, the list
// `doctor` verifies at image build time and a run verifies before any work.
func (r *Registry) Requires(kind, typ string) []string {
	if e, ok := r.kinds[kind][typ]; ok {
		return e.desc.Requires
	}
	return nil
}
