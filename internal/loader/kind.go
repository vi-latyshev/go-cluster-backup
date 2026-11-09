package loader

import (
	"errors"
	"fmt"
)

// KindOptions tunes one kind. A kind is declared by the plugin that consumes
// the seam; the loader holds only typed contributions.
type KindOptions[T any] struct {
	// Unique allows at most one contribution in the view All returns.
	Unique bool
	// Schema makes All see every compiled contribution whatever the selection;
	// without it All sees active contributions only.
	Schema bool
	// Validate is the ordering and uniqueness check the owner also runs.
	Validate func([]T) error
}

// Kind is a typed seam token: T is the interface the consumer of the seam calls.
type Kind[T any] struct {
	name string
	opts KindOptions[T]
}

// NewKind declares a kind under its stable name, the identity error messages
// and diagnostics print.
func NewKind[T any](name string, opts KindOptions[T]) Kind[T] {
	return Kind[T]{name: name, opts: opts}
}

type kindEntry[T any] struct {
	owner string
	value T
}

type kindBox[T any] struct {
	name    string
	opts    KindOptions[T]
	entries []kindEntry[T]
}

// checked is the part of a kind or service box Health reads without knowing T.
type checked interface {
	problems(active func(string) bool) []string
}

func (b *kindBox[T]) values(active func(string) bool) []T {
	out := make([]T, 0, len(b.entries))
	for _, entry := range b.entries {
		if b.opts.Schema || active(entry.owner) {
			out = append(out, entry.value)
		}
	}
	return out
}

func (b *kindBox[T]) problems(active func(string) bool) []string {
	view := b.values(active)
	var out []string
	if b.opts.Unique && len(view) > 1 {
		out = append(out, fmt.Sprintf("kind %q has %d contributions, want at most one", b.name, len(view)))
	}
	if b.opts.Validate != nil {
		if err := b.opts.Validate(view); err != nil {
			out = append(out, fmt.Sprintf("kind %q: %v", b.name, err))
		}
	}
	return out
}

// Register adds one contribution to a kind. It is called from Add's apply,
// which is how a contribution knows its owning plugin and the selection can
// hide it.
func Register[T any](r *Registry, k Kind[T], v T) error {
	if r.current == "" {
		return errors.New("loader: a kind contribution is registered outside Add")
	}
	if k.name == "" {
		return errors.New("loader: kind name is empty")
	}
	box, ok := r.kinds[k.name].(*kindBox[T])
	if !ok {
		if _, exists := r.kinds[k.name]; exists {
			return fmt.Errorf("loader: kind %q was declared with a different value type", k.name)
		}
		box = &kindBox[T]{name: k.name, opts: k.opts}
		r.kinds[k.name] = box
	}
	box.entries = append(box.entries, kindEntry[T]{owner: r.current, value: v})
	return nil
}

// All returns the contributions of one kind: every compiled one for a Schema
// kind, only active ones otherwise. A kind without contributions returns nil.
func All[T any](r *Registry, k Kind[T]) []T {
	box, ok := r.kinds[k.name].(*kindBox[T])
	if !ok {
		return nil
	}
	return box.values(r.isActive)
}

// Service is a unique kind with an exactly-one accessor. Use it only to invert
// a dependency — a low plugin consumes what a higher plugin provides; otherwise
// import directly.
type Service[T any] struct {
	name string
}

// NewService declares a service under its stable name.
func NewService[T any](name string) Service[T] {
	return Service[T]{name: name}
}

type serviceEntry[T any] struct {
	owner string
	value T
}

type serviceBox[T any] struct {
	name    string
	entries []serviceEntry[T]
}

func (b *serviceBox[T]) use(active func(string) bool) (T, bool) {
	var (
		zero  T
		found T
		count int
	)
	for _, entry := range b.entries {
		if active(entry.owner) {
			found = entry.value
			count++
		}
	}
	if count != 1 {
		return zero, false
	}
	return found, true
}

func (b *serviceBox[T]) problems(active func(string) bool) []string {
	count := 0
	for _, entry := range b.entries {
		if active(entry.owner) {
			count++
		}
	}
	if count > 1 {
		return []string{fmt.Sprintf("service %q has %d active providers, want one", b.name, count)}
	}
	return nil
}

// Provide binds one service implementation to the plugin being added.
func Provide[T any](r *Registry, s Service[T], v T) error {
	if r.current == "" {
		return errors.New("loader: a service is provided outside Add")
	}
	if s.name == "" {
		return errors.New("loader: service name is empty")
	}
	box, ok := r.services[s.name].(*serviceBox[T])
	if !ok {
		if _, exists := r.services[s.name]; exists {
			return fmt.Errorf("loader: service %q was declared with a different value type", s.name)
		}
		box = &serviceBox[T]{name: s.name}
		r.services[s.name] = box
	}
	box.entries = append(box.entries, serviceEntry[T]{owner: r.current, value: v})
	return nil
}

// Use returns the one active binding of a service. It answers false when no
// provider is active, and also when more than one is: an ambiguous binding is
// refused, never resolved by registration order, and Health names it.
func Use[T any](r *Registry, s Service[T]) (T, bool) {
	var zero T
	box, ok := r.services[s.name].(*serviceBox[T])
	if !ok {
		return zero, false
	}
	return box.use(r.isActive)
}
