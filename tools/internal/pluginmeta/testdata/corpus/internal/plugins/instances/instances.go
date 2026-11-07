package instances

import "example.test/app/internal/loader"

// Spec is the compiled plugin identity of the typed factory registry.
var Spec = loader.Spec{ID: "instances", Summary: "the typed factory registry"}

// Kind is a typed seam token.
type Kind[T any] struct{ name string }

// NewKind declares a kind under its stable name.
func NewKind[T any](name string) Kind[T] { return Kind[T]{name: name} }

// ID is the stable identity of one factory contribution.
func ID(kind, typ string) string { return kind + "/" + typ }

// Registry holds the compiled factories.
type Registry struct{}
