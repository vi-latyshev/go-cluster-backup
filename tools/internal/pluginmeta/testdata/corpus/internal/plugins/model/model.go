package model

import "example.test/app/internal/loader"

// Spec is the compiled plugin identity of the shared model library.
var Spec = loader.Spec{ID: "model", Summary: "shared domain types"}

// Value contributes nothing to the loader: a method named Apply is not the
// plugin's Apply function.
type Value struct{}

// Apply is a method, so the generator ignores it.
func (Value) Apply() error { return nil }
