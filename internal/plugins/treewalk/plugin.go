package treewalk

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every storage backend that walks a remote tree imports it
// directly.
var Spec = loader.Spec{
	ID:      "treewalk",
	Summary: "bounded parallel traversal of an opaque cursor tree with one classified cause",
}
