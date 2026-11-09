package source

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every source backend imports it directly.
var Spec = loader.Spec{
	ID:      "source",
	Summary: "the seam that produces the bytes of one artifact: a database dump, a cache snapshot, a file tree",
}
