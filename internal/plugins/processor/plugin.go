package processor

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every processor backend imports it directly.
var Spec = loader.Spec{
	ID:      "processor",
	Summary: "the seam that wraps a stream: compression and encryption as one pipeline step",
}
