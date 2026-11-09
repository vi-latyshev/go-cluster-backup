package pluginio

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that streams bytes imports it directly.
var Spec = loader.Spec{
	ID:      "io",
	Summary: "the context-aware copy every plugin streams with",
}
