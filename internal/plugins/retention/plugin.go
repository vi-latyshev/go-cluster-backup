package retention

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every retention policy imports it directly.
var Spec = loader.Spec{
	ID:      "retention",
	Summary: "the seam that decides which stored runs stay on one storage",
}
