package model

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every seam that passes a value imports it directly.
var Spec = loader.Spec{
	ID:      "model",
	Summary: "the flat JSON-tagged data every seam passes around: manifests, reports and sizes",
}
