package pluginerrors

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that classifies a backend failure imports it directly.
var Spec = loader.Spec{
	ID:      "errors",
	Summary: "the serialisable error-class marker every backend call reports",
}
