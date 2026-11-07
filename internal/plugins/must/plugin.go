package must

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that states an impossible error imports it directly.
var Spec = loader.Spec{
	ID:      "must",
	Summary: "the panic for an error that cannot happen by construction",
}
