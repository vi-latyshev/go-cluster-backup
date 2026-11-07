package catalog

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every catalog backend imports it directly.
var Spec = loader.Spec{
	ID:      "catalog",
	Summary: "the seam for the cross-process writer lease one run holds over the destination storages",
}
