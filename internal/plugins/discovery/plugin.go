package discovery

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every discovery backend imports it directly.
var Spec = loader.Spec{
	ID:      "discovery",
	Summary: "the seam that finds sources at run time: the pods and volume claims a discovery backend enumerates",
}
