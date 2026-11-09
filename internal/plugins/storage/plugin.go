package storage

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory of its own, so every storage backend imports the kind's factory here.
var Spec = loader.Spec{
	ID:      "storage",
	Summary: "the seam that writes one object per artifact and reads it back",
}
