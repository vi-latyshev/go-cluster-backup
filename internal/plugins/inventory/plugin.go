package inventory

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so the backup and restore paths import the survey directly.
var Spec = loader.Spec{
	ID:      "inventory",
	Summary: "the storage survey: complete listings, manifest reads and the run list restore starts from",
}
