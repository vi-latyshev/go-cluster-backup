package guard

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled identity of the storage guard: a library the storage
// consumers are compiled against, never a selectable feature. It contributes no
// factory, so the name is its own literal.
var Spec = loader.Spec{
	ID:      "storage-guard",
	Summary: "the storage mutation boundary: read-only ports and write handles issued under one run's authority",
}
