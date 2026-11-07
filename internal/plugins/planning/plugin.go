package planning

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so the run, preflight and restore paths import the planning steps
// directly.
var Spec = loader.Spec{
	ID:      "planning",
	Summary: "discovery runs with retry reconciliation, unit expansion and the offline preflight",
}
