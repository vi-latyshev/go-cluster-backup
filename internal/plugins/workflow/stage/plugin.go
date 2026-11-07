package stage

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of the stage workflow: it contributes no
// factory of its own, so the role that drives it and the assembly that builds
// its options import it directly.
var Spec = loader.Spec{
	ID:      "workflow/stage",
	Summary: "the stage step: sealed artifacts for the run side, one file and one chain record per artifact",
}
