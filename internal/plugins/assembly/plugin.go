package assembly

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of the assembly: the composition root's
// provider. It contributes no factory — it builds every provider and projects
// their typed options for the workflows — so it has no Apply.
var Spec = loader.Spec{
	ID:      "assembly",
	Summary: "the composition root: builds every compiled provider and the workflows over it",
}
