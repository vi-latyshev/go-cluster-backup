package layout

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that renders keys imports it directly.
var Spec = loader.Spec{
	ID:      "layout",
	Summary: "run ids, object keys and the run kind read back from a run id",
}
