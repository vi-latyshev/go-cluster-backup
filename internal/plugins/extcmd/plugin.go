package extcmd

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that drives an external command imports it directly.
var Spec = loader.Spec{
	ID:      "extcmd",
	Summary: "bounded external-command capture, environment replacement and mount-bound file probes",
}
