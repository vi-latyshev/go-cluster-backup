package notifier

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every notifier backend imports it directly.
var Spec = loader.Spec{
	ID:      "notifier",
	Summary: "the seam for every consumer of run events: notification, heartbeat, metrics push",
}
