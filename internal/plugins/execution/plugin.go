package execution

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of the operation-policy library: it
// contributes nothing to the loader, so every workflow that imports it runs one
// retry loop, one plugin panic boundary and one storage clock per storage.
var Spec = loader.Spec{
	ID:      "execution",
	Summary: "operation policy: attempts, backoff, the plugin panic boundary and each storage's time budget",
}
