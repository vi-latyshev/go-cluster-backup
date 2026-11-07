package restore

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this workflow. It contributes no
// factory: the Floor driver builds it from typed options.
var Spec = loader.Spec{
	ID:      "workflow-restore",
	Summary: "restore: run listing, fetch, apply and verification over guarded read ports",
}
