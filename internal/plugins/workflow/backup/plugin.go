package backup

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this workflow. Floor is true because
// this plugin holds the mandatory safety skeleton; its contributions can never
// be disabled.
var Spec = loader.Spec{
	ID:      "workflow-backup",
	Summary: "backup: the Floor skeleton over guarded write handles and the protection ledger",
	Floor:   true,
}
