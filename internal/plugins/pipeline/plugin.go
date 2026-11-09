package pipeline

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of the artifact-pipeline library: it
// contributes no factory, so the backup and stage workflows import it directly.
var Spec = loader.Spec{
	ID:      "pipeline",
	Summary: "artifact production: the processor chain, sealing, the spool file and its progress",
}
