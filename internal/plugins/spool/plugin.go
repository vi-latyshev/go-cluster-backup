package spool

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so the pipeline and transfer plugins import it directly.
var Spec = loader.Spec{
	ID:      "spool",
	Summary: "the run's on-disk buffer: one file per artifact, re-opened for every upload attempt",
}
