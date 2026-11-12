package pgclient

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every PostgreSQL source that drives a client suite imports it
// directly.
var Spec = loader.Spec{
	ID:      "pgclient",
	Summary: "the PostgreSQL client suite: exact-major selection, shared connection arguments and one-attempt command execution",
}
