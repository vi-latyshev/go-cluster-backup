package sqlitecopy

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every source that copies a live database imports it directly.
var Spec = loader.Spec{
	ID:      "sqlitecopy",
	Summary: "the sqlite3 shell argument vector that snapshots a live database through its online backup",
}
