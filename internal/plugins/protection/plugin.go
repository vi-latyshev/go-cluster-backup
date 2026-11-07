package protection

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled identity of the protection provider: the writer lease,
// the occupancy cap and the one deletion ledger. It contributes no factory, so
// the name is its own literal; the assembly projects its typed options.
var Spec = loader.Spec{
	ID:      "protection",
	Summary: "the writer lease, occupancy cap and deletion ledger: the only holder of guarded delete authority",
}
