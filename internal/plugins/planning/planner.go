package planning

import "github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"

// Planner binds the planning steps to one execution policy: the retry policy
// of the path that drives them — the run's, or the restore's on a config
// without a run section.
type Planner struct{ policy execution.Options }

// New binds the planning steps to the policy of the path they serve.
func New(policy execution.Options) Planner { return Planner{policy: policy} }
