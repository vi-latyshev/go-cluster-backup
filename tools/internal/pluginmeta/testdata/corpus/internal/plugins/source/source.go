package source

import "example.test/app/internal/plugins/instances"

// Source dumps one artifact.
type Source interface{ Name() string }

// Kind is the source seam.
var Kind = instances.NewKind[Source]("source")
