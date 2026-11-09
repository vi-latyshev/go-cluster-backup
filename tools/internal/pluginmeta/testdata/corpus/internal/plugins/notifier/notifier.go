package notifier

import "example.test/app/internal/plugins/instances"

// Notifier sends one event.
type Notifier interface{ Name() string }

// Kind is the notifier seam.
var Kind = instances.NewKind[Notifier]("notifier")
