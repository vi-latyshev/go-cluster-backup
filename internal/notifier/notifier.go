// Package notifier is the seam for every consumer of engine events: notification,
// heartbeat, metrics push. The interface obeys the seam rules documented in
// package plugin.
package notifier

import (
	"context"
	"errors"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Notifier delivers one engine event. One attempt per call; a cancelled context
// is an error; an event the notifier has nothing to send for answers ErrSkipped.
// Failing to notify never fails the backup unless the engine config says notifier
// failures are fatal.
type Notifier interface {
	Notify(ctx context.Context, ev model.Event) error
}

// ErrSkipped reports that a notifier deliberately delivers nothing for this
// event: it has no message for it, or is subscribed to other events only. A skip
// is neutral — the engine counts it as neither a failure nor a delivery, so a
// silent notifier can neither fail a run nor stand in for one that delivered the
// event. It is classified permanent, so the engine's retry loop stops at once.
var ErrSkipped = plugin.Permanent(errors.New("the notifier delivers nothing for this event"))

var Kind = plugin.NewKind[Notifier]("notifier")
