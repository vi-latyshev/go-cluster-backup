// Package notifier is the seam for every consumer of engine events: notification,
// heartbeat, metrics push. The interface obeys the seam rules documented in
// package plugin.
package notifier

import (
	"context"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Notifier delivers one engine event. One attempt per call; a cancelled context
// is an error. Failing to notify never fails the backup unless the engine config
// says notifier failures are fatal.
type Notifier interface {
	Notify(ctx context.Context, ev model.Event) error
}

var Kind = plugin.NewKind[Notifier]("notifier")
