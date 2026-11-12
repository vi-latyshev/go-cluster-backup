// Package notifier is the seam for every consumer of run events: notification,
// heartbeat, metrics push. The interface obeys the seam rules documented in
// package instances.
package notifier

import (
	"context"
	"errors"
	"sync"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Notifier delivers one run event. One attempt per call; a cancelled context
// is an error; an event the notifier has nothing to send for answers ErrSkipped().
// Failing to notify never fails the backup unless the run config says notifier
// failures are fatal.
type Notifier interface {
	Notify(ctx context.Context, ev model.Event) error
}

// Bounded is the optional capability a Notifier implements when it can bound
// one call. The run's retry loop then starts a retry only while its wait plus
// this bound still ends before the caller's deadline — what keeps the finished
// notification inside its window instead of sleeping past it. A notifier
// without the capability is budgeted with a bound of zero, so the check still
// refuses a wait that cannot fit before the deadline; the Notifier interface is
// not widened.
type Bounded interface {
	AttemptTimeout() time.Duration
}

// ErrSkipped reports that a notifier deliberately delivers nothing for this
// event: it has no message for it, or is subscribed to other events only. A skip
// is neutral — the run counts it as neither a failure nor a delivery, so a
// silent notifier can neither fail a run nor stand in for one that delivered the
// event. It is classified permanent, so execution's retry loop stops at once.
//
// It builds its one value on first use, so importing the seam does no work; the
// value is the same on every call, which is what lets a caller match it with
// errors.Is.
var ErrSkipped = sync.OnceValue(func() error {
	return pluginerrors.Permanent(errors.New("the notifier delivers nothing for this event"))
})

var Kind = instances.NewKind[Notifier]("notifier")
