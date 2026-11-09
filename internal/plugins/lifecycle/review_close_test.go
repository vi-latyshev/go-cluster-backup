package lifecycle

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type reviewHeldCloser struct{ entered, release chan struct{} }

func (c reviewHeldCloser) Close() error { close(c.entered); <-c.release; return nil }

// TestR2_M4_DeadlineBoundsAnAlreadyStartedClose is the reviewer's probe: a close
// already blocked past the shared deadline must end the WAIT with
// DeadlineExceeded instead of blocking the whole cleanup.
func TestR2_M4_DeadlineBoundsAnAlreadyStartedClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scope := New()
		closer := reviewHeldCloser{make(chan struct{}), make(chan struct{})}
		scope.Own(closer)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- scope.Close(ctx) }()
		<-closer.entered
		time.Sleep(2 * time.Second)
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Close = %v", err)
			}
		default:
			t.Error("Close still blocked after cleanup deadline")
		}
		close(closer.release)
	})
}
