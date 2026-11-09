package treewalk_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/treewalk"
)

func page(items []string, next ...string) treewalk.Page[string, string] {
	return treewalk.Page[string, string]{Items: items, Next: next}
}

type outcome struct {
	items []string
	err   error
}

func walkAsync(ctx context.Context, in treewalk.Input[string, string]) <-chan outcome {
	done := make(chan outcome, 1)
	go func() {
		items, err := treewalk.Walk(ctx, in)
		done <- outcome{items: items, err: err}
	}()
	return done
}

// TestWalkMergesInTraversalOrderNotCompletionOrder: the later sibling and the
// nested siblings finish first, but the result keeps the order the cursors name,
// and every cursor is read exactly once.
func TestWalkMergesInTraversalOrderNotCompletionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bDone := make(chan struct{})
		a2Done := make(chan struct{})
		var calls atomic.Int32
		fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
			calls.Add(1)
			switch cursor {
			case "root":
				return page([]string{"root"}, "a", "b"), nil
			case "a":
				<-bDone // the earlier branch waits for the later one
				return page([]string{"a"}, "a1", "a2"), nil
			case "a1":
				<-a2Done // nested siblings complete in reverse too
				return page([]string{"a1"}), nil
			case "a2":
				close(a2Done)
				return page([]string{"a2"}), nil
			case "b":
				close(bDone)
				return page([]string{"b"}), nil
			}
			return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
		}
		got := <-walkAsync(context.Background(),
			treewalk.Input[string, string]{Cursor: "root", Parallelism: 4, Fetch: fetch})
		if got.err != nil {
			t.Fatalf("Walk: %v", got.err)
		}
		want := []string{"root", "a", "a1", "a2", "b"}
		if fmt.Sprint(got.items) != fmt.Sprint(want) {
			t.Fatalf("items = %q, want %q", got.items, want)
		}
		if n := calls.Load(); n != int32(len(want)) {
			t.Fatalf("%d Fetch calls for %d cursors: a cursor was read twice", n, len(want))
		}
	})
}

// TestWalkBoundsCallsByParallelism: at most P Fetch calls run at once, below 1
// means one, and the whole walk still returns every item in traversal order.
func TestWalkBoundsCallsByParallelism(t *testing.T) {
	for _, parallelism := range []int{-2, 0, 1, 3, 5} {
		t.Run(fmt.Sprintf("P=%d", parallelism), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var active, peak atomic.Int32
				gate := make(chan struct{})
				fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
					if cursor == "root" {
						return page([]string{"root"}, "a", "b", "c"), nil
					}
					n := active.Add(1)
					for {
						old := peak.Load()
						if n <= old || peak.CompareAndSwap(old, n) {
							break
						}
					}
					<-gate
					active.Add(-1)
					return page([]string{cursor}), nil
				}
				done := walkAsync(context.Background(),
					treewalk.Input[string, string]{Cursor: "root", Parallelism: parallelism, Fetch: fetch})
				synctest.Wait()
				wantPeak := int32(min(max(parallelism, 1), 3))
				if got := peak.Load(); got != wantPeak {
					t.Fatalf("peak concurrent Fetch calls = %d, want %d", got, wantPeak)
				}
				close(gate)
				got := <-done
				if got.err != nil {
					t.Fatalf("Walk: %v", got.err)
				}
				want := []string{"root", "a", "b", "c"}
				if fmt.Sprint(got.items) != fmt.Sprint(want) {
					t.Fatalf("items = %q, want %q", got.items, want)
				}
			})
		})
	}
}

// TestWalkWithOneWorkerRunsInTraversalOrderAndNests: P=1 runs calls one at a
// time in the order the result merges, and a Walk nested inside a Fetch with its
// own P=1 does not deadlock behind the outer walk.
func TestWalkWithOneWorkerRunsInTraversalOrderAndNests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []string
		innerFetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
			if cursor == "inner" {
				return page([]string{"inner"}), nil
			}
			return treewalk.Page[string, string]{}, fmt.Errorf("unexpected inner cursor %q", cursor)
		}
		fetch := func(ctx context.Context, cursor string) (treewalk.Page[string, string], error) {
			order = append(order, cursor)
			switch cursor {
			case "root":
				inner, err := treewalk.Walk(ctx, treewalk.Input[string, string]{
					Cursor: "inner", Parallelism: 1, Fetch: innerFetch})
				if err != nil {
					return treewalk.Page[string, string]{}, err
				}
				return treewalk.Page[string, string]{
					Items: append([]string{"root"}, inner...), Next: []string{"a", "b"}}, nil
			case "a", "b":
				return page([]string{cursor}), nil
			}
			return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
		}
		items, err := treewalk.Walk(context.Background(),
			treewalk.Input[string, string]{Cursor: "root", Parallelism: 1, Fetch: fetch})
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		if want := []string{"root", "inner", "a", "b"}; fmt.Sprint(items) != fmt.Sprint(want) {
			t.Fatalf("items = %q, want %q", items, want)
		}
		if want := []string{"root", "a", "b"}; fmt.Sprint(order) != fmt.Sprint(want) {
			t.Fatalf("call order = %q, want %q", order, want)
		}
	})
}

// TestWalkRecoversAPanicWhileASiblingIsHeld: a worker's panic becomes one
// Permanent error, the held sibling still runs to completion on the caller's
// context, and every worker is joined before Walk returns.
func TestWalkRecoversAPanicWhileASiblingIsHeld(t *testing.T) {
	held := make(chan struct{})
	release := make(chan struct{})
	var heldFinished atomic.Bool
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return page(nil, "boom", "held"), nil
		case "boom":
			<-held // the sibling is inside Fetch when this worker panics
			panic("cursor blew up")
		case "held":
			close(held)
			<-release
			heldFinished.Store(true)
			return page([]string{"held"}), nil
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	done := walkAsync(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
	<-held
	close(release)
	got := <-done
	if got.err == nil {
		t.Fatal("a panic in a worker returned no error")
	}
	if class, after := pluginerrors.ClassOf(got.err); class != pluginerrors.ClassPermanent || after != 0 {
		t.Fatalf("panic class = %q, hint %v, want permanent with no hint", class, after)
	}
	if got.items != nil {
		t.Fatalf("items = %q, want no partial result", got.items)
	}
	if !heldFinished.Load() {
		t.Fatal("Walk returned before the held sibling finished")
	}
}

// TestWalkJoinsWorkersOnCallerCancel: cancellation reaches every worker through
// the caller's context, all of them finish, and the walk reports the
// cancellation instead of a partial result.
func TestWalkJoinsWorkersOnCallerCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var finished atomic.Int32
		fetch := func(ctx context.Context, cursor string) (treewalk.Page[string, string], error) {
			if cursor == "root" {
				return page(nil, "a", "b"), nil
			}
			<-ctx.Done()
			finished.Add(1)
			return treewalk.Page[string, string]{}, ctx.Err()
		}
		done := walkAsync(ctx,
			treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
		synctest.Wait()
		cancel()
		got := <-done
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", got.err)
		}
		if got.items != nil {
			t.Fatalf("items = %q, want no partial result", got.items)
		}
		if n := finished.Load(); n != 2 {
			t.Fatalf("%d of 2 workers finished before Walk returned", n)
		}
	})
}

// TestWalkDoesNotStartWithACancelledCaller: a doomed walk makes no Fetch call.
func TestWalkDoesNotStartWithACancelledCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	items, err := treewalk.Walk(ctx, treewalk.Input[string, string]{
		Fetch: func(context.Context, string) (treewalk.Page[string, string], error) {
			called = true
			return page([]string{"unreachable"}), nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if called || items != nil {
		t.Fatalf("cancelled Walk called Fetch (called = %v) or returned items %q", called, items)
	}
}

// TestWalkPrefersANonRetryableCause: a permanent or not-found failure beats a
// sibling's 429 with a hint, a 500 and an unclassified error, whatever order
// they complete in, and exactly one cause is returned.
func TestWalkPrefersANonRetryableCause(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		class pluginerrors.Class
	}{
		{"permanent", pluginerrors.Permanent(errors.New("403 denied")), pluginerrors.ClassPermanent},
		{"not found", pluginerrors.NotFound(errors.New("404 absent")), pluginerrors.ClassNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rateErr := pluginerrors.RetryAfter(errors.New("429 slow down"), 7*time.Second)
			serverErr := pluginerrors.Transient(errors.New("500 try later"))
			plainErr := errors.New("unclassified")
			lower := make(chan struct{}, 3)
			fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
				switch cursor {
				case "root":
					return page(nil, "rate", "server", "plain", "cause"), nil
				case "rate":
					lower <- struct{}{}
					return treewalk.Page[string, string]{}, rateErr
				case "server":
					lower <- struct{}{}
					return treewalk.Page[string, string]{}, serverErr
				case "plain":
					lower <- struct{}{}
					return treewalk.Page[string, string]{}, plainErr
				case "cause":
					<-lower
					<-lower
					<-lower // the non-retryable failure finishes last
					return treewalk.Page[string, string]{}, tc.cause
				}
				return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
			}
			items, err := treewalk.Walk(context.Background(),
				treewalk.Input[string, string]{Cursor: "root", Parallelism: 4, Fetch: fetch})
			if !errors.Is(err, tc.cause) {
				t.Fatalf("err = %v, want the non-retryable cause %v", err, tc.cause)
			}
			if errors.Is(err, rateErr) || errors.Is(err, serverErr) || errors.Is(err, plainErr) {
				t.Fatalf("err = %v joins sibling failures", err)
			}
			if class, _ := pluginerrors.ClassOf(err); class != tc.class {
				t.Fatalf("class = %q, want %q", class, tc.class)
			}
			if items != nil {
				t.Fatalf("items = %q, want no partial result", items)
			}
		})
	}
}

// TestWalkBreaksTiesByTraversalOrder: of two non-retryable failures, the one at
// the earlier cursor is reported even though the later one finished first.
func TestWalkPrefersTheFirstNonRetryableCause(t *testing.T) {
	firstErr := pluginerrors.Permanent(errors.New("first permanent"))
	secondErr := pluginerrors.Permanent(errors.New("second permanent"))
	secondDone := make(chan struct{})
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return page(nil, "first", "second"), nil
		case "first":
			<-secondDone
			return treewalk.Page[string, string]{}, firstErr
		case "second":
			close(secondDone)
			return treewalk.Page[string, string]{}, secondErr
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	items, err := treewalk.Walk(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
	if !errors.Is(err, firstErr) || errors.Is(err, secondErr) {
		t.Fatalf("err = %v, want the traversal-order-first permanent %v", err, firstErr)
	}
	if items != nil {
		t.Fatalf("items = %q, want no partial result", items)
	}
}

// TestWalkPrefersTheLargestRetryHint: among transient failures the walk reports
// the largest hint, not the first cursor and not the first completion.
func TestWalkPrefersTheLargestRetryHint(t *testing.T) {
	smallErr := pluginerrors.RetryAfter(errors.New("429 wait 2s"), 2*time.Second)
	largeErr := pluginerrors.RetryAfter(errors.New("429 wait 9s"), 9*time.Second)
	middleErr := pluginerrors.RetryAfter(errors.New("429 wait 5s"), 5*time.Second)
	middleDone := make(chan struct{})
	largeDone := make(chan struct{})
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return page(nil, "small", "large", "middle"), nil
		case "small":
			<-largeDone // the first cursor in traversal order finishes last
			return treewalk.Page[string, string]{}, smallErr
		case "large":
			<-middleDone
			close(largeDone)
			return treewalk.Page[string, string]{}, largeErr
		case "middle":
			close(middleDone)
			return treewalk.Page[string, string]{}, middleErr
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	items, err := treewalk.Walk(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 3, Fetch: fetch})
	if !errors.Is(err, largeErr) {
		t.Fatalf("err = %v, want the largest-hint cause %v", err, largeErr)
	}
	if class, after := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient || after != 9*time.Second {
		t.Fatalf("class = %q, hint %v, want transient with 9s", class, after)
	}
	if items != nil {
		t.Fatalf("items = %q, want no partial result", items)
	}
}

// TestWalkBreaksRetryHintTiesByTraversalOrder: equal hints keep the earlier
// cursor's cause even though the later cursor finished first.
func TestWalkBreaksRetryHintTiesByTraversalOrder(t *testing.T) {
	firstErr := pluginerrors.RetryAfter(errors.New("first 429"), 3*time.Second)
	secondErr := pluginerrors.RetryAfter(errors.New("second 429"), 3*time.Second)
	secondDone := make(chan struct{})
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return page(nil, "first", "second"), nil
		case "first":
			<-secondDone
			return treewalk.Page[string, string]{}, firstErr
		case "second":
			close(secondDone)
			return treewalk.Page[string, string]{}, secondErr
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	items, err := treewalk.Walk(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
	if !errors.Is(err, firstErr) || errors.Is(err, secondErr) {
		t.Fatalf("err = %v, want the earliest equal-hint cause %v", err, firstErr)
	}
	if items != nil {
		t.Fatalf("items = %q, want no partial result", items)
	}
}

// TestWalkPrefersTheFirstUnclassifiedCause: with no classified failure, the
// earliest unclassified one in traversal order is reported.
func TestWalkPrefersTheFirstUnclassifiedCause(t *testing.T) {
	firstErr := errors.New("first plain failure")
	secondErr := errors.New("second plain failure")
	secondDone := make(chan struct{})
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return page(nil, "first", "second"), nil
		case "first":
			<-secondDone
			return treewalk.Page[string, string]{}, firstErr
		case "second":
			close(secondDone)
			return treewalk.Page[string, string]{}, secondErr
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	items, err := treewalk.Walk(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
	if !errors.Is(err, firstErr) || errors.Is(err, secondErr) {
		t.Fatalf("err = %v, want the earliest unclassified cause %v", err, firstErr)
	}
	if class, _ := pluginerrors.ClassOf(err); class != "" {
		t.Fatalf("class = %q, want unclassified", class)
	}
	if items != nil {
		t.Fatalf("items = %q, want no partial result", items)
	}
}

// TestWalkPrefersCallerCancellation: a done caller context outranks even a
// permanent sibling failure.
func TestWalkPrefersCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		permErr := pluginerrors.Permanent(errors.New("403 denied"))
		fetch := func(ctx context.Context, cursor string) (treewalk.Page[string, string], error) {
			switch cursor {
			case "root":
				return page(nil, "perm", "slow"), nil
			case "perm":
				return treewalk.Page[string, string]{}, permErr
			case "slow":
				<-ctx.Done()
				return treewalk.Page[string, string]{}, ctx.Err()
			}
			return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
		}
		done := walkAsync(ctx,
			treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
		synctest.Wait()
		cancel()
		got := <-done
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", got.err)
		}
		if errors.Is(got.err, permErr) {
			t.Fatal("a permanent sibling failure hid the caller cancellation")
		}
		if got.items != nil {
			t.Fatalf("items = %q, want no partial result", got.items)
		}
	})
}

// TestWalkDropsThePageOfAFailedCall: a Fetch that returns items and an error
// together contributes nothing, and its continuation cursors are never read.
func TestWalkDropsThePageOfAFailedCall(t *testing.T) {
	failure := errors.New("page and failure together")
	var followed atomic.Bool
	fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
		switch cursor {
		case "root":
			return treewalk.Page[string, string]{Items: []string{"ghost"}, Next: []string{"next"}}, failure
		case "next":
			followed.Store(true)
			return page([]string{"unreachable"}), nil
		}
		return treewalk.Page[string, string]{}, fmt.Errorf("unexpected cursor %q", cursor)
	}
	items, err := treewalk.Walk(context.Background(),
		treewalk.Input[string, string]{Cursor: "root", Parallelism: 2, Fetch: fetch})
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want %v", err, failure)
	}
	if items != nil {
		t.Fatalf("items = %q, want no partial result", items)
	}
	if followed.Load() {
		t.Fatal("a failed page's continuation was read")
	}
}

// TestWalkReturnsNothingForAnEmptyTree: an empty first page is a complete walk
// with no items and no error.
func TestWalkReturnsNothingForAnEmptyTree(t *testing.T) {
	items, err := treewalk.Walk(context.Background(), treewalk.Input[string, string]{
		Cursor: "root",
		Fetch: func(context.Context, string) (treewalk.Page[string, string], error) {
			return treewalk.Page[string, string]{}, nil
		},
	})
	if err != nil || items != nil {
		t.Fatalf("Walk = %q, %v, want no items and no error", items, err)
	}
}

// TestWalkUsesOneWorkerByDefault: the zero Parallelism is sequential, proven by
// a call that fails if a second Fetch overlaps it.
func TestWalkUsesOneWorkerByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var active atomic.Int32
		fetch := func(_ context.Context, cursor string) (treewalk.Page[string, string], error) {
			if active.Add(1) != 1 {
				return treewalk.Page[string, string]{}, errors.New("overlapping Fetch calls")
			}
			defer active.Add(-1)
			if cursor == "root" {
				return page(nil, "a", "b"), nil
			}
			return page([]string{cursor}), nil
		}
		got := <-walkAsync(context.Background(),
			treewalk.Input[string, string]{Cursor: "root", Fetch: fetch})
		if got.err != nil {
			t.Fatalf("Walk: %v", got.err)
		}
		if want := []string{"a", "b"}; fmt.Sprint(got.items) != fmt.Sprint(want) {
			t.Fatalf("items = %q, want %q", got.items, want)
		}
	})
}
