// Package treewalk walks a tree of opaque cursors with bounded parallelism.
//
// The package knows nothing about the backend behind a cursor. A Fetch call
// turns one cursor into one page: the items found there and the cursors that
// continue the walk after it. One Fetch call is exactly one backend page and
// one backend attempt — the library never retries, never splits a page and
// never merges two pages into one. A Fetch that fails returns a zero Page: the
// library drops whatever a failed call returned and fails the whole walk.
//
// A walk is repeatable from its input cursor alone. The library keeps no
// checkpoint: a failed walk returns no partial result, and the caller replays
// the whole traversal by calling Walk again with the same input cursor.
//
// Every worker calls Fetch with the caller's context. One worker failing does
// not cancel its siblings: the library keeps scheduling until every reachable
// cursor was read and joins every worker before returning. The single cause it
// then reports is chosen in this order, with traversal order breaking every
// tie:
//  1. the caller's context error, when the caller's context is done;
//  2. the first non-retryable Fetch error — Permanent or NotFound;
//  3. among Transient errors, the one with the largest RetryAfter hint;
//  4. otherwise the first unclassified error.
//
// A permanent failure means a replay cannot help, so a sibling's transient
// error never hides it; the largest hint is the safe lower bound on a wait. The
// chosen error is returned as the Fetch call produced it, with its class and
// retry hint intact. A panic inside Fetch is recovered in the worker that
// called it and becomes a Permanent error.
//
// Items are merged in traversal order — depth first, children in the order
// Next lists them — not in completion order. Parallelism is a plain scalar:
// Walk starts at most Parallelism workers of its own and shares no limiter with
// any other walk, so a nested Walk with Parallelism 1 cannot deadlock behind
// its caller. A value below 1 means 1.
//
// An adapter that supplies Fetch must:
//   - be safe for concurrent calls on one Fetch value;
//   - honour ctx in every call — a cancelled caller must not wait for a
//     backend request that ignores its context;
//   - make exactly one backend request per call and return the page boundary
//     the backend itself names, with Next holding precisely the cursors that
//     continue after that page;
//   - return a zero Page on every error, and never report items it cannot
//     stand behind;
//   - validate and confine a cursor itself before using it: the library treats
//     a cursor as opaque and cannot tell a forged one from a valid one;
//   - enumerate every object exactly once with a finite tree: the library has
//     no cycle detection and no page cap, because a cursor carries no identity
//     the library could compare.
package treewalk

import (
	"context"
	"fmt"
	"sync"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
)

// Page is one backend page: the items found at a cursor and the cursors that
// continue the walk after it. Next in traversal order; an empty Next ends the
// branch.
type Page[Cursor, Item any] struct {
	Items []Item
	Next  []Cursor
}

// Fetch reads one page at cursor. One call is one attempt: it must not retry
// internally, and it must return a zero Page together with any error.
type Fetch[Cursor, Item any] func(ctx context.Context, cursor Cursor) (Page[Cursor, Item], error)

// Input is one walk: where it starts, how many Fetch calls may run at once, and
// how a cursor is read.
type Input[Cursor, Item any] struct {
	Cursor      Cursor
	Parallelism int
	Fetch       Fetch[Cursor, Item]
}

// node is one cursor's place in the traversal tree: what its page held, the
// cursors it led to in Next order, and the error that ended this branch.
type node[Cursor, Item any] struct {
	cursor   Cursor
	items    []Item
	children []*node[Cursor, Item]
	err      error
}

// result is one worker's answer for one node.
type result[Cursor, Item any] struct {
	node *node[Cursor, Item]
	page Page[Cursor, Item]
	err  error
}

// Walk traverses the tree that in.Fetch describes, starting at in.Cursor, with
// at most in.Parallelism concurrent Fetch calls. It returns every item in
// traversal order, or no items at all and one cause when any Fetch failed.
func Walk[Cursor, Item any](ctx context.Context, in Input[Cursor, Item]) ([]Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workers := in.Parallelism
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan *node[Cursor, Item])
	answers := make(chan result[Cursor, Item], workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for job := range jobs {
				page, err := attempt(ctx, in.Fetch, job.cursor)
				answers <- result[Cursor, Item]{node: job, page: page, err: err}
			}
		}()
	}
	root := &node[Cursor, Item]{cursor: in.Cursor}
	stack := []*node[Cursor, Item]{root}
	inflight := 0
	for len(stack) > 0 || inflight > 0 {
		for inflight < workers && len(stack) > 0 {
			job := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			jobs <- job
			inflight++
		}
		done := <-answers
		inflight--
		if done.err != nil {
			done.node.err = done.err
			continue
		}
		done.node.items = done.page.Items
		for i := range done.page.Next {
			done.node.children = append(done.node.children, &node[Cursor, Item]{cursor: done.page.Next[i]})
		}
		// Push the children reversed so the stack pops them in Next order: with
		// one worker the calls then run in the same order the result merges.
		for i := len(done.node.children) - 1; i >= 0; i-- {
			stack = append(stack, done.node.children[i])
		}
	}
	close(jobs)
	group.Wait()
	return merge(ctx, root)
}

// attempt makes one Fetch call and turns a panic in it into a Permanent error
// the rest of the walk can carry. A failed call always yields a zero page.
func attempt[Cursor, Item any](ctx context.Context, fetch Fetch[Cursor, Item], cursor Cursor) (page Page[Cursor, Item], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			page = Page[Cursor, Item]{}
			err = pluginerrors.Permanent(fmt.Errorf("treewalk: fetch panicked: %v", recovered))
		}
	}()
	page, err = fetch(ctx, cursor)
	if err != nil {
		return Page[Cursor, Item]{}, err
	}
	return page, nil
}

// merge flattens the tree depth first, children in Next order, and chooses the
// single cause to report. A failure makes the whole result empty: a walk never
// returns a partial view.
func merge[Cursor, Item any](ctx context.Context, root *node[Cursor, Item]) ([]Item, error) {
	var items []Item
	var nonRetryable, transient, other error
	var transientHint time.Duration
	stack := []*node[Cursor, Item]{root}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.err != nil {
			class, hint := pluginerrors.ClassOf(current.err)
			switch class {
			case pluginerrors.ClassPermanent, pluginerrors.ClassNotFound:
				if nonRetryable == nil {
					nonRetryable = current.err
				}
			case pluginerrors.ClassTransient:
				if transient == nil || hint > transientHint {
					transient, transientHint = current.err, hint
				}
			default:
				if other == nil {
					other = current.err
				}
			}
		} else {
			items = append(items, current.items...)
		}
		for i := len(current.children) - 1; i >= 0; i-- {
			stack = append(stack, current.children[i])
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case nonRetryable != nil:
		return nil, nonRetryable
	case transient != nil:
		return nil, transient
	case other != nil:
		return nil, other
	}
	return items, nil
}
