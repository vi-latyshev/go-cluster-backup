package pipeline

import (
	"context"
	"errors"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
)

// SourceFunc writes the artifact's raw bytes into w, one attempt per call.
type SourceFunc func(ctx context.Context, w io.Writer) error

// Stage is one streaming transform of an artifact: a processor's Encode while
// producing, its Decode while restoring.
type Stage func(ctx context.Context, dst io.Writer, src io.Reader) error

// Chain streams src through stages over io.Pipe links: src writes into the first
// pipe, stage i reads pipe i and writes pipe i+1, the last stage writes to dst.
// A failing stage closes its input with the same error, so an upstream writer
// never blocks on a dead pipe. Every stage and src is panic-safe.
func Chain(ctx context.Context, dst io.Writer, src SourceFunc, stages []Stage) error {
	if len(stages) == 0 {
		return execution.SafeCall(func() error { return src(ctx, dst) })
	}
	n := len(stages)
	type link struct {
		r *io.PipeReader
		w *io.PipeWriter
	}
	links := make([]link, n)
	for i := range links {
		links[i].r, links[i].w = io.Pipe()
	}
	errs := make(chan error, n)
	for i := range stages {
		out := io.Writer(dst)
		if i < n-1 {
			out = links[i+1].w
		}
		go func(i int, out io.Writer) {
			err := execution.SafeCall(func() error { return stages[i](ctx, out, links[i].r) })
			if pw, ok := out.(*io.PipeWriter); ok {
				pw.CloseWithError(err) // nil error closes the pipe cleanly
			}
			if err != nil {
				links[i].r.CloseWithError(err)
			}
			errs <- err
		}(i, out)
	}
	srcErr := execution.SafeCall(func() error { return src(ctx, links[0].w) })
	links[0].w.CloseWithError(srcErr)
	var all []error
	if srcErr != nil {
		all = append(all, srcErr)
	}
	for range stages {
		if err := <-errs; err != nil {
			all = append(all, err)
		}
	}
	return errors.Join(all...)
}
