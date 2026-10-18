package engine

import (
	"context"
	"errors"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

// chain streams src through stages over io.Pipe links: src writes into the first
// pipe, stage i reads pipe i and writes pipe i+1, the last stage writes to dst.
// A failing stage closes its input with the same error, so an upstream writer
// never blocks on a dead pipe. Every stage and src is panic-safe.
func chain(ctx context.Context, dst io.Writer, src func(ctx context.Context, w io.Writer) error,
	stages []func(ctx context.Context, dst io.Writer, src io.Reader) error) error {
	if len(stages) == 0 {
		return safeCall(func() error { return src(ctx, dst) })
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
			err := safeCall(func() error { return stages[i](ctx, out, links[i].r) })
			if pw, ok := out.(*io.PipeWriter); ok {
				pw.CloseWithError(err) // nil error closes the pipe cleanly
			}
			if err != nil {
				links[i].r.CloseWithError(err)
			}
			errs <- err
		}(i, out)
	}
	srcErr := safeCall(func() error { return src(ctx, links[0].w) })
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

// encodeStage adapts a processor's Encode to the chain's stage shape.
func encodeStage(p processor.Processor) func(ctx context.Context, dst io.Writer, src io.Reader) error {
	return func(ctx context.Context, dst io.Writer, src io.Reader) error {
		return p.Encode(ctx, dst, src)
	}
}

// decodeStage adapts a processor's Decode.
func decodeStage(p processor.Processor) func(ctx context.Context, dst io.Writer, src io.Reader) error {
	return func(ctx context.Context, dst io.Writer, src io.Reader) error {
		return p.Decode(ctx, dst, src)
	}
}
