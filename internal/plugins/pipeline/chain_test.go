package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

// chainStage labels its input, making the stage order visible in the output.
func chainStage(want string) Stage {
	return func(ctx context.Context, dst io.Writer, src io.Reader) error {
		body, err := io.ReadAll(src)
		if err != nil {
			return err
		}
		_, err = dst.Write([]byte(want + ":" + string(body) + "|"))
		return err
	}
}

func TestChainOrderAndPropagation(t *testing.T) {
	ctx := context.Background()
	var out strings.Builder
	src := func(_ context.Context, w io.Writer) error {
		_, err := w.Write([]byte("in"))
		return err
	}
	err := Chain(ctx, &out, src, []Stage{
		chainStage("one"),
		chainStage("two"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "two:one:in||" {
		t.Fatalf("chain output %q", out.String())
	}

	// A failing stage propagates upstream: the source's write must fail, not block.
	blocked := errors.New("stage failed")
	err = Chain(ctx, io.Discard, func(ctx context.Context, w io.Writer) error {
		_, werr := w.Write([]byte(strings.Repeat("x", 1<<20)))
		return werr
	}, []Stage{
		func(ctx context.Context, dst io.Writer, src io.Reader) error { return blocked },
	})
	if err == nil || !strings.Contains(err.Error(), "stage failed") {
		t.Fatalf("chain error %v", err)
	}

	// A panicking stage becomes a permanent error and does not hang the source.
	err = Chain(ctx, io.Discard, func(ctx context.Context, w io.Writer) error {
		_, werr := w.Write([]byte(strings.Repeat("x", 1<<20)))
		return werr
	}, []Stage{
		func(ctx context.Context, dst io.Writer, src io.Reader) error { panic("boom") },
	})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("panic not recovered: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("panics must be permanent, got %q", class)
	}
}

func TestChainWithoutStages(t *testing.T) {
	var out strings.Builder
	err := Chain(context.Background(), &out, func(_ context.Context, w io.Writer) error {
		_, werr := w.Write([]byte("direct"))
		return werr
	}, nil)
	if err != nil || out.String() != "direct" {
		t.Fatalf("direct chain: %q, %v", out.String(), err)
	}

	// A panicking source is the same permanent failure as a panicking stage.
	err = Chain(context.Background(), io.Discard, func(context.Context, io.Writer) error { panic("source boom") }, nil)
	if err == nil || !strings.Contains(err.Error(), "source boom") {
		t.Fatalf("source panic not recovered: %v", err)
	}
}

// decodeProcessor is the encode/decode adapter subject: every call is counted
// and answered with its configured error.
type decodeProcessor struct {
	encodes, decodes int
	encodeErr        error
	decodeErr        error
}

func (p *decodeProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, nil
}

func (p *decodeProcessor) Encode(context.Context, io.Writer, io.Reader) error {
	p.encodes++
	return p.encodeErr
}

func (p *decodeProcessor) Decode(context.Context, io.Writer, io.Reader) error {
	p.decodes++
	return p.decodeErr
}

// TestM3_ProcessorMethodValuesAreTheChainStages: with the Encode/Decode adapters
// deleted (review m3 — they only repeated the method signatures), the two real
// callers carry the methods themselves: the producer calls Processor.Encode
// inside its stage wrapper, the restore passes Processor.Decode as a method
// value, which is exactly what it appends to its stage list. A failure in either
// still propagates through Chain unchanged.
func TestM3_ProcessorMethodValuesAreTheChainStages(t *testing.T) {
	src := func(_ context.Context, w io.Writer) error {
		_, err := w.Write([]byte("x"))
		return err
	}
	enc := &decodeProcessor{encodeErr: errors.New("encode refused")}
	err := Chain(context.Background(), io.Discard, src, []Stage{enc.Encode})
	if err == nil || !strings.Contains(err.Error(), "encode refused") {
		t.Fatalf("encode method value error %v", err)
	}
	dec := &decodeProcessor{decodeErr: errors.New("decode refused")}
	err = Chain(context.Background(), io.Discard, src, []Stage{dec.Decode})
	if err == nil || !strings.Contains(err.Error(), "decode refused") {
		t.Fatalf("decode method value error %v", err)
	}
	if enc.encodes != 1 || dec.decodes != 1 {
		t.Fatalf("method calls: %d encodes, %d decodes", enc.encodes, dec.decodes)
	}
}
