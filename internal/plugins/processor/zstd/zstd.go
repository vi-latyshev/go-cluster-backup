// Package zstd is the processor plugin over klauspost/compress's zstd: one
// streaming pass in each direction, bounded by the configured window and
// concurrency. A single instance serves both directions, so a run and a restore
// read the same numbers from the same config file.
package zstd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

// Config carries every knob of this processor. There is no default: the
// config file states what will really happen, including the numbers the
// library would otherwise pick.
type Config struct {
	// Level is one of the four the library's EncoderLevel type defines —
	// "fastest", "default", "better", "best" — not a zstd CLI level. The
	// library maps CLI levels onto those four (level < 3 is fastest, 3..5
	// default, 6..9 better, 10+ best), and a CLI number here would promise a
	// distinction the library does not make; the field doc names each level's
	// CLI equivalent instead.
	Level string `json:"level" required:"true" doc:"the library's encoder level: \"fastest\" (zstd CLI 1-2), \"default\" (CLI 3-5), \"better\" (CLI 6-9), \"best\" (CLI 10+); a CLI number is not accepted because the library collapses all of them onto these four"`
	// Concurrency is the number of encoder/decoder goroutines; memory scales
	// with the window times this value.
	Concurrency int `json:"concurrency" doc:"encoder and decoder goroutines; encoder memory is roughly window_bytes times this number, decoder memory roughly decode_window_bytes times it"`
	// WindowBytes is the maximum back-reference distance: how far back the
	// compressor may look for a repeated string. Must be a power of two between
	// 1 KiB and 512 MiB. Raising it buys more compression than raising the
	// level does, and costs memory in proportion.
	WindowBytes model.ByteSize `json:"window_bytes" required:"true" doc:"encoder maximum back-reference distance in bytes, a power of two between 1024 and 536870912; memory is roughly this times concurrency"`
	// DecodeWindowBytes is the decoder's own ceiling, stated separately on
	// purpose: a restore decodes objects written weeks earlier with the window
	// recorded in their manifests, so it must not follow a later reduction of
	// WindowBytes — cap the decoder at the largest window any run still in
	// retention used, not at the window this run encodes with.
	DecodeWindowBytes model.ByteSize `json:"decode_window_bytes" required:"true" doc:"maximum window a frame may carry for this instance to decode it, a power of two between 1024 and 536870912; must be at least the largest window_bytes used by any run still in retention"`
}

// Config implements plugin.Validator through Validate.
var _ plugin.Validator = Config{}

func (c Config) Validate() error {
	if ok, _ := zstd.EncoderLevelFromString(c.Level); !ok {
		return fmt.Errorf("level %q is not a library encoder level (fastest, default, better, best)", c.Level)
	}
	if c.Concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1, got %d", c.Concurrency)
	}
	for _, field := range []struct {
		name  string
		value int
	}{{"window_bytes", int(c.WindowBytes)}, {"decode_window_bytes", int(c.DecodeWindowBytes)}} {
		name, value := field.name, field.value
		if value < zstd.MinWindowSize || value > zstd.MaxWindowSize {
			return fmt.Errorf("%s must be between %d and %d, got %d", name, zstd.MinWindowSize, zstd.MaxWindowSize, value)
		}
		if value&(value-1) != 0 {
			return fmt.Errorf("%s must be a power of two, got %d", name, value)
		}
	}
	return nil
}

// Register adds the zstd processor to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, processor.Kind, plugin.Spec[Config, processor.Processor]{
		Type:      "zstd",
		Summary:   "zstd frame compression, streaming, with the window and concurrency from the config",
		Stability: plugin.Stable,
		Example:   []byte(`{"level": "better", "concurrency": 1, "window_bytes": 67108864, "decode_window_bytes": 134217728}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (processor.Processor, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Decode ran Config.Validate before New: the level, the
			// concurrency and both windows are already admitted, and a breach
			// of that claim is a panic, not a config error this call can see.
			must.Do(cfg, cfg.Validate())
			return &Processor{cfg: cfg}, nil
		},
	})
}

type Processor struct {
	cfg Config
}

func (p *Processor) Describe(ctx context.Context) (processor.ProcessorTraits, error) {
	if err := ctx.Err(); err != nil {
		return processor.ProcessorTraits{}, err
	}
	return processor.ProcessorTraits{Ext: ".zst"}, nil
}

// Encode compresses src into one zstd frame on dst. No temporary file: the
// encoder writes as it reads, so memory is bounded by the window.
func (p *Processor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	enc, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(levelConfig(p.cfg.Level)),
		zstd.WithEncoderConcurrency(p.cfg.Concurrency),
		zstd.WithWindowSize(int(p.cfg.WindowBytes)),
	)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("zstd encoder: %w", err))
	}
	_, copyErr := plugin.CopyContext(ctx, enc, src)
	// The encoder buffers, so its flush is where a failing writer usually
	// first shows up: Close must run on the error path too, and its error
	// must not be discarded, or a broken destination reads as success.
	closeErr := enc.Close()
	if copyErr != nil || closeErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(fmt.Errorf("zstd encode: %w", errors.Join(copyErr, closeErr)))
	}
	return nil
}

// Decode decompresses a frame from src onto dst. The ceiling is
// decode_window_bytes, deliberately not the encoder's window: an old object
// decodes with the config of today, and lowering the encoding window must not
// make runs still in retention unreadable.
func (p *Processor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The frame's window descriptor is read up front so a refused frame can
	// name both numbers; the peeked bytes stay in the buffer for the decoder.
	br := bufio.NewReader(src)
	header, _ := br.Peek(frameHeaderBytes)
	dec, err := zstd.NewReader(br,
		zstd.WithDecoderMaxWindow(uint64(p.cfg.DecodeWindowBytes.Int64())),
		zstd.WithDecoderConcurrency(p.cfg.Concurrency),
	)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("zstd decoder: %w", err))
	}
	defer dec.Close()
	if _, err := plugin.CopyContext(ctx, dst, dec); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, zstd.ErrWindowSizeExceeded) {
			return plugin.Permanent(fmt.Errorf(
				"zstd decode: the frame needs a window larger than decode_window_bytes=%d (%s); raise it to at least the window_bytes this run was written with",
				p.cfg.DecodeWindowBytes.Int64(), frameWindow(header)))
		}
		return fmt.Errorf("zstd decode: %w", err)
	}
	return nil
}

// frameHeaderBytes covers the magic number, the frame header descriptor, an
// eight-byte content size and the window descriptor: enough to name a frame's
// window without decoding it.
const frameHeaderBytes = 18

// frameWindow describes the frame's declared window for an error message. It
// parses the frame header itself because the decoder's own error carries no
// numbers, and an operator staring at a refused restore needs both.
func frameWindow(header []byte) string {
	const magic = 4
	if len(header) < magic+2 {
		return "window of unreadable frame header"
	}
	fhd := header[magic]
	if fhd&0x20 != 0 {
		// Single segment: the window is the content size and the window
		// descriptor byte is absent.
		return "single-segment frame whose window equals its content size"
	}
	// The window descriptor is the byte right after the frame header
	// descriptor; the dictionary id and content size follow it. The check
	// above already guarantees the byte is present.
	wd := header[magic+1]
	base := uint64(1) << (10 + (wd >> 3))
	window := base + (base/8)*uint64(wd&0x7)
	return fmt.Sprintf("frame window %d bytes", window)
}

// levelConfig translates the config's level name. Validate admitted it, so an
// unrecognised name here is a programming error, not a config one.
func levelConfig(level string) zstd.EncoderLevel {
	_, l := zstd.EncoderLevelFromString(level)
	return l
}

// copyCtx copies until EOF, checking cancellation between reads: a cancelled
// stream stops at the next buffer boundary rather than at the end of the input.
