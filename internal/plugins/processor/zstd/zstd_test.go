package zstd_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor/zstd"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor/processortest"
)

// levels are the four the library's EncoderLevel defines; every one is
// selectable from this config.
var levels = []string{"fastest", "default", "better", "best"}

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := zstd.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, level string, window, decodeWindow, concurrency int) processor.Processor {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"level": level, "concurrency": concurrency, "window_bytes": window, "decode_window_bytes": decodeWindow})
	if err != nil {
		t.Fatal(err)
	}
	p, err := plugin.Build(context.Background(), testRegistry(t), processor.Kind, "zstd", raw,
		plugin.Deps{Instance: "zstd", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestProcessorConformance(t *testing.T) {
	processortest.Run(t, func(t *testing.T) processortest.Fixture {
		return processortest.Fixture{
			Proc:  build(t, "default", 8<<20, 8<<20, 1),
			Plain: bytes.Repeat([]byte("cluster backup round trip "), 2000),
		}
	})
}

func TestRoundTripEveryLevel(t *testing.T) {
	ctx := context.Background()
	inputs := map[string][]byte{
		"compressible":   bytes.Repeat([]byte("the same sentence once more. "), 4_000),
		"incompressible": pseudoRandom(400_000),
		"empty":          {},
	}
	for _, level := range levels {
		for name, plain := range inputs {
			t.Run(level+"/"+name, func(t *testing.T) {
				p := build(t, level, 1<<20, 1<<20, 1)
				var encoded bytes.Buffer
				if err := p.Encode(ctx, &encoded, bytes.NewReader(plain)); err != nil {
					t.Fatalf("Encode: %v", err)
				}
				if len(plain) > 0 && encoded.Len() == 0 {
					t.Fatal("Encode produced no bytes")
				}
				var back bytes.Buffer
				if err := p.Decode(ctx, &back, bytes.NewReader(encoded.Bytes())); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if !bytes.Equal(back.Bytes(), plain) {
					t.Fatalf("round trip returned %d bytes, want %d", back.Len(), len(plain))
				}
			})
		}
	}
}

// TestCompressionActuallyCompresses is the check that would fail if the level
// were ignored and the data stored verbatim under a zstd header.
func TestCompressionActuallyCompresses(t *testing.T) {
	p := build(t, "better", 1<<20, 1<<20, 1)
	plain := bytes.Repeat([]byte("repetition is all this input has. "), 100_000)
	var encoded bytes.Buffer
	if err := p.Encode(context.Background(), &encoded, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	if encoded.Len() >= len(plain)/10 {
		t.Fatalf("compressed %d bytes to %d; identical text should shrink below a tenth", len(plain), encoded.Len())
	}
}

// TestDecodeWindowIsIndependentOfEncodeWindow is the property the decode
// ceiling exists for: an object written with a 128 MiB window must open on an
// instance whose own encode window was lowered to 16 MiB, as long as its
// decode_window_bytes still admits the frame. Lowering window_bytes later
// must never make runs still in retention unreadable.
func TestDecodeWindowIsIndependentOfEncodeWindow(t *testing.T) {
	ctx := context.Background()
	const bigWindow = 128 << 20
	// The frame must really carry the wide window, so the input has to exceed
	// a smaller one; 2 MiB of incompressible bytes forces the header to
	// declare the configured maximum.
	plain := pseudoRandom(2 << 20)
	writer := build(t, "fastest", bigWindow, bigWindow, 1)
	var encoded bytes.Buffer
	if err := writer.Encode(ctx, &encoded, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}

	// Encode window lowered to 16 MiB, decode ceiling still 128 MiB.
	lowered := build(t, "fastest", 16<<20, bigWindow, 1)
	var back bytes.Buffer
	if err := lowered.Decode(ctx, &back, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatalf("a 128 MiB frame was refused by an instance encoding with 16 MiB and decoding with 128 MiB: %v", err)
	}
	if !bytes.Equal(back.Bytes(), plain) {
		t.Fatal("the decoded bytes differ from the original")
	}

	// Decode ceiling below the frame's window: a permanent error naming both.
	capped := build(t, "fastest", 16<<20, 64<<20, 1)
	err := capped.Decode(ctx, &bytes.Buffer{}, bytes.NewReader(encoded.Bytes()))
	if err == nil {
		t.Fatal("a 64 MiB ceiling decoded a 128 MiB frame")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("window refusal is %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "decode_window_bytes=67108864") {
		t.Errorf("the error does not name the configured ceiling: %v", err)
	}
	if !strings.Contains(err.Error(), "134217728") {
		t.Errorf("the error does not name the frame's window: %v", err)
	}
}

// TestMemoryStaysNearTheWindow is the bound this package claims: neither
// direction may hold the stream. 2 MiB of incompressible input through a
// 64 KiB window must not grow the heap by anything like the input size.
func TestMemoryStaysNearTheWindow(t *testing.T) {
	const window = 64 << 10
	const inputSize = 2 << 20
	p := build(t, "fastest", window, window, 1)
	ctx := context.Background()
	plain := pseudoRandom(inputSize)

	// Every direction writes to io.Discard, so the heap delta is the
	// processor's own working set, not the artifact's size. This test needs a
	// frame to decode, so it makes that one separately, outside the measurement.
	seed := build(t, "fastest", window, window, 1)
	var encoded bytes.Buffer
	if err := seed.Encode(ctx, &encoded, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}

	before := heapInUse(t)
	if err := p.Encode(ctx, io.Discard, bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	encodedHeap := heapInUse(t) - before

	before = heapInUse(t)
	if err := p.Decode(ctx, io.Discard, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	decodedHeap := heapInUse(t) - before

	// Generous factor: it must not catch encoder internals, only a design
	// that buffers the whole stream, which would show up as inputSize.
	for name, grew := range map[string]int64{"encode": encodedHeap, "decode": decodedHeap} {
		if limit := int64(inputSize / 4); grew > limit {
			t.Errorf("%s grew the heap by %d bytes for a %d-byte stream through a %d-byte window; the stream is being buffered",
				name, grew, inputSize, window)
		}
	}
}

// TestInvalidConfigs pins the validation this config promises, so a future
// default cannot slip a value back in.
func TestInvalidConfigs(t *testing.T) {
	cases := map[string]string{
		"unknown level":           `{"level": "3", "concurrency": 1, "window_bytes": 8388608, "decode_window_bytes": 8388608}`,
		"no level":                `{"concurrency": 1, "window_bytes": 8388608, "decode_window_bytes": 8388608}`,
		"no decode window":        `{"level": "default", "concurrency": 1, "window_bytes": 8388608}`,
		"zero concurrency":        `{"level": "default", "concurrency": 0, "window_bytes": 8388608, "decode_window_bytes": 8388608}`,
		"window too small":        `{"level": "default", "concurrency": 1, "window_bytes": 512, "decode_window_bytes": 8388608}`,
		"window not power2":       `{"level": "default", "concurrency": 1, "window_bytes": 100000, "decode_window_bytes": 8388608}`,
		"window too large":        `{"level": "default", "concurrency": 1, "window_bytes": 1073741824, "decode_window_bytes": 8388608}`,
		"decode window too large": `{"level": "default", "concurrency": 1, "window_bytes": 8388608, "decode_window_bytes": 1073741824}`,
	}
	r := testRegistry(t)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("processor", "zstd", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
}

// TestBuildReturnsOnCancelledContext: construction owes a cancelled context the
// same prompt return as every other call.
func TestBuildReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	raw, err := json.Marshal(map[string]any{"level": "default", "concurrency": 1, "window_bytes": 1 << 20, "decode_window_bytes": 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	_, err = plugin.Build(ctx, testRegistry(t), processor.Kind, "zstd", raw,
		plugin.Deps{Instance: "zstd", Log: slog.New(slog.DiscardHandler)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Build with a cancelled context returned %v, want context.Canceled", err)
	}
}

func TestDescribeReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := build(t, "default", 1<<20, 1<<20, 1).Describe(ctx); err == nil {
		t.Fatal("Describe ignored a cancelled context")
	}
}

// cancelAtEnd hands over every byte, then cancels the context and reports the
// cancellation: a copy that reaches the end of its input just as the context
// ends, so the plugin's own check — not its entry guard — is what returns.
type cancelAtEnd struct {
	ctx    context.Context
	cancel context.CancelFunc
	data   []byte
	off    int
}

func (c *cancelAtEnd) Read(p []byte) (int, error) {
	if c.off < len(c.data) {
		n := copy(p, c.data[c.off:])
		c.off += n
		return n, nil
	}
	c.cancel()
	return 0, c.ctx.Err()
}

func TestEncodeReturnsOnCancellationMidStream(t *testing.T) {
	p := build(t, "default", 1<<20, 1<<20, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &cancelAtEnd{ctx: ctx, cancel: cancel, data: bytes.Repeat([]byte("payload "), 100_000)}
	if err := p.Encode(ctx, io.Discard, src); !errors.Is(err, context.Canceled) {
		t.Fatalf("Encode cancelled mid-stream returned %v, want context.Canceled", err)
	}
}

func TestDecodeReturnsOnCancellationMidStream(t *testing.T) {
	p := build(t, "default", 1<<20, 1<<20, 1)
	var encoded bytes.Buffer
	if err := p.Encode(context.Background(), &encoded, bytes.NewReader(bytes.Repeat([]byte("payload "), 100_000))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &cancelAtEnd{ctx: ctx, cancel: cancel, data: encoded.Bytes()}
	if err := p.Decode(ctx, io.Discard, src); !errors.Is(err, context.Canceled) {
		t.Fatalf("Decode cancelled mid-stream returned %v, want context.Canceled", err)
	}
}

// TestDecodeRefusesGarbage: a stream that is not a zstd frame is a decode
// failure the caller can see, never a silent empty artifact.
func TestDecodeRefusesGarbage(t *testing.T) {
	p := build(t, "default", 1<<20, 1<<20, 1)
	err := p.Decode(context.Background(), io.Discard, strings.NewReader("this is not a zstd frame"))
	if err == nil {
		t.Fatal("Decode accepted a stream that is not a zstd frame")
	}
	if !strings.Contains(err.Error(), "zstd decode") {
		t.Errorf("the error does not name the decoder: %v", err)
	}
}

// pseudoRandom returns n bytes from a deterministic LCG: incompressible
// enough that no encoder can shrink it, without crypto/rand's cost.
func pseudoRandom(n int) []byte {
	out := make([]byte, n)
	state := uint32(0x9e3779b9)
	for i := range out {
		state = state*1664525 + 1013904223
		out[i] = byte(state >> 24)
	}
	return out
}

func heapInUse(t *testing.T) int64 {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse)
}
