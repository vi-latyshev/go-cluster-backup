package zstd

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// TestFrameWindow pins the header parser an operator's window refusal is read
// through: a frame header too short to hold a window descriptor, a
// single-segment frame (whose window is its content size), and a frame that
// declares one.
func TestFrameWindow(t *testing.T) {
	if got := frameWindow([]byte{0x28, 0xb5, 0x2f, 0xfd}); got != "window of unreadable frame header" {
		t.Errorf("a four-byte header describes %q", got)
	}
	// The frame header descriptor's single-segment bit is 0x20.
	single := make([]byte, 18)
	single[4] = 0x20
	if got := frameWindow(single); got != "single-segment frame whose window equals its content size" {
		t.Errorf("a single-segment header describes %q", got)
	}
	// Window descriptor 0: base 1<<10 with no fraction.
	declared := make([]byte, 18)
	if got := frameWindow(declared); got != "frame window 1024 bytes" {
		t.Errorf("a 1 KiB window describes %q", got)
	}
}

// TestLibraryRejectionsArePermanent exercises the guards around the library's
// own constructors with the values Config.Validate refuses — a window that is
// not a power of two, a decode ceiling below the library's minimum. Validate
// refuses them before an instance exists, so the guards exist to turn a
// rejection the library would otherwise panic on into a classified error.
func TestLibraryRejectionsArePermanent(t *testing.T) {
	encode := &Processor{cfg: Config{Level: "default", Concurrency: 1, WindowBytes: 100000, DecodeWindowBytes: 1 << 20}}
	err := encode.Encode(context.Background(), io.Discard, bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("Encode accepted a window the library refuses")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("an encoder the library refuses is %q, want permanent", class)
	}

	decode := &Processor{cfg: Config{Level: "default", Concurrency: 1, WindowBytes: 1 << 20, DecodeWindowBytes: 512}}
	err = decode.Decode(context.Background(), io.Discard, bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("Decode accepted a ceiling the library refuses")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("a decoder the library refuses is %q, want permanent", class)
	}
}
