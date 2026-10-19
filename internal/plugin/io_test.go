package plugin_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

func TestCopyContextPropagatesReadErrors(t *testing.T) {
	var buf bytes.Buffer
	if _, err := plugin.CopyContext(t.Context(), &buf, failReader{}); err == nil {
		t.Fatal("a read error was swallowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.CopyContext(ctx, &buf, failReader{}); err == nil {
		t.Fatal("a cancelled copy returned success")
	}
}

func TestCopyContextCopiesUntilEOF(t *testing.T) {
	var buf bytes.Buffer
	n, err := plugin.CopyContext(t.Context(), &buf, bytes.NewReader([]byte("payload")))
	if err != nil || n != int64(len("payload")) || buf.String() != "payload" {
		t.Fatalf("CopyContext = %d, %v, %q", n, err, buf.String())
	}
}

// TestCopyContextReportsAWriteFailure: a downstream that fails is reported, not
// retried. The bytes written so far are the count from before the failed write,
// which is zero here — a plugin reports the error, never the short stream.
func TestCopyContextReportsAWriteFailure(t *testing.T) {
	n, err := plugin.CopyContext(t.Context(), failWriter{}, bytes.NewReader([]byte("payload")))
	if err == nil {
		t.Fatal("a write failure was swallowed")
	}
	if n != 0 {
		t.Fatalf("n = %d, want 0 bytes before the failure", n)
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("disk fell over") }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }
