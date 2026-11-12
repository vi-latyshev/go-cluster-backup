package pluginio_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
)

func TestWriteTarIsDeterministic(t *testing.T) {
	entries := func() []pluginio.TarEntry {
		return []pluginio.TarEntry{
			{Name: "data", Mode: 0o644, Read: strings.NewReader("payload"), Size: int64(len("payload"))},
			{Name: "token", Mode: 0o600, Body: []byte("secret")},
		}
	}
	var first, second bytes.Buffer
	if err := pluginio.WriteTar(t.Context(), &first, entries()); err != nil {
		t.Fatal(err)
	}
	if err := pluginio.WriteTar(t.Context(), &second, entries()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("two runs over the same entries produced different archives")
	}

	tr := tar.NewReader(bytes.NewReader(first.Bytes()))
	want := []struct {
		name string
		mode int64
		body string
	}{
		{"data", 0o644, "payload"},
		{"token", 0o600, "secret"},
	}
	for _, w := range want {
		hdr, err := tr.Next()
		if err != nil {
			t.Fatalf("entry %s: %v", w.name, err)
		}
		if hdr.Name != w.name || hdr.Mode != w.mode || hdr.Typeflag != tar.TypeReg {
			t.Fatalf("header = %+v, want %s mode %o regular", hdr, w.name, w.mode)
		}
		if !hdr.ModTime.Equal(time.Unix(0, 0)) {
			t.Fatalf("entry %s modtime = %v, want the fixed epoch", hdr.Name, hdr.ModTime)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != w.body {
			t.Fatalf("entry %s body = %q, want %q", hdr.Name, body, w.body)
		}
	}
	if _, err := tr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("the archive holds more than the two entries: %v", err)
	}
}

func TestWriteTarRefusesAShortRead(t *testing.T) {
	err := pluginio.WriteTar(t.Context(), io.Discard, []pluginio.TarEntry{
		{Name: "data", Mode: 0o600, Read: strings.NewReader("abc"), Size: 5},
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("WriteTar = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteTarPropagatesAReadError(t *testing.T) {
	err := pluginio.WriteTar(t.Context(), io.Discard, []pluginio.TarEntry{
		{Name: "data", Mode: 0o600, Read: failReader{}, Size: 5},
	})
	if err == nil || !strings.Contains(err.Error(), "disk fell over") {
		t.Fatalf("WriteTar = %v, want the reader's failure", err)
	}
}

func TestWriteTarRejectsAnInvalidHeader(t *testing.T) {
	err := pluginio.WriteTar(t.Context(), io.Discard, []pluginio.TarEntry{
		{Name: "data", Mode: 0o600, Read: strings.NewReader(""), Size: -1},
	})
	if err == nil {
		t.Fatal("a negative entry size was accepted")
	}
}

func TestWriteTarReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pluginio.WriteTar(ctx, io.Discard, []pluginio.TarEntry{{Name: "data", Body: []byte("x")}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteTar = %v, want context.Canceled", err)
	}
}

func TestWriteTarReportsAWriteFailure(t *testing.T) {
	t.Run("the header", func(t *testing.T) {
		err := pluginio.WriteTar(t.Context(), &failAfter{limit: 0}, []pluginio.TarEntry{{Name: "data", Body: []byte("x")}})
		if err == nil || !strings.Contains(err.Error(), "tar header") {
			t.Fatalf("WriteTar = %v, want a header failure", err)
		}
	})
	t.Run("the body", func(t *testing.T) {
		err := pluginio.WriteTar(t.Context(), &failAfter{limit: 512}, []pluginio.TarEntry{{Name: "data", Body: []byte("x")}})
		if err == nil || !strings.Contains(err.Error(), "tar body") {
			t.Fatalf("WriteTar = %v, want a body failure", err)
		}
	})
	t.Run("the trailer", func(t *testing.T) {
		err := pluginio.WriteTar(t.Context(), &failAfter{limit: 512 + len("x")}, []pluginio.TarEntry{{Name: "data", Body: []byte("x")}})
		if err == nil || !strings.Contains(err.Error(), "tar trailer") {
			t.Fatalf("WriteTar = %v, want a trailer failure", err)
		}
	})
}

// failAfter accepts bytes up to limit and fails everything after it, so a test
// can place the failure at one part of the stream.
type failAfter struct {
	limit int
	seen  int
}

func (w *failAfter) Write(p []byte) (int, error) {
	if w.seen+len(p) > w.limit {
		return 0, errors.New("no space left")
	}
	w.seen += len(p)
	return len(p), nil
}
