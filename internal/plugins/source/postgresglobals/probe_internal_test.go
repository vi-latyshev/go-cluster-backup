package postgresglobals

import (
	"strings"
	"testing"
)

// TestGlobalsProbeKeepsOnlyTheWindow: the probe keeps the first and last
// probeWindow bytes as the stream passes, so a whole artifact never sits in
// memory, and the far-away middle is not kept.
func TestGlobalsProbeKeepsOnlyTheWindow(t *testing.T) {
	var p globalsProbe
	head := "--\n-- PostgreSQL database cluster dump\n--\n\n" + strings.Repeat("a", 100)
	middle := strings.Repeat("m", 3*probeWindow)
	tail := "\n--\n-- PostgreSQL database cluster dump complete\n--\n\n"
	for _, chunk := range []string{head, middle, tail} {
		if _, err := p.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if len(p.head) != probeWindow {
		t.Fatalf("head holds %d bytes, want %d", len(p.head), probeWindow)
	}
	if len(p.tail) != probeWindow {
		t.Fatalf("tail holds %d bytes, want %d", len(p.tail), probeWindow)
	}
	if err := p.verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestGlobalsProbeRejectsAnEmptyStream: nothing of either end is present.
func TestGlobalsProbeRejectsAnEmptyStream(t *testing.T) {
	var p globalsProbe
	if err := p.verify(); err == nil {
		t.Fatal("an empty stream verified")
	}
	if !hasLine(nil, globalsHeader) {
		return
	}
	t.Fatal("an empty head contains the header")
}

// TestGlobalsProbeAcceptsCRLFAndSeparatorLines: the completion check skips the
// closing comment and blank lines the dump prints after its completion line.
func TestGlobalsProbeAcceptsCRLFAndSeparatorLines(t *testing.T) {
	if !hasLine([]byte("--\r\n-- PostgreSQL database cluster dump\r\n--\r\n"), globalsHeader) {
		t.Fatal("a CRLF header line was not recognised")
	}
	ok := "--\n-- PostgreSQL database cluster dump complete\n--\n\n"
	if !endsWithLine([]byte(ok), globalsFooter) {
		t.Fatalf("the completion line before the closing comment was not recognised: %q", ok)
	}
	if endsWithLine([]byte("--\n-- PostgreSQL database cluster dump complete\n--\nSET x;\n"), globalsFooter) {
		t.Fatal("a statement after the completion line was accepted as the end")
	}
	if endsWithLine(nil, globalsFooter) {
		t.Fatal("an empty tail verified")
	}
}
