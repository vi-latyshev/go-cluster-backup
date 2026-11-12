package postgresglobals

import (
	"fmt"
	"strings"
)

// The two lines pg_dumpall prints around a globals dump, verified verbatim.
const (
	globalsHeader = "-- PostgreSQL database cluster dump"
	globalsFooter = "-- PostgreSQL database cluster dump complete"
)

// probeWindow is how much of either end of the stream the probe keeps: enough
// for pg_dumpall's own header and footer blocks, far from holding a whole
// artifact in memory.
const probeWindow = 4096

// globalsProbe is the streaming completeness check of one pg_dumpall globals
// dump: it keeps the first and the last few kilobytes only and answers whether
// the header and the completion line are there.
type globalsProbe struct {
	head []byte
	tail []byte
}

func (p *globalsProbe) Write(b []byte) (int, error) {
	if room := probeWindow - len(p.head); room > 0 {
		p.head = append(p.head, b[:min(len(b), room)]...)
	}
	p.tail = append(p.tail, b...)
	if len(p.tail) > probeWindow {
		p.tail = p.tail[len(p.tail)-probeWindow:]
	}
	return len(b), nil
}

func (p *globalsProbe) verify() error {
	if !hasLine(p.head, globalsHeader) {
		return fmt.Errorf("its header line %q is missing", globalsHeader)
	}
	if !endsWithLine(p.tail, globalsFooter) {
		return fmt.Errorf("its completion line %q is missing, so the archive was truncated", globalsFooter)
	}
	return nil
}

func hasLine(b []byte, want string) bool {
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSuffix(line, "\r") == want {
			return true
		}
	}
	return false
}

// endsWithLine reads the last line of the tail that carries something: the
// dump ends with the completion line inside a "--" comment block, followed by
// a closing "--" line and blank lines, so the separator line is not the end.
func endsWithLine(b []byte, want string) bool {
	lines := strings.Split(strings.TrimRight(string(b), "\r\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSuffix(lines[i], "\r")
		switch line {
		case "", "--":
			continue
		}
		return line == want
	}
	return false
}
