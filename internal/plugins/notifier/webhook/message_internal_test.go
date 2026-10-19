package webhook

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestSpanMarksWalksBothBranchesOfAWith: the markup backticks of a template are
// read out of the parse tree, so a span written inside the else branch of a
// {{ with }} is found exactly as one in its main branch.
func TestSpanMarksWalksBothBranchesOfAWith(t *testing.T) {
	src := "{{ with .Name }}`a`\n{{ else }}`b`\n{{ end }}"
	parsed, err := parseSource("run_finished", src, messageFuncs())
	if err != nil {
		t.Fatalf("parsing the source: %v", err)
	}
	marks, err := spanMarks("run_finished", src, parsed)
	if err != nil {
		t.Fatalf("spanMarks: %v", err)
	}
	if len(marks) != 4 {
		t.Fatalf("spanMarks found %d backticks, want 4: %v", len(marks), marks)
	}
	for _, at := range marks {
		if src[at] != '`' {
			t.Errorf("mark %d points at %q, not at a backtick", at, src[at])
		}
	}
}

// TestSplitLineRefusesASpanThatBreaksItsLine: a rendered line is one text part
// and one code part, so a marker pair that arrives out of order, or a line that
// carries more after its span, is a message the model cannot write.
func TestSplitLineRefusesASpanThatBreaksItsLine(t *testing.T) {
	open, close := string(spanOpen), string(spanClose)
	cases := []struct {
		name    string
		line    string
		want    Line
		wantErr string
	}{
		{name: "no span", line: "a plain line", want: Line{Text: "a plain line"}},
		{name: "one span", line: "text " + open + "code" + close, want: Line{Text: "text ", Code: "code"}},
		{name: "trailing spaces after the span", line: "a" + open + "b" + close + "  ", want: Line{Text: "a", Code: "b"}},
		{name: "a close with no open", line: "x" + close, wantErr: "closes on another"},
		{name: "an open with no close", line: open + "x", wantErr: "closes on another"},
		{name: "a close before an open", line: close + "x" + open, wantErr: "closes on another"},
		{name: "text after the span", line: open + "a" + close + " tail", wantErr: "follows the code span"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := splitLine("run_finished", tc.line)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("splitLine(%q): %v", tc.line, err)
				}
				if got != tc.want {
					t.Fatalf("splitLine(%q) = %+v, want %+v", tc.line, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("splitLine(%q) = %+v, want a refusal", tc.line, got)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("splitLine(%q) refused with %q, want it to name %q", tc.line, err, tc.wantErr)
			}
		})
	}
}

// TestCleanDataReachesIntoMapsAndInterfaces: the cleaning walk covers whatever a
// nested slice, map or struct holds, and a map is copied rather than rewritten
// under its owner.
func TestCleanDataReachesIntoMapsAndInterfaces(t *testing.T) {
	const dirty = "a\x01b"
	const clean = "a�b"

	in := map[string]string{"k": dirty}
	out := cleanData(reflect.ValueOf(in)).Interface().(map[string]string)
	if out["k"] != clean {
		t.Errorf("a map value cleaned to %q, want %q", out["k"], clean)
	}
	if in["k"] != dirty {
		t.Errorf("cleanData rewrote the caller's map: %q", in["k"])
	}

	var nilMap map[string]string
	if got := cleanData(reflect.ValueOf(nilMap)); !got.IsNil() {
		t.Errorf("a nil map came back as %v, want it copied as it is", got)
	}

	type box struct{ V any }
	boxed := cleanData(reflect.ValueOf(box{V: dirty})).Interface().(box)
	if boxed.V != clean {
		t.Errorf("an interface field cleaned to %v, want %q", boxed.V, clean)
	}
}

// TestBodyRefusesAFormatItDoesNotKnow: the two shipped formats are the whole
// set; anything else is a config the plugin cannot build a body for.
func TestBodyRefusesAFormatItDoesNotKnow(t *testing.T) {
	_, err := Config{BodyFormat: "xml"}.body(Message{Title: "t"}, model.Event{})
	if err == nil {
		t.Fatal("body built a request for an unknown body_format")
	}
	if !strings.Contains(err.Error(), "body_format") {
		t.Errorf("the refusal does not name body_format: %v", err)
	}
}
