package webhook

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"
	"unicode/utf8"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// The message model of this plugin: a message is plain text the config file
// writes, one template per event, and nothing here knows a receiver.
//
// A message template renders into lines: a line that renders empty is dropped,
// so a template line holding only an action needs no trim marker; the first
// line left is the title, and the lines below it are the body.
//
// A pair of literal backticks in the template's own text marks a line's
// monospace part — `text `code`` renders into a line whose text is "text " and
// whose code is "code". The backticks are read from the template's parse tree,
// never from the rendered text: a backtick that arrives from data (a name, a
// reason) is a character of that data and never markup.

const (
	// spanOpen and spanClose stand in for the markup backticks of a message
	// template before it is parsed for execution, one byte for one byte. The
	// rendered text then carries them exactly where the author put the span, so
	// splitting a line into its parts needs no knowledge of what the template
	// rendered; only the template's own text is rewritten, and the parse tree
	// says which bytes those are.
	spanOpen  = '\x01'
	spanClose = '\x02'
)

// Line is one line of a rendered message: its text and, when the template
// marked one, the monospace part of it.
type Line struct {
	Text string
	Code string
}

// Message is one rendered event message: the title of its first line and the
// lines below it.
type Message struct {
	Title string
	Lines []Line
}

// Text is the whole message as plain text — the title, then every line with its
// code part in place, one per line. It is what a receiver taking a single
// string needs, and what a payload's .Text is.
func (m Message) Text() string {
	lines := make([]string, 0, len(m.Lines)+1)
	lines = append(lines, m.Title)
	for _, l := range m.Lines {
		lines = append(lines, l.Text+l.Code)
	}
	return strings.Join(lines, "\n")
}

// payloadData is what a payload template is executed with: the message as the
// message templates rendered it, and the engine's event as it arrived.
type payloadData struct {
	Title string
	Lines []Line
	Text  string
	Event model.Event
}

// eventMessage is one event's message template, ready to render.
type eventMessage struct {
	event string
	tmpl  *template.Template
}

// newMessage prepares the message template of one event: it parses the source,
// reads the markup backticks out of the parse tree, refuses a line that breaks
// the span rule, and parses again with each markup backtick replaced by its
// marker. Every error names the event and, where the line is at fault, the line
// of the template.
func newMessage(event string, tmpl plugin.Template) (*eventMessage, error) {
	src := tmpl.Text()
	if strings.ContainsAny(src, "\x01\x02") {
		return nil, fmt.Errorf("events/%s: the template carries a control character this plugin marks a code span with", event)
	}
	funcs := messageFuncs()
	// The source was parsed once already, by plugin.Decode, with this same
	// function set and missingkey=error: a syntax error is reported there, so
	// parsing it here cannot fail.
	parsed := must.Do(parseSource(event, src, funcs))
	marks, err := spanMarks(event, src, parsed)
	if err != nil {
		return nil, err
	}
	if len(marks) == 0 {
		return &eventMessage{event: event, tmpl: parsed}, nil
	}
	// markSpans rewrites only bytes inside text nodes, so the marked source
	// parses exactly as the source it came from did.
	marked := must.Do(parseSource(event, markSpans(src, marks), funcs))
	return &eventMessage{event: event, tmpl: marked}, nil
}

// parseSource parses one message source the way the framework parses a template
// field: the core function set plus this plugin's own, and a missing map key as
// an error.
func parseSource(name, src string, funcs template.FuncMap) (*template.Template, error) {
	return template.New(name).Option("missingkey=error").Funcs(funcs).Parse(src)
}

// messageFuncs is what a message template may call: the framework's core set
// plus this plugin's own (see funcs.go). The payload template gets the same set
// from plugin.Decode; a message template is parsed here, so it has to be handed
// the same one.
func messageFuncs() template.FuncMap {
	funcs := plugin.CoreFuncs()
	for name, fn := range (Config{}).TemplateFuncs() {
		funcs[name] = fn
	}
	return funcs
}

// spanMarks is the byte offset of every markup backtick of one source. A
// backtick counts only in the template's own text: one inside an action, a
// comment or a string literal of the template is not markup, and neither is one
// the data carries at run time. Every line must carry a pair of them or none —
// a line is a text part and one code part — and nothing may follow the closing
// one, since a code span ends its line.
func spanMarks(event, src string, tmpl *template.Template) ([]int, error) {
	var marks []int
	for _, t := range tmpl.Templates() {
		// Parse installs a tree on every template it registers, and
		// Template.New registers nothing: an entry here always has one.
		collectBackticks(t.Root, &marks)
	}
	slices.Sort(marks)
	var errs []error
	for i := 0; i < len(marks); {
		line := lineOf(src, marks[i])
		j := i
		for j < len(marks) && lineOf(src, marks[j]) == line {
			j++
		}
		switch n := j - i; {
		case n == 1:
			errs = append(errs, fmt.Errorf("events/%s: line %d: a code span is a pair of backticks and this line carries one", event, line))
		case n > 2:
			errs = append(errs, fmt.Errorf("events/%s: line %d: %d backticks; a line carries one code span", event, line, n))
		default:
			after := src[marks[i+1]+1 : lineEnd(src, marks[i+1])]
			if strings.TrimRight(after, " \t\r") != "" {
				errs = append(errs, fmt.Errorf("events/%s: line %d: the code span ends the line, and %q follows it", event, line, strings.TrimSpace(after)))
			}
		}
		i = j
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return marks, nil
}

// collectBackticks appends the source offset of every backtick one node's own
// text carries. A text node keeps the offset it was read at, so a mark is exact
// whatever the template's structure — a nested range, an if, a trim marker.
func collectBackticks(n parse.Node, out *[]int) {
	switch t := n.(type) {
	case *parse.ListNode:
		if t == nil {
			return
		}
		for _, c := range t.Nodes {
			collectBackticks(c, out)
		}
	case *parse.TextNode:
		for i, b := range t.Text {
			if b == '`' {
				*out = append(*out, int(t.Pos)+i)
			}
		}
	case *parse.IfNode:
		collectBackticks(t.List, out)
		collectBackticks(t.ElseList, out)
	case *parse.RangeNode:
		collectBackticks(t.List, out)
		collectBackticks(t.ElseList, out)
	case *parse.WithNode:
		collectBackticks(t.List, out)
		collectBackticks(t.ElseList, out)
	}
}

// markSpans replaces every markup backtick of one source by its marker, opening
// and closing in the order the marks were found.
func markSpans(src string, marks []int) string {
	out := []byte(src)
	for i, at := range marks {
		if i%2 == 0 {
			out[at] = spanOpen
		} else {
			out[at] = spanClose
		}
	}
	return string(out)
}

// lineOf is the 1-based line of one byte offset of the source; lineEnd is the
// offset of the newline that closes its line, or the end of the source.
func lineOf(src string, at int) int { return strings.Count(src[:at], "\n") + 1 }

func lineEnd(src string, at int) int {
	if i := strings.IndexByte(src[at:], '\n'); i >= 0 {
		return at + i
	}
	return len(src)
}

// render executes the message template against one event and folds the result:
// the lines that rendered empty are dropped, the first line kept is the title
// as plain text, and every following line is split into its text and its code
// part.
func (m *eventMessage) render(ev model.Event) (Message, error) {
	var b strings.Builder
	if err := m.tmpl.Execute(&b, ev); err != nil {
		return Message{}, fmt.Errorf("events/%s: rendering the message: %w", m.event, err)
	}
	kept := nonEmptyLines(b.String())
	if len(kept) == 0 {
		return Message{}, fmt.Errorf("events/%s: the message renders to no line at all, and its first line is its title", m.event)
	}
	title, err := splitLine(m.event, kept[0])
	if err != nil {
		return Message{}, err
	}
	out := Message{Title: title.Text + title.Code, Lines: make([]Line, 0, len(kept)-1)}
	for _, line := range kept[1:] {
		l, err := splitLine(m.event, line)
		if err != nil {
			return Message{}, err
		}
		out.Lines = append(out.Lines, l)
	}
	return out, nil
}

// nonEmptyLines splits one render into its lines and drops the empty ones: a
// template line that holds only an action renders to nothing, so the author
// never writes a trim marker to keep the message clean. What is kept keeps its
// leading spaces.
//
// A line that is exactly blankMarker is kept as an empty line: that is what
// {{ blank }} asked for, and it is a different intention from a line that
// happened to render empty. It is how a message separates its sections.
func nonEmptyLines(s string) []string {
	out := make([]string, 0, 16)
	for _, line := range strings.Split(s, "\n") {
		switch line {
		case blankMarker:
			out = append(out, "")
		case "":
		default:
			out = append(out, line)
		}
	}
	return out
}

// splitLine splits one rendered line by the markers of the marked template: the
// text before the opening one, then the code part, which nothing may follow. A
// span that renders a line break of its own is refused — the model has one code
// part per line — and so is a marker pair that arrived out of order.
func splitLine(event, line string) (Line, error) {
	open := strings.IndexByte(line, spanOpen)
	firstClose := strings.IndexByte(line, spanClose)
	if open < 0 && firstClose < 0 {
		return Line{Text: line}, nil
	}
	if open < 0 || firstClose < open {
		return Line{}, fmt.Errorf("events/%s: a code span opens on one line of the render and closes on another; a span stays within its line", event)
	}
	// firstClose is at or after open here, so the closing marker is inside
	// rest and end is never negative.
	rest := line[open+1:]
	end := strings.IndexByte(rest, spanClose)
	if tail := strings.TrimRight(rest[end+1:], " \t\r"); tail != "" {
		return Line{}, fmt.Errorf("events/%s: %q follows the code span on its line; a code span ends its line", event, strings.TrimSpace(tail))
	}
	return Line{Text: line[:open], Code: rest[:end]}, nil
}

// cleanEvent is the one place data meets this plugin: every string the event
// carries is cleaned before a template or a body builder ever sees it. A name, a
// reason or an error string from the cluster is arbitrary text — a byte that is
// not valid UTF-8 makes the body's YAML document unwritable and loses the whole
// message, and the two control bytes this plugin marks a code span with would be
// read as markup, tearing a rendered line in two.
func cleanEvent(ev model.Event) model.Event {
	return cleanData(reflect.ValueOf(ev)).Interface().(model.Event)
}

// cleanData returns a copy of one value with every string cleaned, whatever a
// nested slice, map or struct holds. A nil, a number and a timestamp are copied
// as they are; an unexported field is copied whole, since reflect cannot set one
// and none of them is a message's text.
func cleanData(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.String:
		cleaned := foldLines(sanitize(v.String()))
		if cleaned == v.String() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.SetString(cleaned)
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			out.Index(i).Set(cleanData(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		for _, key := range v.MapKeys() {
			out.SetMapIndex(key, cleanData(v.MapIndex(key)))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := range v.NumField() {
			if f := out.Field(i); f.CanSet() {
				f.Set(cleanData(v.Field(i)))
			}
		}
		return out
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		inner := cleanData(v.Elem())
		if v.Kind() == reflect.Pointer {
			p := reflect.New(v.Type().Elem())
			p.Elem().Set(inner)
			out.Set(p)
			return out
		}
		out.Set(inner)
		return out
	}
	return v
}

// sanitize replaces what a message must never carry from data: a byte that is
// not valid UTF-8 and any rune outside the set the body's YAML document can
// hold — which includes the two bytes this plugin marks a code span with. The
// set is the grammar's own, spelled as the parser spells it, so the next class
// it refuses is refused here too rather than losing a night's message to
// "control characters are not allowed". A tab and a line break stay: they are
// what a message is laid out with. The whole string is written, first byte
// included: everything before the first dirty rune is the message's own head,
// not something to drop.
//
// It is applied both to a string of data and to a whole rendered document (the
// YAML body), which is why it keeps line breaks: in a document they are the
// structure. Folding the breaks of DATA is a different job, done in cleanData.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	changed := false
	for _, r := range s {
		// utf8.RuneError is both a character a document may hold and the rune
		// a range yields for an invalid byte; replacing it with itself is the
		// one case where the write is the whole repair.
		if r == utf8.RuneError || !documentRune(r) {
			changed, r = true, utf8.RuneError
		}
		b.WriteRune(r)
	}
	if !changed {
		return s
	}
	return b.String()
}

// foldLines replaces the line breaks of one string of DATA with a space. This
// message model is lines: a rendered line is a text part and one code part, and
// a span that opens on one line and closes on another is refused — so data
// carrying a newline does not produce a longer message, it produces NO message
// at all, and the night is delivered nowhere. `errors.Join` is the ordinary
// source (one artifact's error is several sentences, one per line) and a
// `{{ .Error }}` inside a code span is the ordinary victim. A space is what the
// reader would have seen anyway; a lost run_finished is not.
//
// Only data is folded, never a rendered document: the body's own line breaks
// are the YAML structure sanitize has to leave alone.
func foldLines(s string) string {
	if !strings.ContainsAny(s, "\n\r") {
		return s
	}
	return strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(s)
}

// documentRune reports whether one rune of data may stand in the body's YAML
// document: the set the YAML 1.2 grammar calls printable, in the ranges the
// parser's own check uses — #x9, #xA, #xD, #x20-#x7E, #x85, #xA0-#xD7FF,
// #xE000-#xFFFD and #x10000-#x10FFFF (go.yaml.in/yaml/v3, readerc.go). A C1
// control, a noncharacter and a lone surrogate's bytes fall outside it; a
// carriage return and a next-line are inside it and are refused here, because
// the lexer reads both as a line break — a CR rewrites the line it lands in and
// a NEL is folded away, so either would silently rewrite the data.
func documentRune(r rune) bool {
	switch {
	case r == '\t' || r == '\n':
		return true
	case r == '\r' || r == 0x85:
		return false
	case r >= 0x20 && r <= 0x7E:
		return true
	case r >= 0xA0 && r <= 0xD7FF:
		return true
	case r >= 0xE000 && r <= 0xFFFD:
		return true
	case r >= 0x10000 && r <= 0x10FFFF:
		return true
	}
	return false
}
