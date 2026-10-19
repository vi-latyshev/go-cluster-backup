package human

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// EventAttrKey is the record attribute every emitted event travels under: the
// engine and the plugins hand an Event over as one attribute, and this handler
// is what turns it back into a line.
const EventAttrKey = "event"

// Handler is the slog handler that renders every record through Render. It is
// the one renderer of this service's log — the engine's events, the plugins'
// events and the command layer's plain diagnostics all pass through it, so
// what a person sees, and what stays behind debug, is decided in this package
// and nowhere else.
//
// One record is written through as it happens, under the handler's lock:
// `kubectl logs -f` shows the run live, line by line, with nothing buffered
// between the engine and the container's log file.
type Handler struct {
	// mu is a pointer so a derived handler (WithAttrs) shares the one lock
	// without copying it.
	mu    *sync.Mutex
	w     io.Writer
	level slog.Level
	attrs []slog.Attr // the instance's own: kind, type, instance
}

// NewHandler builds the renderer over one writer at one level.
func NewHandler(w io.Writer, level slog.Level) *Handler {
	return &Handler{mu: &sync.Mutex{}, w: w, level: level}
}

func (h *Handler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	debug := h.level <= slog.LevelDebug
	prefix := prefixOf(h.attrs)
	var body string
	if ev, ok := eventOf(r); ok {
		body = Render(prefix, ev, debug)
	} else {
		// A plain diagnostic record — a plugin's own Debug/Warn, or the command
		// layer's. It is rendered as it stands, with its attributes at debug.
		body = r.Message
		if prefix != "" {
			body = prefix + ": " + body
		}
		if debug {
			if extra := debugAttrs(r, h.attrs); extra != "" {
				body += "  {" + extra + "}"
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, "["+Stamp(r.Time)+"] "+body+"\n")
	return err
}

func (h *Handler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.attrs = append(append([]slog.Attr{}, h.attrs...), as...)
	return &c
}

// WithGroup is part of slog.Handler. This renderer flattens groups into the
// attribute names it prints, so the group name is kept only for that.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.attrs = append(append([]slog.Attr{}, h.attrs...), slog.String("group", name))
	return &c
}

// eventOf pulls the structured event out of one record, when it carries one.
func eventOf(r slog.Record) (Event, bool) {
	var ev Event
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != EventAttrKey {
			return true
		}
		e, ok := a.Value.Any().(Event)
		if ok {
			ev, found = e, true
			return false
		}
		return true
	})
	return ev, found
}

// prefixOf renders an instance's own attributes the way every line of its log
// reads: "<kind> <type>/<instance>". An engine-level record carries none and
// gets no prefix. The last value of a repeated key wins, because the engine
// annotates a logger and a plugin may annotate it again.
func prefixOf(attrs []slog.Attr) string {
	vals := map[string]string{}
	for _, a := range attrs {
		vals[a.Key] = a.Value.String()
	}
	if vals["kind"] == "" {
		return pairs(attrs)
	}
	head := vals["kind"]
	if vals["type"] != "" {
		head += " " + vals["type"]
	}
	if vals["instance"] != "" {
		head += "/" + vals["instance"]
	}
	return head
}

// debugAttrs is the machine detail of one plain record, at debug level.
func debugAttrs(r slog.Record, attrs []slog.Attr) string {
	var out []string
	seen := map[string]bool{"kind": true, "type": true, "instance": true, "group": true, EventAttrKey: true}
	add := func(a slog.Attr) {
		if seen[a.Key] {
			return
		}
		seen[a.Key] = true
		out = append(out, a.Key+"="+a.Value.String())
	}
	r.Attrs(func(a slog.Attr) bool { add(a); return true })
	for _, a := range attrs {
		add(a)
	}
	return strings.Join(out, ", ")
}

func pairs(attrs []slog.Attr) string {
	var out []string
	for _, a := range attrs {
		out = append(out, a.Key+"="+a.Value.String())
	}
	return strings.Join(out, " ")
}
