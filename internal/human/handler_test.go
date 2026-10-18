package human

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

// TestEventConstructorsNameWhatAValueIs: an emitter names the event and the kind
// of each value, and the constructor is what carries the kind.
func TestEventConstructorsNameWhatAValueIs(t *testing.T) {
	cases := []struct {
		got  Attr
		want Attr
	}{
		{Text("k", "v"), Attr{Key: "k", Kind: KText, Text: "v"}},
		{Count("k", 3), Attr{Key: "k", Kind: KCount, Num: 3}},
		{Bytes("k", 4), Attr{Key: "k", Kind: KSize, Num: 4}},
		{Duration("k", time.Second), Attr{Key: "k", Kind: KDuration, Dur: time.Second}},
		{ObjectKey("k", "o"), Attr{Key: "k", Kind: KKey, Text: "o"}},
		{Digest("k", "d"), Attr{Key: "k", Kind: KDigest, Text: "d"}},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("constructor = %+v, want %+v", c.got, c.want)
		}
	}
	ev := New("e", Text("k", "v"))
	if ev.Name != "e" || len(ev.Attrs) != 1 || ev.Severity != slog.LevelInfo {
		t.Errorf("New = %+v, want one info event", ev)
	}
}

// TestEventSeverities: the event carries the level it is logged at, so a failure
// stays visible under --log-level error.
func TestEventSeverities(t *testing.T) {
	cases := []struct {
		got  Event
		want slog.Level
	}{
		{New("e"), slog.LevelInfo},
		{Warning("e", Text("k", "v")), slog.LevelWarn},
		{Failure("e"), slog.LevelError},
		{Debug("e"), slog.LevelDebug},
	}
	for _, c := range cases {
		if c.got.Severity != c.want {
			t.Errorf("%s severity = %v, want %v", c.got.Name, c.got.Severity, c.want)
		}
	}
	if len(Warning("e", Text("k", "v")).Attrs) != 1 {
		t.Errorf("Warning dropped its attributes")
	}
	if len(Failure("e", Text("k", "v")).Attrs) != 1 || len(Debug("e", Text("k", "v")).Attrs) != 1 {
		t.Errorf("Failure or Debug dropped its attributes")
	}
}

// TestHandlerRendersAnEvent: an event handed over as one attribute becomes its
// line, with the record's own clock in front.
func TestHandlerRendersAnEvent(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelInfo)
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)
	r := slog.NewRecord(at, slog.LevelInfo, "", 0)
	r.AddAttrs(slog.Any(EventAttrKey, New("catalog.saved", Text("run", "r1"))))
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "[04:13:51] saved run r1\n"; got != want {
		t.Errorf("Handle = %q, want %q", got, want)
	}
}

// TestHandlerRendersAPlainRecord: a plugin's or the command layer's own
// diagnostic carries no event; it is rendered as it stands, under its instance's
// prefix, and its extra attributes appear only at debug.
func TestHandlerRendersAPlainRecord(t *testing.T) {
	var buf bytes.Buffer
	base := NewHandler(&buf, slog.LevelInfo)
	h := base.WithAttrs([]slog.Attr{slog.String("kind", "source"), slog.String("type", "postgres"), slog.String("instance", "main")})
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)
	r := slog.NewRecord(at, slog.LevelInfo, "connected", 0)
	r.AddAttrs(slog.String("address", "host:5432"))
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "[04:13:51] source postgres/main: connected\n"; got != want {
		t.Errorf("info record = %q, want %q", got, want)
	}

	buf.Reset()
	debug := NewHandler(&buf, slog.LevelDebug).WithAttrs([]slog.Attr{
		slog.String("kind", "source"), slog.String("type", "postgres"), slog.String("instance", "main"),
		slog.String("extra", "v"),
	})
	r = slog.NewRecord(at, slog.LevelInfo, "connected", 0)
	r.AddAttrs(slog.String("address", "host:5432"), slog.Any(EventAttrKey, "not-an-event"))
	if err := debug.Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	want := "[04:13:51] source postgres/main: connected  {address=host:5432, extra=v}\n"
	if got := buf.String(); got != want {
		t.Errorf("debug record = %q, want %q", got, want)
	}
}

// TestHandlerRendersAnEngineRecordWithoutAPrefix: a record carrying no instance
// attributes gets no prefix, and its attributes read as bare pairs.
func TestHandlerRendersAnEngineRecordWithoutAPrefix(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(&buf, slog.LevelDebug).WithAttrs([]slog.Attr{slog.String("run", "r1")})
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)
	r := slog.NewRecord(at, slog.LevelInfo, "starting", 0)
	if err := h.Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	want := "[04:13:51] run=r1: starting  {run=r1}\n"
	if got := buf.String(); got != want {
		t.Errorf("engine record = %q, want %q", got, want)
	}
}

// TestHandlerEnabled: the handler answers for its own level, which is what
// --log-level warn and --log-level error rely on.
func TestHandlerEnabled(t *testing.T) {
	h := NewHandler(&bytes.Buffer{}, slog.LevelWarn)
	cases := []struct {
		level slog.Level
		want  bool
	}{
		{slog.LevelDebug, false},
		{slog.LevelInfo, false},
		{slog.LevelWarn, true},
		{slog.LevelError, true},
	}
	for _, c := range cases {
		if got := h.Enabled(context.Background(), c.level); got != c.want {
			t.Errorf("Enabled(%v) = %t, want %t", c.level, got, c.want)
		}
	}
}

// TestHandlerWithGroup: a group name is flattened into the attribute names the
// renderer prints; an empty name derives nothing.
func TestHandlerWithGroup(t *testing.T) {
	base := NewHandler(&bytes.Buffer{}, slog.LevelInfo)
	if got := base.WithGroup(""); got != slog.Handler(base) {
		t.Errorf("WithGroup(\"\") derived a handler")
	}
	grouped := base.WithGroup("g").(*Handler)
	if len(grouped.attrs) != 1 || grouped.attrs[0].Key != "group" || grouped.attrs[0].Value.String() != "g" {
		t.Errorf("WithGroup kept %+v", grouped.attrs)
	}
	if len(base.attrs) != 0 {
		t.Errorf("WithGroup mutated the receiver")
	}
}

// TestPrefixOf: an instance's own attributes render as "<kind> <type>/<instance>",
// the last value of a repeated key winning; a record with no kind renders as
// bare key=value pairs.
func TestPrefixOf(t *testing.T) {
	cases := []struct {
		name  string
		attrs []slog.Attr
		want  string
	}{
		{"kind type instance", []slog.Attr{
			slog.String("kind", "source"), slog.String("type", "postgres"), slog.String("instance", "main"),
		}, "source postgres/main"},
		{"kind type", []slog.Attr{
			slog.String("kind", "storage"), slog.String("type", "local"),
		}, "storage local"},
		{"kind only", []slog.Attr{slog.String("kind", "engine")}, "engine"},
		{"last repeated wins", []slog.Attr{
			slog.String("kind", "source"), slog.String("instance", "a"), slog.String("instance", "b"),
		}, "source/b"},
		{"no kind", []slog.Attr{slog.String("run", "r1"), slog.Int("attempt", 2)}, "run=r1 attempt=2"},
		{"empty", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := prefixOf(c.attrs); got != c.want {
				t.Errorf("prefixOf = %q, want %q", got, c.want)
			}
		})
	}
}

// TestDebugAttrs: the machine detail of a plain record is its own attributes,
// with the instance's own and the event's kept out.
func TestDebugAttrs(t *testing.T) {
	attrs := []slog.Attr{slog.String("kind", "source"), slog.String("type", "postgres"), slog.String("extra", "v")}
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)
	r := slog.NewRecord(at, slog.LevelInfo, "m", 0)
	r.AddAttrs(slog.String("address", "host:5432"), slog.String("extra", "record"), slog.String("group", "g"))
	got := debugAttrs(r, attrs)
	want := "address=host:5432, extra=record"
	if got != want {
		t.Errorf("debugAttrs = %q, want %q", got, want)
	}
}

// TestPairs: a record with no kind renders its attributes as bare pairs.
func TestPairs(t *testing.T) {
	got := pairs([]slog.Attr{slog.String("run", "r1"), slog.Int("n", 2)})
	if want := "run=r1 n=2"; got != want {
		t.Errorf("pairs = %q, want %q", got, want)
	}
}
