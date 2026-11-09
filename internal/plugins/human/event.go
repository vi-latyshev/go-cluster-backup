package human

import (
	"log/slog"
	"time"
)

// Kind says how one attribute is rendered, and whether the default output shows
// it at all. It is what lets ONE renderer decide what a person reads: a plain
// key=value pair carries no such decision, which is how a sha256 and a
// 40-character object key end up on the screen at info level.
type Kind uint8

const (
	// KText is free text a person reads: a reason, a path, an origin, a name.
	KText Kind = iota
	// KCount is an integer count.
	KCount
	// KSize is a byte count, rendered with a unit.
	KSize
	// KDuration is a duration, rounded.
	KDuration
	// KKey is an object key: machine detail, shown only at debug.
	KKey
	// KDigest is a content hash: machine detail, shown only at debug.
	KDigest
)

// Attr is one typed attribute of an event.
type Attr struct {
	Key  string
	Kind Kind
	Text string
	Num  int64
	Dur  time.Duration
}

// The constructors name what a value IS, so the renderer never guesses:
func Text(key, v string) Attr                   { return Attr{Key: key, Kind: KText, Text: v} }
func Count(key string, v int64) Attr            { return Attr{Key: key, Kind: KCount, Num: v} }
func Bytes(key string, v int64) Attr            { return Attr{Key: key, Kind: KSize, Num: v} }
func Duration(key string, v time.Duration) Attr { return Attr{Key: key, Kind: KDuration, Dur: v} }
func ObjectKey(key, v string) Attr              { return Attr{Key: key, Kind: KKey, Text: v} }
func Digest(key, v string) Attr                 { return Attr{Key: key, Kind: KDigest, Text: v} }

// Event is one thing that happened, named and described by typed attributes.
// The name is stable — a reader greps for it and a test matches on it — and the
// renderer is the only place that decides how it reads. An emitter never builds
// a sentence: it names the event and hands over the values.
//
// Severity is the level the event is logged at. It is carried by the event
// rather than assumed by the emitter's helper, so `--log-level warn` and
// `--log-level error` still show the failures: a run whose catalog could not be
// saved must not be silent because its line was logged at info.
type Event struct {
	Name     string
	Attrs    []Attr
	Severity slog.Level
}

// New builds one informational event.
func New(name string, attrs ...Attr) Event { return Event{Name: name, Attrs: attrs} }

// Warning builds an event a person reads as a warning: it did not fail, and it
// is not routine either.
func Warning(name string, attrs ...Attr) Event {
	return Event{Name: name, Attrs: attrs, Severity: slog.LevelWarn}
}

// Failure builds an event something failed: an artifact, a storage, the
// catalog, a notification.
func Failure(name string, attrs ...Attr) Event {
	return Event{Name: name, Attrs: attrs, Severity: slog.LevelError}
}

// Debug builds an event that is machine detail: like the object keys and digests
// the renderer appends, it is written only at --log-level debug, so a routine
// fact the default output need not carry never reaches a person's screen.
func Debug(name string, attrs ...Attr) Event {
	return Event{Name: name, Attrs: attrs, Severity: slog.LevelDebug}
}
