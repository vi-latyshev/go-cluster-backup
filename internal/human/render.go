package human

import (
	"fmt"
	"strings"
	"time"
)

// Render is the ONE renderer of this service's log: every event, from the
// engine and from every plugin, becomes its line here and nowhere else. It
// decides which attributes a person sees (a reason, a storage, a path, a size,
// a duration) and which stay behind --log-level debug (an object key, a
// sha256); an emitter that pre-formatted its own sentence would have taken that
// decision away from it.
//
// prefix is the instance the event belongs to — "<kind> <type>/<instance>" — or
// empty for the engine's own events.
func Render(prefix string, ev Event, debug bool) string {
	line := render(ev)
	if prefix != "" {
		line = prefix + ": " + line
	}
	if debug {
		if extra := machine(ev); extra != "" {
			line += "  {" + extra + "}"
		}
	}
	return line
}

// LineEvent is Render with the log's own clock in front, for the lines a
// command writes as its result rather than through a logger — the closing
// summary. It goes through the same table, so the summary reads like every
// other line.
func LineEvent(at time.Time, prefix string, ev Event, debug bool) string {
	return "[" + Stamp(at) + "] " + Render(prefix, ev, debug)
}

// render is the table: one case per event name, each composing the sentence
// from the attributes it names. An event name with no case renders as the name
// plus its text attributes, so a new event is readable before its case is
// written — never silently blank.
func render(ev Event) string {
	b := bagOf(ev)
	switch ev.Name {
	// ── the run's own frame ─────────────────────────────────────────────
	case "run.start":
		return fmt.Sprintf("%s run %s starting — config %s, storages: %s", b.text("kind"), b.text("run"), b.text("config"), b.text("storages"))
	case "run.previous":
		return fmt.Sprintf("previous run %s (unfinished=%s, last good %s)", b.text("run"), b.text("unfinished"), b.text("last_good"))
	case "run.summary":
		return fmt.Sprintf("%s run %s %s in %s (exit %d)", b.text("kind"), b.text("run"), b.text("outcome"), Dur(b.dur("took")), b.num("exit"))
	case "run.storage":
		return fmt.Sprintf("%s: %s, %d objects, %s, kept %d, deleted %d, freed %s",
			b.text("storage"), b.text("outcome"), b.num("objects"), Size(b.num("stored")),
			b.num("kept"), b.num("deleted"), Size(b.num("freed")))
	case "stage.start":
		return fmt.Sprintf("stage starting — config %s, dir %s", b.text("config"), b.text("dir"))
	case "stage.summary":
		return fmt.Sprintf("stage %s in %s (exit %d)", b.text("outcome"), Dur(b.dur("took")), b.num("exit"))

	// ── what the run saw ────────────────────────────────────────────────
	case "run.totals":
		return fmt.Sprintf("  artifacts: %d ok, %d failed, %d excluded (%s → %s)",
			b.num("ok"), b.num("failed"), b.num("excluded"), Size(b.num("raw")), Size(b.num("stored")))
	case "artifact.machine":
		return fmt.Sprintf("  machine %s", b.text("detail"))
	case "discovery.summary":
		return fmt.Sprintf("discovery: %d artifacts (%d covered or ignored)", b.num("artifacts"), b.num("covered"))
	case "discovery.excluded":
		return fmt.Sprintf("excluded %s (%s): %s", b.text("object"), b.text("kind"), b.text("reason"))
	case "artifact.start":
		return fmt.Sprintf("── %s (%s)", b.text("artifact"), b.text("importance"))
	case "artifact.done":
		line := fmt.Sprintf("✓ %s  %s → %s (%s) in %s", b.text("artifact"),
			Size(b.num("raw")), Size(b.num("stored")), Ratio(b.num("raw"), b.num("stored")), Dur(b.dur("took")))
		// What the artifact is NOT a copy of, when it is not a copy of
		// everything: entries the source left out, files it padded. Zero says
		// nothing and needs no line.
		if sk, pad := b.num("skipped"), b.num("padded"); sk != 0 || pad != 0 {
			line += fmt.Sprintf("  (skipped %d, padded %d)", sk, pad)
		}
		return line
	case "artifact.failed":
		return fmt.Sprintf("✗ %s: %s", b.text("artifact"), b.text("reason"))
	case "artifact.stats":
		return fmt.Sprintf("   ↳ %s: skipped %d, padded %d", b.text("artifact"), b.num("skipped"), b.num("padded"))
	case "spool.write":
		return fmt.Sprintf("spool %s: %s", b.text("file"), Size(b.num("size")))

	// ── one plugin's own step ───────────────────────────────────────────
	case "retry":
		return fmt.Sprintf("retry %s (attempt %d, waiting %s): %s", b.text("what"), b.num("attempt"), Dur(b.dur("wait")), b.text("reason"))
	case "source.dump.start":
		return "dumping " + b.text("origin")
	case "source.progress":
		return Size(b.num("dumped")) + " dumped so far"
	case "processor.done":
		return fmt.Sprintf("%s → %s (%s) in %s", Size(b.num("in")), Size(b.num("out")),
			Ratio(b.num("in"), b.num("out")), Dur(b.dur("took")))
	case "processor.failed":
		return fmt.Sprintf("✗ %s → %s in %s: %s", Size(b.num("in")), Size(b.num("out")), Dur(b.dur("took")), b.text("reason"))
	case "storage.upload.start":
		return "uploading " + b.text("artifact")
	case "storage.upload.done":
		return fmt.Sprintf("✓ %s (%s)", b.text("artifact"), Size(b.num("size")))
	case "storage.upload.failed":
		return fmt.Sprintf("✗ %s: %s", b.text("artifact"), b.text("reason"))
	case "storage.cap.refused":
		return fmt.Sprintf("✗ %s: refused, over the configured cap — %s", b.text("artifact"), b.text("reason"))
	case "storage.cap.overflow":
		return fmt.Sprintf("✗ over the configured cap by %s: %s", Size(b.num("over")), b.text("reason"))
	case "lease.acquired":
		return fmt.Sprintf("%s run holds the writer lease", b.text("kind"))
	case "lease.waiting":
		return fmt.Sprintf("%s run waiting for the writer lease: %s", b.text("kind"), b.text("reason"))
	case "lease.refused":
		return fmt.Sprintf("%s run refused: %s", b.text("kind"), b.text("reason"))
	case "lease.failed":
		return fmt.Sprintf("✗ %s run could not take the writer lease: %s", b.text("kind"), b.text("reason"))
	case "lease.released":
		return "released the writer lease"
	case "lease.release.failed":
		return fmt.Sprintf("✗ releasing the writer lease: %s", b.text("reason"))
	case "storage.multipart":
		return fmt.Sprintf("multipart %s: %d/%d parts of %s", b.text("artifact"),
			b.num("parts"), b.num("total"), Size(b.num("part_size")))
	case "storage.retention":
		return fmt.Sprintf("retention: kept %d, deleted %d, freed %s", b.num("kept"), b.num("deleted"), Size(b.num("freed")))
	case "storage.retention.failed":
		return "✗ retention: " + b.text("reason")
	case "storage.retention.keep-current":
		return "retention: never dropping the current run " + b.text("run")
	case "storage.retention.keep-floor":
		return fmt.Sprintf("retention: keeping the last verified run %s: %s", b.text("run"), b.text("reason"))
	case "storage.retention.keep-in-flight":
		return fmt.Sprintf("retention: keeping run %s: %s", b.text("run"), b.text("reason"))
	case "storage.retention.skipped":
		return "retention: nothing deleted — " + b.text("reason")
	case "storage.retention.delete-failed":
		return fmt.Sprintf("✗ retention: deleting run %s: %s", b.text("run"), b.text("reason"))
	case "storage.survey.failed":
		return "✗ listing what the storage holds: " + b.text("reason")
	case "storage.describe.failed":
		return "✗ describing the storage: " + b.text("reason")
	case "storage.check.failed":
		return "✗ the preflight check failed: " + b.text("reason")
	case "catalog.saved":
		return "saved run " + b.text("run")
	case "catalog.failed":
		return fmt.Sprintf("✗ saving run %s: %s", b.text("run"), b.text("reason"))
	case "notify.sent":
		return "✓ sent " + b.text("event")
	case "notify.failed":
		return fmt.Sprintf("✗ %s: %s", b.text("event"), b.text("reason"))
	case "notify.check-failed":
		return fmt.Sprintf("✗ %s: the check failed, the event is not delivered: %s", b.text("event"), b.text("reason"))
	case "source.file.padded":
		return fmt.Sprintf("padded %s: %s", b.text("path"), b.text("reason"))
	case "source.entry.skipped":
		return fmt.Sprintf("left out %s: %s", b.text("path"), b.text("reason"))
	case "storage.folder.empty":
		return fmt.Sprintf("an empty folder was left behind: %s (%s)", b.text("folder"), b.text("reason"))
	}
	return fallback(ev)
}

// fallback renders an event with no case: its name and its text attributes, so
// a new event is readable the moment it is emitted.
func fallback(ev Event) string {
	parts := []string{ev.Name}
	for _, a := range ev.Attrs {
		if a.Kind == KText || a.Kind == KCount {
			parts = append(parts, a.Key+"="+valueOf(a))
		}
	}
	return strings.Join(parts, " ")
}

// machine is the machine detail of one event — the object keys and the digests
// — rendered only at debug. Everything else is in the sentence already.
func machine(ev Event) string {
	var out []string
	for _, a := range ev.Attrs {
		if a.Kind == KKey || a.Kind == KDigest {
			out = append(out, a.Key+"="+a.Text)
		}
	}
	return strings.Join(out, ", ")
}

func valueOf(a Attr) string {
	switch a.Kind {
	case KSize:
		return Size(a.Num)
	case KDuration:
		return Dur(a.Dur)
	default:
		if a.Text != "" {
			return a.Text
		}
		return fmt.Sprintf("%d", a.Num)
	}
}

// ArtifactLabel is how one artifact is named in the log: its group and its name,
// which is what the manifest and the catalog key it under. It is here rather
// than in each caller because the engine and the command layer both render an
// artifact's name and the two must never drift.
func ArtifactLabel(group, name string) string {
	if group == "" {
		return name
	}
	return group + "/" + name
}

// bag is one event's attributes by key, for the table above.
type bag map[string]Attr

func bagOf(ev Event) bag {
	b := make(bag, len(ev.Attrs))
	for _, a := range ev.Attrs {
		b[a.Key] = a
	}
	return b
}

func (b bag) text(k string) string { return valueOf(b[k]) }
func (b bag) num(k string) int64   { return b[k].Num }
func (b bag) dur(k string) time.Duration {
	return b[k].Dur
}
