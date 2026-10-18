package human

import (
	"testing"
	"time"
)

// TestRenderTable pins the rendered line of every event the service emits: the
// table in render is the log's own contract, so one case per name is what a
// reader of the log is promised. A name that loses its case falls back to the
// name-plus-text form, which this table would then catch.
func TestRenderTable(t *testing.T) {
	cases := []struct {
		name string
		ev   Event
		want string
	}{
		{"run.start", New("run.start",
			Text("kind", "scheduled"), Text("run", "r1"), Text("config", "/c.yaml"), Text("storages", "exchange")),
			"scheduled run r1 starting — config /c.yaml, storages: exchange"},
		{"run.previous", New("run.previous",
			Text("run", "r0"), Text("unfinished", "true"), Text("last_good", "never")),
			"previous run r0 (unfinished=true, last good never)"},
		{"run.summary", New("run.summary",
			Text("kind", "manual"), Text("run", "r1"), Text("outcome", "ok"),
			Duration("took", 6*time.Second), Count("exit", 0)),
			"manual run r1 ok in 6s (exit 0)"},
		{"run.storage", New("run.storage",
			Text("storage", "exchange"), Text("outcome", "ok"), Count("objects", 3), Bytes("stored", 1024),
			Count("kept", 2), Count("deleted", 1), Bytes("freed", 512)),
			"exchange: ok, 3 objects, 1 KiB, kept 2, deleted 1, freed 512 B"},
		{"stage.start", New("stage.start", Text("config", "/c.yaml"), Text("dir", "/tmp/stage")),
			"stage starting — config /c.yaml, dir /tmp/stage"},
		{"stage.summary", New("stage.summary",
			Text("outcome", "ok"), Duration("took", 1500*time.Millisecond), Count("exit", 0)),
			"stage ok in 1.5s (exit 0)"},
		{"run.totals", New("run.totals",
			Count("ok", 2), Count("failed", 1), Count("excluded", 4), Bytes("raw", 2048), Bytes("stored", 1024)),
			"  artifacts: 2 ok, 1 failed, 4 excluded (2 KiB → 1 KiB)"},
		{"artifact.machine", New("artifact.machine", Text("detail", "g/n key=k sha256=a")),
			"  machine g/n key=k sha256=a"},
		{"discovery.summary", New("discovery.summary", Count("artifacts", 5), Count("covered", 2)),
			"discovery: 5 artifacts (2 covered or ignored)"},
		{"discovery.excluded", New("discovery.excluded",
			Text("object", "obj"), Text("kind", "secret"), Text("reason", "ignored")),
			"excluded obj (secret): ignored"},
		{"artifact.start", New("artifact.start", Text("artifact", "g/n"), Text("importance", "required")),
			"── g/n (required)"},
		{"artifact.done", New("artifact.done",
			Text("artifact", "g/n"), Bytes("raw", 2048), Bytes("stored", 1024), Duration("took", 2*time.Second)),
			"✓ g/n  2 KiB → 1 KiB (2.0x) in 2s"},
		{"artifact.done/skipped", New("artifact.done",
			Text("artifact", "g/n"), Bytes("raw", 2048), Bytes("stored", 1024), Duration("took", 2*time.Second),
			Count("skipped", 2), Count("padded", 1)),
			"✓ g/n  2 KiB → 1 KiB (2.0x) in 2s  (skipped 2, padded 1)"},
		{"artifact.failed", New("artifact.failed", Text("artifact", "g/n"), Text("reason", "boom")),
			"✗ g/n: boom"},
		{"artifact.stats", New("artifact.stats",
			Text("artifact", "g/n"), Count("skipped", 2), Count("padded", 1)),
			"   ↳ g/n: skipped 2, padded 1"},
		{"spool.write", New("spool.write", Text("file", "/s/a"), Bytes("size", 512)),
			"spool /s/a: 512 B"},
		{"retry", New("retry",
			Text("what", "upload"), Count("attempt", 2), Duration("wait", time.Second), Text("reason", "timeout")),
			"retry upload (attempt 2, waiting 1s): timeout"},
		{"source.dump.start", New("source.dump.start", Text("origin", "test:thing")),
			"dumping test:thing"},
		{"source.progress", New("source.progress", Bytes("dumped", 1024)),
			"1 KiB dumped so far"},
		{"processor.done", New("processor.done",
			Bytes("in", 2048), Bytes("out", 1024), Duration("took", time.Second)),
			"2 KiB → 1 KiB (2.0x) in 1s"},
		{"processor.failed", New("processor.failed",
			Bytes("in", 2048), Bytes("out", 1024), Duration("took", time.Second), Text("reason", "boom")),
			"✗ 2 KiB → 1 KiB in 1s: boom"},
		{"storage.upload.start", New("storage.upload.start", Text("artifact", "g/n")),
			"uploading g/n"},
		{"storage.upload.done", New("storage.upload.done", Text("artifact", "g/n"), Bytes("size", 1024)),
			"✓ g/n (1 KiB)"},
		{"storage.upload.failed", New("storage.upload.failed", Text("artifact", "g/n"), Text("reason", "boom")),
			"✗ g/n: boom"},
		{"storage.cap.refused", New("storage.cap.refused", Text("artifact", "g/n"), Text("reason", "over cap")),
			"✗ g/n: refused, over the configured cap — over cap"},
		{"storage.cap.overflow", New("storage.cap.overflow", Bytes("over", 1024), Text("reason", "floor")),
			"✗ over the configured cap by 1 KiB: floor"},
		{"lease.acquired", New("lease.acquired", Text("kind", "manual")),
			"manual run holds the writer lease"},
		{"lease.waiting", New("lease.waiting", Text("kind", "scheduled"), Text("reason", "held")),
			"scheduled run waiting for the writer lease: held"},
		{"lease.refused", New("lease.refused", Text("kind", "manual"), Text("reason", "held")),
			"manual run refused: held"},
		{"lease.failed", New("lease.failed", Text("kind", "manual"), Text("reason", "boom")),
			"✗ manual run could not take the writer lease: boom"},
		{"lease.released", New("lease.released"), "released the writer lease"},
		{"lease.release.failed", New("lease.release.failed", Text("reason", "boom")),
			"✗ releasing the writer lease: boom"},
		{"storage.multipart", New("storage.multipart",
			Text("artifact", "g/n"), Count("parts", 2), Count("total", 5), Bytes("part_size", 1<<20)),
			"multipart g/n: 2/5 parts of 1 MiB"},
		{"storage.retention", New("storage.retention",
			Count("kept", 3), Count("deleted", 2), Bytes("freed", 2048)),
			"retention: kept 3, deleted 2, freed 2 KiB"},
		{"storage.retention.failed", New("storage.retention.failed", Text("reason", "boom")),
			"✗ retention: boom"},
		{"storage.retention.keep-current", New("storage.retention.keep-current", Text("run", "r1")),
			"retention: never dropping the current run r1"},
		{"storage.retention.keep-floor", New("storage.retention.keep-floor",
			Text("run", "r1"), Text("reason", "verified")),
			"retention: keeping the last verified run r1: verified"},
		{"storage.retention.keep-in-flight", New("storage.retention.keep-in-flight",
			Text("run", "r1"), Text("reason", "recent")),
			"retention: keeping run r1: recent"},
		{"storage.retention.skipped", New("storage.retention.skipped", Text("reason", "nothing")),
			"retention: nothing deleted — nothing"},
		{"storage.retention.delete-failed", New("storage.retention.delete-failed",
			Text("run", "r1"), Text("reason", "boom")),
			"✗ retention: deleting run r1: boom"},
		{"storage.survey.failed", New("storage.survey.failed", Text("reason", "boom")),
			"✗ listing what the storage holds: boom"},
		{"storage.describe.failed", New("storage.describe.failed", Text("reason", "boom")),
			"✗ describing the storage: boom"},
		{"storage.check.failed", New("storage.check.failed", Text("reason", "boom")),
			"✗ the preflight check failed: boom"},
		{"catalog.saved", New("catalog.saved", Text("run", "r1")), "saved run r1"},
		{"catalog.failed", New("catalog.failed", Text("run", "r1"), Text("reason", "boom")),
			"✗ saving run r1: boom"},
		{"notify.sent", New("notify.sent", Text("event", "finished")), "✓ sent finished"},
		{"notify.failed", New("notify.failed", Text("event", "finished"), Text("reason", "boom")),
			"✗ finished: boom"},
		{"notify.check-failed", New("notify.check-failed", Text("event", "finished"), Text("reason", "boom")),
			"✗ finished: the check failed, the event is not delivered: boom"},
		{"source.file.padded", New("source.file.padded", Text("path", "/f"), Text("reason", "short")),
			"padded /f: short"},
		{"source.entry.skipped", New("source.entry.skipped", Text("path", "/f"), Text("reason", "socket")),
			"left out /f: socket"},
		{"storage.folder.empty", New("storage.folder.empty", Text("folder", "a/b"), Text("reason", "leftover")),
			"an empty folder was left behind: a/b (leftover)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Render("", c.ev, false); got != c.want {
				t.Errorf("Render(%s) = %q, want %q", c.ev.Name, got, c.want)
			}
		})
	}
}

// TestRenderPrefixesAnInstance: the same event under an instance's own logger
// reads as that instance's line.
func TestRenderPrefixesAnInstance(t *testing.T) {
	ev := New("catalog.saved", Text("run", "r1"))
	if got, want := Render("catalog kubernetes_configmap/main", ev, false), "catalog kubernetes_configmap/main: saved run r1"; got != want {
		t.Errorf("Render = %q, want %q", got, want)
	}
}

// TestRenderAppendsMachineDetailAtDebug: an object key and a digest stay out of
// the default line and appear behind --log-level debug, appended in braces.
func TestRenderAppendsMachineDetailAtDebug(t *testing.T) {
	ev := New("catalog.saved", Text("run", "r1"), ObjectKey("key", "runs/r1/manifest.json"), Digest("sha256", "abc"))
	if got, want := Render("", ev, false), "saved run r1"; got != want {
		t.Errorf("info line = %q, want %q", got, want)
	}
	got := Render("", ev, true)
	want := "saved run r1  {key=runs/r1/manifest.json, sha256=abc}"
	if got != want {
		t.Errorf("debug line = %q, want %q", got, want)
	}
}

// TestRenderFallback: an event with no case of its own still reads — its name
// and its text and count attributes, machine detail left out.
func TestRenderFallback(t *testing.T) {
	ev := New("brand.new", Text("k", "v"), Count("n", 2), Bytes("b", 10), ObjectKey("key", "hidden"))
	if got, want := Render("", ev, false), "brand.new k=v n=2"; got != want {
		t.Errorf("fallback = %q, want %q", got, want)
	}
}

// TestLineEventCarriesTheClock pins the summary form: the same table, the log's
// clock in front.
func TestLineEventCarriesTheClock(t *testing.T) {
	at := time.Date(2026, 10, 18, 4, 13, 51, 0, time.UTC)
	ev := New("run.summary", Text("kind", "scheduled"), Text("run", "r1"), Text("outcome", "ok"),
		Duration("took", 2*time.Second), Count("exit", 0))
	got := LineEvent(at, "", ev, false)
	want := "[04:13:51] scheduled run r1 ok in 2s (exit 0)"
	if got != want {
		t.Errorf("LineEvent = %q, want %q", got, want)
	}
}

// TestArtifactLabel: the group is a namespace, not part of the name; a group-less
// artifact is its name alone.
func TestArtifactLabel(t *testing.T) {
	if got := ArtifactLabel("g", "n"); got != "g/n" {
		t.Errorf("ArtifactLabel(g, n) = %q, want %q", got, "g/n")
	}
	if got := ArtifactLabel("", "n"); got != "n" {
		t.Errorf("ArtifactLabel(, n) = %q, want %q", got, "n")
	}
}

// TestValueOfRendersByKind pins the one place an attribute's value becomes a
// string: a size carries a unit, a duration its rounding, text its own string and
// a count its number.
func TestValueOfRendersByKind(t *testing.T) {
	cases := []struct {
		in   Attr
		want string
	}{
		{Bytes("k", 1024), "1 KiB"},
		{Duration("k", 1500*time.Millisecond), "1.5s"},
		{Text("k", "v"), "v"},
		{Count("k", 7), "7"},
	}
	for _, c := range cases {
		if got := valueOf(c.in); got != c.want {
			t.Errorf("valueOf(%+v) = %q, want %q", c.in, got, c.want)
		}
	}
}
