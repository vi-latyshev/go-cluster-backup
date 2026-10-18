package cli

import (
	"io"
	"strconv"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// writeLine writes one plain human line: the [HH:MM:SS] clock, the rendered
// message, a newline. The restore plans and reports use it; everything the run
// itself says goes through writeEvent, the same renderer the log uses.
func writeLine(w io.Writer, at time.Time, format string, args ...any) {
	writef(w, "%s\n", human.Line(at, format, args...))
}

// writeEvent writes one event as the closing block of a command writes it: the
// same renderer the log uses, so the summary reads like every other line and
// the machine detail stays behind --log-level debug here too.
func writeEvent(w io.Writer, at time.Time, ev human.Event, debug bool) {
	writef(w, "%s\n", human.LineEvent(at, "", ev, debug))
}

// printRunReport renders the closing block of one run or stage: the outcome
// first, then the totals, then one line per storage. It is a summary of work
// already described line by line as it happened, so it names no artifact's key
// and no sha256 — those live in the manifest and the catalog. With machine true
// (--log-level debug) it adds one line per artifact carrying them.
//
// It describes work already done, so a write failure cannot change the outcome:
// the exit code is the report's, by contract.
func printRunReport(w io.Writer, rep model.RunReport, machine bool) {
	now := time.Now()
	if rep.RunID == "" {
		writeEvent(w, now, human.New("stage.summary",
			human.Text("outcome", string(rep.Outcome)),
			human.Duration("took", rep.Duration.Std()),
			human.Count("exit", int64(rep.ExitCode))), machine)
	} else {
		writeEvent(w, now, human.New("run.summary",
			human.Text("kind", string(model.NormalizeKind(rep.Kind))),
			human.Text("run", rep.RunID),
			human.Text("outcome", string(rep.Outcome)),
			human.Duration("took", rep.Duration.Std()),
			human.Count("exit", int64(rep.ExitCode))), machine)
	}
	if rep.Previous.RunID != "" {
		lastGood := "never"
		if !rep.Previous.LastGoodAt.IsZero() {
			lastGood = rep.Previous.LastGoodAt.UTC().Format(time.RFC3339)
		}
		writeEvent(w, now, human.New("run.previous",
			human.Text("run", rep.Previous.RunID),
			human.Text("unfinished", strconv.FormatBool(rep.Previous.Unfinished)),
			human.Text("last_good", lastGood)), machine)
	}

	ok, failed, raw, stored := 0, 0, int64(0), int64(0)
	for _, a := range rep.Artifacts {
		if a.Outcome == model.ArtifactOK {
			ok++
			raw += a.RawBytes
			stored += a.Bytes
			continue
		}
		failed++
	}
	writeEvent(w, now, human.New("run.totals",
		human.Count("ok", int64(ok)), human.Count("failed", int64(failed)),
		human.Count("excluded", int64(len(rep.Excluded))),
		human.Bytes("raw", raw), human.Bytes("stored", stored)), machine)
	// A failure is the one thing the summary must not bury: name each one with
	// the reason it failed, before the storage lines.
	for _, a := range rep.Artifacts {
		if a.Outcome != model.ArtifactOK {
			reason := a.Error
			if a.Hint != "" {
				// The hint is the actionable half; the full reason is one line
				// above it in the log, so the summary leads with what to do.
				reason = a.Hint
			}
			writeEvent(w, now, human.New("artifact.failed",
				human.Text("artifact", human.ArtifactLabel(a.Artifact.Group, a.Artifact.Name)),
				human.Text("reason", reason)), machine)
		}
		// What the artifact is not a copy of, when it is not a copy of
		// everything (see model.DumpStats).
		if a.Stats.SkippedEntries != 0 || a.Stats.PaddedFiles != 0 {
			writeEvent(w, now, human.New("artifact.stats",
				human.Text("artifact", human.ArtifactLabel(a.Artifact.Group, a.Artifact.Name)),
				human.Count("skipped", a.Stats.SkippedEntries),
				human.Count("padded", a.Stats.PaddedFiles)), machine)
		}
	}
	// The covered/ignored objects are machine detail: the summary counts them and
	// names each one only at --log-level debug. A clean night must not read as a
	// problem; an undeclared claim is a failed artifact and appears above.
	if machine {
		for _, x := range rep.Excluded {
			writeEvent(w, now, human.New("discovery.excluded",
				human.Text("object", x.Name), human.Text("kind", string(x.Kind)), human.Text("reason", x.Reason)), machine)
		}
	}
	for _, s := range rep.Storages {
		writeEvent(w, now, human.New("run.storage",
			human.Text("storage", s.Storage), human.Text("outcome", s.Outcome),
			human.Count("objects", int64(s.Objects)), human.Bytes("stored", s.Bytes),
			human.Count("kept", int64(s.Kept)), human.Count("deleted", int64(len(s.Deleted))),
			human.Bytes("freed", s.Freed)), machine)
		if s.Error != "" {
			writeEvent(w, now, human.New("storage.upload.failed",
				human.Text("artifact", s.Storage), human.Text("reason", s.Error)), machine)
		}
	}
	if machine {
		for _, a := range rep.Artifacts {
			writeEvent(w, now, human.New("artifact.machine",
				human.Text("detail", human.ArtifactLabel(a.Artifact.Group, a.Artifact.Name)+
					" key="+a.Key+" sha256="+a.SHA256+
					" bytes="+strconv.FormatInt(a.Bytes, 10)+" raw_bytes="+strconv.FormatInt(a.RawBytes, 10)+
					" duration="+a.Duration.String())), machine)
		}
	}
}

// printFetchPlan renders what a fetch would do before it does it: the run, the
// object as the manifest records it, and every storage of the walk with its
// verdict, so a multi-gigabyte transfer is announced and a refusal is answered
// with facts. It goes to the report writer, which is stderr when the artifact
// itself will go to stdout.
func printFetchPlan(w io.Writer, plan engine.FetchPlan) {
	now := time.Now()
	if plan.RunID == "" {
		// A by-key fetch has no manifest: only the key is known before the
		// download.
		writeLine(w, now, "plan: key %s", plan.Key)
	} else {
		writeLine(w, now, "plan: run %s artifact %s (%s, %s stored, %s raw)",
			plan.RunID, plan.Artifact, plan.Sensitivity, human.Size(plan.Bytes), human.Size(plan.RawBytes))
	}
	for _, h := range plan.Storages {
		if h.Holds {
			writeLine(w, now, "  %s: holds a copy", h.Storage)
			continue
		}
		writeLine(w, now, "  %s: %s", h.Storage, h.Reason)
	}
}

// printFetchReport renders what one fetch resolved and where it wrote from. It
// goes to the report writer, which is stderr when the artifact itself went to
// stdout.
func printFetchReport(w io.Writer, rep engine.FetchReport) {
	now := time.Now()
	writeLine(w, now, "✓ fetched %s from %s (%s stored, %s raw, verified=%t, %s)",
		rep.Artifact, rep.Storage, human.Size(rep.Bytes), human.Size(rep.RawBytes), rep.Verified, rep.Sensitivity)
	for _, s := range rep.Skipped {
		writeLine(w, now, "  %s: %s", s.Storage, s.Reason)
	}
}
