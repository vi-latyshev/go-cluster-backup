package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// Stage runs the sources of the stage section and leaves their sealed artifacts
// in stage.dir, one file per artifact named <artifact name><ext>. That name is
// the contract the run-side staged source reads back: it looks for the file
// named <name> or <name>.<ext> in the same directory — never a file whose name
// merely starts with this one. A stage report carries no run id: the file name
// is the identity, and the stage section has no run section to read one from.
func (e *Engine) Stage(ctx context.Context, req StageRequest) (model.RunReport, error) {
	cfg := e.cfg
	if cfg.Stage == nil || cfg.Stage.Dir == "" {
		return model.RunReport{}, errors.New("stage: config needs the stage section with a dir")
	}
	if len(e.stageSources) == 0 {
		return model.RunReport{}, errors.New("stage: config has no stage.sources")
	}
	// The stage's own check set: its sources and the processors their pipelines
	// name, never the run's instances — this container has other mounts.
	if errs := e.binaryErrors(scopeStage); len(errs) > 0 {
		return model.RunReport{}, fmt.Errorf("preflight: %w", errors.Join(errs...))
	}
	checks := e.scopedChecks(ctx, e.checkPolicy(), scopeStage)
	started := time.Now().UTC()
	rep := model.RunReport{Schema: model.ManifestSchema, StartedAt: started, Outcome: model.OutcomeRunning}
	e.event(human.New("stage.start", human.Text("config", req.ConfigPath), human.Text("dir", cfg.Stage.Dir)))

	outcome := model.OutcomeDelivered
	for _, sb := range e.stageSources {
		e.event(human.New("artifact.start",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)),
			human.Text("importance", string(sb.artifact.Importance))))
		ar := e.stageOne(ctx, sb, checks)
		if ar.Outcome != model.ArtifactOK {
			if ar.Artifact.Importance == model.ImportanceRequired {
				outcome = model.OutcomeFailed
			} else if outcome == model.OutcomeDelivered {
				outcome = model.OutcomeDegraded
			}
		}
		rep.Artifacts = append(rep.Artifacts, ar)
	}
	rep.Duration = model.Duration(time.Since(started))
	rep.Outcome = outcome
	switch outcome {
	case model.OutcomeDegraded:
		rep.ExitCode = cfg.Stage.DegradedExitCode
	case model.OutcomeFailed:
		rep.ExitCode = cfg.Stage.FailedExitCode
	}
	return rep, nil
}

// stageOne stages one artifact. ar is a named result so the deferred duration
// lands on the value the caller receives: `return ar` copies a struct, so a
// closure over a local would set the duration on a copy the caller never sees.
func (e *Engine) stageOne(ctx context.Context, sb sourceBinding, checks failedChecks) (ar model.ArtifactResult) {
	ar = model.ArtifactResult{Artifact: sb.artifact, Pipeline: sb.pipeline, Outcome: model.ArtifactFailed}
	start := time.Now()
	defer func() { ar.Duration = model.Duration(time.Since(start)) }()
	if err := checks.err(sb.checkKind, sb.name); err != nil {
		ar.Error = fmt.Sprintf("source check: %v", err)
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	if err := pipelineCheckProblem(checks, sb.refs); err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	if strings.Contains(sb.artifact.Name, "/") || strings.HasPrefix(sb.artifact.Name, ".") {
		ar.Error = fmt.Sprintf("stage artifact name %q must be a plain file name", sb.artifact.Name)
		return ar
	}
	// A stage writes one sealed file per artifact, and the run side reads it
	// back by that one name. An instance whose domain holds several artifacts
	// has no such name, and staging only the first of them would stage a
	// fraction of the data under a name that reads as all of it.
	if en, ok := sb.inst.(source.Enumerator); ok {
		units, single, err := listUnits(ctx, e.checkPolicy(), sb.name, en)
		switch {
		case single:
		case err != nil:
			ar.Error = fmt.Sprintf("stage does not expand a multi-unit source: enumerating %s: %v", sb.name, err)
			e.event(human.Failure("artifact.failed",
				human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
			return ar
		default:
			ar.Error = fmt.Sprintf("stage does not expand a multi-unit source: %s holds %d artifacts (the first is %q); stage a source that is one artifact, or a run that backs them up",
				sb.name, len(units), units[0].Name)
			e.event(human.Failure("artifact.failed",
				human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
			return ar
		}
	}
	plan := artifactPlan{instance: sb.name, srcType: sb.typ, src: sb.inst, artifact: sb.artifact, pipeline: sb.pipeline,
		refs: sb.refs, procs: sb.procs, stages: encodeStages(sb.procs)}
	if err := e.resolveTraits(ctx, &plan); err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	ar.Artifact = plan.artifact
	e.sourceEvent(plan.srcType, plan.instance, human.New("source.dump.start", human.Text("origin", plan.source.Origin)))
	dir := e.cfg.Stage.Dir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		ar.Error = err.Error()
		return ar
	}
	// The record carries the artifact's exact file name tail and the chain; from
	// here on the file is named by it, never by a directory scan.
	rec := source.StagedRecord{Ext: plan.artifact.Ext, Chain: plan.chain}
	// What a previous stage left for this artifact goes before anything new is
	// produced: a stage directory that survives the container would otherwise
	// hand the run yesterday's artifact and record as tonight's after a stage
	// that failed — the run cannot tell the two apart, and the mode-0644 file
	// from last night reads exactly like one written now. With nothing left
	// behind, a failed stage fails the run's staged source loudly.
	if err := clearStaged(dir, sb.artifact.Name, rec); err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	// Every attempt publishes through its own temp file, so a half-written
	// artifact is never visible to the run-side "staged" source, and a stage
	// config that carries a retry policy gets that many attempts. The chain
	// record is published inside the dump's own step — after the bytes are
	// produced, before the artifact's rename — so a stage that fails always
	// leaves the "no record, no artifact" pair the run refuses: a record
	// without an artifact fails the staged source loudly ("no staged
	// artifact"), while an artifact without a record would be bytes nobody can
	// prove are sealed.
	final := filepath.Join(dir, source.StagedArtifactName(sb.artifact.Name, rec))
	var raw, bytes int64
	var sum []byte
	_, err := attempt(ctx, e.checkPolicy(), "stage "+sb.artifact.Name, func(ctx context.Context, _ int) (struct{}, error) {
		err := publishFile(dir, final, func(f *os.File) error {
			hw := &hashWriter{w: f, h: sha256.New()}
			gotRaw, cerr := e.dumpChain(ctx, plan, hw)
			raw, bytes, sum = gotRaw, hw.n, hw.h.Sum(nil)
			if cerr != nil {
				return cerr
			}
			return writeStagedRecord(rec, source.StagedChainFile(dir, sb.artifact.Name))
		})
		return struct{}{}, err
	})
	if err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	ar.Outcome = model.ArtifactOK
	ar.RawBytes = raw
	ar.Bytes = bytes
	ar.SHA256 = hex.EncodeToString(sum)
	e.event(human.New("artifact.done",
		human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)),
		human.Bytes("raw", raw), human.Bytes("stored", bytes), human.Duration("took", time.Since(start))))
	return ar
}

// hashWriter counts and hashes what flows into the staged file.
type hashWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (hw *hashWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	hw.n += int64(n)
	hw.h.Write(p[:n])
	return n, err
}

// writeStagedRecord publishes the record of what the stage ran beside the
// artifact (source.StagedChainFile): the artifact's exact file name tail and the
// processor chain in encode order, which the run side records in the manifest
// and a restore decodes with.
func writeStagedRecord(rec source.StagedRecord, path string) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("chain record: %w", err))
	}
	return publishFile(filepath.Dir(path), path, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

// publishFile writes one file into dir under its final name: a temp file inside
// the same directory, the caller's write, mode 0644 — the deployment contract
// for a file another container reads, whatever uid it runs as: the image's own
// default is the unprivileged 1000, this cluster's CronJob runs both containers
// as root — and an atomic rename, so a half-written file is never visible. The
// temp file is removed on every failure path.
func publishFile(dir, final string, write func(*os.File) error) error {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return plugin.Permanent(err)
	}
	name := tmp.Name()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return plugin.Transient(err)
	}
	if err := os.Chmod(name, 0o644); err != nil {
		_ = os.Remove(name)
		return plugin.Transient(err)
	}
	if err := os.Rename(name, final); err != nil {
		_ = os.Remove(name)
		return plugin.Transient(err)
	}
	return nil
}

// clearStaged removes what a previous stage run left for one artifact: exactly
// the file the record names — the old record's own name when the pipeline
// changed since last night, so a differently named artifact cannot linger as an
// orphan — and the record itself, which the run publishes again from the new
// chain. On a stage directory that survives the container this is what keeps a
// failed stage from leaving yesterday's bytes to be uploaded as tonight's. A
// record that cannot be read is a permanent failure: guessing the old file's
// name would be the collision this whole contract exists to prevent.
func clearStaged(dir, name string, rec source.StagedRecord) error {
	if old, err := source.ReadStagedRecord(dir, name); err == nil {
		if err := removeIfPresent(filepath.Join(dir, source.StagedArtifactName(name, old))); err != nil {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return plugin.Permanent(fmt.Errorf("staged chain record: %w", err))
	}
	if err := removeIfPresent(filepath.Join(dir, source.StagedArtifactName(name, rec))); err != nil {
		return err
	}
	return removeIfPresent(source.StagedChainFile(dir, name))
}

// removeIfPresent removes one path, and a path that is already gone is done.
func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return plugin.Transient(err)
	}
	return nil
}
