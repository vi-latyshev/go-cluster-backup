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
// the contract the run-side staged source reads back: the reader opens exactly
// <name><ext> from the record — never a file whose name merely starts with this
// one. A stage publishes into an empty directory it owns and never deletes:
// stage.dir must hold no entry when the stage starts, and a directory that holds
// one (a previous stage's pair, a stale temp, anything) is refused before any
// source runs. A stage re-run on data a pod retained is refused, fail-closed, by
// design. A stage report carries no run id: the file name is the identity, and
// the stage section has no run section to read one from.
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
	dir := cfg.Stage.Dir
	if err := emptyStageDir(dir); err != nil {
		return model.RunReport{}, err
	}
	reserved := newStageReservations(dir, e.stageSources)
	checks := e.scopedChecks(ctx, e.checkPolicy(), scopeStage)
	started := time.Now().UTC()
	rep := model.RunReport{Schema: model.ManifestSchema, StartedAt: started, Outcome: model.OutcomeRunning}
	e.event(human.New("stage.start", human.Text("config", req.ConfigPath), human.Text("dir", dir)))

	outcome := model.OutcomeDelivered
	for _, sb := range e.stageSources {
		e.event(human.New("artifact.start",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)),
			human.Text("importance", string(sb.artifact.Importance))))
		ar := e.stageOne(ctx, sb, checks, reserved)
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

// emptyStageDir refuses a stage.dir that holds anything: a stage never deletes,
// so a pair a previous stage left would read exactly like this one's and the run
// would upload yesterday's bytes as tonight's. The ReadDir error and the
// non-empty case fold into one refusal so every start state is one message.
func emptyStageDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err == nil && len(entries) > 0 {
		err = fmt.Errorf("it holds %s", entries[0].Name())
	}
	if err != nil {
		return fmt.Errorf("stage: stage.dir %s must be an empty directory this pod owns when a stage starts — a stage never deletes, and a pair a previous stage left would read exactly like this one's: %w", dir, err)
	}
	return nil
}

// stageReservations is what this stage invocation has claimed in its empty
// directory. Every artifact's chain record is claimed up front — it depends on
// the artifact name alone — and an artifact's file path the moment it is known,
// whether or not that artifact later publishes. A later artifact that would take
// either fails before it produces bytes, so one artifact's file can never answer
// another's record: not after a failed check, and not after a failed publish.
type stageReservations struct {
	owners map[string]string // path -> the instance that claimed it
}

func newStageReservations(dir string, sources []sourceBinding) *stageReservations {
	r := &stageReservations{owners: make(map[string]string, len(sources))}
	for _, sb := range sources {
		// A name the plain-name guard rejects claims nothing: deriving a path
		// from it could alias a healthy sibling's record or file.
		if !plainStageName(sb.artifact.Name) {
			continue
		}
		path := source.StagedChainFile(dir, sb.artifact.Name)
		if _, claimed := r.owners[path]; !claimed {
			r.owners[path] = sb.name
		}
	}
	return r
}

// plainStageName reports whether an artifact name can name a file in the stage
// directory: no separator and no leading dot. The artifact guard and the
// reservation pass share it, so a rejected name grants no ownership.
func plainStageName(name string) bool {
	return !strings.Contains(name, "/") && !strings.HasPrefix(name, ".")
}

// claimRecord refuses an artifact whose chain record another artifact of this
// stage already claimed, whatever became of that artifact.
func (r *stageReservations) claimRecord(dir, name, instance string) error {
	if owner, claimed := r.owners[source.StagedChainFile(dir, name)]; claimed && owner != instance {
		return fmt.Errorf("stage artifact %s: its chain record is already claimed by %s, another artifact of this stage", name, owner)
	}
	return nil
}

// claimFile refuses a file path another artifact of this stage claimed, and
// claims the path for this artifact whether or not it goes on to publish. The
// directory started empty (emptyStageDir), so anything present at a path this
// stage wrote is owned by the reservation above; Lstat is here for a filesystem
// error that is not absence, which is the real error and never a collision.
func (r *stageReservations) claimFile(path, name, instance string) error {
	if owner, claimed := r.owners[path]; claimed && owner != instance {
		return fmt.Errorf("stage artifact %s would replace %s, which another artifact of this stage published", name, filepath.Base(path))
	}
	if _, err := os.Lstat(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stage artifact %s: %w", name, err)
	}
	r.owners[path] = instance
	return nil
}

// stageOne stages one artifact. ar is a named result so the deferred duration
// lands on the value the caller receives: `return ar` copies a struct, so a
// closure over a local would set the duration on a copy the caller never sees.
func (e *Engine) stageOne(ctx context.Context, sb sourceBinding, checks failedChecks, reserved *stageReservations) (ar model.ArtifactResult) {
	ar = model.ArtifactResult{Artifact: sb.artifact, Pipeline: sb.pipeline, Outcome: model.ArtifactFailed}
	start := time.Now()
	defer func() { ar.Duration = model.Duration(time.Since(start)) }()
	dir := e.cfg.Stage.Dir
	// The name is refused before any path is derived from it: a name carrying a
	// separator must never reach outside the stage directory.
	if !plainStageName(sb.artifact.Name) {
		ar.Error = fmt.Sprintf("stage artifact name %q must be a plain file name", sb.artifact.Name)
		return ar
	}
	// The chain record is claimed before this artifact's check: another artifact
	// of this stage that owns the name owns the identity, whatever became of it.
	if err := reserved.claimRecord(dir, sb.artifact.Name, sb.name); err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
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
	// The record carries the artifact's exact file name tail and the chain; from
	// here on the file is named by it, never by a directory scan.
	rec := source.StagedRecord{Ext: plan.artifact.Ext, Chain: plan.chain}
	// Every attempt publishes through its own temp file, so a half-written
	// artifact is never visible to the run-side "staged" source, and a stage
	// config that carries a retry policy gets that many attempts. The chain
	// record is published inside the dump's own step — after the bytes are
	// produced, before the artifact's rename — so a stage that fails always
	// leaves the "no record, no artifact" pair the run refuses: a record
	// without an artifact fails the staged source loudly ("no staged
	// artifact"), while an artifact without a record would be bytes nobody can
	// prove are sealed.
	// The ext the writer appends is the source's and every processor's trait
	// joined: StagedArtifactName refuses one that would step out of the stage
	// dir, so a hostile or broken trait fails here, before a byte is produced.
	name, err := source.StagedArtifactName(sb.artifact.Name, rec)
	if err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	final := filepath.Join(dir, name)
	// The final name is claimed before a byte is produced, and stays claimed
	// even if this artifact fails: the directory started empty, so anything at
	// the path now is an earlier artifact of this stage, and a later artifact
	// must not take a name this one owns whatever became of it.
	if err := reserved.claimFile(final, sb.artifact.Name, sb.name); err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(sb.artifact.Group, sb.artifact.Name)), human.Text("reason", ar.Error)))
		return ar
	}
	var raw, bytes int64
	var sum []byte
	_, err = attempt(ctx, e.checkPolicy(), "stage "+sb.artifact.Name, func(ctx context.Context, _ int) (struct{}, error) {
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
// default is the unprivileged 1000, so the reader may run as another user — and
// an atomic rename, so a half-written file is never visible. The temp file is
// removed on every failure path.
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
