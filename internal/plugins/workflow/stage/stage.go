// Package stage is the stage-job workflow: it produces the sealed artifacts a
// backup run reads back from the shared volume, one file per artifact named
// <artifact name><ext> with its chain record beside it. The name is the contract
// the run-side staged source opens — never a file whose name merely starts with
// this one — and the record is the chain a restore decodes with.
//
// A stage publishes into an empty directory it owns and never deletes: a
// directory that holds anything when the stage starts is refused before any
// source runs, so a pair a previous stage left can never read as this one's.
package stage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	json "encoding/json/v2"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Producer is the pipeline the stage produces with, the consumer's own seam over
// internal/plugins/pipeline: *pipeline.Producer implements it.
type Producer interface {
	// Resolve fills the artifact's extension, sealing flag and recorded chain,
	// and refuses a secret artifact whose bytes would leave unsealed.
	Resolve(ctx context.Context, plan pipeline.Plan) (pipeline.Resolved, error)
	// Dump streams the artifact's sealed bytes into dst, reports the raw bytes
	// the source produced and emits the dump's own start line.
	Dump(ctx context.Context, dst io.Writer, plan pipeline.Plan, res pipeline.Resolved) (int64, error)
}

// Check names one instance whose Check failed in this stage's own scope: its
// sources and the processors their pipelines name, never the run's instances —
// this container has other mounts.
type Check struct {
	Kind string
	Name string
}

// Source is one stage.sources instance, already built by assembly: the config
// envelope the report and the stage directory are named from, the source that
// produces its bytes, and the processor steps its pipeline applies.
type Source struct {
	// Name is the config instance name, unique within the section.
	Name string
	// CheckKind is the kind this source is checked under: "stage_source" for a
	// stage source — the same name may be declared in both sections, and one's
	// failed check must never fail the other's work.
	CheckKind string
	// Type is the factory type, recorded for the source's log prefix.
	Type string
	// Instance is the built source; a source that enumerates is refused: a
	// stage writes one file per artifact, and a unit's own artifact has no name
	// here.
	Instance source.Source
	// Artifact is the config envelope: name, group, importance, sensitivity.
	Artifact model.Artifact
	// Pipeline is the config pipeline name, kept for the report; the bytes are
	// produced by Steps.
	Pipeline string
	// Steps are the processor instances of the pipeline, in config order.
	Steps []pipeline.Step
}

// Options is the typed input an assembly projects from configuration. Every
// field is required except the ones documented as optional.
type Options struct {
	// Dir is the stage directory this invocation owns: absent is created,
	// present must be empty.
	Dir string
	// ConfigPath is the configuration file the stage ran from, for the report
	// line only.
	ConfigPath string
	// DegradedExitCode and FailedExitCode are the stage section's own: a stage
	// config has no run section to take them from.
	DegradedExitCode int
	FailedExitCode   int
	// Sources are the stage section's sources, in the order the report lists
	// them.
	Sources []Source
	// Checks is the failed-Check table of this stage's own scope. The stage asks
	// for it only after the start directory is accepted: a refused stage must
	// not touch any plugin. Nil is an empty table.
	Checks func() map[Check]error
	// Retry is the policy of the stage's own calls: unit enumeration and the
	// publish attempts. The pipeline's calls carry their own.
	Retry execution.Options
	// Pipeline resolves each artifact's traits and chain and streams its bytes.
	Pipeline Producer
	// Log receives the stage and artifact events; nil disables them.
	Log *slog.Logger
}

// Run stages every source of the section into Options.Dir and reports one
// artifact result per source. A required artifact that failed fails the stage; a
// best-effort one degrades it. A report carries no run id: the file name is the
// identity, and the stage section has no run section to read one from.
func Run(ctx context.Context, o Options) (model.RunReport, error) {
	if o.Dir == "" {
		return model.RunReport{}, errors.New("stage: config needs the stage section with a dir")
	}
	if len(o.Sources) == 0 {
		return model.RunReport{}, errors.New("stage: config has no stage.sources")
	}
	if o.Pipeline == nil {
		return model.RunReport{}, errors.New("stage: no pipeline")
	}
	if err := emptyDir(o.Dir); err != nil {
		return model.RunReport{}, err
	}
	var checks map[Check]error
	if o.Checks != nil {
		checks = o.Checks()
	}
	reserved := newReservations(o.Dir, o.Sources)
	started := time.Now().UTC()
	rep := model.RunReport{Schema: model.ManifestSchema, StartedAt: started, Outcome: model.OutcomeRunning}
	o.emit(human.New("stage.start", human.Text("config", o.ConfigPath), human.Text("dir", o.Dir)))

	outcome := model.OutcomeDelivered
	for _, src := range o.Sources {
		o.emit(human.New("artifact.start",
			human.Text("artifact", human.ArtifactLabel(src.Artifact.Group, src.Artifact.Name)),
			human.Text("importance", string(src.Artifact.Importance))))
		ar := o.stageOne(ctx, src, checks, reserved)
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
		rep.ExitCode = o.DegradedExitCode
	case model.OutcomeFailed:
		rep.ExitCode = o.FailedExitCode
	}
	return rep, nil
}

// emit writes one stage event; a nil logger disables them.
func (o Options) emit(ev human.Event) {
	if o.Log == nil {
		return
	}
	o.Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// readDir lists the stage directory. The seam exists for its one unprovokable
// failure — a directory the process just created and owns, which it then cannot
// list — and is overridden only in tests.
var readDir = os.ReadDir

// emptyDir refuses a stage.dir that holds anything: a stage never deletes, so a
// pair a previous stage left would read exactly like this one's and the run
// would upload yesterday's bytes as tonight's. The ReadDir error and the
// non-empty case fold into one refusal so every start state is one message.
func emptyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	entries, err := readDir(dir)
	if err == nil && len(entries) > 0 {
		err = fmt.Errorf("it holds %s", entries[0].Name())
	}
	if err != nil {
		return fmt.Errorf("stage: stage.dir %s must be an empty directory this pod owns when a stage starts — a stage never deletes, and a pair a previous stage left would read exactly like this one's: %w", dir, err)
	}
	return nil
}

// reservations is what this stage invocation has claimed in its empty
// directory. Every artifact's chain record is claimed up front — it depends on
// the artifact name alone — and an artifact's file path the moment it is known,
// whether or not that artifact later publishes. A later artifact that would take
// either fails before it produces bytes, so one artifact's file can never answer
// another's record: not after a failed check, and not after a failed publish.
type reservations struct {
	owners map[string]string // path -> the instance that claimed it
}

func newReservations(dir string, sources []Source) *reservations {
	r := &reservations{owners: make(map[string]string, len(sources))}
	for _, src := range sources {
		// A name the plain-name guard rejects claims nothing: deriving a path
		// from it could alias a healthy sibling's record or file.
		if !plainName(src.Artifact.Name) {
			continue
		}
		path := source.StagedChainFile(dir, src.Artifact.Name)
		if _, claimed := r.owners[path]; !claimed {
			r.owners[path] = src.Name
		}
	}
	return r
}

// plainName reports whether an artifact name can name a file in the stage
// directory: no separator and no leading dot. The artifact guard and the
// reservation pass share it, so a rejected name grants no ownership.
func plainName(name string) bool {
	return !strings.Contains(name, "/") && !strings.HasPrefix(name, ".")
}

// claimRecord refuses an artifact whose chain record another artifact of this
// stage already claimed, whatever became of that artifact.
func (r *reservations) claimRecord(dir, name, instance string) error {
	if owner, claimed := r.owners[source.StagedChainFile(dir, name)]; claimed && owner != instance {
		return fmt.Errorf("stage artifact %s: its chain record is already claimed by %s, another artifact of this stage", name, owner)
	}
	return nil
}

// claimFile refuses a file path another artifact of this stage claimed, and
// claims the path for this artifact whether or not it goes on to publish. The
// directory started empty (emptyDir), so anything present at a path this stage
// wrote is owned by the reservation above; Lstat is here for a filesystem error
// that is not absence, which is the real error and never a collision.
func (r *reservations) claimFile(path, name, instance string) error {
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
func (o Options) stageOne(ctx context.Context, src Source, checks map[Check]error, reserved *reservations) (ar model.ArtifactResult) {
	ar = model.ArtifactResult{Artifact: src.Artifact, Pipeline: src.Pipeline, Outcome: model.ArtifactFailed}
	start := time.Now()
	defer func() { ar.Duration = model.Duration(time.Since(start)) }()
	dir := o.Dir
	fail := func(reason string) {
		ar.Error = reason
		o.emit(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(src.Artifact.Group, src.Artifact.Name)), human.Text("reason", reason)))
	}
	// The name is refused before any path is derived from it: a name carrying a
	// separator must never reach outside the stage directory.
	if !plainName(src.Artifact.Name) {
		fail(fmt.Sprintf("stage artifact name %q must be a plain file name", src.Artifact.Name))
		return ar
	}
	// The chain record is claimed before this artifact's check: another artifact
	// of this stage that owns the name owns the identity, whatever became of it.
	if err := reserved.claimRecord(dir, src.Artifact.Name, src.Name); err != nil {
		fail(err.Error())
		return ar
	}
	if err := checks[Check{Kind: src.CheckKind, Name: src.Name}]; err != nil {
		fail(fmt.Sprintf("source check: %v", err))
		return ar
	}
	if err := pipelineCheck(checks, src.Steps); err != nil {
		fail(err.Error())
		return ar
	}
	// A stage writes one sealed file per artifact, and the run side reads it
	// back by that one name. An instance whose domain holds several artifacts
	// has no such name, and staging only the first of them would stage a
	// fraction of the data under a name that reads as all of it.
	if en, ok := src.Instance.(source.Enumerator); ok {
		units, single, err := planning.New(o.Retry).ListUnits(ctx, src.Name, en)
		switch {
		case single:
		case err != nil:
			fail(fmt.Sprintf("stage does not expand a multi-unit source: enumerating %s: %v", src.Name, err))
			return ar
		default:
			fail(fmt.Sprintf("stage does not expand a multi-unit source: %s holds %d artifacts (the first is %q); stage a source that is one artifact, or a run that backs them up",
				src.Name, len(units), units[0].Name))
			return ar
		}
	}
	plan := pipeline.Plan{
		Source: src.Instance, SourceInstance: src.Name, SourceType: src.Type,
		Artifact: src.Artifact, Steps: src.Steps,
	}
	res, err := o.Pipeline.Resolve(ctx, plan)
	if err != nil {
		fail(err.Error())
		return ar
	}
	ar.Artifact = res.Artifact
	// The record carries the artifact's exact file name tail and the chain; from
	// here on the file is named by it, never by a directory scan.
	rec := source.StagedRecord{Ext: res.Artifact.Ext, Chain: res.Chain}
	// The ext the writer appends is the source's and every processor's trait
	// joined: StagedArtifactName refuses one that would step out of the stage
	// dir, so a hostile or broken trait fails here, before a byte is produced.
	name, err := source.StagedArtifactName(src.Artifact.Name, rec)
	if err != nil {
		fail(err.Error())
		return ar
	}
	final := filepath.Join(dir, name)
	// The final name is claimed before a byte is produced, and stays claimed
	// even if this artifact fails: the directory started empty, so anything at
	// the path now is an earlier artifact of this stage, and a later artifact
	// must not take a name this one owns whatever became of it.
	if err := reserved.claimFile(final, src.Artifact.Name, src.Name); err != nil {
		fail(err.Error())
		return ar
	}
	var raw, bytes int64
	var sum []byte
	// Every attempt publishes through its own temp file, so a half-written
	// artifact is never visible to the run-side "staged" source, and a stage
	// config that carries a retry policy gets that many attempts. The chain
	// record is published inside the dump's own step — after the bytes are
	// produced, before the artifact's rename — so a stage that fails always
	// leaves the "no record, no artifact" pair the run refuses: a record
	// without an artifact fails the staged source loudly ("no staged
	// artifact"), while an artifact without a record would be bytes nobody can
	// prove are sealed.
	_, err = execution.Attempt(ctx, o.Retry, "stage "+src.Artifact.Name, func(ctx context.Context, _ int) (struct{}, error) {
		err := publishFile(dir, final, func(f *os.File) error {
			hw := &hashWriter{w: f, h: sha256.New()}
			gotRaw, cerr := o.Pipeline.Dump(ctx, hw, plan, res)
			raw, bytes, sum = gotRaw, hw.n, hw.h.Sum(nil)
			if cerr != nil {
				return cerr
			}
			return writeStagedRecord(rec, source.StagedChainFile(dir, src.Artifact.Name))
		})
		return struct{}{}, err
	})
	if err != nil {
		fail(err.Error())
		return ar
	}
	ar.Outcome = model.ArtifactOK
	ar.RawBytes = raw
	ar.Bytes = bytes
	ar.SHA256 = hex.EncodeToString(sum)
	o.emit(human.New("artifact.done",
		human.Text("artifact", human.ArtifactLabel(src.Artifact.Group, src.Artifact.Name)),
		human.Bytes("raw", raw), human.Bytes("stored", bytes), human.Duration("took", time.Since(start))))
	return ar
}

// pipelineCheck fails the artifact whose pipeline holds a processor that failed
// its Check: an artifact routed through it is not produced, so a secret artifact
// whose sealing processor is the one that failed never leaves the process.
func pipelineCheck(checks map[Check]error, steps []pipeline.Step) error {
	for _, step := range steps {
		if err := checks[Check{Kind: "processor", Name: step.Instance}]; err != nil {
			return fmt.Errorf("processor %s check: %w", step.Instance, err)
		}
	}
	return nil
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
		return pluginerrors.Permanent(fmt.Errorf("chain record: %w", err))
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
		return pluginerrors.Permanent(err)
	}
	name := tmp.Name()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return pluginerrors.Transient(err)
	}
	if err := os.Chmod(name, 0o644); err != nil {
		_ = os.Remove(name)
		return pluginerrors.Transient(err)
	}
	if err := os.Rename(name, final); err != nil {
		_ = os.Remove(name)
		return pluginerrors.Transient(err)
	}
	return nil
}
