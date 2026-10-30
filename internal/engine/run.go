package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// artifactPlan is one artifact of the run: its source, envelope, resolved
// pipeline and stages. A plan with problem != nil fails before any byte.
type artifactPlan struct {
	instance string
	srcType  string
	src      source.Source
	artifact model.Artifact
	pipeline string
	refs     []string // processor instance names of the pipeline, in order
	procs    []processor.Processor
	stages   []func(ctx context.Context, dst io.Writer, src io.Reader) error
	// chain and source are what the manifest records: the actual steps and the
	// non-secret origin, so a restore never depends on a current config.
	chain   []model.ProcessorStep
	source  model.SourceRef
	problem error
	// hint is the short, actionable half of problem, when the object's own
	// failure carried one (see discovery.Found.Hint). It is recorded on the
	// artifact beside the error, never instead of it.
	hint string
}

// producedArtifact is one artifact's bytes sitting in the spool.
type producedArtifact struct {
	artifact model.Artifact
	chain    []model.ProcessorStep
	source   model.SourceRef
	file     string
	bytes    int64
	raw      int64
	sha256   string
	// stats is what the source reported about the bytes beyond their validity:
	// entries it left out, files it padded (see source.DumpStatsReporter).
	stats model.DumpStats
	took  time.Duration
}

// storageRunState accumulates one storage's result through the run. Upload and
// retention errors are kept apart: a failed retention run is visible in the
// report but never downgrades a verified copy.
type storageRunState struct {
	name   string
	typ    string
	clock  storageClock
	mu     sync.Mutex
	result model.StorageResult
	tried  bool
	// manifestOK is set once this run's manifest was written to this storage and
	// verified there. The retention floor may credit the run being written as a
	// holder of an artifact only where it is true: a copy no manifest records is
	// a copy a restore cannot find.
	manifestOK bool
	// uploadErr records any failed upload, verification or manifest write: the
	// copy on this storage is incomplete.
	uploadErr    error
	retentionErr error
	// capExceeded is set when this storage refused an object because the run's
	// own configured byte cap was reached — the machine-readable reason a manual
	// run reports instead of a storage outage, with the limit and the bytes that
	// tripped it.
	capExceeded bool
	capLimit    int64
	capUsed     int64
	fatal       error // a permanent failure: further uploads to this storage are skipped
}

// reached reports whether this run put anything on this storage — an artifact
// or the manifest. A storage the run never touched holds no run to account for.
func (st *storageRunState) reached() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.result.Objects > 0
}

// manifestSaved reports whether this run's manifest was written to this storage
// and verified there.
func (st *storageRunState) manifestSaved() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.manifestOK
}

// markManifest records the manifest as saved and verified on this storage.
func (st *storageRunState) markManifest() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.manifestOK = true
}

func (st *storageRunState) setUploadError(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.uploadErr = errors.Join(st.uploadErr, err)
}

func (st *storageRunState) setRetentionError(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.retentionErr = errors.Join(st.retentionErr, err)
}

// refuseCap records a cap refusal: this storage stops taking objects and the
// report carries the machine-readable reason and the numbers behind it.
func (st *storageRunState) refuseCap(err error, limit, used int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.capExceeded = true
	st.capLimit = limit
	st.capUsed = used
	st.uploadErr = errors.Join(st.uploadErr, err)
	st.fatal = err
}

// capRefused reports whether this storage refused the manual run on its cap.
func (st *storageRunState) capRefused() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.capExceeded
}

func (st *storageRunState) markObject(bytes int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.result.Objects++
	st.result.Bytes += bytes
}

func (st *storageRunState) addAttempts(n int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.result.Attempts += n
}

// Run executes one backup run. It returns an error only when the run cannot
// start at all (config, preflight); everything after that is the report's
// outcome, never an error.
func (e *Engine) Run(ctx context.Context, req RunRequest) (model.RunReport, error) {
	cfg := e.cfg
	if cfg.Run == nil {
		return model.RunReport{}, errors.New("run: config needs the run section")
	}
	if !req.Kind.Valid() {
		return model.RunReport{}, fmt.Errorf("run: kind must be %q or %q, got %q", model.RunKindScheduled, model.RunKindManual, req.Kind)
	}
	prefix, err := runPrefixLiteral(cfg.Layout.RunPrefix.Text())
	if err != nil {
		return model.RunReport{}, err
	}
	if req.Kind == model.RunKindManual && e.lease == nil {
		// A manual run writes where a scheduled one may race it, and no lease can
		// serialise the two: refuse before anything is touched.
		return model.RunReport{}, errors.New("run: a manual run needs a catalog that can hold the writer lease, and this one cannot")
	}
	for _, sb := range e.storages {
		if sb.retention[model.RunKindScheduled] == nil {
			return model.RunReport{}, fmt.Errorf("run: storages/%s/retention/scheduled is missing; a run needs a scheduled retention policy per storage", sb.name)
		}
		if sb.cfg.MaxTotalBytes.Int64() <= 0 {
			return model.RunReport{}, fmt.Errorf("run: storages/%s/max_total_bytes is missing; a run needs a configured cap per storage", sb.name)
		}
		if req.Kind == model.RunKindManual && sb.retention[model.RunKindManual] == nil {
			return model.RunReport{}, fmt.Errorf("run: storages/%s/retention/manual is missing; a manual run needs a manual retention policy per storage", sb.name)
		}
	}
	if errs := e.binaryErrors(scopeRun); len(errs) > 0 {
		// A missing binary is the deployment, not a blip: the run refuses
		// before any plugin work rather than failing every artifact at night.
		return model.RunReport{}, fmt.Errorf("preflight: %w", errors.Join(errs...))
	}
	outer := ctx
	ctx, cancel := context.WithTimeout(ctx, cfg.Run.Deadline.Std())
	defer cancel()

	// The sources this run's discoveries build belong to the run: they are
	// closed when it ends, whatever the outcome, and never accumulate on the
	// Engine across runs.
	scope := &closerScope{}
	defer func() {
		if err := scope.close(); err != nil {
			e.event(human.Failure("sources.close.failed", human.Text("reason", err.Error())))
		}
	}()

	started := time.Now().UTC()
	rep := model.RunReport{Schema: model.ManifestSchema, RunID: e.runID(req.Kind, started), Kind: req.Kind,
		StartedAt: started, Outcome: model.OutcomeRunning}
	e.event(human.New("run.start",
		human.Text("kind", string(req.Kind)),
		human.Text("run", rep.RunID),
		human.Text("config", req.ConfigPath),
		human.Text("storages", strings.Join(e.storageNames(), ", "))))

	// One instance's failed Check fails that instance's own work, never the
	// run: a backup of everything that still works beats no backup at all.
	checks := e.scopedChecks(ctx, e.checkPolicy(), scopeRun)
	if err := checks.err("catalog", "catalog"); err != nil {
		e.event(human.Failure("catalog.failed", human.Text("run", rep.RunID), human.Text("reason", err.Error())))
	}
	plans, excluded := e.collectArtifacts(ctx, checks, scope)
	// Every object the run saw and deliberately did not back up is part of the
	// report from here on: the manifest, the catalog and the finished event all
	// carry this list, in every outcome.
	rep.Excluded = excluded
	// The covered/ignored count rides the summary line; the per-object lines are
	// machine detail now, kept for --log-level debug. A night that backed
	// everything up must not read as a problem: the only exclusion worth a line
	// at info is an undeclared claim, and that is a failed artifact (see the
	// discovery's undeclared_volumes), named among the failures.
	e.event(human.New("discovery.summary",
		human.Count("artifacts", int64(len(plans))), human.Count("covered", int64(len(excluded)))))
	for _, x := range excluded {
		e.event(human.Debug("discovery.excluded",
			human.Text("object", x.Name), human.Text("kind", string(x.Kind)), human.Text("reason", x.Reason)))
	}

	order := e.storages
	results := map[string]*storageRunState{}
	for _, sb := range order {
		st := newStorageRunState(sb)
		results[sb.name] = st
		if err := checks.err("storage", sb.name); err != nil {
			// A failed Check costs the storage only when it is permanent: the
			// uploads run minutes later, and a blip of the check's few seconds
			// must not cost the night's copy. Every upload still carries its own
			// attempts, and a permanent failure sets the same fatal state.
			if class, _ := plugin.ClassOf(err); class == plugin.ClassPermanent {
				st.mu.Lock()
				st.tried = true
				st.fatal = err
				st.mu.Unlock()
				st.setUploadError(fmt.Errorf("check: %w", err))
				e.storageEvent(sb, human.Failure("storage.check.failed",
					human.Text("reason", "permanent, the storage is out for this run: "+err.Error())))
				continue
			}
			e.storageEvent(sb, human.Failure("storage.check.failed",
				human.Text("reason", "the run proceeds and its uploads try anyway: "+err.Error())))
		}
		e.describeStorage(ctx, sb, st)
	}

	if req.DryRun {
		return e.dryRunReport(ctx, rep, plans, order, results), nil
	}

	// The writer lease is taken before any storage write — the stale sweep
	// below deletes — so two runs sharing storage roots never overlap: the
	// deployment's concurrencyPolicy does not cover a manually created Job.
	lease, held, unknownUntil, err := e.acquireWriterLease(ctx, req.Kind, rep.RunID)
	refused := false
	switch {
	case err == nil:
		// The claim, if any, is held below.
	case errors.Is(err, catalog.ErrLeaseHeld) && req.Kind == model.RunKindManual:
		// A manual run refuses at once while the holder is alive: no storage is
		// touched, the outcome is failed, and the lease is the machine-readable
		// reason. The refusal goes through the common finish path below — the
		// finished notification and the catalog entry still run.
		refused = true
		e.event(human.Failure("lease.refused",
			human.Text("kind", string(req.Kind)), human.Text("run", rep.RunID), human.Text("reason", err.Error())))
		rep.Lease = &model.LeaseState{Refused: true, Holder: held.Holder, HolderKind: held.Kind}
	default:
		if req.Kind == model.RunKindManual {
			// A manual run that could not even reach the claim — no catalog can
			// hold a lease — cannot write at all: it fails to start, like the
			// missing-capability check above.
			return model.RunReport{}, fmt.Errorf("run: writer lease: %w", err)
		}
		// A scheduled run that could not take the claim — the bounded wait for a
		// live holder expired, or the failure is permanent — fails, but not before
		// the report is written: the run never wrote a byte, so the common finish
		// below still runs the finished notification and the catalog entry, exactly
		// as the manual refusal does. The lease is the machine-readable reason.
		refused = true
		e.event(human.Failure("lease.failed",
			human.Text("kind", string(req.Kind)), human.Text("run", rep.RunID), human.Text("reason", err.Error())))
		rep.Lease = &model.LeaseState{Holder: held.Holder, HolderKind: held.Kind, Error: err.Error()}
	}
	if lease != nil && !refused {
		defer e.releaseWriterLease(outer, *lease, rep.RunID)
	}

	var (
		artifactResults []model.ArtifactResult
		verifiedMatrix  = map[string]map[string]bool{}
		notifierFailed  bool
		surveys         map[string]survey
		caps            map[string]*capState
		state           *retentionState
	)
	if !refused {
		// The spool comes before the first event: a run that cannot buffer has not
		// started, and no notifier hears about a run that never will.
		sp, err := spool.New(cfg.Run.SpoolDir)
		if err != nil {
			return model.RunReport{}, err
		}

		rep.Previous = e.previous(ctx)
		if rep.Previous.RunID != "" {
			last := "never"
			if !rep.Previous.LastGoodAt.IsZero() {
				last = rep.Previous.LastGoodAt.UTC().Format(time.RFC3339)
			}
			e.event(human.New("run.previous",
				human.Text("run", rep.Previous.RunID),
				human.Text("unfinished", strconv.FormatBool(rep.Previous.Unfinished)),
				human.Text("last_good", last)))
		}
		e.inspectPreviousJob(ctx, &rep)
		e.saveCatalog(ctx, rep)
		notifierFailed = e.notify(ctx, model.Event{Name: model.EventStarted, At: started, Run: rep}, checks)
		// The quarantine is decided BEFORE the first survey call. A survey that
		// starts under a quarantine and finishes after it expired is still a
		// snapshot taken under it — the dead holder's in-flight operations may
		// have landed while the listing was running — so the whole run keeps
		// occupancy unknown, and a manual run is refused on every storage. No
		// re-survey: a second snapshot would be a second guess, not a proof.
		quarantined := !unknownUntil.IsZero() && time.Now().Before(unknownUntil)
		surveys = map[string]survey{}
		for _, sb := range order {
			surveys[sb.name] = e.surveyStorage(ctx, sb, prefix, e.retry, results[sb.name])
		}
		if err := e.checkRunIDFree(surveys, rep.RunID); err != nil {
			return model.RunReport{}, err
		}
		caps = map[string]*capState{}
		for _, sb := range order {
			caps[sb.name] = newCapState(sb.cfg.MaxTotalBytes.Int64(), surveyBytes(surveys[sb.name]), surveys[sb.name].ok)
			if quarantined {
				caps[sb.name].markUnknown()
			}
		}
		// A run that was killed outright left its unfinished uploads behind: sweep
		// them before this run's own uploads start. The cutoff reaches back exactly
		// the longest a live run can last, so an upload a run still might be writing
		// is never taken — the lease already guarantees no other writer is live, and
		// the cutoff stays a second, independent rail. Whatever the sweep cannot
		// count or abort leaves the storage's occupancy unknown, which refuses a
		// manual run there.
		staleCutoff := e.staleCutoff()
		for _, sb := range order {
			st := results[sb.name]
			// The full inventory, any age, is what makes an earlier writer's parts
			// visible: the stale selection below aborts only the old ones, and a
			// young or undated upload still bills bytes no listing shows. It runs
			// through the same call pipeline as every storage call — the storage's
			// budget, the engine's retries and counted attempts — and an inventory
			// that cannot be read leaves occupancy unknown loudly.
			if inv, ok := sb.inst.(storage.MultipartInventory); ok {
				var uploads []storage.StaleUpload
				_, err := storageCall(&st.clock, ctx, func(ctx context.Context) (struct{}, error) {
					return attempt(ctx, e.retry, "inventory multipart "+sb.name, func(ctx context.Context, _ int) (struct{}, error) {
						st.addAttempts(1)
						var ierr error
						uploads, ierr = safeValue(func() ([]storage.StaleUpload, error) { return inv.Inventory(ctx) })
						return struct{}{}, ierr
					})
				})
				switch {
				case err != nil:
					caps[sb.name].markUnknown()
					e.storageEvent(sb, human.Failure("multipart.inventory.failed",
						human.Text("reason", "the foreign parts stay uncounted, occupancy is unknown: "+err.Error())))
				case len(uploads) > 0:
					caps[sb.name].markUnknown()
				}
			}
			if e.sweepStaleUploads(ctx, sb, results[sb.name], staleCutoff) {
				caps[sb.name].markUnknown()
			}
		}
		state = newRetentionState(surveys)

		// Artifacts, one at a time: produce into the spool, upload to every storage,
		// remove the file, next artifact.
		for _, plan := range plans {
			e.event(human.New("artifact.start",
				human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)),
				human.Text("importance", string(plan.artifact.Importance))))
			ar, verified := e.processArtifact(ctx, sp, plan, rep.RunID, req.Kind, order, results, caps)
			artifactResults = append(artifactResults, ar)
			verifiedMatrix[plan.artifact.Name] = verified
		}
	}

	if !refused {
		// The manifest of this run is written BEFORE retention, and on the caller's
		// context rather than the run deadline: the floor may credit this run as a
		// holder of an artifact only on a storage that has already saved and
		// verified its manifest, so a manifest write that fails — or a process that
		// dies here — must leave the previous run's copy in place instead of
		// releasing the pin onto objects nobody can find through a manifest. It
		// takes the first half of the finishing allowance and runs beside the other
		// storages' manifests, so one storage stalling on its own record costs only
		// its own.
		//
		manifests, cancelManifests := context.WithTimeout(outer, cfg.Run.FinishAllowance.Std()/2)
		rep.Artifacts = artifactResults
		rep.Storages = e.assembleResults(order, results)
		rep.Duration = model.Duration(time.Since(started))
		interim := e.reportOutcome(&rep, notifierFailed)
		e.writeManifests(manifests, req.Kind, order, results, interim, &rep, caps)
		cancelManifests()

		// Retention runs only now, under the run's own deadline, over this run's own
		// state: every storage the run reached is in the overlay — an incomplete one
		// included, so the run counts itself in .Kept — the run-level map is marked
		// and the per-artifact ledger credited, but only where the manifest above is
		// saved and the artifact verified there. The previous same-day run therefore
		// stays pinned exactly until a newer copy a restore can actually reach
		// exists. The policy and the cap share one deletion state per storage.
		overlay := currentOverlay(rep.RunID, req.Kind, started, artifactResults, verifiedMatrix, order, results)
		for name, sr := range overlay {
			if sr.Verified {
				state.mark(sr.RunID, name)
			}
		}
		state.markCurrent(rep.RunID, req.Kind, started, artifactResults, verifiedMatrix, results)
		e.retentionSweep(ctx, started, req.Kind, order, surveys, state, results, overlay, caps)
	}

	// The rest of the ending keeps its own half of the allowance: the finished
	// notification and the catalog entry must still tell anyone what happened
	// even when retention spent the deadline's last minutes, so they get a
	// window that starts here rather than one retention has already eaten into.
	// The allowance grows out of the caller's context, so a pod being terminated
	// still ends the process the way the deployment asked — an in-process
	// allowance cannot cover a kill, and the next run's "previous run did not
	// report" stays the signal for that.
	finish, cancelFinish := context.WithTimeout(outer, cfg.Run.FinishAllowance.Std()/2)
	defer cancelFinish()

	rep.Artifacts = artifactResults
	rep.Storages = e.assembleResults(order, results)
	rep.Duration = model.Duration(time.Since(started))
	rep.Outcome = e.reportOutcome(&rep, notifierFailed)
	rep.ExitCode = e.exitCode(rep.Outcome)

	if e.notify(finish, model.Event{Name: model.EventFinished, At: time.Now().UTC(), Run: rep}, checks) &&
		cfg.Run.NotifierFailureIsFatal {
		rep.Outcome = model.OutcomeFailed
		rep.ExitCode = e.exitCode(rep.Outcome)
	}
	e.saveCatalog(finish, rep)
	return rep, nil
}

func (e *Engine) exitCode(outcome model.Outcome) int {
	switch outcome {
	case model.OutcomeDegraded:
		return e.cfg.Run.DegradedExitCode
	case model.OutcomeFailed:
		return e.cfg.Run.FailedExitCode
	default:
		return 0
	}
}

// newStorageRunState starts one storage's state for one run, its clock carrying
// the configured time budget.
func newStorageRunState(sb *storageBinding) *storageRunState {
	st := &storageRunState{name: sb.name, typ: sb.cfg.Type, clock: storageClock{budget: sb.cfg.TimeBudget.Std()}}
	st.result = model.StorageResult{Storage: sb.name, Type: sb.cfg.Type}
	return st
}

// describeStorage validates a storage's declaration once per run; a failure
// makes the storage fail immediately — nothing else can be attempted without a
// storage that answers. Nothing the run does reads the traits themselves, so
// only the failure signal matters here.
func (e *Engine) describeStorage(ctx context.Context, sb *storageBinding, st *storageRunState) {
	_, err := storageCall(&st.clock, ctx, func(ctx context.Context) (storage.StorageTraits, error) {
		return attempt(ctx, e.retry, "describe "+sb.name, func(ctx context.Context, _ int) (storage.StorageTraits, error) {
			return safeValue(func() (storage.StorageTraits, error) { return sb.inst.Describe(ctx) })
		})
	})
	if err != nil {
		st.setUploadError(fmt.Errorf("describe: %w", err))
		st.mu.Lock()
		st.tried = true
		st.fatal = err
		st.mu.Unlock()
		e.storageEvent(sb, human.Failure("storage.describe.failed", human.Text("reason", err.Error())))
	}
}

// collectArtifacts resolves the static sources and runs every discovery,
// turning everything into plans and every deliberate exclusion into one record;
// a plan whose resolution failed carries the reason. An instance whose preflight
// Check failed fails its own plan here: the artifact is recorded as failed and
// nothing of it is produced, while every other artifact still is.
//
// An instance whose domain holds several artifacts becomes one plan per unit
// (see expandUnits), unless its own build or check failed — then it is one
// failed plan, because a domain nobody could read is not a set of units.
func (e *Engine) collectArtifacts(ctx context.Context, checks failedChecks, scope *closerScope) ([]artifactPlan, []model.Exclusion) {
	plans := make([]artifactPlan, 0, len(e.sources))
	var excluded []model.Exclusion
	for _, sb := range e.sources {
		plan := artifactPlan{
			instance: sb.name, srcType: sb.typ, src: sb.inst, artifact: sb.artifact, pipeline: sb.pipeline,
			refs: sb.refs, procs: sb.procs, stages: encodeStages(sb.procs),
		}
		if err := checks.err("source", sb.name); err != nil {
			plan.problem = fmt.Errorf("source check: %w", err)
		} else {
			plan.problem = pipelineCheckProblem(checks, sb.refs)
		}
		plans = append(plans, e.expandPlan(ctx, e.retry, plan, scope)...)
	}
	for _, db := range e.discoveries {
		if err := checks.err("discovery", db.name); err != nil {
			e.at("discovery", "", db.name, human.Failure("discovery.failed",
				human.Text("reason", "the check failed: "+err.Error())))
			plans = append(plans, discoveryProblemPlan(db.name, fmt.Errorf("discovery %s check: %w", db.name, err)))
			continue
		}
		found, err := e.discoverAll(ctx, e.retry, db.name, db.inst, retryableProblem)
		if err != nil {
			e.at("discovery", "", db.name, human.Failure("discovery.failed", human.Text("reason", err.Error())))
			plans = append(plans, discoveryProblemPlan(db.name, fmt.Errorf("discovery %s failed: %w", db.name, err)))
			continue
		}
		for _, f := range found {
			if f.Excluded != "" {
				// The object was seen and deliberately produces no artifact:
				// one record of the decision, and nothing of it is built — an
				// excluded volume never reads its source's config. The group is
				// the one the discovery named, which is empty when no rule would
				// have produced the object.
				excluded = append(excluded, model.Exclusion{
					Name: f.Artifact.Name, Group: f.ExcludedGroup, Kind: f.Excluded, Reason: f.ExcludedReason,
				})
				continue
			}
			plan := artifactPlan{instance: f.Name, artifact: f.Artifact}
			switch {
			case f.Problem != "":
				// The object matched a rule but its source could not be
				// rendered or resolved: one failed artifact with the rule's
				// importance, and every other Found still runs.
				plan.problem = fmt.Errorf("discovery %s: %w", db.name, errors.New(f.Problem))
				plan.hint = f.Hint
			case f.Artifact.Name == "" || f.Artifact.Group == "":
				plan.problem = fmt.Errorf("discovery %s returned an artifact without group/name", db.name)
			case f.Artifact.Sensitivity != model.SensitivityPlain && f.Artifact.Sensitivity != model.SensitivitySecret:
				plan.problem = fmt.Errorf("discovery %s: artifact %s has sensitivity %q", db.name, f.Artifact.Name, f.Artifact.Sensitivity)
			case f.Artifact.Importance != model.ImportanceRequired && f.Artifact.Importance != model.ImportanceBestEffort:
				plan.problem = fmt.Errorf("discovery %s: artifact %s has importance %q", db.name, f.Artifact.Name, f.Artifact.Importance)
			}
			// The rule's pipeline: the processor instance names it wrote, resolved
			// against this config's processors.
			procs, refs, pipelineErr := e.resolvePipeline(ctx, "/discoveries/"+db.name+"/pipeline", f.Pipeline)
			plan.pipeline, plan.refs, plan.procs = pipelineName(refs), refs, procs
			if plan.problem == nil && pipelineErr != nil {
				plan.problem = pipelineErr
			}
			if plan.problem == nil {
				if err := checks.err("source", f.Name); err != nil {
					plan.problem = fmt.Errorf("source check: %w", err)
				} else if err := pipelineCheckProblem(checks, refs); err != nil {
					plan.problem = err
				}
			}
			if plan.problem == nil {
				s, err := plugin.Build(ctx, e.reg, source.Kind, f.Type, f.Config, e.deps("source", f.Type, f.Name))
				if err != nil {
					plan.problem = err
				} else {
					// The run owns what it built from the moment it exists: a
					// source whose Check fails is still closed with the run.
					scope.track(s)
					if err := e.checkSource(ctx, s, f.Name); err != nil {
						// A discovered source's Check is the same contract as a
						// static one's: it fails its own artifact, nothing else.
						plan.problem = err
					} else {
						plan.src = s
					}
				}
			}
			plan.srcType = f.Type
			plan.stages = encodeStages(plan.procs)
			plans = append(plans, e.expandPlan(ctx, e.retry, plan, scope)...)
		}
	}
	seen := map[string]bool{}
	for i := range plans {
		name := plans[i].artifact.Name
		if seen[name] && plans[i].problem == nil {
			plans[i].problem = fmt.Errorf("artifact name %q repeats within the run", name)
		}
		seen[name] = true
	}
	return plans, excluded
}

// expandPlan turns one resolved plan into the plans of the artifacts its
// instance holds (see expandUnits): the parent itself when its source is one
// artifact, one plan per unit otherwise. A plan that already carries a problem
// is returned as it stands — there is nothing to enumerate through, and the
// instance's own failure is its own artifact.
func (e *Engine) expandPlan(ctx context.Context, p retryPolicy, plan artifactPlan, scope *closerScope) []artifactPlan {
	if plan.problem != nil {
		return []artifactPlan{plan}
	}
	units := e.expandUnits(ctx, p, expandedUnit{
		instance: plan.instance, srcType: plan.srcType, src: plan.src, artifact: plan.artifact,
	}, scope)
	out := make([]artifactPlan, 0, len(units))
	for _, u := range units {
		unit := plan
		unit.instance, unit.srcType, unit.src, unit.artifact, unit.problem = u.instance, u.srcType, u.src, u.artifact, u.problem
		out = append(out, unit)
	}
	return out
}

// discoveryProblemPlan is how a run that could not look at the cluster reports
// it: there is no artifact record of a discovery, so the discovery itself
// becomes a required artifact that failed — a run that could not see its
// sources is never delivered.
func discoveryProblemPlan(name string, problem error) artifactPlan {
	return artifactPlan{
		instance: "discovery/" + name,
		artifact: model.Artifact{Name: "discovery/" + name, Group: "discovery",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		problem: problem,
	}
}

// checkSource runs one discovered source's Check under the run's retry policy;
// a plugin without a Check has nothing to fail.
func (e *Engine) checkSource(ctx context.Context, s source.Source, name string) error {
	checker, ok := s.(plugin.Checker)
	if !ok {
		return nil
	}
	_, err := attempt(ctx, e.retry, "check source "+name, func(ctx context.Context, _ int) (struct{}, error) {
		return struct{}{}, safeCall(func() error { return checker.Check(ctx) })
	})
	return err
}

// pipelineCheckProblem is the failure of a processor that failed its Check: an
// artifact routed through it is not produced, so a secret artifact whose
// sealing processor is the one that failed never leaves the process.
func pipelineCheckProblem(checks failedChecks, refs []string) error {
	for _, ref := range refs {
		if err := checks.err("processor", ref); err != nil {
			return fmt.Errorf("processor %s check: %w", ref, err)
		}
	}
	return nil
}

func encodeStages(procs []processor.Processor) []func(ctx context.Context, dst io.Writer, src io.Reader) error {
	var stages []func(ctx context.Context, dst io.Writer, src io.Reader) error
	for _, p := range procs {
		stages = append(stages, encodeStage(p))
	}
	return stages
}

// resolveTraits fills the plan's extension, sealed flag and stages, and enforces
// the secret invariant: a secret artifact whose bytes would leave the process
// unsealed is refused before the first byte.
func (e *Engine) resolveTraits(ctx context.Context, plan *artifactPlan) error {
	srcTr, err := attempt(ctx, e.retry, "describe source "+plan.instance, func(ctx context.Context, _ int) (source.SourceTraits, error) {
		return safeValue(func() (source.SourceTraits, error) { return plan.src.Describe(ctx) })
	})
	if err != nil {
		return fmt.Errorf("source %s describe: %w", plan.instance, err)
	}
	plan.artifact.Ext = srcTr.Ext
	plan.source = model.SourceRef{Instance: plan.instance, Type: plan.srcType, Origin: srcTr.Origin}
	sealed := srcTr.Sealed
	chain, err := e.artifactChain(ctx, plan)
	if err != nil {
		return err
	}
	for _, step := range chain {
		sealed = sealed || step.Seals
	}
	plan.chain = chain
	plan.artifact.Sealed = sealed
	if plan.artifact.Sensitivity == model.SensitivitySecret && !sealed {
		return plugin.Permanent(fmt.Errorf("artifact %s is secret but its pipeline has no sealing processor: refusing before the first byte", plan.artifact.Name))
	}
	return nil
}

// artifactChain is where the artifact's chain comes from: the process that
// produced the bytes, when the source carries a recorded chain (the stage's,
// read back by the staged source) — then the artifact's extension already
// carries every processor's suffix, because the file was named with it — or this
// run's own pipeline, whose processors are described for their extension and
// their sealing fact.
func (e *Engine) artifactChain(ctx context.Context, plan *artifactPlan) ([]model.ProcessorStep, error) {
	if rc, ok := plan.src.(source.RecordedChain); ok {
		// buildSource refuses a pipeline on such a source, so plan.procs is
		// empty here; the chain is the run's to record and a restore's to reverse.
		chain, err := attempt(ctx, e.retry, "recorded chain "+plan.instance, func(ctx context.Context, _ int) ([]model.ProcessorStep, error) {
			return safeValue(func() ([]model.ProcessorStep, error) { return rc.RecordedChain(ctx) })
		})
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("source %s recorded chain: %w", plan.instance, err)
		}
		return chain, nil
	}
	chain := make([]model.ProcessorStep, 0, len(plan.procs))
	for i, p := range plan.procs {
		tr, err := attempt(ctx, e.retry, "describe processor", func(ctx context.Context, _ int) (processor.ProcessorTraits, error) {
			return safeValue(func() (processor.ProcessorTraits, error) { return p.Describe(ctx) })
		})
		if err != nil {
			return nil, fmt.Errorf("processor %d describe: %w", i, err)
		}
		plan.artifact.Ext += tr.Ext
		step := model.ProcessorStep{Ext: tr.Ext, Seals: tr.Seals}
		if i < len(plan.refs) {
			step.Instance = plan.refs[i]
			step.Type = e.cfg.Processors[plan.refs[i]].Type
		}
		chain = append(chain, step)
	}
	return chain, nil
}

// produce resolves traits, checks the spool reserve, and streams the artifact
// into a fresh spool file per attempt.
func (e *Engine) produce(ctx context.Context, sp *spool.Spool, plan artifactPlan) (producedArtifact, error) {
	if err := e.resolveTraits(ctx, &plan); err != nil {
		return producedArtifact{}, err
	}
	free, err := sp.FreeBytes()
	if err != nil {
		return producedArtifact{}, err
	}
	if free < e.cfg.Run.SpoolReserveBytes.Int64() {
		return producedArtifact{}, plugin.Permanent(fmt.Errorf("spool has %d free bytes, reserve is %d", free, e.cfg.Run.SpoolReserveBytes))
	}
	e.sourceEvent(plan.srcType, plan.instance, human.New("source.dump.start", human.Text("origin", plan.source.Origin)))
	start := time.Now()
	pa, err := attempt(ctx, e.retry, "produce "+plan.artifact.Name, func(ctx context.Context, _ int) (producedArtifact, error) {
		w, err := sp.Create(plan.artifact.Group + "-" + plan.artifact.Name + plan.artifact.Ext)
		if err != nil {
			return producedArtifact{}, plugin.Permanent(err)
		}
		// The cap is the spool's own bound: an artifact past it fails itself —
		// permanently, because the next attempt produces the same bytes — while
		// every other artifact, and the manifest, still fit.
		limit := &limitWriter{w: w, limit: e.cfg.Run.MaxArtifactBytes.Int64()}
		raw, cerr := e.dumpChain(ctx, plan, limit)
		if closeErr := w.Close(); cerr == nil {
			cerr = closeErr
		}
		if cerr != nil {
			_ = sp.Remove(w.Name())
			return producedArtifact{}, cerr
		}
		stats, err := e.dumpStats(ctx, plan)
		if err != nil {
			_ = sp.Remove(w.Name())
			return producedArtifact{}, err
		}
		return producedArtifact{artifact: plan.artifact, chain: plan.chain, source: plan.source,
			file: w.Name(), bytes: w.Size(), raw: raw, sha256: w.SHA256(), stats: stats}, nil
	})
	if err != nil {
		return producedArtifact{}, fmt.Errorf("artifact %s: %w", plan.artifact.Name, err)
	}
	pa.took = time.Since(start)
	return pa, nil
}

// dumpStats asks the source what it left out of the stream it just wrote. A
// source without the capability — or answering errors.ErrUnsupported — leaves
// out nothing the run has to report. A source that cannot answer fails its own
// artifact: the counts are how a tree that was read while it lived stays
// visible, and an artifact whose report cannot be produced is not a copy anyone
// can lean on.
func (e *Engine) dumpStats(ctx context.Context, plan artifactPlan) (model.DumpStats, error) {
	reporter, ok := plan.src.(source.DumpStatsReporter)
	if !ok {
		return model.DumpStats{}, nil
	}
	stats, err := attempt(ctx, e.retry, "dump stats "+plan.instance, func(ctx context.Context, _ int) (model.DumpStats, error) {
		return safeValue(func() (model.DumpStats, error) { return reporter.DumpStats(ctx) })
	})
	if errors.Is(err, errors.ErrUnsupported) {
		return model.DumpStats{}, nil
	}
	if err != nil {
		return model.DumpStats{}, fmt.Errorf("reading what the source left out: %w", err)
	}
	return stats, nil
}

// limitWriter refuses the write that would cross the run's artifact cap. It sits
// between the pipeline's last stage and the spool, so the refusal stops the
// source itself — the bytes of a too-large volume never fill the spool the pod
// runs on.
type limitWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.written+int64(len(p)) > l.limit {
		return 0, plugin.Permanent(fmt.Errorf("artifact exceeds run.max_artifact_bytes: %d bytes written, cap is %d",
			l.written+int64(len(p)), l.limit))
	}
	n, err := l.w.Write(p)
	l.written += int64(n)
	return n, err
}

// dumpChain streams the source through its stages into dst, counting the raw
// bytes the source produced. Every stage is wrapped so the log shows what it
// did, and the dump reports its running total while it runs.
func (e *Engine) dumpChain(ctx context.Context, plan artifactPlan, dst io.Writer) (int64, error) {
	raw := &countWriter{}
	src := func(ctx context.Context, w io.Writer) error {
		raw.w = w
		return plan.src.Dump(ctx, raw)
	}
	stop := e.watchProgress(ctx, plan, raw)
	defer stop()
	err := chain(ctx, dst, src, e.traceStages(plan))
	return raw.n.Load(), err
}

// watchProgress reports the bytes a dump has produced, every configured
// interval, so a long dump is visibly alive rather than one line minutes later.
// It returns a stop function the caller defers; with the interval at 0 it does
// nothing at all.
func (e *Engine) watchProgress(ctx context.Context, plan artifactPlan, raw *countWriter) func() {
	if e.cfg.Run == nil {
		return func() {}
	}
	interval := e.cfg.Run.ProgressInterval.Std()
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	// stopped is the acknowledgement: stop() waits for it, so no tick already
	// selected — and no log write already in flight — can outlive the dump it
	// describes. Without it a progress line could land after the artifact's own
	// "done" line, or after the run ended.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(interval)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				n := raw.n.Load()
				if n == last {
					continue
				}
				last = n
				e.sourceEvent(plan.srcType, plan.instance, human.New("source.progress", human.Bytes("dumped", n)))
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}

// traceStages wraps every processor step so the log shows the bytes in, the
// bytes out and the time it took, under the processor instance's own name. A
// failing step reports its error here too, where the sizes are known.
func (e *Engine) traceStages(plan artifactPlan) []func(context.Context, io.Writer, io.Reader) error {
	out := make([]func(context.Context, io.Writer, io.Reader) error, len(plan.stages))
	for i, st := range plan.stages {
		out[i] = func(ctx context.Context, dst io.Writer, src io.Reader) error {
			in := &countReader{r: src}
			w := &countWriter{w: dst}
			start := time.Now()
			err := st(ctx, w, in)
			e.logProcessor(plan, i, in.n.Load(), w.n.Load(), time.Since(start), err)
			return err
		}
	}
	return out
}

func (e *Engine) logProcessor(plan artifactPlan, i int, in, out int64, took time.Duration, err error) {
	typ, name := "", fmt.Sprintf("step%d", i+1)
	if i < len(plan.refs) {
		name = plan.refs[i]
		typ = e.cfg.Processors[plan.refs[i]].Type
	}
	if err != nil {
		e.at("processor", typ, name, human.Failure("processor.failed",
			human.Bytes("in", in), human.Bytes("out", out), human.Duration("took", took), human.Text("reason", err.Error())))
		return
	}
	e.at("processor", typ, name, human.New("processor.done",
		human.Bytes("in", in), human.Bytes("out", out), human.Duration("took", took)))
}

// countWriter counts what flows through it. The counter is atomic because a
// dump's progress watcher reads it from another goroutine while the source
// writes.
type countWriter struct {
	w io.Writer
	n atomic.Int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	return n, err
}

// countReader counts what flows out of it, atomically for the same reason.
type countReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// processArtifact produces one artifact and uploads it to every storage,
// independently: one storage failing never stops the others.
//
// ar and verified are named results so the deferred duration lands on the value
// the caller receives: `return ar` copies a struct, so a closure over a local
// would set the duration on a copy the caller never sees — which is why every
// artifact's duration read 0s.
func (e *Engine) processArtifact(ctx context.Context, sp *spool.Spool, plan artifactPlan, runID string, kind model.RunKind,
	order []*storageBinding, results map[string]*storageRunState, caps map[string]*capState) (ar model.ArtifactResult, verified map[string]bool) {
	ar = model.ArtifactResult{Artifact: plan.artifact, Pipeline: plan.pipeline, Outcome: model.ArtifactFailed, Hint: plan.hint}
	verified = map[string]bool{}
	start := time.Now()
	defer func() { ar.Duration = model.Duration(time.Since(start)) }()
	if plan.problem != nil {
		ar.Error = plan.problem.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
		return ar, verified
	}
	pa, err := e.produce(ctx, sp, plan)
	if err != nil {
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
		return ar, verified
	}
	defer func() { _ = sp.Remove(pa.file) }()
	e.event(human.New("spool.write", human.Text("file", pa.file), human.Bytes("size", pa.bytes)))
	ar.Artifact = pa.artifact
	ar.Chain = pa.chain
	ar.Source = pa.source
	ar.Stats = pa.stats
	key, err := e.artifactKey(runID, pa.artifact)
	if err != nil {
		ar.Error = err.Error()
		return ar, verified
	}
	ar.Key = key
	sem := make(chan struct{}, e.cfg.Run.StorageParallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	label := human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)
	for _, sb := range order {
		wg.Add(1)
		go func(sb *storageBinding) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			// Each storage announces its own upload and reports its own result,
			// inside its own goroutine: a storage that stalls must not hold back
			// the line of one that finished minutes ago.
			e.storageEvent(sb, human.New("storage.upload.start", human.Text("artifact", label)))
			ok := e.uploadObject(ctx, sb, results[sb.name], pa, sp, key, kind, caps)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				verified[sb.name] = true
				e.storageEvent(sb, human.New("storage.upload.done",
					human.Text("artifact", label), human.Bytes("size", pa.bytes)))
			}
		}(sb)
	}
	wg.Wait()
	took := time.Since(start)
	if len(verified) > 0 {
		ar.Outcome = model.ArtifactOK
		ar.RawBytes = pa.raw
		ar.Bytes = pa.bytes
		ar.SHA256 = pa.sha256
		e.event(human.New("artifact.done",
			human.Text("artifact", label),
			human.Bytes("raw", pa.raw), human.Bytes("stored", pa.bytes), human.Duration("took", took),
			human.Count("skipped", pa.stats.SkippedEntries), human.Count("padded", pa.stats.PaddedFiles),
			human.Digest("sha256", pa.sha256), human.ObjectKey("key", key)))
	} else {
		ar.Error = "no storage verified the artifact"
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
	}
	return ar, verified
}

// uploadObject puts one object and verifies it: success is declared only when
// Stat shows the object at the expected size (and hash, when the storage exposes
// one). A storage that can upload one object as parts gets them driven by the
// engine (see multipartUpload), one call per part, so a failed part does not cost
// the parts already accepted; every other storage — and every object that fits a
// single request — keeps the whole-object Put. Every attempt re-opens the spool
// file.
func (e *Engine) uploadObject(ctx context.Context, sb *storageBinding, st *storageRunState, pa producedArtifact,
	sp *spool.Spool, key string, kind model.RunKind, caps map[string]*capState) bool {
	st.mu.Lock()
	st.tried = true
	fatal := st.fatal
	st.mu.Unlock()
	if fatal != nil {
		return false
	}
	label := human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name)
	// The ledger books the object before the write for both kinds; only a manual
	// run is refused on it. A scheduled run's writes are counted and the cap
	// phase frees room afterwards.
	if !e.reserveWrite(sb, st, caps[sb.name], key, pa.bytes, label, kind == model.RunKindManual) {
		return false
	}
	_, err := storageCall(&st.clock, ctx, func(ctx context.Context) (struct{}, error) {
		if handled, err := e.multipartUpload(ctx, sb, st, sp, pa, key); handled {
			return struct{}{}, err
		}
		return struct{}{}, e.putWhole(ctx, sb, st, sp, pa, key)
	})
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassPermanent && ctx.Err() == nil {
			st.mu.Lock()
			st.fatal = err
			st.mu.Unlock()
		}
		// The artifact's label, never its key: this error reaches the report
		// and the summary at info, and the key is machine detail (the log event
		// carries it as an ObjectKey attr, shown only at debug).
		st.setUploadError(fmt.Errorf("put %s: %w", label, err))
		e.storageEvent(sb, human.Failure("storage.upload.failed",
			human.Text("artifact", label),
			human.Text("reason", err.Error()), human.ObjectKey("key", key)))
		return false
	}
	if c := caps[sb.name]; c != nil {
		c.commit(key, pa.bytes)
	}
	st.markObject(pa.bytes)
	return true
}

// reserveWrite books one object's bytes in the storage's ledger before its
// write, for both kinds. For a manual run (refuse) it is also the admission
// test: a write past the cap, or with an unknown occupancy, is refused, marks
// the storage with the machine-readable reason and stops it there — the run
// keeps whatever it already uploaded and deletes nothing. A scheduled write is
// always booked and always proceeds; the cap phase frees room afterwards.
func (e *Engine) reserveWrite(sb *storageBinding, st *storageRunState, cap *capState, key string, size int64, label string, refuse bool) bool {
	if cap == nil || cap.reserve(key, size, refuse) {
		return true
	}
	limit, used := cap.limits()
	err := fmt.Errorf("cap_exceeded: %s would put the storage past its configured max_total_bytes (%s of %s used)",
		label, model.ByteSize(used), model.ByteSize(limit))
	st.refuseCap(err, limit, used)
	e.storageEvent(sb, human.Failure("storage.cap.refused",
		human.Text("artifact", label), human.Bytes("size", size), human.Text("reason", err.Error())))
	return false
}

// putWhole stores one object with a single Put per attempt: the whole object is
// re-sent on a retry, which is what a storage without the multipart capability
// gets, and what every small object gets from a storage that has one.
func (e *Engine) putWhole(ctx context.Context, sb *storageBinding, st *storageRunState,
	sp *spool.Spool, pa producedArtifact, key string) error {
	_, err := attempt(ctx, e.retry, "put "+human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name), func(ctx context.Context, _ int) (struct{}, error) {
		st.addAttempts(1)
		f, err := sp.Open(pa.file)
		if err != nil {
			return struct{}{}, plugin.Permanent(err)
		}
		defer func() { _ = f.Close() }()
		if _, err := safeValue(func() (model.ObjectInfo, error) {
			return sb.inst.Put(ctx, storage.PutRequest{Key: key, Size: pa.bytes, SHA256: pa.sha256}, f)
		}); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, e.verifyObject(ctx, sb, key, human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name), pa.bytes)
	})
	return err
}

// verifyObject proves one uploaded object is there: Stat must report the size
// that was sent. An object that is not visible right after its upload is a failed
// commit of this call, never the not_found of an absent key.
func (e *Engine) verifyObject(ctx context.Context, sb *storageBinding, key, label string, size int64) error {
	stt, err := safeValue(func() (model.ObjectInfo, error) { return sb.inst.Stat(ctx, key) })
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return plugin.Transient(fmt.Errorf("object %s is not visible after the upload (not committed)", label))
		}
		return err
	}
	if stt.Size != size {
		return plugin.Transient(fmt.Errorf("size after upload is %d, want %d: the upload is not committed", stt.Size, size))
	}
	return nil
}

// writeManifests writes the run report as the last object of THIS run on every
// storage that accepted at least one object, and verifies it there. A failed
// write or verification marks that storage's copy incomplete and leaves its
// manifest unsaved, which is what stops the retention floor from crediting this
// run on that storage: the manifest is written BEFORE retention, never after,
// because a copy no manifest records is a copy a restore cannot find.
//
// The write runs under its own half of the run's finishing allowance and on the
// caller's context rather than the deadline, and past the storage's own time
// budget: the objects are already there, and the record of them is what makes
// them findable. Every storage is written beside the others — a storage that
// stalls spends its own slice and no one else's, which is what leaves the
// notification and the catalog entry their half of the allowance.
func (e *Engine) writeManifests(ctx context.Context, kind model.RunKind, order []*storageBinding, results map[string]*storageRunState,
	outcome model.Outcome, rep *model.RunReport, caps map[string]*capState) {
	rep.Outcome = outcome
	body, err := json.Marshal(rep)
	if err != nil {
		// A report that cannot be marshalled cannot be stored; every storage that
		// took objects is marked incomplete, loudly.
		e.event(human.Failure("manifest.marshal.failed", human.Text("reason", err.Error())))
		for _, sb := range order {
			st := results[sb.name]
			st.mu.Lock()
			if st.tried && st.result.Objects > 0 {
				st.uploadErr = errors.Join(st.uploadErr, fmt.Errorf("manifest marshal: %w", err))
				st.fatal = err
			}
			st.mu.Unlock()
		}
		return
	}
	sum := model.SHA256Hex(body)
	key, err := e.manifestKey(rep.RunID)
	if err != nil {
		for _, sb := range order {
			results[sb.name].setUploadError(err)
		}
		return
	}
	var wg sync.WaitGroup
	for _, sb := range order {
		st := results[sb.name]
		st.mu.Lock()
		accepted := st.result.Objects > 0
		st.mu.Unlock()
		if !accepted {
			continue
		}
		// The manifest is an object write like any other: it is booked in the
		// ledger for both kinds, and a manual run whose artifacts filled the cap
		// does not slip its manifest in.
		if !e.reserveWrite(sb, st, caps[sb.name], key, int64(len(body)), "manifest.json", kind == model.RunKindManual) {
			continue
		}
		wg.Add(1)
		go func(sb *storageBinding, st *storageRunState) {
			defer wg.Done()
			// The budget is skipped on purpose and the time is still booked:
			// the objects are already there, and the record of them is worth
			// the seconds the storage's own budget would refuse.
			start := time.Now()
			_, err := attempt(ctx, e.retry, "manifest "+sb.name, func(ctx context.Context, _ int) (struct{}, error) {
				st.addAttempts(1)
				if _, err := safeValue(func() (model.ObjectInfo, error) {
					return sb.inst.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body)), SHA256: sum},
						bytes.NewReader(body))
				}); err != nil {
					return struct{}{}, err
				}
				// The manifest is verified like an artifact: a Put that returned
				// ok but left nothing visible is a run nobody can find, and the
				// floor must not lean on it.
				return struct{}{}, e.verifyObject(ctx, sb, key, "manifest.json", int64(len(body)))
			})
			st.clock.add(time.Since(start))
			if err != nil {
				st.setUploadError(fmt.Errorf("manifest: %w", err))
				e.storageEvent(sb, human.Failure("manifest.write.failed", human.Text("reason", err.Error())))
				return
			}
			if c := caps[sb.name]; c != nil {
				c.commit(key, int64(len(body)))
			}
			st.markManifest()
			st.markObject(int64(len(body)))
		}(sb, st)
	}
	wg.Wait()
}

// assembleResults renders the per-storage results in config order.
func (e *Engine) assembleResults(order []*storageBinding, results map[string]*storageRunState) []model.StorageResult {
	out := make([]model.StorageResult, 0, len(order))
	for _, sb := range order {
		st := results[sb.name]
		st.mu.Lock()
		res := st.result
		tried, uploadErr, retentionErr := st.tried, st.uploadErr, st.retentionErr
		res.CapExceeded = st.capExceeded
		res.CapLimitBytes = st.capLimit
		res.CapUsedBytes = st.capUsed
		st.mu.Unlock()
		res.Duration = model.Duration(st.clock.total())
		switch {
		case !tried:
			res.Outcome = model.StorageSkipped
		case res.Objects > 0 && uploadErr == nil:
			res.Outcome = model.StorageVerified
		default:
			res.Outcome = model.StorageFailed
		}
		if joined := errors.Join(uploadErr, retentionErr); joined != nil {
			res.Error = joined.Error()
		}
		out = append(out, res)
	}
	return out
}

// reportOutcome turns the report into delivered / degraded / failed.
func (e *Engine) reportOutcome(rep *model.RunReport, notifierFailed bool) model.Outcome {
	if rep.Lease != nil && (rep.Lease.Refused || rep.Lease.Error != "") {
		// The run never wrote: the writer lease decided it. A refusal or a failed
		// wait is failed whatever the (empty) artifact and storage lists would say.
		return model.OutcomeFailed
	}
	verifiedStorages := 0
	anyFailure := false
	for _, s := range rep.Storages {
		switch s.Outcome {
		case model.StorageVerified:
			verifiedStorages++
		default:
			anyFailure = true
		}
	}
	requiredOK := true
	for _, a := range rep.Artifacts {
		if a.Outcome != model.ArtifactOK {
			anyFailure = true
			if a.Artifact.Importance == model.ImportanceRequired {
				requiredOK = false
			}
		}
	}
	if notifierFailed && e.cfg.Run.NotifierFailureIsFatal {
		anyFailure = true
	}
	if !requiredOK || verifiedStorages < e.cfg.Run.MinVerifiedStorages {
		return model.OutcomeFailed
	}
	if anyFailure {
		return model.OutcomeDegraded
	}
	return model.OutcomeDelivered
}

// currentOverlay projects this run's own upload results into the retention
// state, so the guard sees the copies written minutes ago as well.
//
// Every storage the run reached is present — an incomplete one included, or a
// partially delivered run would leave .Kept short by the run itself — but only
// a storage where every artifact verified AND the manifest is saved carries
// Verified: a run the floor may lean on is one a restore can find through its
// manifest, and one that holds a copy of everything it claims.
func currentOverlay(runID string, kind model.RunKind, started time.Time, artifacts []model.ArtifactResult,
	verifiedMatrix map[string]map[string]bool, order []*storageBinding,
	results map[string]*storageRunState) map[string]model.StoredRun {
	out := map[string]model.StoredRun{}
	for _, sb := range order {
		st := results[sb.name]
		if !st.reached() {
			continue // the run put nothing here: it is not a run this storage holds
		}
		all := len(artifacts) > 0
		var size int64
		for _, a := range artifacts {
			if !verifiedMatrix[a.Artifact.Name][sb.name] {
				all = false
				continue
			}
			size += a.Bytes
		}
		saved := st.manifestSaved()
		out[sb.name] = model.StoredRun{
			RunID: runID, Kind: kind, At: started, Bytes: size,
			Verified: all && saved, Manifest: saved,
		}
	}
	return out
}

// previous reads the catalog for the newest SCHEDULED run: an unfinished one is
// a run that started and never reported, LastGoodAt the newest scheduled run
// that was delivered or degraded. A manual run is neither — its absence from
// this answer is what keeps a manual failure from reading as a missed night —
// and a manual run still within its deadline counts as active, never as
// abandoned.
func (e *Engine) previous(ctx context.Context) model.PreviousRun {
	if e.catalog == nil {
		return model.PreviousRun{}
	}
	runs, err := attempt(ctx, e.retry, "catalog load", func(ctx context.Context, _ int) ([]model.RunReport, error) {
		return safeValue(func() ([]model.RunReport, error) {
			// No limit: the history is bounded by the catalog's own max_runs, and
			// a page of manual runs must not hide the last scheduled one.
			return e.catalog.Load(ctx, catalog.LoadRequest{})
		})
	})
	if err != nil {
		e.event(human.Failure("catalog.load.failed", human.Text("reason", err.Error())))
		return model.PreviousRun{}
	}
	prev := model.PreviousRun{}
	for _, r := range runs {
		if model.NormalizeKind(r.Kind) != model.RunKindScheduled {
			continue
		}
		if prev.RunID == "" {
			prev.RunID = r.RunID
			if r.Outcome == model.OutcomeRunning {
				prev.Unfinished = true
			}
		}
		if r.Outcome == model.OutcomeDelivered || r.Outcome == model.OutcomeDegraded {
			prev.LastGoodAt = r.StartedAt
			break
		}
	}
	return prev
}

// inspectPreviousJob fills rep.Previous.Failure from the cluster when the catalog
// can read the previous Job of this run's own CronJob — the failures that leave
// no catalog record, so .Run.Previous alone would never show them. A catalog
// without the capability, no pod identity, or a read that fails all leave the
// run untouched: a missing grant is surfaced, never fatal.
func (e *Engine) inspectPreviousJob(ctx context.Context, rep *model.RunReport) {
	inspector, ok := e.catalog.(catalog.PreviousRunInspector)
	if !ok {
		return
	}
	namespace, pod, _ := e.holderIdentity()
	failure, err := safeValue(func() (*model.PreviousFailure, error) {
		return inspector.PreviousJobFailure(ctx, namespace, pod)
	})
	switch {
	case err != nil:
		rep.Previous.Unavailable = "previous run status unavailable: " + err.Error()
		e.event(human.Warning("run.previous.unavailable", human.Text("reason", err.Error())))
	case failure != nil:
		rep.Previous.Failure = failure
		e.event(human.New("run.previous.failed", human.Text("job", failure.Job), human.Text("reason", failure.Reason)))
	}
}

func (e *Engine) saveCatalog(ctx context.Context, rep model.RunReport) {
	if e.catalog == nil {
		return
	}
	_, err := attempt(ctx, e.retry, "catalog save", func(ctx context.Context, _ int) (struct{}, error) {
		return struct{}{}, safeCall(func() error { return e.catalog.Save(ctx, rep) })
	})
	if err != nil {
		e.at("catalog", e.catalogType(), "catalog", human.Failure("catalog.failed",
			human.Text("run", rep.RunID), human.Text("reason", err.Error())))
		return
	}
	e.at("catalog", e.catalogType(), "catalog", human.New("catalog.saved", human.Text("run", rep.RunID)))
}

// catalogType is the catalog instance's registered type, for the log's prefix.
func (e *Engine) catalogType() string {
	if e.cfg.Catalog == nil {
		return ""
	}
	return e.cfg.Catalog.Type
}

// notify delivers one event to every notifier; true when any of them failed. A
// notifier that failed its Check is not called and counts as failed — the same
// way one whose call failed does, so notifier_failure_is_fatal decides.
func (e *Engine) notify(ctx context.Context, ev model.Event, checks failedChecks) bool {
	failed := false
	for _, nb := range e.notifiers {
		if err := checks.err("notifier", nb.name); err != nil {
			failed = true
			e.at("notifier", e.notifierType(nb.name), nb.name, human.Failure("notify.check-failed",
				human.Text("event", ev.Name), human.Text("reason", err.Error())))
			continue
		}
		_, err := attempt(ctx, e.retry, "notify "+nb.name, func(ctx context.Context, _ int) (struct{}, error) {
			return struct{}{}, safeCall(func() error { return nb.inst.Notify(ctx, ev) })
		})
		if err != nil {
			failed = true
			e.at("notifier", e.notifierType(nb.name), nb.name, human.Failure("notify.failed",
				human.Text("event", ev.Name), human.Text("reason", err.Error())))
			continue
		}
		e.at("notifier", e.notifierType(nb.name), nb.name, human.New("notify.sent", human.Text("event", ev.Name)))
	}
	return failed
}

// notifierType is the registered type of one notifier instance, for the log's
// prefix; empty when the config does not carry it.
func (e *Engine) notifierType(name string) string {
	if inst, ok := e.cfg.Notifiers[name]; ok {
		return inst.Type
	}
	return ""
}

// capState accounts one storage's occupancy against its configured cap and
// admits a manual run's writes. Occupancy is the survey's own listing bytes —
// every run of every kind, manifests included — plus what this run has written
// or is writing: an object reserved before its write is not reserved twice on a
// retry, and an unknown occupancy (a survey that failed) is never free.
//
// A partially written multipart upload is not in any listing; the object's full
// size is reserved before its write, so the cap covers it from the moment the
// upload starts, and committing it only moves the reservation into occupancy.
type capState struct {
	cap       int64
	occupancy int64
	known     bool
	reserved  map[string]int64
	mu        sync.Mutex
}

func newCapState(cap, surveyBytes int64, known bool) *capState {
	return &capState{cap: cap, occupancy: surveyBytes, known: known, reserved: map[string]int64{}}
}

// reserve books one object's bytes before its write — for BOTH kinds, so a
// committed-but-unverified scheduled write stays counted too. refuse makes it an
// admission test: a manual write past the cap, or with an unknown occupancy, is
// refused; a scheduled write always proceeds and the cap phase frees room later.
func (c *capState) reserve(key string, n int64, refuse bool) bool {
	if c.cap <= 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.reserved[key]; ok {
		return true // a retry of an object already reserved
	}
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	if refuse && (!c.known || c.occupancy+pending+n > c.cap) {
		return false
	}
	c.reserved[key] = n
	return true
}

// markUnknown records that the storage's occupancy cannot be established — an
// unfinished multipart upload of an earlier writer whose bytes no listing
// exposes, or a sweep that could not list them. A manual run is then refused on
// this storage: unknown is never free.
func (c *capState) markUnknown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.known = false
}

// commit books a written object: its reservation becomes occupancy.
func (c *capState) commit(key string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.reserved, key)
	c.occupancy += n
}

// releaseBytes subtracts a CONFIRMED deletion. Nothing else ever lowers the
// ledger: an object that might still exist stays counted, so the cap never reads
// free space that is not free.
func (c *capState) releaseBytes(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.occupancy -= n
	if c.occupancy < 0 {
		c.occupancy = 0
	}
}

// used is the storage's occupancy as this run knows it: the survey's listing
// plus every write this run committed, minus confirmed deletions, with pending
// uploads counted as used. It is the one number both admission and eviction read.
func (c *capState) used() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	return c.occupancy + pending
}

// limits reports the configured cap and the bytes held when a refusal fired:
// the occupancy, reservations included, at the moment of the question.
func (c *capState) limits() (limit, used int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	return c.cap, c.occupancy + pending
}

// surveyBytes is the real listing bytes of one storage under the run prefix:
// every run of every kind, manifests and partially delivered objects included.
// A survey that failed holds no answer, and its caller passes known=false.
func surveyBytes(sur survey) int64 {
	if !sur.ok {
		return 0
	}
	var total int64
	for _, r := range sur.runs {
		for _, o := range r.objects {
			total += o.Size
		}
	}
	return total
}

// checkRunIDFree refuses a run whose id a storage already holds: an id is
// second-precise, and two runs of the same id would overwrite each other's
// objects and manifest. It runs before the first upload.
func (e *Engine) checkRunIDFree(surveys map[string]survey, runID string) error {
	for _, name := range sortedKeys(surveys) {
		sur := surveys[name]
		if !sur.ok {
			continue
		}
		if _, taken := sur.runs[runID]; taken {
			return fmt.Errorf("run: id %s already exists on storage %s; a run id must be unique", runID, name)
		}
	}
	return nil
}

// dryRunReport resolves every plan without producing or uploading anything; the
// storages stay skipped and nothing is written anywhere.
func (e *Engine) dryRunReport(ctx context.Context, rep model.RunReport, plans []artifactPlan,
	order []*storageBinding, results map[string]*storageRunState) model.RunReport {
	rep.Artifacts = make([]model.ArtifactResult, 0, len(plans))
	outcome := model.OutcomeDelivered
	for _, plan := range plans {
		ar := model.ArtifactResult{Artifact: plan.artifact, Pipeline: plan.pipeline, Outcome: model.ArtifactOK}
		switch {
		case plan.problem != nil:
			ar.Outcome = model.ArtifactFailed
			ar.Error = plan.problem.Error()
		default:
			if err := e.resolveTraits(ctx, &plan); err != nil {
				ar.Outcome = model.ArtifactFailed
				ar.Error = err.Error()
			} else {
				ar.Artifact = plan.artifact
			}
		}
		if ar.Outcome != model.ArtifactOK {
			outcome = model.OutcomeFailed
		}
		rep.Artifacts = append(rep.Artifacts, ar)
	}
	rep.Duration = model.Duration(time.Since(rep.StartedAt))
	rep.Storages = e.assembleResults(order, results)
	rep.Outcome = outcome
	rep.ExitCode = e.exitCode(outcome)
	return rep
}
