package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/transfer"
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
	// steps are the pipeline's processors, in config order, with the instance
	// names the manifest records.
	steps   []pipeline.Step
	problem error
	// hint is the short, actionable half of problem, when the object's own
	// failure carried one (see discovery.Found.Hint). It is recorded on the
	// artifact beside the error, never instead of it.
	hint string
}

// storageRunState accumulates one storage's result through the run. Upload and
// retention errors are kept apart: a failed retention run is visible in the
// report but never downgrades a verified copy.
type storageRunState struct {
	name   string
	typ    string
	clock  *execution.Clock
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
	// writesStarted is set when the first potentially asynchronous write of this
	// run begins on this storage. A storage that never started a write — a
	// permanent preflight failure, an open failure, a cap refusal — cannot settle
	// anything later, so it must not keep the run's own pending id.
	writesStarted bool
	// capExceeded is set when this storage refused an object because the run's
	// own configured byte cap was reached — the machine-readable reason a manual
	// run reports instead of a storage outage, with the limit and the bytes that
	// tripped it.
	capExceeded bool
	capLimit    int64
	capUsed     int64
	fatal       error // a permanent failure: further uploads to this storage are skipped
}

// markWritesStarted records that this storage's first write begins now.
func (st *storageRunState) markWritesStarted() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.writesStarted = true
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
//
// A cancelled caller — SIGTERM — starts the cancel sequence while the run is
// still unwinding: admission closes, the request goroutines are joined, the
// claim is released with its durable uncertainty, exactly this run's own upload
// IDs are aborted, and the invocation scope closes. Without a complete join the
// lease is never released.
func (e *Workflow) Run(ctx context.Context, req RunRequest) (model.RunReport, error) {
	e.resetInvocation()
	type outcome struct {
		rep model.RunReport
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		rep, err := e.runOperation(ctx, req)
		e.mu.Lock()
		close(e.opDone)
		e.mu.Unlock()
		done <- outcome{rep: rep, err: err}
	}()
	select {
	case o := <-done:
		if ctx.Err() == nil {
			return o.rep, o.err
		}
		_ = e.shutdown(ctx)
		return o.rep, o.err
	case <-ctx.Done():
		serr := e.shutdown(ctx)
		var phase *lifecycle.PhaseError
		if errors.As(serr, &phase) && phase.Phase == lifecycle.PhaseJoin {
			// A request goroutine that ignored cancellation cannot be stopped by
			// a deadline: the report is unavailable and the claim stays held.
			return model.RunReport{}, serr
		}
		o := <-done
		// Cancellation itself is not a run failure: phase failures are logged
		// and diagnostic, and the report stands.
		_ = serr
		return o.rep, o.err
	}
}

func (e *Workflow) runOperation(ctx context.Context, req RunRequest) (model.RunReport, error) {
	run := e.run
	if !e.runConfigured {
		return model.RunReport{}, errors.New("run: config needs the run section")
	}
	if !req.Kind.Valid() {
		return model.RunReport{}, fmt.Errorf("run: kind must be %q or %q, got %q", model.RunKindScheduled, model.RunKindManual, req.Kind)
	}
	prefix, err := e.layout.Prefix()
	if err != nil {
		return model.RunReport{}, err
	}
	if req.Kind == model.RunKindManual && !e.catalogConfigured {
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
	if errs := e.binaryErrors(); len(errs) > 0 {
		// A missing binary is the deployment, not a blip: the run refuses
		// before any plugin work rather than failing every artifact at night.
		return model.RunReport{}, fmt.Errorf("preflight: %w", errors.Join(errs...))
	}
	outer := ctx
	ctx, cancel := context.WithTimeout(ctx, run.Deadline.Std())
	defer cancel()
	// The run's own cancellation switch: CancelRequests closes it, so producers
	// and request contexts stop before the join.
	ctx, cancelOp := context.WithCancel(ctx)
	e.setOpCancel(cancelOp)
	defer e.clearOpCancel()
	defer cancelOp()

	// The sources this run's discoveries build belong to the invocation's one
	// scope: the role closes it after the operation returned, and the cancel
	// sequence closes it last. The workflow never closes it itself.
	scope := e.scope
	if scope == nil {
		scope = lifecycle.New()
	}

	started := time.Now().UTC()
	rep := model.RunReport{Schema: model.ManifestSchema, RunID: e.layout.RunID(req.Kind, started), Kind: req.Kind,
		StartedAt: started, Outcome: model.OutcomeRunning}
	e.event(human.New("run.start",
		human.Text("kind", string(req.Kind)),
		human.Text("run", rep.RunID),
		human.Text("config", req.ConfigPath),
		human.Text("storages", strings.Join(e.storageNames(), ", "))))

	// One instance's failed Check fails that instance's own work, never the
	// run: a backup of everything that still works beats no backup at all.
	checks := e.scopedChecks(ctx)
	if err := checks.err("catalog", "catalog"); err != nil {
		e.event(human.Failure("catalog.failed", human.Text("run", rep.RunID), human.Text("reason", err.Error())))
	}
	plans, excluded := e.collectArtifacts(ctx, checks, scope)
	// Every object the run saw and deliberately did not back up is part of the
	// report from here on: the manifest and the finished event both carry this
	// list, in every outcome.
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
			if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassPermanent {
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
	claim, held, err := e.lease.Acquire(ctx, req.Kind, rep.RunID)
	refused := false
	switch {
	case err == nil:
		// The claim, if any, is held below.
	case errors.Is(err, catalog.ErrLeaseHeld) && req.Kind == model.RunKindManual:
		// A manual run refuses at once while the holder is alive: no storage is
		// touched, the outcome is failed, and the lease is the machine-readable
		// reason. The refusal goes through the common finish path below — the
		// finished notification still runs.
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
		// below still runs the finished notification, exactly as the manual
		// refusal does. The lease is the machine-readable reason.
		refused = true
		e.event(human.Failure("lease.failed",
			human.Text("kind", string(req.Kind)), human.Text("run", rep.RunID), human.Text("reason", err.Error())))
		rep.Lease = &model.LeaseState{Holder: held.Holder, HolderKind: held.Kind, Error: err.Error()}
	}
	if claim != nil && !refused {
		// The ledger's write authority comes only from verified protection state:
		// the held claim is bound before the survey, alongside the destinations
		// every authority-gated write needs.
		e.ledger.BindLease(e.lease)
		e.setClaim(claim)
		defer func() {
			if outer.Err() != nil {
				return // the cancel sequence releases after a complete join
			}
			if rerr := e.lease.Release(outer); rerr != nil {
				e.event(human.Failure("lease.release.failed", human.Text("run", rep.RunID), human.Text("reason", rerr.Error())))
			}
		}()
	}

	var (
		artifactResults  []model.ArtifactResult
		verifiedMatrix   = map[string]map[string]bool{}
		notifierFailed   bool
		surveys          map[string]inventory.Survey
		surveyed         []reporting.SurveyedRun
		uncertaintyArmed bool
		handles          map[string]guard.Handle
		transfers        map[string]*transfer.Transfer
	)
	if !refused {
		// The spool comes before the first event: a run that cannot buffer has not
		// started, and no notifier hears about a run that never will.
		sp, err := spool.New(run.SpoolDir)
		if err != nil {
			return model.RunReport{}, err
		}

		notifierFailed, _ = e.notify(ctx, model.Event{Name: model.EventStarted, At: started, Run: rep}, checks)
		// The record's uncertainty is decided BEFORE the first survey call and
		// settled after it. A survey that starts under an uncertainty and
		// finishes after a later release is still a snapshot taken under it — the
		// dead holder's in-flight operations may have landed while the listing
		// was running — so the run starts with occupancy unknown and a manual run
		// would be refused on every storage. Settlement does not guess a clock:
		// two consecutive full surveys of the predecessor's prefix that agree on
		// their inventory are the proof, and only that clears the record.
		uncertain := claim != nil && claim.Uncertainty != nil
		// The observation attempt is recorded durably BEFORE the survey: a process
		// that dies mid-survey or a failed invalidation leaves the in-flight marker
		// behind, so the next survey starts a fresh observation instead of
		// clearing a stale digest with a listing it never finished. A marker that
		// cannot be written refuses the run before its survey: an observation
		// nobody could start is not evidence, and surveying would manufacture
		// exactly the bridge the marker exists to prevent.
		if req.Kind == model.RunKindScheduled && uncertain {
			if err := e.lease.BeginObservation(ctx); err != nil {
				refused = true
				e.event(human.Failure("lease.uncertain.failed", human.Text("run", rep.RunID),
					human.Text("reason", "the observation attempt is not durable: "+err.Error())))
				rep.Lease = &model.LeaseState{Error: err.Error()}
			}
		}
		if !refused {
			// The surveys run together — a slow listing on one storage costs its own
			// seconds, not every later storage's — and the run waits for all of them
			// before the collision check or the first write: nothing is written on one
			// storage while another is still being surveyed.
			surveys = map[string]inventory.Survey{}
			var surveyMu sync.Mutex
			var surveyWG sync.WaitGroup
			for _, sb := range order {
				surveyWG.Add(1)
				go func(sb *storageBinding) {
					defer surveyWG.Done()
					// The run owns the storage's budget: the survey is one interval of
					// it, bounded by what is left, never a fresh budget per call.
					st := results[sb.name]
					end := st.clock.Begin()
					defer end()
					sctx, cancel := st.clock.Bound(ctx)
					defer cancel()
					s := e.inventory.SurveyStorage(sctx, prefix, inventory.Storage{
						Name: sb.name, Reader: sb.port, CallParallelism: sb.cfg.CallParallelism,
						Log: e.deps("storage", sb.cfg.Type, sb.name).Log,
					})
					surveyMu.Lock()
					surveys[sb.name] = s
					surveyMu.Unlock()
				}(sb)
			}
			surveyWG.Wait()
			if uncertain {
				if _, serr := e.lease.SettleInheritedUncertainty(ctx, req.Kind, rep.RunID, e.storageNames(), surveys); serr != nil {
					e.event(human.Failure("lease.uncertain.failed", human.Text("run", rep.RunID),
						human.Text("reason", "occupancy stays unknown: "+serr.Error())))
				}
			}
		}
		// The finished event gets the previous-run record; the manifests this run
		// surveyed are its only storage input, and the Job lookup runs under the
		// finish window below — never under the run deadline.
		if !refused {
			surveyed = surveyedRuns(order, surveys)
			if err := e.checkRunIDFree(surveys, rep.RunID); err != nil {
				return model.RunReport{}, err
			}
			e.ledger.Start(surveys)
			// The write authority exists only now: the lease is held, this run's id is
			// free and its admission checks passed. Every write below goes through a
			// handle issued under it; closing it revokes them, and deletion is not a
			// method of a handle at all.
			// The authority outlives the deadline by the finishing allowance: the
			// manifest and the finished report are written after every artifact call
			// has stopped, and a handle expired with the deadline would lose them.
			runDeadline, _ := ctx.Deadline()
			authority := e.ledger.Authorize(runDeadline.Add(run.FinishAllowance.Std()))
			defer authority.Close()
			handles = map[string]guard.Handle{}
			for _, sb := range order {
				h, err := sb.port.Open(authority)
				if err != nil {
					st := results[sb.name]
					st.mu.Lock()
					st.tried = true
					st.fatal = err
					st.mu.Unlock()
					st.setUploadError(fmt.Errorf("open: %w", err))
					e.storageEvent(sb, human.Failure("storage.open.failed", human.Text("reason", err.Error())))
					continue
				}
				handles[sb.name] = h
			}
			// One transfer per storage for the whole run: it keeps the upload IDs it
			// opened, so the shutdown abort can destroy exactly this run's own.
			transfers = map[string]*transfer.Transfer{}
			for _, sb := range order {
				if _, ok := handles[sb.name]; !ok {
					continue
				}
				transfers[sb.name] = e.transferFor(sb, req.Kind, results[sb.name])
			}
			e.setTransfers(transfers)
			// A run that was killed outright left its unfinished uploads behind: sweep
			// them before this run's own uploads start. The cutoff reaches back exactly
			// the longest a live run can last, so an upload a run still might be writing
			// is never taken — the lease already guarantees no other writer is live, and
			// the cutoff stays a second, independent rail. Whatever the sweep cannot
			// count or abort leaves the storage's occupancy unknown, which refuses a
			// manual run there.
			staleCutoff := time.Now().UTC().Add(-(run.Deadline.Std() + run.FinishAllowance.Std()))
			for _, sb := range order {
				// The full inventory, any age, is what makes an earlier writer's parts
				// visible: the stale selection below aborts only the old ones, and a
				// young or undated upload still bills bytes no listing shows. It runs
				// through the same call pipeline as every storage call — the storage's
				// budget, execution's retries and counted attempts — and an inventory
				// that cannot be read leaves occupancy unknown loudly.
				tr, ok := transfers[sb.name]
				if !ok {
					continue // the destination never opened: its writes already failed
				}
				attempts, unknown := tr.Sweep(ctx, handles[sb.name], staleCutoff)
				results[sb.name].addAttempts(attempts)
				if unknown {
					e.ledger.Unknown(sb.name)
				}
			}
			// A storage whose accepted write may settle after the call returns must
			// be durably marked before the first such write: an inherited record
			// gains this run's id (it is never dropped), so the settlement digest
			// covers every pending prefix, this run's own writes included.
			async := writesAsynchronously(order)
			if async && claim == nil {
				// No durable record exists to mark: a run that cannot make its
				// async writes conservative refuses before writing.
				refused = true
				e.event(human.Failure("lease.failed",
					human.Text("kind", string(req.Kind)), human.Text("run", rep.RunID),
					human.Text("reason", "an asynchronous storage needs a durable writer lease to mark")))
				rep.Lease = &model.LeaseState{Error: "an asynchronous storage needs a durable writer lease to mark"}
			} else if writesAsynchronously(order) {
				if err := e.lease.MarkUncertainty(ctx); err != nil {
					// The started event has already gone out and no byte is written:
					// the run ends through the common finish, like a refused lease.
					refused = true
					e.event(human.Failure("lease.failed",
						human.Text("kind", string(req.Kind)), human.Text("run", rep.RunID), human.Text("reason", err.Error())))
					rep.Lease = &model.LeaseState{Error: err.Error()}
				} else {
					uncertaintyArmed = true
				}
			}
			// Retention must never delete an INHERITED pending prefix, and an earlier
			// holder's accepted writes make every storage's occupancy unknown. This
			// run's own id is in the record too, but its pending bytes are tracked
			// exactly by the ledger's reservations: marking it would declare this
			// run's own occupancy unknown and refuse its manual writes on a cap it
			// fits.
			for _, id := range e.lease.PendingRunIDs() {
				if id == rep.RunID {
					continue
				}
				e.ledger.MarkUncertain(id)
			}
		}

		if !refused {
			// Artifacts, one at a time: produce into the spool, upload to every storage,
			// remove the file, next artifact.
			for _, plan := range plans {
				e.event(human.New("artifact.start",
					human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)),
					human.Text("importance", string(plan.artifact.Importance))))
				ar, verified := e.processArtifact(ctx, sp, plan, rep.RunID, req.Kind, order, results, handles, transfers)
				artifactResults = append(artifactResults, ar)
				verifiedMatrix[plan.artifact.Name] = verified
			}
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
		manifests, cancelManifests := context.WithTimeout(outer, run.FinishAllowance.Std()/2)
		rep.Artifacts = artifactResults
		rep.Storages = e.assembleResults(order, results)
		rep.Duration = model.Duration(time.Since(started))
		interim := e.reportOutcome(&rep, notifierFailed)
		e.writeManifests(manifests, req.Kind, order, results, interim, &rep, handles)
		cancelManifests()

		// Retention runs only now, under the run's own deadline, over this run's own
		// state: every storage the run reached is in the overlay — an incomplete one
		// included, so the run counts itself in .Kept — the run-level map is marked
		// and the per-artifact ledger credited, but only where the manifest above is
		// saved and the artifact verified there. The previous same-day run therefore
		// stays pinned exactly until a newer copy a restore can actually reach
		// exists. The policy and the cap share one deletion state per storage.
		overlay := currentOverlay(rep.RunID, req.Kind, started, artifactResults, verifiedMatrix, order, results)
		e.retentionSweeps(ctx, started, req.Kind, order, surveys, results, overlay, artifactResults, verifiedMatrix)
	}

	// The rest of the ending keeps its own half of the allowance: the finished
	// notification must still tell anyone what happened even when retention
	// spent the deadline's last minutes, so it gets a window that starts here
	// rather than one retention has already eaten into.
	// The allowance grows out of the caller's context, so a pod being terminated
	// still ends the process the way the deployment asked — an in-process
	// allowance cannot cover a kill, and the next run's "previous run did not
	// report" stays the signal for that.
	finish, cancelFinish := context.WithTimeout(outer, run.FinishAllowance.Std()/2)
	defer cancelFinish()

	rep.Artifacts = artifactResults
	rep.Storages = e.assembleResults(order, results)
	rep.Duration = model.Duration(time.Since(started))
	rep.Outcome = e.reportOutcome(&rep, notifierFailed)
	rep.ExitCode = e.exitCode(rep.Outcome)
	// Every finished event carries the previous-run record, refused runs
	// included: their storage input is empty, but the previous Job status is
	// still available to the finish window.
	if e.reporter != nil {
		rep.Previous = e.reporter.Previous(finish, surveyed)
		e.reportPrevious(rep.Previous)
	}

	// The verdict is composed first, and a finished notification nobody received
	// is the only notification failure allowed to lower it: a message that did
	// reach a receiver already names the outcome, so failing the run under it
	// would contradict what was delivered. With the flag off nothing changes.
	if failed, delivered := e.notify(finish, model.Event{Name: model.EventFinished, At: time.Now().UTC(), Run: rep}, checks); failed &&
		!delivered && run.NotifierFailureIsFatal {
		rep.Outcome = model.OutcomeFailed
		rep.ExitCode = e.exitCode(rep.Outcome)
	}
	// Only the run that armed the mark may clear it, and only once every write
	// has a terminal proof. An inherited uncertainty names an earlier run: its
	// pending prefix is never dropped here, only by a settlement that proves the
	// whole set still.
	if uncertaintyArmed && claim != nil && uploadsSettled(order, results) {
		// Only this run's own id leaves: inherited ids are never dropped here,
		// and the two-full-survey settlement remains their only exit. A failed
		// durable removal keeps the id (conservative) and is a warning: this
		// run's writes are proven, so the outcome stands.
		if err := e.lease.RemoveOwnPending(finish); err != nil {
			e.event(human.Warning("lease.uncertain.failed", human.Text("run", rep.RunID),
				human.Text("reason", "the record stays uncertain, the run's outcome stands: "+err.Error())))
		}
	}
	return rep, nil
}

func (e *Workflow) exitCode(outcome model.Outcome) int {
	switch outcome {
	case model.OutcomeDegraded:
		return e.run.DegradedExitCode
	case model.OutcomeFailed:
		return e.run.FailedExitCode
	default:
		return 0
	}
}

// newStorageRunState starts one storage's state for one run, its clock carrying
// the configured time budget.
func newStorageRunState(sb *storageBinding) *storageRunState {
	st := &storageRunState{name: sb.name, typ: sb.cfg.Type, clock: execution.NewClock(sb.cfg.TimeBudget.Std())}
	st.result = model.StorageResult{Storage: sb.name, Type: sb.cfg.Type}
	return st
}

// describeStorage validates a storage's declaration once per run; a failure
// makes the storage fail immediately — nothing else can be attempted without a
// storage that answers. Nothing the run does reads the traits themselves, so
// only the failure signal matters here.
func (e *Workflow) describeStorage(ctx context.Context, sb *storageBinding, st *storageRunState) {
	_, err := execution.Call(st.clock, ctx, func(ctx context.Context) (storage.StorageTraits, error) {
		return execution.Attempt(ctx, e.retry, "describe "+sb.name, func(ctx context.Context, _ int) (storage.StorageTraits, error) {
			return execution.SafeValue(func() (storage.StorageTraits, error) { return sb.port.Describe(ctx) })
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
// (see planning.Expand), unless its own build or check failed — then it is one
// failed plan, because a domain nobody could read is not a set of units.
func (e *Workflow) collectArtifacts(ctx context.Context, checks failedChecks, scope *lifecycle.Scope) ([]artifactPlan, []model.Exclusion) {
	plans := make([]artifactPlan, 0, len(e.sources))
	var excluded []model.Exclusion
	for _, sb := range e.sources {
		plan := artifactPlan{
			instance: sb.name, srcType: sb.typ, src: sb.inst, artifact: sb.artifact, pipeline: sb.pipeline,
			refs: sb.refs, steps: stepsOf(e.processorTypes, sb.refs, sb.procs),
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
		found, err := planning.New(e.retry).Discover(ctx, db.name, db.inst, planning.AnyRetryable)
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
			plan.pipeline, plan.refs, plan.steps = pipelineName(refs), refs, stepsOf(e.processorTypes, refs, procs)
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
				s, err := e.buildSource(ctx, f.Type, f.Name, f.Config)
				if err != nil {
					plan.problem = err
				} else {
					// The run owns what it built from the moment it exists: a
					// source whose Check fails is still closed with the run.
					own(scope, s)
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
// instance holds (see planning.Expand): the parent itself when its source is one
// artifact, one plan per unit otherwise. A plan that already carries a problem
// is returned as it stands — there is nothing to enumerate through, and the
// instance's own failure is its own artifact.
func (e *Workflow) expandPlan(ctx context.Context, p execution.Options, plan artifactPlan, scope *lifecycle.Scope) []artifactPlan {
	if plan.problem != nil {
		return []artifactPlan{plan}
	}
	units := planning.New(p).Expand(ctx, planning.Unit{
		Instance: plan.instance, Type: plan.srcType, Source: plan.src, Artifact: plan.artifact,
	}, planning.Expansion{
		Build: func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error) {
			return e.buildSource(ctx, typ, instance, raw)
		},
		Check: e.checkSource,
		Own:   func(c io.Closer) { scope.Own(c) },
	})
	out := make([]artifactPlan, 0, len(units))
	for _, u := range units {
		unit := plan
		unit.instance, unit.srcType, unit.src, unit.artifact, unit.problem = u.Instance, u.Type, u.Source, u.Artifact, u.Problem
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
func (e *Workflow) checkSource(ctx context.Context, s source.Source, name string) error {
	checker, ok := s.(instances.Checker)
	if !ok {
		return nil
	}
	_, err := execution.Attempt(ctx, e.retry, "check source "+name, func(ctx context.Context, _ int) (struct{}, error) {
		return struct{}{}, execution.SafeCall(func() error { return checker.Check(ctx) })
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

// transferFor binds the transfer provider to one destination of this run: the
// handle is the only write authority, the ledger is where its overlapping
// upload books, and the storage wall-clock budget is the caller's.
func (e *Workflow) transferFor(sb *storageBinding, kind model.RunKind, st *storageRunState) *transfer.Transfer {
	return transfer.New(transfer.Options{
		Retry: e.retry, Clock: st.clock, Log: e.deps("storage", sb.cfg.Type, sb.name).Log,
		Accounting: e.ledger.Accounting(), Storage: sb.name, MaxArtifactBytes: e.run.MaxArtifactBytes.Int64(),
		Manual: kind == model.RunKindManual, CallParallelism: sb.cfg.CallParallelism,
	})
}

// stepsOf pairs the pipeline's's processor instances with the types they were
// declared with: the manifest records both, so a plan stays readable after a
// plugin rename.
func stepsOf(types map[string]string, refs []string, procs []processor.Processor) []pipeline.Step {
	steps := make([]pipeline.Step, 0, len(procs))
	for i, p := range procs {
		step := pipeline.Step{Processor: p}
		if i < len(refs) {
			step.Instance = refs[i]
			step.Type = types[refs[i]]
		}
		steps = append(steps, step)
	}
	return steps
}

// processArtifact produces one artifact and uploads it to every storage,
// independently: one storage failing never stops the others.
//
// ar and verified are named results so the deferred duration lands on the value
// the caller receives: `return ar` copies a struct, so a closure over a local
// would set the duration on a copy the caller never sees — which is why every
// artifact's duration read 0s.
func (e *Workflow) processArtifact(ctx context.Context, sp *spool.Spool, plan artifactPlan, runID string, kind model.RunKind,
	order []*storageBinding, results map[string]*storageRunState,
	handles map[string]guard.Handle, transfers map[string]*transfer.Transfer) (ar model.ArtifactResult, verified map[string]bool) {
	ar = model.ArtifactResult{Artifact: plan.artifact, Pipeline: plan.pipeline, Outcome: model.ArtifactFailed, Hint: plan.hint}
	verified = map[string]bool{}
	// failed marks a destination whose overlapping transfer reported its own
	// failure: the whole-object fallback must not retry it in this artifact.
	failed := map[string]bool{}
	start := time.Now()
	defer func() { ar.Duration = model.Duration(time.Since(start)) }()
	if plan.problem != nil {
		ar.Error = plan.problem.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
		return ar, verified
	}
	// The plan's traits are resolved before the first byte: the object key exists
	// before its bytes, so a multipart storage can reserve the cap and open its
	// upload while the producer is still writing.
	res, err := e.pipe.Resolve(ctx, pipeline.Plan{
		Source: plan.src, SourceInstance: plan.instance, SourceType: plan.srcType,
		Artifact: plan.artifact, Steps: plan.steps,
	})
	if err != nil {
		ar.Error = fmt.Errorf("artifact %s: %w", plan.artifact.Name, err).Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
		return ar, verified
	}
	key, err := e.layout.ArtifactKey(runID, res.Artifact)
	if err != nil {
		ar.Error = err.Error()
		return ar, verified
	}
	ar.Key = key
	label := human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)
	g := pipeline.NewGrowth()
	sem := make(chan struct{}, e.run.StorageParallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	// followDone is one completion signal per multipart storage: the whole-object
	// fallback of a storage waits only its own follower, never an unrelated one.
	followDone := map[string]chan struct{}{}
	for _, sb := range order {
		// A storage whose permanent failure is already known starts no new
		// upload operation: not a Follow, not a Begin, not a Complete.
		if storageExcluded(results, sb) {
			continue
		}
		if _, ok := handles[sb.name].Uploader(); !ok {
			continue
		}
		done := make(chan struct{})
		followDone[sb.name] = done
		wg.Add(1)
		go func(sb *storageBinding) {
			defer wg.Done()
			defer close(done)
			sem <- struct{}{}
			defer func() { <-sem }()
			// A multipart storage follows the growing file from its first byte,
			// inside its own slot: a storage that stalls must not hold back the
			// line of one that finished minutes ago.
			st := results[sb.name]
			st.mu.Lock()
			st.tried = true
			st.mu.Unlock()
			out, err := transfers[sb.name].Follow(ctx, handles[sb.name], sp, g,
				transfer.Request{Key: key, Label: label})
			if out.Started {
				// Entering Follow is not a mutation: the transfer reports whether
				// a backend operation actually started. A capacity refusal before
				// it leaves the storage unstarted, so it cannot hold the run's own
				// pending id.
				results[sb.name].markWritesStarted()
			}
			st.addAttempts(out.Attempts)
			if err != nil {
				e.failUpload(ctx, sb, st, label, key, err)
			}
			taken := out.Handled || err != nil
			mu.Lock()
			failed[sb.name] = taken && err != nil
			mu.Unlock()
			mu.Lock()
			defer mu.Unlock()
			if out.Handled && err == nil {
				st.markObject(out.Bytes)
				verified[sb.name] = true
				e.storageEvent(sb, human.New("storage.upload.done",
					human.Text("artifact", label), human.Bytes("size", out.Bytes)))
			}
		}(sb)
	}
	producePlan := pipeline.Plan{
		Source: plan.src, SourceInstance: plan.instance, SourceType: plan.srcType,
		Artifact: plan.artifact, Steps: plan.steps,
	}
	pa, err := e.pipe.Produce(ctx, sp, producePlan, res, g)
	if err != nil {
		// The storages that overlapped are still unwinding: wait for them before
		// the artifact is declared failed, so no goroutine outlives its file.
		wg.Wait()
		ar.Error = err.Error()
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", label), human.Text("reason", ar.Error)))
		return ar, verified
	}
	defer func() { _ = sp.Remove(pa.File) }()
	e.event(human.New("spool.write", human.Text("file", pa.File), human.Bytes("size", pa.Bytes)))
	ar.Artifact = pa.Artifact
	ar.Chain = pa.Chain
	ar.Source = pa.Source
	ar.Stats = pa.Stats
	// Every storage the followers did not verify uploads the finished file: a
	// destination without the capability, one that fits one request, and one
	// whose overlap was refused all take the whole-object path. Each storage
	// proceeds independently — a plain peer starts immediately, a multipart
	// storage waits only its OWN follower — so one stalled backend cannot spend
	// the run deadline while a healthy peer has a free parallelism slot.
	whole := func(sb *storageBinding) {
		wg.Add(1)
		go func(sb *storageBinding) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			e.storageEvent(sb, human.New("storage.upload.start", human.Text("artifact", label)))
			ok := e.uploadObject(ctx, transfers[sb.name], handles[sb.name], sb, results[sb.name], pa, sp, key, kind)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				verified[sb.name] = true
				e.storageEvent(sb, human.New("storage.upload.done",
					human.Text("artifact", label), human.Bytes("size", pa.Bytes)))
			}
		}(sb)
	}
	for _, sb := range order {
		if storageExcluded(results, sb) {
			continue
		}
		done, follows := followDone[sb.name]
		if !follows {
			whole(sb)
			continue
		}
		wg.Add(1)
		go func(sb *storageBinding, done chan struct{}) {
			defer wg.Done()
			<-done
			mu.Lock()
			taken := verified[sb.name] || failed[sb.name]
			mu.Unlock()
			if taken || storageExcluded(results, sb) {
				return
			}
			wg.Add(1)
			go func(sb *storageBinding) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				e.storageEvent(sb, human.New("storage.upload.start", human.Text("artifact", label)))
				ok := e.uploadObject(ctx, transfers[sb.name], handles[sb.name], sb, results[sb.name], pa, sp, key, kind)
				mu.Lock()
				defer mu.Unlock()
				if ok {
					verified[sb.name] = true
					e.storageEvent(sb, human.New("storage.upload.done",
						human.Text("artifact", label), human.Bytes("size", pa.Bytes)))
				}
			}(sb)
		}(sb, done)
	}
	wg.Wait()
	took := time.Since(start)
	if len(verified) > 0 {
		ar.Outcome = model.ArtifactOK
		ar.RawBytes = pa.Raw
		ar.Bytes = pa.Bytes
		ar.SHA256 = pa.SHA256
		e.event(human.New("artifact.done",
			human.Text("artifact", label),
			human.Bytes("raw", pa.Raw), human.Bytes("stored", pa.Bytes), human.Duration("took", took),
			human.Count("skipped", pa.Stats.SkippedEntries), human.Count("padded", pa.Stats.PaddedFiles),
			human.Digest("sha256", pa.SHA256), human.ObjectKey("key", key)))
	} else {
		ar.Error = "no storage verified the artifact"
		e.event(human.Failure("artifact.failed",
			human.Text("artifact", human.ArtifactLabel(plan.artifact.Group, plan.artifact.Name)), human.Text("reason", ar.Error)))
	}
	return ar, verified
}

// storageExcluded reports whether this run already knows a permanent failure of
// one storage: such a storage starts no new upload operation.
func storageExcluded(results map[string]*storageRunState, sb *storageBinding) bool {
	st := results[sb.name]
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.fatal != nil
}

// uploadObject puts one object and verifies it: success is declared only when
// Stat shows the object at the expected size (and hash, when the storage exposes
// one). A storage that can upload one object as parts gets them driven by
// transfer (see multipartUpload), one call per part, so a failed part does not cost
// the parts already accepted; every other storage — and every object that fits a
// single request — keeps the whole-object Put. Every attempt re-opens the spool
// file.
func (e *Workflow) uploadObject(ctx context.Context, tf *transfer.Transfer, h guard.Handle, sb *storageBinding,
	st *storageRunState, pa pipeline.Artifact, sp *spool.Spool, key string, kind model.RunKind) bool {
	st.mu.Lock()
	st.tried = true
	fatal := st.fatal
	st.mu.Unlock()
	if fatal != nil {
		return false
	}
	label := human.ArtifactLabel(pa.Artifact.Group, pa.Artifact.Name)
	// The ledger books the object before the write for both kinds; only a manual
	// run is refused on it. A scheduled run's writes are counted and the cap
	// phase frees room afterwards.
	if !e.reserveWrite(sb, st, key, pa.Bytes, label, kind == model.RunKindManual) {
		return false
	}
	st.markWritesStarted()
	// The multipart path books its own calls against the budget: it may wait for
	// a producer between them, and that wait is not the storage's time. The
	// whole-object path keeps one call around its retries.
	var err error
	if out, merr := tf.Upload(ctx, h, sp, transfer.Request{
		Key: key, Label: label, File: pa.File, Size: pa.Bytes, SHA256: pa.SHA256,
	}); out.Handled || merr != nil {
		st.addAttempts(out.Attempts)
		err = merr
	} else {
		_, err = execution.Call(st.clock, ctx, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, e.putWhole(ctx, h, sb, st, sp, pa, key)
		})
	}
	if err != nil {
		e.failUpload(ctx, sb, st, label, key, err)
		return false
	}
	e.ledger.Commit(sb.name, key, pa.Bytes)
	st.markObject(pa.Bytes)
	return true
}

// failUpload records one storage's failed upload: a permanent error takes the
// storage out of the run, and the error reaches the report. The artifact's
// label, never its key: this error reaches the report and the summary at info,
// and the key is machine detail (the log event carries it as an ObjectKey attr,
// shown only at debug).
func (e *Workflow) failUpload(ctx context.Context, sb *storageBinding, st *storageRunState, label, key string, err error) {
	if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassPermanent && ctx.Err() == nil {
		st.mu.Lock()
		st.fatal = err
		st.mu.Unlock()
	}
	st.mu.Lock()
	st.tried = true
	st.mu.Unlock()
	st.setUploadError(fmt.Errorf("put %s: %w", label, err))
	e.storageEvent(sb, human.Failure("storage.upload.failed",
		human.Text("artifact", label),
		human.Text("reason", err.Error()), human.ObjectKey("key", key)))
}

// reserveWrite books one object's bytes in the storage's ledger before its
// write, for both kinds. For a manual run (refuse) it is also the admission
// test: a write past the cap, or with an unknown occupancy, is refused, marks
// the storage with the machine-readable reason and stops it there — the run
// keeps whatever it already uploaded and deletes nothing. A scheduled write is
// always booked and always proceeds; the cap phase frees room afterwards.
func (e *Workflow) reserveWrite(sb *storageBinding, st *storageRunState, key string, size int64, label string, refuse bool) bool {
	if e.ledger.Admit(sb.name, key, size, refuse) {
		return true
	}
	limit, used := e.ledger.Limits(sb.name)
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
func (e *Workflow) putWhole(ctx context.Context, h guard.Handle, sb *storageBinding, st *storageRunState,
	sp *spool.Spool, pa pipeline.Artifact, key string) error {
	_, err := execution.Attempt(ctx, e.retry, "put "+human.ArtifactLabel(pa.Artifact.Group, pa.Artifact.Name), func(ctx context.Context, _ int) (struct{}, error) {
		st.addAttempts(1)
		f, err := sp.Open(pa.File)
		if err != nil {
			return struct{}{}, pluginerrors.Permanent(err)
		}
		defer func() { _ = f.Close() }()
		if _, err := execution.SafeValue(func() (model.ObjectInfo, error) {
			return h.Put(ctx, storage.PutRequest{Key: key, Size: pa.Bytes, SHA256: pa.SHA256}, f)
		}); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, e.verifyObject(ctx, sb.port, key, human.ArtifactLabel(pa.Artifact.Group, pa.Artifact.Name), pa.Bytes)
	})
	return err
}

// verifyObject proves one uploaded object is there: Stat must report the size
// that was sent. An object that is not visible right after its upload is a failed
// commit of this call, never the not_found of an absent key.
func (e *Workflow) verifyObject(ctx context.Context, port guard.Port, key, label string, size int64) error {
	stt, err := execution.SafeValue(func() (model.ObjectInfo, error) { return port.Stat(ctx, key) })
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			return pluginerrors.Transient(fmt.Errorf("object %s is not visible after the upload (not committed)", label))
		}
		return err
	}
	if stt.Size != size {
		return pluginerrors.Transient(fmt.Errorf("size after upload is %d, want %d: the upload is not committed", stt.Size, size))
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
// notification its half of the allowance.
func (e *Workflow) writeManifests(ctx context.Context, kind model.RunKind, order []*storageBinding, results map[string]*storageRunState,
	outcome model.Outcome, rep *model.RunReport, handles map[string]guard.Handle) {
	rep.Outcome = outcome
	// The report is a fixed struct of JSON-safe fields: marshalling it cannot
	// fail.
	body, _ := json.Marshal(rep)
	sum := model.SHA256Hex(body)
	key, err := e.layout.ManifestKey(rep.RunID)
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
		if !e.reserveWrite(sb, st, key, int64(len(body)), "manifest.json", kind == model.RunKindManual) {
			continue
		}
		st.markWritesStarted()
		wg.Add(1)
		go func(sb *storageBinding, st *storageRunState) {
			defer wg.Done()
			// The budget is skipped on purpose and the time is still booked:
			// the objects are already there, and the record of them is worth
			// the seconds the storage's own budget would refuse.
			end := st.clock.Begin()
			_, err := execution.Attempt(ctx, e.retry, "manifest "+sb.name, func(ctx context.Context, _ int) (struct{}, error) {
				st.addAttempts(1)
				if _, err := execution.SafeValue(func() (model.ObjectInfo, error) {
					return handles[sb.name].Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body)), SHA256: sum},
						bytes.NewReader(body))
				}); err != nil {
					return struct{}{}, err
				}
				// The manifest is verified like an artifact: a Put that returned
				// ok but left nothing visible is a run nobody can find, and the
				// floor must not lean on it.
				return struct{}{}, e.verifyObject(ctx, sb.port, key, "manifest.json", int64(len(body)))
			})
			end()
			if err != nil {
				st.setUploadError(fmt.Errorf("manifest: %w", err))
				e.storageEvent(sb, human.Failure("manifest.write.failed", human.Text("reason", err.Error())))
				return
			}
			e.ledger.Commit(sb.name, key, int64(len(body)))
			st.markManifest()
			st.markObject(int64(len(body)))
		}(sb, st)
	}
	wg.Wait()
}

// assembleResults renders the per-storage results in config order.
func (e *Workflow) assembleResults(order []*storageBinding, results map[string]*storageRunState) []model.StorageResult {
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
		res.Duration = model.Duration(st.clock.Total())
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

// reportOutcome turns the report into delivered / degraded / failed. notifierFailed
// is the started notification's failure: it degrades a run at most. The finished
// notification is the caller's later override, and only when nobody received it.
func (e *Workflow) reportOutcome(rep *model.RunReport, notifierFailed bool) model.Outcome {
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
	if notifierFailed && e.run.NotifierFailureIsFatal {
		anyFailure = true
	}
	if !requiredOK || verifiedStorages < e.run.MinVerifiedStorages {
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

// previous flattens the surveys into the reporting input: one record per run
// per storage, a failed survey contributing nothing. The reporting plugin
// derives the newest scheduled run and the last-good instant from it, then
// reads the previous Job's status.
// surveyedRuns flattens the completed surveys into the reporting input: one
// record per run per storage, a failed survey contributing nothing.
func surveyedRuns(order []*storageBinding, surveys map[string]inventory.Survey) []reporting.SurveyedRun {
	var surveyed []reporting.SurveyedRun
	for _, sb := range order {
		s := surveys[sb.name]
		if !s.OK {
			continue
		}
		for id, r := range s.Runs {
			outcome := model.Outcome("")
			if r.Stored.Manifest && r.Manifest != nil {
				outcome = r.Manifest.Outcome
			}
			surveyed = append(surveyed, reporting.SurveyedRun{
				Storage: sb.name, RunID: id, Kind: r.Stored.Kind,
				StartedAt: r.Stored.At, Manifest: r.Stored.Manifest, Outcome: outcome,
			})
		}
	}
	return surveyed
}

// reportPrevious keeps the run log's previous-run lines, now driven by the
// reporting plugin's answer instead of a catalog history.
func (e *Workflow) reportPrevious(prev model.PreviousRun) {
	if prev.RunID != "" {
		last := "never"
		if !prev.LastGoodAt.IsZero() {
			last = prev.LastGoodAt.UTC().Format(time.RFC3339)
		}
		e.event(human.New("run.previous",
			human.Text("run", prev.RunID),
			human.Text("unfinished", strconv.FormatBool(prev.Unfinished)),
			human.Text("last_good", last)))
	}
	if prev.Unavailable != "" {
		e.event(human.Warning("run.previous.unavailable", human.Text("reason", prev.Unavailable)))
	}
	if prev.Failure != nil {
		e.event(human.New("run.previous.failed",
			human.Text("job", prev.Failure.Job), human.Text("reason", prev.Failure.Reason)))
	}
}

// notify delivers one event to every notifier; failed reports whether any of
// them failed, delivered whether any of them took the event. A notifier that
// failed its Check is not called and counts as failed — the same way one whose
// call failed does. A notifier answering notifier.ErrSkipped has nothing for this
// event and is neutral: it is neither a failure nor a delivery, so it can neither
// fail a run nor stand in for a notifier that delivered. The caller decides what
// a failure means: notifier_failure_is_fatal is fatal for a finished event only
// when no notifier delivered it.
func (e *Workflow) notify(ctx context.Context, ev model.Event, checks failedChecks) (failed, delivered bool) {
	for _, nb := range e.notifiers {
		if err := checks.err("notifier", nb.name); err != nil {
			failed = true
			e.at("notifier", e.notifierType(nb.name), nb.name, human.Failure("notify.check-failed",
				human.Text("event", ev.Name), human.Text("reason", err.Error())))
			continue
		}
		_, err := execution.Attempt(ctx, e.retry, "notify "+nb.name, func(ctx context.Context, _ int) (struct{}, error) {
			return struct{}{}, execution.SafeCall(func() error { return nb.inst.Notify(ctx, ev) })
		})
		if errors.Is(err, notifier.ErrSkipped()) {
			continue
		}
		if err != nil {
			failed = true
			e.at("notifier", e.notifierType(nb.name), nb.name, human.Failure("notify.failed",
				human.Text("event", ev.Name), human.Text("reason", err.Error())))
			continue
		}
		delivered = true
		e.at("notifier", e.notifierType(nb.name), nb.name, human.New("notify.sent", human.Text("event", ev.Name)))
	}
	return failed, delivered
}

// retentionSweeps runs one sweep per storage, inside that storage's own bounded
// context, after its manifest phase: the protection ledger is the one delete
// authority and the one occupancy ledger, so the policy and the cap share a
// single deletion state.
func (e *Workflow) retentionSweeps(ctx context.Context, started time.Time, kind model.RunKind,
	order []*storageBinding, surveys map[string]inventory.Survey, results map[string]*storageRunState,
	overlay map[string]model.StoredRun, artifacts []model.ArtifactResult,
	verifiedMatrix map[string]map[string]bool) {
	for _, sb := range order {
		st := results[sb.name]
		var ov *model.StoredRun
		if o, ok := overlay[sb.name]; ok {
			ov = &o
		}
		res, err := execution.Call(st.clock, ctx, func(ctx context.Context) (protection.SweepResult, error) {
			r := e.ledger.Sweep(ctx, protection.Sweep{
				RunStart: started, Kind: kind, Storage: sb.name,
				Survey: surveys[sb.name], Overlay: ov,
				Artifacts:     artifacts,
				Verified:      verifiedFor(verifiedMatrix, sb.name),
				ManifestSaved: st.manifestSaved(),
				Skip:          retentionSkip(st, kind),
			})
			return r, r.Err
		})
		st.mu.Lock()
		st.result.Kept = res.Kept
		st.result.Deleted = res.Deleted
		st.result.Freed = res.Freed
		st.mu.Unlock()
		if err != nil {
			st.setRetentionError(err)
		}
	}
}

// verifiedFor is one storage's column of the verification matrix: which
// artifacts this run has a verified copy of there.
func verifiedFor(matrix map[string]map[string]bool, storage string) map[string]bool {
	out := map[string]bool{}
	for artifact, perStorage := range matrix {
		if perStorage[storage] {
			out[artifact] = true
		}
	}
	return out
}

// retentionSkip is the workflow's one reason a manual run deletes nothing on a
// storage: it did not complete there.
func retentionSkip(st *storageRunState, kind model.RunKind) string {
	if kind == model.RunKindManual && (st.capRefused() || !st.manifestSaved()) {
		return "the manual run did not complete on this storage: nothing is deleted"
	}
	return ""
}

// writesAsynchronously reports whether any destination may accept a write that
// settles after the call returns: such a run must durably mark its own run id
// before the first write.
func writesAsynchronously(order []*storageBinding) bool {
	for _, sb := range order {
		if sb.port.WritesAsynchronously() {
			return true
		}
	}
	return false
}

// uploadsSettled reports whether every storage finished its writes with a
// terminal proof: no upload, verification or manifest write failed. Only then
// may the run that marked the record clear it — a failed or cancelled upload may
// still settle at the provider after the run exits. It reads workflow state only.
func uploadsSettled(order []*storageBinding, results map[string]*storageRunState) bool {
	for _, sb := range order {
		st := results[sb.name]
		st.mu.Lock()
		started, err := st.writesStarted, st.uploadErr
		st.mu.Unlock()
		if !started {
			// No write of this run ever began here: a preflight exclusion or an
			// admission refusal cannot leave bytes that settle later.
			continue
		}
		if err != nil {
			return false
		}
	}
	return true
}

// checkRunIDFree refuses a run whose id a storage already holds: an id is
// second-precise, and two runs of the same id would overwrite each other's
// objects and manifest. It runs before the first upload.
func (e *Workflow) checkRunIDFree(surveys map[string]inventory.Survey, runID string) error {
	for _, name := range sortedKeys(surveys) {
		sur := surveys[name]
		if !sur.OK {
			continue
		}
		if _, taken := sur.Runs[runID]; taken {
			return fmt.Errorf("run: id %s already exists on storage %s; a run id must be unique", runID, name)
		}
	}
	return nil
}

// dryRunReport resolves every plan without producing or uploading anything; the
// storages stay skipped and nothing is written anywhere.
func (e *Workflow) dryRunReport(ctx context.Context, rep model.RunReport, plans []artifactPlan,
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
			res, err := e.pipe.Resolve(ctx, pipeline.Plan{
				Source: plan.src, SourceInstance: plan.instance, SourceType: plan.srcType,
				Artifact: plan.artifact, Steps: plan.steps,
			})
			if err != nil {
				ar.Outcome = model.ArtifactFailed
				ar.Error = err.Error()
			} else {
				ar.Artifact = res.Artifact
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
