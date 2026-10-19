package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// survey is what one storage held when the run started: every run present under
// the run prefix, its objects, and its manifest when the manifest is there. A
// survey with ok == false means "do not know": that storage's own retention does
// not run, and for the deletion floor it counts as holding nothing — the floor
// covers only what the engine surveyed this run.
type survey struct {
	storage string
	ok      bool
	err     error
	runs    map[string]*surveyRun
}

type surveyRun struct {
	stored   model.StoredRun
	objects  []model.ObjectInfo
	manifest *model.RunReport
	// manifestKey is where the manifest sits among objects, '' when the key
	// could not be rendered; deletion removes it first (deleteOrder).
	manifestKey string
	// manifestProblem is set when the manifest is listed but could not be read,
	// parsed or accepted — never treated as "this run has no manifest": a run
	// whose manifest is unreadable is refused deletion for this sweep (canDrop),
	// because the survey cannot see which artifacts it holds.
	manifestProblem string
}

// surveyStorage lists one storage under the run prefix and reads the manifests.
// Retention takes its run list from the manifests in the storage itself, which is
// exactly this survey. A non-nil st applies the storage's run time budget (a run
// concept) and books the survey's time against it; restore paths pass nil.
func (e *Engine) surveyStorage(ctx context.Context, sb *storageBinding, prefix string, p retryPolicy, st *storageRunState) survey {
	if st != nil {
		start := time.Now()
		var cancel context.CancelFunc
		ctx, cancel = st.clock.call(ctx)
		defer func() {
			cancel()
			st.clock.add(time.Since(start))
		}()
	}
	res := survey{storage: sb.name, runs: map[string]*surveyRun{}}
	ro := readOnlyStorage{sb.inst} // a survey reads only, whatever path it serves
	var objs []model.ObjectInfo
	token := ""
	for page := 0; ; page++ {
		if page > 500 {
			res.ok = false
			res.err = fmt.Errorf("listing did not terminate after 500 pages")
			return res
		}
		got, err := attempt(ctx, p, "list "+sb.name, func(ctx context.Context, _ int) (storage.ListPage, error) {
			return safeValue(func() (storage.ListPage, error) {
				return ro.List(ctx, storage.ListRequest{Prefix: prefix, Token: token})
			})
		})
		if err != nil {
			res.ok = false
			res.err = err
			e.storageEvent(sb, human.Failure("storage.survey.failed", human.Text("reason", err.Error())))
			return res
		}
		objs = append(objs, got.Objects...)
		if got.NextToken == "" {
			break
		}
		token = got.NextToken
	}
	for _, o := range objs {
		rel := strings.TrimPrefix(o.Key, prefix)
		id, _, found := strings.Cut(rel, "/")
		if !found || id == "" {
			continue // an object outside any run directory is not ours to reason about
		}
		r := res.runs[id]
		if r == nil {
			r = &surveyRun{}
			res.runs[id] = r
		}
		r.objects = append(r.objects, o)
	}
	for _, id := range sortedKeys(res.runs) {
		r := res.runs[id]
		mkey, err := e.manifestKey(id)
		if err != nil {
			// The key cannot be rendered, so whether a manifest exists is
			// unknown: the run is never dropped while that is true.
			r.manifestProblem = "the manifest key does not render: " + err.Error()
			e.storageEvent(sb, human.Failure("storage.manifest-key.failed",
				human.Text("run", id), human.Text("reason", err.Error())))
		} else {
			for _, o := range r.objects {
				if o.Key == mkey {
					r.manifestKey = mkey
					rep, err := e.fetchManifest(ctx, sb, mkey, p)
					if err == nil && rep != nil && rep.Schema != model.ManifestSchema {
						// The report is kept so the restore side can refuse it in
						// its own words; for a deletion decision it is a problem.
						err = fmt.Errorf("manifest schema %d is not %d, which this build reads", rep.Schema, model.ManifestSchema)
					}
					if err != nil {
						r.manifestProblem = err.Error()
						e.storageEvent(sb, human.Failure("storage.manifest-unreadable",
							human.Text("run", id), human.Text("reason", err.Error()), human.ObjectKey("key", mkey)))
					}
					r.manifest = rep
					break
				}
			}
		}
		var bytes int64
		at := time.Time{}
		for _, o := range r.objects {
			bytes += o.Size
			if o.ModTime.After(at) {
				at = o.ModTime
			}
		}
		verified := false
		// The run's kind comes from its id, which every key carries; the
		// manifest's own kind must agree. A disagreement — including a manifest
		// whose kind this build does not know, which can never equal the id's — is
		// a problem run, never permission to delete: the survey cannot trust what
		// the run says about itself.
		kind := e.idKind(id)
		if r.manifest != nil && r.manifestProblem == "" {
			mk := model.NormalizeKind(r.manifest.Kind)
			if mk != kind {
				r.manifestProblem = fmt.Sprintf("the manifest says kind %s but the run id says %s", mk, kind)
			}
		}
		if r.manifest != nil && r.manifestProblem == "" {
			at = r.manifest.StartedAt
			// A run that came out failed is missing something required on every
			// storage that took it: it must never stand in for "the last
			// verified run" the deletion guard protects.
			verified = r.manifest.Outcome != model.OutcomeFailed &&
				storageOutcome(r.manifest, sb.name) == model.StorageVerified
		}
		r.stored = model.StoredRun{RunID: id, Kind: kind, At: at, Bytes: bytes, Verified: verified, Manifest: r.manifest != nil && r.manifestProblem == ""}
	}
	res.ok = true
	return res
}

// fetchManifest reads one manifest and returns (nil, nil) only when the object
// is genuinely absent (ClassNotFound). Any other failure — a read error after
// the retries, an unparsable body — is returned as an error for the caller to
// record: "the manifest is listed but could not be read" must never be mistaken
// for "this run has no manifest", which is what a deletion decision would take
// it for. A parseable manifest of a foreign schema is returned as it is: the
// survey refuses it as a deletion input, the restore side refuses it as an
// artifact, each in its own words.
func (e *Engine) fetchManifest(ctx context.Context, sb *storageBinding, key string, p retryPolicy) (*model.RunReport, error) {
	body, err := attempt(ctx, p, "get manifest "+sb.name, func(ctx context.Context, _ int) ([]byte, error) {
		var buf bytes.Buffer
		if err := safeCall(func() error { return readOnlyStorage{sb.inst}.Get(ctx, key, &buf) }); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	})
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return nil, nil // genuinely absent
		}
		return nil, fmt.Errorf("reading the manifest: %w", err)
	}
	var rep model.RunReport
	if err := json.Unmarshal(body, &rep); err != nil {
		return nil, fmt.Errorf("parsing the manifest: %w", err)
	}
	return &rep, nil
}

// deleteOrder puts the manifest first: a delete that stops half-way then leaves
// a run without a manifest rather than a manifest promising objects that are
// gone — the survey reads a missing manifest as "not verified", which is true.
func deleteOrder(r *surveyRun) []model.ObjectInfo {
	manifest := ""
	if r.manifest != nil {
		manifest = r.manifestKey
	}
	out := make([]model.ObjectInfo, 0, len(r.objects))
	for _, o := range r.objects {
		if o.Key != manifest {
			out = append(out, o)
		}
	}
	for _, o := range r.objects {
		if o.Key == manifest {
			out = append([]model.ObjectInfo{o}, out...)
		}
	}
	return out
}

func storageOutcome(rep *model.RunReport, storageName string) string {
	for _, s := range rep.Storages {
		if s.Storage == storageName {
			return s.Outcome
		}
	}
	return ""
}

// retentionState tracks, live, which storages hold a verified copy of each run,
// and which runs hold a verified copy of each artifact. Each storage's policy
// deletes what it decides independently — this state is the one cross-storage
// rail, and it enforces two absolutes:
//
//   - a run whose manifest is listed but could not be read, parsed or accepted
//     is never deleted: the survey cannot see which artifacts it holds, and an
//     unreadable manifest is not an absent one;
//   - the newest verified copy of every artifact, per storage, stays: a run
//     holding the newest copy of some artifact ON THE STORAGE BEING SWEPT is
//     never deleted there, by tiers or by bytes — a newer copy on another
//     storage does not release it. With a standing failed (one
//     undeclared volume fails every night while the databases are delivered)
//     no run is verified as a whole, and this per-artifact rule is the floor
//     that keeps the last complete copy of each artifact alive;
//   - at least one verified complete run stays across everything the engine
//     surveyed this run.
//
// Successfully deleted copies are forgotten as they go, so two storages cannot
// each delete the other's last copy within one run.
type retentionState struct {
	mu       sync.Mutex
	verified map[string]map[string]bool
	// kinds names every run's kind, so the per-artifact pin and the run-level
	// guard are read per kind: a manual copy never releases a scheduled pin, and
	// a scheduled run never stands in for the last verified manual one.
	kinds map[string]model.RunKind
	// held is the per-artifact copy ledger, keyed by storage and then by kind:
	// artifact identity -> storage -> kind -> run id -> the run's instant. A
	// copy counts, on ONE storage, when the run's manifest there calls the
	// artifact ok AND the object the manifest names is present in that storage's
	// own listing — the only per-artifact evidence a survey has. A run whose
	// manifest is missing or does not parse contributes nothing. The kind key is
	// what keeps the two partitions' pins apart.
	held map[string]map[string]map[model.RunKind]map[string]time.Time
	// problems names the runs whose manifest is listed but could not be read,
	// parsed or accepted. Such a run is never dropped in this sweep: nothing
	// can be said about the copies it holds.
	problems map[string]string
}

func newRetentionState(surveys map[string]survey) *retentionState {
	st := &retentionState{
		verified: map[string]map[string]bool{},
		kinds:    map[string]model.RunKind{},
		held:     map[string]map[string]map[model.RunKind]map[string]time.Time{},
		problems: map[string]string{},
	}
	for name, s := range surveys {
		if !s.ok {
			continue
		}
		for id, r := range s.runs {
			st.kinds[id] = r.stored.Kind
			if r.stored.Verified {
				st.mark(id, name)
			}
			if r.manifestProblem != "" {
				st.problems[id] = r.manifestProblem
			}
			st.recordHeld(name, id, r)
		}
	}
	return st
}

// recordHeld adds one surveyed run's artifact copies on one storage to the
// ledger. It runs under no lock only during construction; markHeld takes the
// lock for the runtime path.
func (st *retentionState) recordHeld(storage, runID string, r *surveyRun) {
	if r.manifest == nil || r.manifestProblem != "" {
		return
	}
	for _, ar := range r.manifest.Artifacts {
		if ar.Outcome != model.ArtifactOK || ar.Key == "" {
			continue
		}
		if !holdsObject(r.objects, ar) {
			continue
		}
		st.markHeld(artifactIdentity(ar.Artifact), storage, r.stored.Kind, runID, r.stored.At)
	}
}

// holdsObject reports whether the listing shows the object the manifest names,
// at the size the manifest records: a truncated or half-written object is not
// a copy, and a manifest and an object that disagree are not evidence.
func holdsObject(objects []model.ObjectInfo, ar model.ArtifactResult) bool {
	for _, o := range objects {
		if o.Key == ar.Key && o.Size == ar.Bytes {
			return true
		}
	}
	return false
}

// artifactIdentity is the same artifact every night: group and name survive the
// run id in the key, a rename or a removal does not.
func artifactIdentity(a model.Artifact) string { return a.Group + "/" + a.Name }

func (st *retentionState) markHeld(artifact, storage string, kind model.RunKind, runID string, at time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.held[artifact] == nil {
		st.held[artifact] = map[string]map[model.RunKind]map[string]time.Time{}
	}
	if st.held[artifact][storage] == nil {
		st.held[artifact][storage] = map[model.RunKind]map[string]time.Time{}
	}
	if st.held[artifact][storage][kind] == nil {
		st.held[artifact][storage][kind] = map[string]time.Time{}
	}
	st.held[artifact][storage][kind][runID] = at
}

// markCurrent records the run being written as a holder of an artifact on each
// storage that holds the artifact AND has this run's manifest saved and
// verified, before any floor decision of this sweep. A survey cannot see the
// run yet — it is not in the storage's listing — so without this the ledger
// still names the previous run the newest holder of an artifact this run just
// uploaded, and the floor pins that run against the very policy that wanted it
// gone: the shape of a second run on the same day, whose earlier run would
// never rotate away.
//
// The manifest condition is the whole point: a copy is credited only where a
// restore can find it through the manifest, so a run whose manifest write
// failed never releases the previous run's copy on that storage — the objects
// it left behind with no manifest are not a copy anyone can lean on.
func (st *retentionState) markCurrent(runID string, kind model.RunKind, at time.Time, artifacts []model.ArtifactResult,
	verifiedMatrix map[string]map[string]bool, results map[string]*storageRunState) {
	st.mu.Lock()
	st.kinds[runID] = kind
	st.mu.Unlock()
	saved := map[string]bool{}
	for name, r := range results {
		saved[name] = r.manifestSaved()
	}
	for _, a := range artifacts {
		if a.Artifact.Name == "" {
			continue
		}
		for storage, verified := range verifiedMatrix[a.Artifact.Name] {
			if verified && saved[storage] {
				st.markHeld(artifactIdentity(a.Artifact), storage, kind, runID, at)
			}
		}
	}
}

func (st *retentionState) mark(runID, storage string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.verified[runID] == nil {
		st.verified[runID] = map[string]bool{}
	}
	st.verified[runID][storage] = true
}

// canDrop reports whether deleting this run from this storage keeps the floor;
// the second return is the reason for a refusal, for the run log.
//
// The first rule outranks the policy entirely: a run holding the newest
// verified copy of some artifact ON THIS STORAGE, among runs of its own kind,
// is refused, because deleting it would leave that artifact with no copy of that
// kind on this storage until a newer run of the kind takes one. A manual copy
// therefore never releases a scheduled pin, and a manual run is pinned only
// while it is the storage's only verified copy of the artifact at all — so
// keep-last N and the scheduled run's eviction can still reclaim a manual run
// that a scheduled copy already covers.
//
// The second rule is the run-level one, read across every kind: at least one
// verified complete run must remain across everything the engine surveyed. It is
// deliberately not per kind — a manual run gets no permanent pin beyond the
// per-artifact rule above, so the scheduled run's eviction can reclaim it.
func (st *retentionState) canDrop(runID, storage string) (bool, string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if reason := st.latestCopy(runID, storage); reason != "" {
		return false, reason
	}
	if problem, ok := st.problems[runID]; ok {
		return false, "its manifest is listed but could not be read (" + problem + "): the copies it holds are unknown"
	}
	if !st.verified[runID][storage] {
		return true, "" // the deletion removes no verified copy
	}
	for id, set := range st.verified {
		if id == runID {
			if len(set) > 1 {
				return true, "" // this run survives on another storage
			}
			continue
		}
		if len(set) > 0 {
			return true, "" // another verified run remains
		}
	}
	return false, "deleting it would leave no verified run anywhere the engine surveyed"
}

// latestCopy names an artifact whose copy this run holds ON THIS STORAGE and
// whose deletion the floor refuses, or "" when the run holds no such copy here.
// Newest orders by the run's instant, ties by run id the way NewestFirst does.
// The storage is part of the question: a newer copy on another storage does not
// release this storage's own copy of the same artifact.
func (st *retentionState) latestCopy(runID, storage string) string {
	kind := st.kinds[runID]
	for artifact, byStorage := range st.held {
		byKind := byStorage[storage]
		runs := byKind[kind]
		at, ok := runs[runID]
		if !ok {
			continue
		}
		if kind == model.RunKindManual {
			// A manual copy gets no pin of its own beyond being the storage's
			// only verified copy of the artifact: otherwise keep-last N and the
			// scheduled run's eviction could never reclaim one.
			if totalHolders(byKind) == 1 {
				return "it holds the only verified copy of " + artifact
			}
			continue
		}
		newest := true
		for id, other := range runs {
			if id == runID {
				continue
			}
			if other.After(at) || (other.Equal(at) && id < runID) {
				newest = false
				break
			}
		}
		if newest {
			return "it holds the newest verified copy of " + artifact
		}
	}
	return ""
}

// totalHolders counts every run of every kind holding an artifact on one storage.
func totalHolders(byKind map[model.RunKind]map[string]time.Time) int {
	n := 0
	for _, runs := range byKind {
		n += len(runs)
	}
	return n
}

func (st *retentionState) forget(runID, storage string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.verified[runID], storage)
	for _, byStorage := range st.held {
		for kind := range byStorage[storage] {
			delete(byStorage[storage][kind], runID)
		}
	}
}

// retentionSweep runs retention on every storage after the manifests: the
// current run kind's own policy, and — for a scheduled run — the engine's cap.
// ONE pass per storage, ONE deleter: a confirmed deletion is excluded from the
// cap's candidates, its bytes are subtracted from the ledger once, the policy's
// floor reaches the cap from that same decision, and a destructive error ends
// the storage's sweep instead of starting a second one. A manual run deletes
// nothing where it did not complete: a cap-refused run, or one whose manifest
// was not saved, promised "deletes nothing".
func (e *Engine) retentionSweep(ctx context.Context, runStart time.Time, kind model.RunKind, order []*storageBinding, surveys map[string]survey,
	state *retentionState, results map[string]*storageRunState, overlay map[string]model.StoredRun, caps map[string]*capState) {
	for _, sb := range order {
		st := results[sb.name]
		if kind == model.RunKindManual && (st.capRefused() || !st.manifestSaved()) {
			e.storageEvent(sb, human.New("storage.retention.skipped",
				human.Text("reason", "the manual run did not complete on this storage: nothing is deleted")))
			continue
		}
		var kept int
		var deleted []string
		var freed int64
		_, err := storageCall(&st.clock, ctx, func(ctx context.Context) (struct{}, error) {
			var perr error
			kept, deleted, freed, perr = e.retentionPhase(ctx, runStart, kind, sb, surveys[sb.name], state, overlay, caps[sb.name])
			return struct{}{}, perr
		})
		st.mu.Lock()
		st.result.Kept = kept
		st.result.Deleted = deleted
		st.result.Freed = freed
		st.mu.Unlock()
		if err != nil {
			st.setRetentionError(err)
			e.storageEvent(sb, human.Failure("storage.retention.failed", human.Text("reason", err.Error())))
			continue
		}
		e.storageEvent(sb, human.New("storage.retention",
			human.Count("kept", int64(kept)), human.Count("deleted", int64(len(deleted))), human.Bytes("freed", freed)))
	}
}

// retentionPhase runs the current kind's policy and the cap over ONE deletion
// state, and reports what it kept, deleted and freed.
func (e *Engine) retentionPhase(ctx context.Context, runStart time.Time, kind model.RunKind, sb *storageBinding, sur survey,
	state *retentionState, overlay map[string]model.StoredRun, cap *capState) (kept int, deleted []string, freed int64, err error) {
	if !sur.ok {
		return 0, nil, 0, fmt.Errorf("survey: %w", sur.err)
	}
	current := ""
	if ov, ok := overlay[sb.name]; ok {
		current = ov.RunID
	}
	runs := make([]model.StoredRun, 0, len(sur.runs)+1)
	present := map[string]bool{}
	for _, id := range sortedKeys(sur.runs) {
		runs = append(runs, sur.runs[id].stored)
		present[id] = true
	}
	if ov, ok := overlay[sb.name]; ok && !present[ov.RunID] {
		runs = append(runs, ov)
	}
	d := &deleter{ctx: ctx, e: e, sb: sb, sur: sur, state: state, current: current,
		cutoff: e.staleCutoff(), cap: cap}
	if pol := sb.retention[kind]; pol != nil {
		partition := partitionKind(runs, kind)
		decision, derr := attempt(ctx, e.retry, "retention "+sb.name, func(ctx context.Context, _ int) (retention.Decision, error) {
			return safeValue(func() (retention.Decision, error) {
				return pol.Decide(ctx, retention.DecideRequest{Now: runStart, Runs: partition})
			})
		})
		if derr != nil {
			return len(runs), nil, 0, derr
		}
		for _, id := range decision.Drop {
			if d.failed != nil {
				break // a destructive sweep stops at its first error
			}
			d.drop(id)
		}
		d.protected = toSet(decision.KeepFloor)
	}
	// The cap is the engine's bound, not the policy's schedule: it runs in the
	// same pass, over the same deletion state, and only if nothing failed.
	if kind == model.RunKindScheduled && d.failed == nil {
		d.evictForCap(cap, runs)
	}
	return len(runs) - len(d.deleted), d.deleted, d.freed, d.failed
}

// staleCutoff is the instant before which no live run can still be writing: its
// deadline plus its finish allowance, the same bound Kubernetes'
// activeDeadlineSeconds kills the pod at. Another run's incomplete objects are
// left alone until they are older than this — that is the proof, independent of
// any lease, that their writer has stopped.
func (e *Engine) staleCutoff() time.Time {
	if e.cfg.Run == nil {
		// No run section: a restore-shaped config never reaches retention. A test
		// that drives the sweep directly gets the permissive bound rather than a
		// panic.
		return time.Now().UTC()
	}
	return time.Now().UTC().Add(-(e.cfg.Run.Deadline.Std() + e.cfg.Run.FinishAllowance.Std()))
}

// deleter applies one storage's deletions, tracking what it freed and the first
// failure. The same path serves a policy's drops and the cap's evictions, so the
// floor, the stop rule and the occupancy ledger cannot come to mean two things.
type deleter struct {
	ctx       context.Context
	e         *Engine
	sb        *storageBinding
	sur       survey
	state     *retentionState
	current   string
	cutoff    time.Time
	protected map[string]bool
	cap       *capState
	deleted   []string
	freed     int64
	failed    error
}

// drop deletes one run's objects — manifest first — unless the floor refuses it.
// It returns the bytes it freed. A deletion that fails ends the sweep: the
// ledger's evidence and the destructive operation must never drift apart, and a
// copy half torn down is no longer evidence for what may be deleted next.
func (d *deleter) drop(id string) (freed int64) {
	if d.failed != nil {
		return 0
	}
	if id == d.current {
		d.e.storageEvent(d.sb, human.New("storage.retention.keep-current", human.Text("run", id)))
		return 0
	}
	r := d.sur.runs[id]
	if r == nil {
		return 0
	}
	// Another run's incomplete objects are left alone until the bound after which
	// its pod cannot be alive any more: an in-flight Put that started before a
	// takeover must never land on a run the taker is deleting.
	if !r.stored.Manifest && r.stored.At.After(d.cutoff) {
		d.e.storageEvent(d.sb, human.Warning("storage.retention.keep-in-flight",
			human.Text("run", id), human.Text("reason", "the run has no manifest and is younger than the longest a live run can last")))
		return 0
	}
	if keep, reason := d.state.canDrop(id, d.sb.name); !keep {
		d.e.storageEvent(d.sb, human.Warning("storage.retention.keep-floor", human.Text("run", id), human.Text("reason", reason)))
		return 0
	}
	for _, o := range deleteOrder(r) {
		_, err := attempt(d.ctx, d.e.retry, "delete run "+id, func(ctx context.Context, _ int) (struct{}, error) {
			return struct{}{}, safeCall(func() error { return d.sb.inst.Delete(ctx, o.Key) })
		})
		if err != nil {
			d.failed = fmt.Errorf("deleting run %s did not complete: %w", id, err)
			d.e.storageEvent(d.sb, human.Failure("storage.retention.delete-failed",
				human.Text("run", id), human.Text("reason", err.Error()), human.ObjectKey("key", o.Key)))
			return 0
		}
		freed += o.Size
		// The copy stops being evidence the moment its manifest is gone: a later
		// deletion must not lean on a run that is being torn down.
		if o.Key == r.manifestKey {
			d.state.forget(id, d.sb.name)
		}
	}
	d.state.forget(id, d.sb.name)
	if d.cap != nil {
		d.cap.releaseBytes(freed)
	}
	d.deleted = append(d.deleted, id)
	d.freed += freed
	return freed
}

// evictForCap frees room on one storage for a scheduled run that went over its
// configured cap, in the fixed order: an inactive incomplete attempt first, then
// the oldest manual runs, then the remaining scheduled history by the byte
// budget. The policy's floor ids and the deletion guard win every step; leftover
// overflow is reported, never a failed run.
func (d *deleter) evictForCap(cap *capState, runs []model.StoredRun) {
	if cap == nil || cap.cap <= 0 || d.failed != nil || cap.used() <= cap.cap {
		return
	}
	for _, id := range d.candidates(runs, func(r model.StoredRun) bool { return !r.Manifest }) {
		if cap.used() <= cap.cap || d.failed != nil {
			return
		}
		d.drop(id)
	}
	for _, id := range d.candidates(runs, func(r model.StoredRun) bool {
		return r.Manifest && model.NormalizeKind(r.Kind) == model.RunKindManual
	}) {
		if cap.used() <= cap.cap || d.failed != nil {
			return
		}
		d.drop(id)
	}
	// The remaining scheduled history, newest first, trimmed by the byte budget:
	// what is left of the cap once every other kind's bytes are set aside. The
	// policy's own floor is never a candidate.
	scheduled := partitionKind(runs, model.RunKindScheduled)
	retention.NewestFirst(scheduled)
	remaining := make([]model.StoredRun, 0, len(scheduled))
	var scheduledBytes int64
	for _, r := range scheduled {
		if r.RunID == d.current || d.deletedHas(r.RunID) || d.protected[r.RunID] {
			continue
		}
		remaining = append(remaining, r)
		scheduledBytes += r.Bytes
	}
	budget := cap.cap - (cap.used() - scheduledBytes)
	if budget < 0 {
		budget = 0
	}
	n := retention.Budget(remaining, budget, 0)
	for _, r := range remaining[n:] {
		if cap.used() <= cap.cap || d.failed != nil {
			break
		}
		d.drop(r.RunID)
	}
	if d.failed == nil && cap.used() > cap.cap {
		d.e.storageEvent(d.sb, human.Warning("storage.cap.overflow",
			human.Bytes("over", cap.used()-cap.cap),
			human.Text("reason", "the floor protects more than the cap allows")))
	}
}

// candidates lists the ids matching pred, oldest first, skipping the current run,
// everything already deleted and the policy's floor.
func (d *deleter) candidates(runs []model.StoredRun, pred func(model.StoredRun) bool) []string {
	var matched []model.StoredRun
	for _, r := range runs {
		if r.RunID == d.current || d.deletedHas(r.RunID) || d.protected[r.RunID] || !pred(r) {
			continue
		}
		matched = append(matched, r)
	}
	retention.NewestFirst(matched)
	out := make([]string, 0, len(matched))
	for i := len(matched) - 1; i >= 0; i-- {
		out = append(out, matched[i].RunID)
	}
	return out
}

func (d *deleter) deletedHas(id string) bool {
	for _, x := range d.deleted {
		if x == id {
			return true
		}
	}
	return false
}

func toSet(ids []string) map[string]bool {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// partitionKind is one homogeneous run list for one partition's policy: the
// engine splits by kind so a policy never sees the other kind's runs.
func partitionKind(runs []model.StoredRun, kind model.RunKind) []model.StoredRun {
	out := make([]model.StoredRun, 0, len(runs))
	for _, r := range runs {
		if model.NormalizeKind(r.Kind) == kind {
			out = append(out, r)
		}
	}
	return out
}
