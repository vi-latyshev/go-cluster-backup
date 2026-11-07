package protection

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
)

// Sweep is one storage's retention pass. The workflow calls it inside that
// storage's own bounded context, after its manifest phase, once per storage.
type Sweep struct {
	RunStart time.Time
	Kind     model.RunKind
	Storage  string
	Survey   inventory.Survey
	// Overlay is the run being written, when it has a copy on this storage.
	Overlay *model.StoredRun
	// Artifacts are the run's produced artifacts, Verified says which of them
	// this storage verified, and ManifestSaved says this storage holds the run's
	// manifest. Together they let the sweep credit the current run as a holder
	// exactly as the run's own results stand — before any policy or cap decision.
	Artifacts     []model.ArtifactResult
	Verified      map[string]bool
	ManifestSaved bool
	// Skip is the reason this storage's sweep deletes nothing — a manual run that
	// did not complete there. "" runs the sweep.
	Skip string
}

// SweepResult is what one storage's sweep kept, deleted and freed.
type SweepResult struct {
	Kept    int
	Deleted []string
	Freed   int64
	Err     error
}

// Sweep runs the run kind's policy and — for a scheduled run — the occupancy
// cap over ONE deletion state, through the ledger's delete authority. A manual
// run that did not complete on this storage deletes nothing there. One pass per
// storage, ONE deleter: a confirmed deletion is excluded from the cap's
// candidates, its bytes are subtracted from the ledger once, and a destructive
// error ends the storage's sweep instead of starting a second one.
func (l *Ledger) Sweep(ctx context.Context, s Sweep) SweepResult {
	if s.Skip != "" {
		l.event(s.Storage, human.New("storage.retention.skipped", human.Text("reason", s.Skip)))
		return SweepResult{}
	}
	res := l.retentionPhase(ctx, s)
	if res.Err != nil {
		l.event(s.Storage, human.Failure("storage.retention.failed", human.Text("reason", res.Err.Error())))
		return res
	}
	l.event(s.Storage, human.New("storage.retention",
		human.Count("kept", int64(res.Kept)), human.Count("deleted", int64(len(res.Deleted))), human.Bytes("freed", res.Freed)))
	return res
}

func (l *Ledger) retentionPhase(ctx context.Context, s Sweep) SweepResult {
	sur := s.Survey
	if !sur.OK {
		return SweepResult{Err: fmt.Errorf("survey: %w", sur.Err)}
	}
	l.creditCurrent(s)
	current := ""
	if s.Overlay != nil {
		current = s.Overlay.RunID
	}
	runs := make([]model.StoredRun, 0, len(sur.Runs)+1)
	present := map[string]bool{}
	for _, id := range slices.Sorted(maps.Keys(sur.Runs)) {
		runs = append(runs, sur.Runs[id].Stored)
		present[id] = true
	}
	if s.Overlay != nil && !present[s.Overlay.RunID] {
		runs = append(runs, *s.Overlay)
	}
	d := &deleter{ctx: ctx, l: l, storage: s.Storage, sur: sur, current: current,
		cutoff: l.o.StaleCutoff, cap: l.caps[s.Storage]}
	if policies := l.o.Policies[s.Storage]; policies != nil {
		if policy := policies[s.Kind]; policy != nil {
			partition := partitionKind(runs, s.Kind)
			decision, err := execution.Attempt(ctx, l.o.Retry, "retention "+s.Storage,
				func(ctx context.Context, _ int) (retention.Decision, error) {
					return execution.SafeValue(func() (retention.Decision, error) {
						return policy.Decide(ctx, retention.DecideRequest{Now: s.RunStart, Runs: partition})
					})
				})
			if err != nil {
				return SweepResult{Kept: len(runs), Err: err}
			}
			for _, id := range decision.Drop {
				if d.failed != nil {
					break // a destructive sweep stops at its first error
				}
				d.drop(id)
			}
			d.protected = toSet(decision.KeepFloor)
		}
	}
	// The cap is this ledger's bound, not the policy's schedule: it runs in the
	// same pass, over the same deletion state, and only if nothing failed.
	if s.Kind == model.RunKindScheduled && d.failed == nil {
		d.evictForCap(d.cap, runs)
	}
	return SweepResult{Kept: len(runs) - len(d.deleted), Deleted: d.deleted, Freed: d.freed, Err: d.failed}
}

// creditCurrent records the run being written in the retention state before any
// policy or cap decision: its verified copies on this storage count as held
// copies, and the run itself counts as a verified run because its overlay says
// every artifact verified and the manifest is saved. Without this the sweep
// still reads the previous run as the newest holder of an artifact this run has
// just replaced, and the floor pins that run against the very policy that
// wanted it gone.
func (l *Ledger) creditCurrent(s Sweep) {
	if s.Overlay == nil {
		return
	}
	if s.Overlay.Verified {
		l.state.mark(s.Overlay.RunID, s.Storage)
	}
	matrix := make(map[string]map[string]bool, len(s.Verified))
	for name, ok := range s.Verified {
		matrix[name] = map[string]bool{s.Storage: ok}
	}
	l.state.markCurrent(s.Overlay.RunID, s.Overlay.Kind, s.Overlay.At, s.Artifacts, matrix,
		map[string]bool{s.Storage: s.ManifestSaved})
}

// deleteOrder puts the manifest first: a delete that stops half-way then leaves
// a run without a manifest rather than a manifest promising objects that are
// gone — the survey reads a missing manifest as "not verified", which is true.
func deleteOrder(r inventory.Run) []model.ObjectInfo {
	manifest := ""
	if r.Manifest != nil {
		manifest = r.ManifestKey
	}
	out := make([]model.ObjectInfo, 0, len(r.Objects))
	for _, o := range r.Objects {
		if o.Key != manifest {
			out = append(out, o)
		}
	}
	for _, o := range r.Objects {
		if o.Key == manifest {
			out = append([]model.ObjectInfo{o}, out...)
		}
	}
	return out
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
//     storage does not release it. With a standing failed (one undeclared volume
//     fails every night while the databases are delivered) no run is verified as
//     a whole, and this per-artifact rule is the floor that keeps the last
//     complete copy of each artifact alive;
//   - at least one verified complete run stays across everything the ledger
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
	// artifact identity -> storage -> kind -> run id -> the run's instant.
	held map[string]map[string]map[model.RunKind]map[string]time.Time
	// problems names the runs whose manifest is listed but could not be read,
	// parsed or accepted. Such a run is never dropped in this sweep.
	problems map[string]string
	// uncertainRuns names every run the lease record's uncertainty points at:
	// their provider writes may still land, and their prefixes are the inventory
	// a later survey compares. None is ever dropped — neither by a policy nor by
	// the cap.
	uncertainRuns map[string]bool
}

func newRetentionState(surveys map[string]inventory.Survey) *retentionState {
	st := &retentionState{
		verified:      map[string]map[string]bool{},
		kinds:         map[string]model.RunKind{},
		held:          map[string]map[string]map[model.RunKind]map[string]time.Time{},
		problems:      map[string]string{},
		uncertainRuns: map[string]bool{},
	}
	for name, s := range surveys {
		if !s.OK {
			continue
		}
		for id, r := range s.Runs {
			st.kinds[id] = r.Stored.Kind
			if r.Stored.Verified {
				st.mark(id, name)
			}
			if r.ManifestProblem != "" {
				st.problems[id] = r.ManifestProblem
			}
			st.recordHeld(name, id, r)
		}
	}
	return st
}

// recordHeld adds one surveyed run's artifact copies on one storage to the
// ledger. It runs under no lock only during construction; markHeld takes the
// lock for the runtime path.
func (st *retentionState) recordHeld(storage, runID string, r inventory.Run) {
	if r.Manifest == nil || r.ManifestProblem != "" {
		return
	}
	for _, ar := range r.Manifest.Artifacts {
		if ar.Outcome != model.ArtifactOK || ar.Key == "" {
			continue
		}
		if !holdsObject(r.Objects, ar) {
			continue
		}
		st.markHeld(artifactIdentity(ar.Artifact), storage, r.Stored.Kind, runID, r.Stored.At)
	}
}

// holdsObject reports whether the listing shows the object the manifest names,
// at the size the manifest records: a truncated or half-written object is not a
// copy, and a manifest and an object that disagree are not evidence.
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
// verified, before any floor decision of this sweep. A survey cannot see the run
// yet — it is not in the storage's listing — so without this the ledger still
// names the previous run the newest holder of an artifact this run just
// uploaded, and the floor pins that run against the very policy that wanted it
// gone.
func (st *retentionState) markCurrent(runID string, kind model.RunKind, at time.Time, artifacts []model.ArtifactResult,
	verifiedMatrix map[string]map[string]bool, saved map[string]bool) {
	st.mu.Lock()
	st.kinds[runID] = kind
	st.mu.Unlock()
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

// markUncertain adds one run to the pending set. Every member's prefix is the
// inventory the settlement proof compares, so the sweep leaves all of them
// whole; adding a holder never replaces another.
func (st *retentionState) markUncertain(runID string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.uncertainRuns == nil {
		st.uncertainRuns = map[string]bool{}
	}
	st.uncertainRuns[runID] = true
}

// canDrop reports whether deleting this run from this storage keeps the floor;
// the second return is the reason for a refusal, for the run log.
//
// The first rule outranks the policy entirely: a run holding the newest verified
// copy of some artifact ON THIS STORAGE, among runs of its own kind, is refused,
// because deleting it would leave that artifact with no copy of that kind on
// this storage until a newer run of the kind takes one. A manual copy therefore
// never releases a scheduled pin, and a manual run is pinned only while it is
// the storage's only verified copy of the artifact at all — so keep-last N and
// the scheduled run's eviction can still reclaim a manual run that a scheduled
// copy already covers.
//
// The second rule is the run-level one, read across every kind: at least one
// verified complete run must remain across everything the ledger surveyed. It is
// deliberately not per kind — a manual run gets no permanent pin beyond the
// per-artifact rule above.
func (st *retentionState) canDrop(runID, storage string) (bool, string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.uncertainRuns[runID] {
		return false, "the lease record's uncertainty names this run: its inventory is the settlement proof"
	}
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
	return false, "deleting it would leave no verified run anywhere the ledger surveyed"
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

// deleter applies one storage's deletions, tracking what it freed and the first
// failure. The same path serves a policy's drops and the cap's evictions, so the
// floor, the stop rule and the occupancy ledger cannot come to mean two things.
type deleter struct {
	ctx       context.Context
	l         *Ledger
	storage   string
	sur       inventory.Survey
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
		d.l.event(d.storage, human.New("storage.retention.keep-current", human.Text("run", id)))
		return 0
	}
	r, ok := d.sur.Runs[id]
	if !ok {
		return 0
	}
	// Another run's incomplete objects are left alone until the bound after which
	// its pod cannot be alive any more: an in-flight Put that started before a
	// takeover must never land on a run the taker is deleting.
	if !r.Stored.Manifest && r.Stored.At.After(d.cutoff) {
		d.l.event(d.storage, human.Warning("storage.retention.keep-in-flight",
			human.Text("run", id), human.Text("reason", "the run has no manifest and is younger than the longest a live run can last")))
		return 0
	}
	if keep, reason := d.l.state.canDrop(id, d.storage); !keep {
		d.l.event(d.storage, human.Warning("storage.retention.keep-floor", human.Text("run", id), human.Text("reason", reason)))
		return 0
	}
	remover := d.l.removers[d.storage]
	for _, o := range deleteOrder(r) {
		err := execution.AttemptErr(d.ctx, d.l.o.Retry, "delete run "+id, func(ctx context.Context, _ int) error {
			return execution.SafeCall(func() error { return remover.Delete(ctx, o.Key) })
		})
		if err != nil {
			d.failed = fmt.Errorf("deleting run %s did not complete: %w", id, err)
			d.l.event(d.storage, human.Failure("storage.retention.delete-failed",
				human.Text("run", id), human.Text("reason", err.Error()), human.ObjectKey("key", o.Key)))
			return 0
		}
		freed += o.Size
		// The copy stops being evidence the moment its manifest is gone: a later
		// deletion must not lean on a run that is being torn down.
		if o.Key == r.ManifestKey {
			d.l.state.forget(id, d.storage)
		}
	}
	d.l.state.forget(id, d.storage)
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
	if cap == nil || cap.cap <= 0 || d.failed != nil {
		return
	}
	if cap.used() <= cap.cap {
		return
	}
	// Unknown occupancy never authorises a destructive sweep: bytes another
	// writer's accepted commit may still add are not counted, so a "freed" byte
	// is not proven free and a verified run would be deleted on a guess. Manual
	// admission is already refused on the same unknown; a scheduled run reports
	// the overflow it cannot safely reclaim.
	if !cap.occupancyKnown() {
		d.l.event(d.storage, human.Warning("storage.cap.unknown",
			human.Text("reason", "occupancy is unknown: cap eviction is closed")))
		return
	}
	for _, id := range d.candidates(runs, func(r model.StoredRun) bool { return !r.Manifest }) {
		if cap.used() <= cap.cap || d.failed != nil {
			return
		}
		d.drop(id)
	}
	for _, id := range d.candidates(runs, func(r model.StoredRun) bool {
		return r.Manifest && r.Kind == model.RunKindManual
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
		d.l.event(d.storage, human.Warning("storage.cap.overflow",
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
// ledger splits by kind so a policy never sees the other kind's runs.
func partitionKind(runs []model.StoredRun, kind model.RunKind) []model.StoredRun {
	out := make([]model.StoredRun, 0, len(runs))
	for _, r := range runs {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}
