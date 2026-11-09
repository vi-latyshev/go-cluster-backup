package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"slices"
	"sort"
	"syscall"
	"time"

	pluginio "github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

type FetchRequest struct {
	// RunID is an exact run id; "" together with Newest selects for me.
	RunID string
	// Newest selects the newest run whose copy of the artifact passes
	// verification — per artifact, not per run: a degraded run can hold a good copy.
	Newest   bool
	Artifact string
	// Storages is the walk order; empty means restore.storage_order.
	Storages []string
	Decode   bool
	// RawStorage and RawKey fetch one object by key with no manifest — the only
	// way to reach an unfinished run. It excludes RunID, Newest and Artifact.
	RawStorage string
	RawKey     string
}

// StorageSkip says why one storage was passed over.
type StorageSkip struct {
	Storage string
	Reason  string
}

type FetchReport struct {
	RunID, Artifact, Storage, Key string
	Bytes, RawBytes               int64
	SHA256                        string
	// Sensitivity is the artifact's sensitivity as the manifest records it, ""
	// for a raw by-key fetch: only an artifact fetch has a manifest to read it
	// from, and a caller deciding what may be done with plaintext needs it.
	Sensitivity model.Sensitivity
	// Verified is false only for a raw by-key fetch: without a manifest there
	// is nothing to verify against.
	Verified bool
	Skipped  []StorageSkip
}

// StorageHold says whether one storage holds the object a fetch would read, and
// why not when it does not.
type StorageHold struct {
	Storage string
	Holds   bool
	Reason  string
}

// FetchPlan is what a FetchRequest resolves to before a single byte is
// downloaded: the run and the object as the manifest records them, plus every
// storage of the walk and whether it holds a copy. A raw by-key plan has no
// manifest — RunID, Artifact, Sensitivity and SHA256 stay empty — and its hold
// is the storage's own answer to a Stat, nothing more.
type FetchPlan struct {
	RunID       string
	Artifact    string
	Key         string
	Sensitivity model.Sensitivity
	// Bytes and RawBytes are what the manifest recorded; SHA256 is the hash of
	// the stored object.
	Bytes    int64
	RawBytes int64
	SHA256   string
	Chain    []model.ProcessorStep
	Source   model.SourceRef
	Storages []StorageHold
}

// ResolveFetch describes what a Fetch would download, without downloading or
// decoding anything: the same selection — an exact run, or the newest run whose
// copy of the artifact is ok — the same walk order and the same refusals, so
// the two cannot disagree on run and storage. A caller uses it to decide what
// may be done with the artifact before a multi-gigabyte download starts.
//
// The plan reports what the manifests and the storages' listings say; whether
// the bytes actually verify is Fetch's, which re-checks size and SHA-256 before
// a byte reaches a destination or a decoder.
func (w *Workflow) ResolveFetch(ctx context.Context, req FetchRequest) (FetchPlan, error) {
	if err := checkFetchRequest(req); err != nil {
		return FetchPlan{}, err
	}
	if !w.o.Configured {
		return FetchPlan{}, errNoRestoreSection("resolve")
	}
	if req.RawStorage != "" {
		return w.resolveRaw(ctx, req)
	}
	storages, err := w.orderStorages(req.Storages)
	if err != nil {
		return FetchPlan{}, err
	}
	prefix, err := w.prefix("fetch")
	if err != nil {
		return FetchPlan{}, err
	}
	surveys := map[string]inventory.Survey{}
	getSurvey := w.surveyFunc(ctx, prefix, surveys)
	candidates, err := w.fetchCandidates(ctx, req, storages, getSurvey)
	if err != nil {
		return FetchPlan{}, err
	}
	var lastHolds []StorageHold
	for _, runID := range candidates {
		holds := make([]StorageHold, 0, len(storages))
		var ar *model.ArtifactResult
		for _, sb := range storages {
			sur, ok := getSurvey(sb)
			if !ok {
				holds = append(holds, StorageHold{Storage: sb.name, Reason: "the storage survey failed"})
				continue
			}
			res, reason := inspectRun(sur, runID, req.Artifact)
			if res == nil {
				holds = append(holds, StorageHold{Storage: sb.name, Reason: reason})
				continue
			}
			if reason = listingReason(sur.Runs[runID], res); reason != "" {
				holds = append(holds, StorageHold{Storage: sb.name, Reason: reason})
				continue
			}
			holds = append(holds, StorageHold{Storage: sb.name, Holds: true})
			if ar == nil {
				ar = res
			}
		}
		if ar != nil {
			return FetchPlan{RunID: runID, Artifact: req.Artifact, Key: ar.Key, Sensitivity: ar.Artifact.Sensitivity,
				Bytes: ar.Bytes, RawBytes: ar.RawBytes, SHA256: ar.SHA256, Chain: ar.Chain, Source: ar.Source,
				Storages: holds}, nil
		}
		lastHolds = holds
		if req.RunID != "" {
			break
		}
	}
	return FetchPlan{RunID: candidates[0], Artifact: req.Artifact, Storages: lastHolds},
		fmt.Errorf("fetch: artifact %q is not fetchable: %s", req.Artifact, joinHolds(lastHolds))
}

// resolveRaw describes a by-key fetch: there is no manifest here, so nothing is
// verified and Sensitivity stays empty — a raw fetch writes the stored bytes,
// never plaintext. The hold is a Stat, nothing more.
func (w *Workflow) resolveRaw(ctx context.Context, req FetchRequest) (FetchPlan, error) {
	sb := w.storageByName(req.RawStorage)
	if sb == nil {
		return FetchPlan{}, fmt.Errorf("fetch: unknown storage %q (known: %v)", req.RawStorage, w.storageNames())
	}
	hold := StorageHold{Storage: sb.name}
	_, err := execution.Attempt(ctx, w.o.Policy, "stat "+req.RawKey, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
		return execution.SafeValue(func() (model.ObjectInfo, error) { return sb.port.Stat(ctx, req.RawKey) })
	})
	if err != nil {
		hold.Reason = err.Error()
	} else {
		hold.Holds = true
	}
	return FetchPlan{Key: req.RawKey, Storages: []StorageHold{hold}}, nil
}

// listingReason compares the manifest's record with the storage's own listing:
// the object must be there at the recorded size. A listing is not a promise —
// Fetch still re-checks — but it is the same listing the survey already read.
func listingReason(sr inventory.Run, ar *model.ArtifactResult) string {
	for _, o := range sr.Objects {
		if o.Key != ar.Key {
			continue
		}
		if o.Size != ar.Bytes {
			return fmt.Sprintf("the object is %d bytes in the listing, the manifest says %d", o.Size, ar.Bytes)
		}
		return ""
	}
	return "the object is not in the storage listing"
}

// Fetch downloads one artifact (or one raw object) into dst and returns the
// report. The object is buffered in the restore spool and verified against the
// manifest — size and SHA-256 — before a single byte reaches dst or a decoder.
// A decode reverses the chain straight from that verified spool file into dst:
// the plaintext of a secret artifact never lands on the spool, which only Apply
// needs as a re-readable file.
func (w *Workflow) Fetch(ctx context.Context, req FetchRequest, dst io.Writer) (FetchReport, error) {
	if err := checkFetchRequest(req); err != nil {
		return FetchReport{}, err
	}
	stored := req
	stored.Decode = false
	rep, ar, file, sp, err := w.fetchToFile(ctx, stored, 0)
	if err != nil {
		return rep, err
	}
	defer func() { _ = sp.Remove(file) }()
	if !req.Decode {
		f, err := spoolOpen(sp, file)
		if err != nil {
			return rep, err
		}
		defer func() { _ = f.Close() }()
		if _, err := pluginio.CopyContext(ctx, dst, f); err != nil {
			return rep, fmt.Errorf("fetch: writing to the destination: %w", err)
		}
		return rep, nil
	}
	if err := w.decodeTo(ctx, sp, file, ar, dst); err != nil {
		return rep, err
	}
	return rep, nil
}

// decodeTo reverses the manifest's chain over the verified spool file into dst.
func (w *Workflow) decodeTo(ctx context.Context, sp *spool.Spool, file string, ar model.ArtifactResult, dst io.Writer) error {
	stages, err := w.decodeStages(ctx, ar.Chain)
	if err != nil {
		return err
	}
	src, err := spoolOpen(sp, file)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	return decodeInto(ctx, ar, stages, dst, src)
}

// decodeInto reverses the manifest's chain from src into dst. It is the one
// decode helper: a fetch's spool decode, an apply's decode into its second
// spool file and both streaming verifications go through it, so the two
// refusal texts live in one place.
func decodeInto(ctx context.Context, ar model.ArtifactResult, stages []pipeline.Stage, dst io.Writer, src io.Reader) error {
	cerr := pipeline.Chain(ctx, dst, func(ctx context.Context, w io.Writer) error {
		_, err := pluginio.CopyContext(ctx, w, src)
		return err
	}, stages)
	if cerr == nil {
		return nil
	}
	if errors.Is(cerr, errors.ErrUnsupported) {
		return pluginerrors.Permanent(fmt.Errorf("fetch: this config cannot decode the artifact: %w", cerr))
	}
	return pluginerrors.Permanent(fmt.Errorf("fetch: decoding %s: %w", ar.Key, cerr))
}

func checkFetchRequest(req FetchRequest) error {
	raw := req.RawStorage != "" || req.RawKey != ""
	if raw {
		if req.RawStorage == "" || req.RawKey == "" {
			return errors.New("fetch: RawStorage and RawKey go together")
		}
		if req.RunID != "" || req.Newest || req.Artifact != "" {
			return errors.New("fetch: a raw by-key fetch excludes RunID, Newest and Artifact")
		}
		if req.Decode {
			return errors.New("fetch: a raw by-key fetch never decodes; Decode must be false")
		}
		return nil
	}
	if req.RunID == "" && !req.Newest {
		return errors.New("fetch: RunID or Newest is required")
	}
	if req.RunID != "" && req.Newest {
		return errors.New("fetch: RunID and Newest exclude each other")
	}
	if req.Artifact == "" {
		return errors.New("fetch: Artifact is required")
	}
	return nil
}

// spoolFreeBytes reads the free space of the restore spool. It is a seam: the
// call underneath is a statfs, and a filesystem error is not something a
// configuration can reach, so a test replaces it to prove the workflow reports
// one rather than carrying on with a spool it cannot measure.
var spoolFreeBytes = func(sp *spool.Spool) (int64, error) { return sp.FreeBytes() }

// The spool's own file operations are seams for the same reason: an OS failure
// on a file this process just created is not reachable from a configuration.
var (
	spoolOpen   = (*spool.Spool).Open
	spoolCreate = (*spool.Spool).Create
	// closeSpoolFile closes the sink behind a space guard.
	closeSpoolFile = func(c io.Closer) error { return c.Close() }
	// fileSeek rewinds a decoded spool file the in-place probe read.
	fileSeek = (*os.File).Seek
)

// errOverflow stops a download the moment it delivers more than the manifest
// recorded: the bytes beyond the recorded size can neither be hashed nor kept.
var errOverflow = errors.New("the object is longer than the manifest says")

// meter counts and hashes a download as it passes, and refuses a byte beyond
// the manifest's recorded size before it reaches any sink. Both sinks have one.
type meter struct {
	w        io.Writer
	hash     hash.Hash
	n        int64
	limit    int64
	overflow bool
	// over is how many bytes the download had delivered when it crossed the
	// limit, for the skip's reason.
	over int64
}

func (m *meter) Write(p []byte) (int, error) {
	if m.overflow {
		return 0, errOverflow
	}
	if m.n+int64(len(p)) > m.limit {
		m.overflow = true
		m.over = m.n + int64(len(p))
		return 0, errOverflow
	}
	n, err := m.w.Write(p)
	m.n += int64(n)
	m.hash.Write(p[:n])
	return n, err
}

func (m *meter) sum() string { return hex.EncodeToString(m.hash.Sum(nil)) }

// Close closes the sink underneath, so a storage that closes the writer it was
// handed ends the spool file the workflow then closes again and reports.
func (m *meter) Close() error {
	if c, ok := m.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// detachWriter keeps a download moving after its downstream stopped reading: the
// first write error is swallowed and every later write is accounted for, so the
// meter still hashes the whole stream and the storage's Get can finish. A
// decoder therefore never receives a write error the workflow created.
type detachWriter struct {
	w      io.Writer
	failed bool
}

func (d *detachWriter) Write(p []byte) (int, error) {
	if !d.failed {
		if _, err := d.w.Write(p); err != nil {
			d.failed = true
		}
	}
	return len(p), nil
}

// spoolSpaceError is the one refusal a spool write reports when it would eat
// into the reserve: every number a deployment needs to size the spool is on it.
type spoolSpaceError struct {
	free, need, reserve, written int64
	cause                        error
}

func (s *spoolSpaceError) Error() string {
	msg := fmt.Sprintf("restore spool: free %d bytes, need %d, reserve %d, written %d", s.free, s.need, s.reserve, s.written)
	if s.cause != nil {
		msg += ": " + s.cause.Error()
	}
	return msg
}

func (s *spoolSpaceError) Unwrap() error { return s.cause }

func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// copyError is one copy's failure and what the walk does with it: a skip names
// the storage passed over; a hard error stops the walk. The wrapped error keeps
// its own class, so the attempt loop still retries a transient one.
type copyError struct {
	err    error
	skip   bool
	reason string
}

func (c *copyError) Error() string { return c.err.Error() }
func (c *copyError) Unwrap() error { return c.err }

// spaceGuard refuses a spool write that would eat into the restore reserve. It
// measures the filesystem again only after half the previous headroom has been
// written, so a statfs per chunk stays cheap even on a network spool.
type spaceGuard struct {
	w       io.WriteCloser
	sp      *spool.Spool
	reserve int64
	budget  int64
	written int64
	// err is the guard's own terminal verdict — a statfs failure or a space
	// refusal — checked after the Get because a storage may recolor a write
	// error into its own class.
	err error
}

func (g *spaceGuard) Write(p []byte) (int, error) {
	if g.err != nil {
		return 0, g.err
	}
	if g.budget < int64(len(p)) {
		free, err := spoolFreeBytes(g.sp)
		if err != nil {
			g.err = pluginerrors.Permanent(fmt.Errorf("restore spool: %w", err))
			return 0, g.err
		}
		headroom := free - g.reserve
		if headroom < int64(len(p)) {
			g.err = pluginerrors.Permanent(&spoolSpaceError{free: free, need: g.written + int64(len(p)), reserve: g.reserve, written: g.written})
			return 0, g.err
		}
		g.budget = headroom / 2
		if g.budget < int64(len(p)) {
			g.budget = int64(len(p))
		}
	}
	g.budget -= int64(len(p))
	n, err := g.w.Write(p)
	g.written += int64(n)
	if err != nil {
		if isNoSpace(err) {
			g.err = pluginerrors.Permanent(&spoolSpaceError{need: g.written, reserve: g.reserve, written: g.written, cause: err})
			return n, g.err
		}
		return n, err
	}
	return n, nil
}

// Close closes the spool file, and a shortage the filesystem only reports on
// close is a space refusal like any other.
func (g *spaceGuard) Close() error {
	err := closeSpoolFile(g.w)
	if err != nil && isNoSpace(err) {
		g.err = pluginerrors.Permanent(&spoolSpaceError{need: g.written, reserve: g.reserve, written: g.written, cause: err})
	}
	return err
}

// copyResult is what one verified copy hands back: the manifest's record and the
// sink's own answer — the spool file to read, or the probe report.
type copyResult struct {
	ar    model.ArtifactResult
	file  string
	probe source.ProbeReport
}

// fetchToFile is the shared body of Fetch and Apply: it resolves the object and
// downloads it to a verified spool file. extra is the space one copy needs on
// the spool beyond its own bytes — an apply that decodes into a second spool
// file asks for the artifact's raw size before the first byte.
func (w *Workflow) fetchToFile(ctx context.Context, req FetchRequest, extra int64) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
	if !w.o.Configured {
		return FetchReport{}, model.ArtifactResult{}, "", nil, errNoRestoreSection("restore")
	}
	if req.RawStorage != "" && req.Decode {
		// A raw fetch writes the stored bytes; a caller asking for a decode is
		// asking for plaintext it would never get, so refuse instead of ignoring
		// the request — and the report of a raw fetch can then honestly leave
		// Sensitivity empty.
		return FetchReport{}, model.ArtifactResult{}, "", nil, errors.New("fetch: a raw by-key fetch never decodes; Decode must be false")
	}
	sp, err := spool.New(w.o.SpoolDir)
	if err != nil {
		return FetchReport{}, model.ArtifactResult{}, "", nil, err
	}
	free, err := spoolFreeBytes(sp)
	if err != nil {
		return FetchReport{}, model.ArtifactResult{}, "", nil, err
	}
	if free < w.o.Reserve {
		return FetchReport{}, model.ArtifactResult{}, "", nil, fmt.Errorf("restore spool has %d free bytes, reserve is %d", free, w.o.Reserve)
	}
	if req.RawStorage != "" {
		rep, file, err := w.fetchRaw(ctx, req, sp)
		return rep, model.ArtifactResult{}, file, sp, err
	}
	return w.fetchArtifact(ctx, req, sp, extra)
}

func (w *Workflow) fetchRaw(ctx context.Context, req FetchRequest, sp *spool.Spool) (FetchReport, string, error) {
	sb := w.storageByName(req.RawStorage)
	if sb == nil {
		return FetchReport{}, "", fmt.Errorf("fetch: unknown storage %q (known: %v)", req.RawStorage, w.storageNames())
	}
	wr, err := execution.Attempt(ctx, w.o.Policy, "get "+req.RawKey, func(ctx context.Context, _ int) (*spool.Writer, error) {
		f, err := spoolCreate(sp, "raw")
		if err != nil {
			return nil, pluginerrors.Permanent(err)
		}
		g := &spaceGuard{w: f, sp: sp, reserve: w.o.Reserve}
		if err := execution.SafeCall(func() error { return sb.port.Get(ctx, req.RawKey, g) }); err != nil {
			_ = f.Close()
			_ = sp.Remove(f.Name())
			if g.err != nil {
				return nil, g.err
			}
			return nil, err
		}
		if err := g.Close(); err != nil {
			_ = sp.Remove(f.Name())
			if g.err != nil {
				return nil, g.err
			}
			return nil, pluginerrors.Transient(err)
		}
		if g.err != nil {
			_ = sp.Remove(f.Name())
			return nil, g.err
		}
		return f, nil
	})
	if err != nil {
		return FetchReport{}, "", fmt.Errorf("fetch: raw object %s on %s: %w", req.RawKey, sb.name, err)
	}
	return FetchReport{Storage: sb.name, Key: req.RawKey, Bytes: wr.Size(), SHA256: wr.SHA256(), Verified: false}, wr.Name(), nil
}

func (w *Workflow) fetchArtifact(ctx context.Context, req FetchRequest, sp *spool.Spool, extra int64) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
	fail := func(rep FetchReport, err error) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
		return rep, model.ArtifactResult{}, "", sp, err
	}
	storages, err := w.orderStorages(req.Storages)
	if err != nil {
		return fail(FetchReport{}, err)
	}
	prefix, err := w.prefix("fetch")
	if err != nil {
		return fail(FetchReport{}, err)
	}
	surveys := map[string]inventory.Survey{}
	getSurvey := w.surveyFunc(ctx, prefix, surveys)

	runID, sb, out, skips, err := walkDownload(ctx, w, req, storages, getSurvey,
		func(ctx context.Context, sb *binding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			file, skip, err := w.downloadSpool(ctx, sb, ar, sp, extra)
			return copyResult{ar: *ar, file: file}, skip, err
		})
	rep := FetchReport{RunID: runID, Artifact: req.Artifact, Skipped: skips}
	if err != nil {
		return fail(rep, err)
	}
	rep.Storage = sb.name
	rep.Key = out.ar.Key
	rep.Bytes = out.ar.Bytes
	rep.RawBytes = out.ar.RawBytes
	rep.SHA256 = out.ar.SHA256
	rep.Sensitivity = out.ar.Artifact.Sensitivity
	rep.Verified = true
	return rep, out.ar, out.file, sp, nil
}

// fetchCandidates resolves the runs a fetch tries, newest first: an exact RunID
// is the only candidate, Newest is every run where the artifact is ok on at
// least one storage. ResolveFetch, Fetch and a stored verification share it so
// the three cannot disagree.
func (w *Workflow) fetchCandidates(ctx context.Context, req FetchRequest, storages []*binding,
	getSurvey func(*binding) (inventory.Survey, bool)) ([]string, error) {
	if req.RunID != "" {
		return []string{req.RunID}, nil
	}
	ats := map[string]time.Time{}
	for _, sb := range storages {
		sur, ok := getSurvey(sb)
		if !ok {
			continue
		}
		for _, id := range slices.Sorted(maps.Keys(sur.Runs)) {
			sr := sur.Runs[id]
			if sr.Manifest != nil && artifactOK(sr.Manifest, req.Artifact) {
				ats[id] = sr.Manifest.StartedAt
			}
		}
	}
	candidates := make([]string, 0, len(ats))
	for id := range ats {
		candidates = append(candidates, id)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !ats[candidates[i]].Equal(ats[candidates[j]]) {
			return ats[candidates[i]].After(ats[candidates[j]])
		}
		return candidates[i] > candidates[j]
	})
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no run on any storage holds artifact %q as ok", req.Artifact)
	}
	return candidates, nil
}

// inspectRun reads one run's manifest on one storage and reports whether the
// artifact may have a copy there: the artifact's record when it may, and the
// reason it may not otherwise. It is the one place the manifest-level refusals
// live, shared by a download and by a plan.
func inspectRun(sur inventory.Survey, runID, artifact string) (*model.ArtifactResult, string) {
	sr, ok := sur.Runs[runID]
	if !ok {
		return nil, "the run is not present on this storage"
	}
	if sr.ManifestProblem != "" {
		return nil, sr.ManifestProblem
	}
	if sr.Manifest == nil {
		return nil, "the run has no manifest"
	}
	ar := findArtifact(sr.Manifest, artifact)
	if ar == nil {
		return nil, "the artifact is absent from the run"
	}
	if ar.Outcome != model.ArtifactOK {
		return nil, fmt.Sprintf("the artifact failed in run %s; %s", runID, nearestOK(sur, artifact, runID))
	}
	if ar.Key == "" || ar.SHA256 == "" {
		return nil, "the manifest does not record the object key and sha256"
	}
	return ar, ""
}

// walkDownload walks the runs and storages one download tries. For each copy it
// runs the preflight — the survey's record, the Stat, the size — and hands the
// record to take, which downloads and verdicts the copy. A skip passes to the
// next storage; a hard error stops the walk. It is the one walk Fetch, Apply
// and a verification use.
func walkDownload(ctx context.Context, w *Workflow, req FetchRequest, storages []*binding,
	getSurvey func(*binding) (inventory.Survey, bool),
	take func(ctx context.Context, sb *binding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error),
) (string, *binding, copyResult, []StorageSkip, error) {
	var zero copyResult
	candidates, err := w.fetchCandidates(ctx, req, storages, getSurvey)
	if err != nil {
		return "", nil, zero, nil, fmt.Errorf("fetch: %w", err)
	}
	var lastSkips []StorageSkip
	for _, runID := range candidates {
		var skips []StorageSkip
		for _, sb := range storages {
			sur, ok := getSurvey(sb)
			if !ok {
				skips = append(skips, StorageSkip{Storage: sb.name, Reason: "the storage survey failed"})
				continue
			}
			ar, reason := inspectRun(sur, runID, req.Artifact)
			if ar == nil {
				skips = append(skips, StorageSkip{Storage: sb.name, Reason: reason})
				continue
			}
			st, err := execution.Attempt(ctx, w.o.Policy, "stat "+ar.Artifact.Name, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
				return execution.SafeValue(func() (model.ObjectInfo, error) { return sb.port.Stat(ctx, ar.Key) })
			})
			if err != nil {
				if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
					skips = append(skips, StorageSkip{Storage: sb.name, Reason: "the object is not found"})
				} else {
					skips = append(skips, StorageSkip{Storage: sb.name, Reason: fmt.Sprintf("stat: %v", err)})
				}
				continue
			}
			if st.Size != ar.Bytes {
				skips = append(skips, StorageSkip{Storage: sb.name,
					Reason: fmt.Sprintf("the object is %d bytes, the manifest says %d", st.Size, ar.Bytes)})
				continue
			}
			out, skip, err := take(ctx, sb, ar)
			if err != nil {
				return runID, sb, zero, skips, err
			}
			if skip != nil {
				skips = append(skips, *skip)
				continue
			}
			return runID, sb, out, skips, nil
		}
		lastSkips = skips
		if req.RunID != "" {
			break
		}
	}
	return candidates[0], nil, zero, lastSkips,
		fmt.Errorf("fetch: artifact %q is not fetchable: %s", req.Artifact, joinSkips(lastSkips))
}

// downloadSpool downloads one copy into a verified spool file. extra is the
// space the copy needs beyond its own bytes. A space refusal is hard: the same
// spool serves every storage, so the walk never falls back on an older run or
// another storage.
func (w *Workflow) downloadSpool(ctx context.Context, sb *binding, ar *model.ArtifactResult, sp *spool.Spool, extra int64) (string, *StorageSkip, error) {
	need := ar.Bytes + extra
	free, err := spoolFreeBytes(sp)
	if err != nil {
		return "", nil, pluginerrors.Permanent(fmt.Errorf("restore spool: %w", err))
	}
	if free-w.o.Reserve < need {
		return "", nil, pluginerrors.Permanent(&spoolSpaceError{free: free, need: need, reserve: w.o.Reserve})
	}
	file, err := execution.Attempt(ctx, w.o.Policy, "get "+ar.Key, func(ctx context.Context, _ int) (string, error) {
		return spoolCopy(ctx, sb.port, ar, sp, w.o.Reserve)
	})
	if err != nil {
		if ce, ok := errors.AsType[*copyError](err); ok {
			if ce.skip {
				return "", &StorageSkip{Storage: sb.name, Reason: ce.reason}, nil
			}
			err = ce.err
		}
		return "", nil, err
	}
	return file, nil, nil
}

// spoolCopy is one attempt of a spool copy: the download is measured, hashed and
// held to the manifest's size and SHA-256 before the file is handed back.
func spoolCopy(ctx context.Context, ro storage.Reader, ar *model.ArtifactResult, sp *spool.Spool, reserve int64) (string, error) {
	f, err := sp.Create("fetch")
	if err != nil {
		return "", &copyError{skip: true, reason: "download: " + err.Error(), err: pluginerrors.Permanent(err)}
	}
	name := f.Name()
	fail := func(err error) (string, error) {
		_ = sp.Remove(name)
		return "", err
	}
	g := &spaceGuard{w: f, sp: sp, reserve: reserve}
	m := &meter{w: g, hash: sha256.New(), limit: ar.Bytes}
	getErr := execution.SafeCall(func() error { return ro.Get(ctx, ar.Key, m) })
	closeErr := g.Close()
	switch {
	case m.overflow:
		return fail(&copyError{skip: true, reason: overflowReason(m.over, ar.Bytes), err: pluginerrors.Permanent(errOverflow)})
	case g.err != nil:
		return fail(&copyError{err: g.err})
	case getErr != nil:
		return fail(&copyError{skip: true, reason: "download: " + getErr.Error(), err: getErr})
	case closeErr != nil:
		return fail(&copyError{skip: true, reason: "download: " + closeErr.Error(), err: closeErr})
	case m.n != ar.Bytes:
		return fail(&copyError{skip: true, reason: overflowReason(m.n, ar.Bytes),
			err: pluginerrors.Permanent(errors.New(overflowReason(m.n, ar.Bytes)))})
	case m.sum() != ar.SHA256:
		reason := fmt.Sprintf("sha256 is %s, the manifest says %s", m.sum(), ar.SHA256)
		return fail(&copyError{skip: true, reason: reason, err: pluginerrors.Permanent(errors.New(reason))})
	}
	return name, nil
}

func overflowReason(got, want int64) string {
	return fmt.Sprintf("downloaded %d bytes, the manifest says %d", got, want)
}

// streamProbe consumes a decoded artifact as it flows: the probe a verification
// runs, or a read that proves the chain alone.
type streamProbe func(ctx context.Context, src io.Reader) (source.ProbeReport, error)

// discardProbe reads the decoded stream and keeps nothing: what a decoded
// verification needs to prove the chain without touching the disk.
func discardProbe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	n, err := pluginio.CopyContext(ctx, io.Discard, src)
	return source.ProbeReport{Bytes: n}, err
}

// downloadStream downloads one copy and decodes it as it flows into consume.
// Every attempt downloads again — there is no spool file to rewind — so a
// consumer retried after a transient failure sees the stream from its start.
func (w *Workflow) downloadStream(ctx context.Context, sb *binding, ar *model.ArtifactResult,
	stages []pipeline.Stage, consume streamProbe) (source.ProbeReport, *StorageSkip, error) {
	probe, err := execution.Attempt(ctx, w.o.Policy, "get "+ar.Key, func(ctx context.Context, _ int) (source.ProbeReport, error) {
		return streamCopy(ctx, sb.port, ar, stages, consume)
	})
	if err != nil {
		if ce, ok := errors.AsType[*copyError](err); ok {
			if ce.skip {
				return source.ProbeReport{}, &StorageSkip{Storage: sb.name, Reason: ce.reason}, nil
			}
			err = ce.err
		}
		return source.ProbeReport{}, nil, err
	}
	return probe, nil, nil
}

// streamCopy is one attempt of a streaming copy. The download feeds a meter and
// a pipe; a decoder drains that pipe into a second; the consumer reads the
// second. The download closes the first pipe with the stream's verdict — a Get
// error, an overflow, a size or a SHA-256 mismatch — and a clean EOF only when
// the bytes matched the manifest. The stream's verdict is decided before the
// consumer's own error, so a consumer that refused a corrupt copy early cannot
// hide the corruption from the walk.
func streamCopy(ctx context.Context, ro storage.Reader, ar *model.ArtifactResult,
	stages []pipeline.Stage, consume streamProbe) (source.ProbeReport, error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	m := &meter{w: &detachWriter{w: inW}, hash: sha256.New(), limit: ar.Bytes}

	gdone := make(chan struct{})
	var getErr error
	go func() {
		defer close(gdone)
		getErr = execution.SafeCall(func() error { return ro.Get(attemptCtx, ar.Key, m) })
		var stop error
		switch {
		case m.overflow:
			stop = errOverflow
		case getErr != nil:
			stop = getErr
		case m.n != ar.Bytes:
			stop = errors.New(overflowReason(m.n, ar.Bytes))
		case m.sum() != ar.SHA256:
			stop = fmt.Errorf("sha256 is %s, the manifest says %s", m.sum(), ar.SHA256)
		}
		_ = inW.CloseWithError(stop)
	}()

	ddone := make(chan struct{})
	var decodeErr error
	go func() {
		defer close(ddone)
		decodeErr = decodeInto(attemptCtx, *ar, stages, outW, inR)
		// A decode that stopped early must not leave the download blocked on a
		// pipe nobody reads.
		_ = inR.CloseWithError(decodeErr)
		_ = outW.CloseWithError(decodeErr)
	}()

	probe, consumerErr := execution.SafeValue(func() (source.ProbeReport, error) { return consume(attemptCtx, outR) })
	if errors.Is(consumerErr, errors.ErrUnsupported) {
		cancel()
		// No drain: cancel, and close both pipes so the decoder and the
		// download are released wherever they are blocked.
		_ = inR.CloseWithError(consumerErr)
		_ = outR.CloseWithError(consumerErr)
	} else {
		// The consumer may have left before the stream ended: draining is what
		// lets the decoder and the download finish, and it keeps a processor's
		// own write error from being mistaken for a decode failure.
		_, _ = io.Copy(io.Discard, outR)
		_ = outR.Close()
	}
	<-gdone
	<-ddone

	// The stream's own verdict comes first, every time: a consumer that refused
	// a corrupt copy early must not hide its SHA-256 mismatch and cost the walk
	// the healthy copy on another storage. Only a stream that finished exactly
	// as the manifest records it leaves the consumer's own error to be retried
	// by its class.
	switch {
	case errors.Is(consumerErr, errors.ErrUnsupported):
		return source.ProbeReport{}, &copyError{err: pluginerrors.Permanent(consumerErr)}
	case m.overflow:
		return source.ProbeReport{}, &copyError{skip: true, reason: overflowReason(m.over, ar.Bytes), err: pluginerrors.Permanent(errOverflow)}
	case getErr != nil:
		return source.ProbeReport{}, &copyError{skip: true, reason: "download: " + getErr.Error(), err: getErr}
	case m.n != ar.Bytes:
		reason := overflowReason(m.n, ar.Bytes)
		return source.ProbeReport{}, &copyError{skip: true, reason: reason, err: pluginerrors.Permanent(errors.New(reason))}
	case m.sum() != ar.SHA256:
		reason := fmt.Sprintf("sha256 is %s, the manifest says %s", m.sum(), ar.SHA256)
		return source.ProbeReport{}, &copyError{skip: true, reason: reason, err: pluginerrors.Permanent(errors.New(reason))}
	case decodeErr != nil:
		return source.ProbeReport{}, &copyError{err: decodeErr}
	case consumerErr != nil:
		return source.ProbeReport{}, &copyError{err: consumerErr}
	}
	return probe, nil
}

// nearestOK names the newest other run of the same storage where the artifact is ok.
func nearestOK(sur inventory.Survey, artifact, exceptRun string) string {
	best := ""
	var bestAt time.Time
	for id, sr := range sur.Runs {
		if id == exceptRun || sr.Manifest == nil {
			continue
		}
		if !artifactOK(sr.Manifest, artifact) {
			continue
		}
		if best == "" || sr.Manifest.StartedAt.After(bestAt) {
			best = id
			bestAt = sr.Manifest.StartedAt
		}
	}
	if best == "" {
		return "no other run of this storage has it ok"
	}
	return "the nearest run where it is ok: " + best
}
