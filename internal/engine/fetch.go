package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strings"
	"syscall"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// readOnlyStorage wraps a storage on every restore path: those paths read, and
// code that ever tries to write through them gets ErrUnsupported, not a surprise
// deletion.
type readOnlyStorage struct{ storage.Storage }

func (r readOnlyStorage) Put(context.Context, storage.PutRequest, io.Reader) (model.ObjectInfo, error) {
	return model.ObjectInfo{}, fmt.Errorf("storage is read-only on a restore path: Put: %w", errors.ErrUnsupported)
}

func (r readOnlyStorage) Delete(context.Context, string) error {
	return fmt.Errorf("storage is read-only on a restore path: Delete: %w", errors.ErrUnsupported)
}

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
	// way to reach an unfinished run or a previous-format archive. It excludes
	// RunID, Newest and Artifact.
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

type StoredRunsRequest struct{ Storages []string }

type StoredRunView struct {
	RunID string
	// Kind is the run's identity, read from its id — available even for a run
	// that never wrote a manifest.
	Kind        model.RunKind
	Storage     string
	HasManifest bool
	Report      model.RunReport // zero when the manifest is missing
}

type ApplyRunRequest struct {
	// Fetch must not be a raw by-key fetch: applying needs the manifest's chain,
	// source type and origin. Decode is forced true.
	Fetch  FetchRequest
	Target string
	Mode   source.ApplyMode
	// Confirm must equal the target's Origin verbatim for an in-place apply.
	Confirm string
	// AllowForeignOrigin accepts a target whose origin differs from the
	// manifest's — a deliberate, explicit mismatch, never a tolerance.
	AllowForeignOrigin bool
}

type VerifyRequest struct {
	Fetch FetchRequest
	// Depth is "stored", "decoded" or "probed".
	Depth string
	// Target names the source whose Prober runs at "probed" depth.
	Target string
}

type VerifyResult struct {
	Artifact string
	// RunID is the run that was checked: the requested one, or the one the
	// Newest selector resolved to.
	RunID   string
	Depth   string
	OK      bool
	Storage string
	Bytes   int64
	// ApplySpoolBytes is what an apply of this record would need on the restore
	// spool before the reserve — the stored object plus its decoded copy — for
	// a decoded or probed check that read the manifest. Zero otherwise.
	ApplySpoolBytes int64
	Error           string
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
func (e *Engine) ResolveFetch(ctx context.Context, req FetchRequest) (FetchPlan, error) {
	if err := checkFetchRequest(req); err != nil {
		return FetchPlan{}, err
	}
	if e.cfg.Restore == nil {
		return FetchPlan{}, errors.New("resolve: config has no restore section")
	}
	if req.RawStorage != "" {
		return e.resolveRaw(ctx, req)
	}
	storages, err := e.orderStorages(req.Storages)
	if err != nil {
		return FetchPlan{}, err
	}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		return FetchPlan{}, errors.New("fetch: config has no layout section; a run survey needs layout.run_prefix")
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		return FetchPlan{}, err
	}
	surveys := map[string]survey{}
	getSurvey := e.surveyFunc(ctx, prefix, surveys)
	candidates, err := e.fetchCandidates(ctx, req, storages, getSurvey)
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
			if reason = listingReason(sur.runs[runID], res); reason != "" {
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
func (e *Engine) resolveRaw(ctx context.Context, req FetchRequest) (FetchPlan, error) {
	sb := e.storageByName(req.RawStorage)
	if sb == nil {
		return FetchPlan{}, fmt.Errorf("fetch: unknown storage %q (known: %v)", req.RawStorage, e.storageNames())
	}
	hold := StorageHold{Storage: sb.name}
	_, err := attempt(ctx, e.restoreRetry, "stat "+req.RawKey, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
		return safeValue(func() (model.ObjectInfo, error) { return readOnlyStorage{sb.inst}.Stat(ctx, req.RawKey) })
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
func listingReason(sr *surveyRun, ar *model.ArtifactResult) string {
	for _, o := range sr.objects {
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

func joinHolds(holds []StorageHold) string {
	skips := make([]StorageSkip, 0, len(holds))
	for _, h := range holds {
		if !h.Holds {
			skips = append(skips, StorageSkip{Storage: h.Storage, Reason: h.Reason})
		}
	}
	return joinSkips(skips)
}

// Fetch downloads one artifact (or one raw object) into dst and returns the
// report. The object is buffered in the restore spool and verified against the
// manifest — size and SHA-256 — before a single byte reaches dst or a decoder.
// A decode reverses the chain straight from that verified spool file into dst:
// the plaintext of a secret artifact never lands on the spool, which only Apply
// needs as a re-readable file.
func (e *Engine) Fetch(ctx context.Context, req FetchRequest, dst io.Writer) (FetchReport, error) {
	if err := checkFetchRequest(req); err != nil {
		return FetchReport{}, err
	}
	stored := req
	stored.Decode = false
	rep, ar, file, sp, err := e.fetchToFile(ctx, stored, 0)
	if err != nil {
		return rep, err
	}
	defer func() { _ = sp.Remove(file) }()
	if !req.Decode {
		f, err := sp.Open(file)
		if err != nil {
			return rep, err
		}
		defer func() { _ = f.Close() }()
		if _, err := plugin.CopyContext(ctx, dst, f); err != nil {
			return rep, fmt.Errorf("fetch: writing to the destination: %w", err)
		}
		return rep, nil
	}
	if err := e.decodeTo(ctx, sp, file, ar, dst); err != nil {
		return rep, err
	}
	return rep, nil
}

// decodeTo reverses the manifest's chain over the verified spool file into dst.
func (e *Engine) decodeTo(ctx context.Context, sp *spool.Spool, file string, ar model.ArtifactResult, dst io.Writer) error {
	stages, err := e.decodeStages(ctx, ar.Chain)
	if err != nil {
		return err
	}
	src, err := sp.Open(file)
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
func decodeInto(ctx context.Context, ar model.ArtifactResult, stages []func(ctx context.Context, dst io.Writer, src io.Reader) error, dst io.Writer, src io.Reader) error {
	cerr := chain(ctx, dst, func(ctx context.Context, w io.Writer) error {
		_, err := plugin.CopyContext(ctx, w, src)
		return err
	}, stages)
	if cerr == nil {
		return nil
	}
	if errors.Is(cerr, errors.ErrUnsupported) {
		return plugin.Permanent(fmt.Errorf("fetch: this config cannot decode the artifact: %w", cerr))
	}
	return plugin.Permanent(fmt.Errorf("fetch: decoding %s: %w", ar.Key, cerr))
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
// configuration can reach, so a test replaces it to prove the engine reports one
// rather than carrying on with a spool it cannot measure.
var spoolFreeBytes = func(sp *spool.Spool) (int64, error) { return sp.FreeBytes() }

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
// handed ends the spool file the engine then closes again and reports.
func (m *meter) Close() error {
	if c, ok := m.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// detachWriter keeps a download moving after its downstream stopped reading: the
// first write error is swallowed and every later write is accounted for, so the
// meter still hashes the whole stream and the storage's Get can finish. A
// decoder therefore never receives a write error the engine created.
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
			g.err = plugin.Permanent(fmt.Errorf("restore spool: %w", err))
			return 0, g.err
		}
		headroom := free - g.reserve
		if headroom < int64(len(p)) {
			g.err = plugin.Permanent(&spoolSpaceError{free: free, need: g.written + int64(len(p)), reserve: g.reserve, written: g.written})
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
			g.err = plugin.Permanent(&spoolSpaceError{need: g.written, reserve: g.reserve, written: g.written, cause: err})
			return n, g.err
		}
		return n, err
	}
	return n, nil
}

// Close closes the spool file, and a shortage the filesystem only reports on
// close is a space refusal like any other.
func (g *spaceGuard) Close() error {
	err := g.w.Close()
	if err != nil && isNoSpace(err) {
		g.err = plugin.Permanent(&spoolSpaceError{need: g.written, reserve: g.reserve, written: g.written, cause: err})
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

// fetchToFile is the shared engine body of Fetch and Apply: it resolves the
// object and downloads it to a verified spool file. extra is the space one copy
// needs on the spool beyond its own bytes — an apply that decodes into a second
// spool file asks for the artifact's raw size before the first byte.
func (e *Engine) fetchToFile(ctx context.Context, req FetchRequest, extra int64) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
	if e.cfg.Restore == nil {
		return FetchReport{}, model.ArtifactResult{}, "", nil, errors.New("restore: config has no restore section")
	}
	if req.RawStorage != "" && req.Decode {
		// A raw fetch writes the stored bytes; a caller asking for a decode is
		// asking for plaintext it would never get, so refuse instead of ignoring
		// the request — and the report of a raw fetch can then honestly leave
		// Sensitivity empty.
		return FetchReport{}, model.ArtifactResult{}, "", nil, errors.New("fetch: a raw by-key fetch never decodes; Decode must be false")
	}
	sp, err := spool.New(e.restoreSpoolDir)
	if err != nil {
		return FetchReport{}, model.ArtifactResult{}, "", nil, err
	}
	free, err := spoolFreeBytes(sp)
	if err != nil {
		return FetchReport{}, model.ArtifactResult{}, "", nil, err
	}
	if free < e.restoreReserve {
		return FetchReport{}, model.ArtifactResult{}, "", nil, fmt.Errorf("restore spool has %d free bytes, reserve is %d", free, e.restoreReserve)
	}
	if req.RawStorage != "" {
		rep, file, err := e.fetchRaw(ctx, req, sp)
		return rep, model.ArtifactResult{}, file, sp, err
	}
	return e.fetchArtifact(ctx, req, sp, extra)
}

func (e *Engine) fetchRaw(ctx context.Context, req FetchRequest, sp *spool.Spool) (FetchReport, string, error) {
	sb := e.storageByName(req.RawStorage)
	if sb == nil {
		return FetchReport{}, "", fmt.Errorf("fetch: unknown storage %q (known: %v)", req.RawStorage, e.storageNames())
	}
	ro := readOnlyStorage{sb.inst}
	w, err := attempt(ctx, e.restoreRetry, "get "+req.RawKey, func(ctx context.Context, _ int) (*spool.Writer, error) {
		f, err := sp.Create("raw")
		if err != nil {
			return nil, plugin.Permanent(err)
		}
		g := &spaceGuard{w: f, sp: sp, reserve: e.restoreReserve}
		if err := safeCall(func() error { return ro.Get(ctx, req.RawKey, g) }); err != nil {
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
			return nil, plugin.Transient(err)
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
	return FetchReport{Storage: sb.name, Key: req.RawKey, Bytes: w.Size(), SHA256: w.SHA256(), Verified: false}, w.Name(), nil
}

func (e *Engine) fetchArtifact(ctx context.Context, req FetchRequest, sp *spool.Spool, extra int64) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
	fail := func(rep FetchReport, err error) (FetchReport, model.ArtifactResult, string, *spool.Spool, error) {
		return rep, model.ArtifactResult{}, "", sp, err
	}
	storages, err := e.orderStorages(req.Storages)
	if err != nil {
		return fail(FetchReport{}, err)
	}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		return fail(FetchReport{}, errors.New("fetch: config has no layout section; a run survey needs layout.run_prefix"))
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		return fail(FetchReport{}, err)
	}
	surveys := map[string]survey{}
	getSurvey := e.surveyFunc(ctx, prefix, surveys)

	runID, sb, out, skips, err := walkDownload(ctx, e, req, storages, getSurvey,
		func(ctx context.Context, sb *storageBinding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			file, skip, err := e.downloadSpool(ctx, sb, ar, sp, extra)
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

// surveyFunc memoizes one storage survey per call group, so every path of one
// fetch — the run selection and each storage's turn — sees the same listing.
func (e *Engine) surveyFunc(ctx context.Context, prefix string, surveys map[string]survey) func(*storageBinding) (survey, bool) {
	return func(sb *storageBinding) (survey, bool) {
		if sur, ok := surveys[sb.name]; ok {
			return sur, sur.ok
		}
		sur := e.surveyStorage(ctx, sb, prefix, e.restoreRetry, nil)
		surveys[sb.name] = sur
		return sur, sur.ok
	}
}

// fetchCandidates resolves the runs a fetch tries, newest first: an exact RunID
// is the only candidate, Newest is every run where the artifact is ok on at
// least one storage. ResolveFetch, Fetch and a stored verification share it so
// the three cannot disagree.
func (e *Engine) fetchCandidates(ctx context.Context, req FetchRequest, storages []*storageBinding,
	getSurvey func(*storageBinding) (survey, bool)) ([]string, error) {
	if req.RunID != "" {
		return []string{req.RunID}, nil
	}
	ats := map[string]time.Time{}
	for _, sb := range storages {
		sur, ok := getSurvey(sb)
		if !ok {
			continue
		}
		for _, id := range sortedKeys(sur.runs) {
			sr := sur.runs[id]
			if sr.manifest != nil && artifactOK(sr.manifest, req.Artifact) {
				ats[id] = sr.manifest.StartedAt
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

func joinSkips(skips []StorageSkip) string {
	if len(skips) == 0 {
		return "no storage was tried"
	}
	parts := make([]string, 0, len(skips))
	for _, s := range skips {
		parts = append(parts, fmt.Sprintf("%s: %s", s.Storage, s.Reason))
	}
	return strings.Join(parts, "; ")
}

func artifactOK(rep *model.RunReport, name string) bool {
	for _, a := range rep.Artifacts {
		if a.Artifact.Name == name {
			return a.Outcome == model.ArtifactOK
		}
	}
	return false
}

func findArtifact(rep *model.RunReport, name string) *model.ArtifactResult {
	for i := range rep.Artifacts {
		if rep.Artifacts[i].Artifact.Name == name {
			return &rep.Artifacts[i]
		}
	}
	return nil
}

// inspectRun reads one run's manifest on one storage and reports whether the
// artifact may have a copy there: the artifact's record when it may, and the
// reason it may not otherwise. It is the one place the manifest-level refusals
// live, shared by a download and by a plan.
func inspectRun(sur survey, runID, artifact string) (*model.ArtifactResult, string) {
	sr := sur.runs[runID]
	if sr == nil {
		return nil, "the run is not present on this storage"
	}
	if sr.manifest == nil {
		return nil, "the run has no manifest"
	}
	if sr.manifest.Schema != model.ManifestSchema {
		return nil, fmt.Sprintf("manifest schema %d; this build reads %d", sr.manifest.Schema, model.ManifestSchema)
	}
	ar := findArtifact(sr.manifest, artifact)
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
func walkDownload(ctx context.Context, e *Engine, req FetchRequest, storages []*storageBinding,
	getSurvey func(*storageBinding) (survey, bool),
	take func(ctx context.Context, sb *storageBinding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error),
) (string, *storageBinding, copyResult, []StorageSkip, error) {
	var zero copyResult
	candidates, err := e.fetchCandidates(ctx, req, storages, getSurvey)
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
			ro := readOnlyStorage{sb.inst}
			st, err := attempt(ctx, e.restoreRetry, "stat "+ar.Artifact.Name, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
				return safeValue(func() (model.ObjectInfo, error) { return ro.Stat(ctx, ar.Key) })
			})
			if err != nil {
				if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
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
func (e *Engine) downloadSpool(ctx context.Context, sb *storageBinding, ar *model.ArtifactResult, sp *spool.Spool, extra int64) (string, *StorageSkip, error) {
	need := ar.Bytes + extra
	free, err := spoolFreeBytes(sp)
	if err != nil {
		return "", nil, plugin.Permanent(fmt.Errorf("restore spool: %w", err))
	}
	if free-e.restoreReserve < need {
		return "", nil, plugin.Permanent(&spoolSpaceError{free: free, need: need, reserve: e.restoreReserve})
	}
	ro := readOnlyStorage{sb.inst}
	file, err := attempt(ctx, e.restoreRetry, "get "+ar.Key, func(ctx context.Context, _ int) (string, error) {
		return spoolCopy(ctx, ro, ar, sp, e.restoreReserve)
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
func spoolCopy(ctx context.Context, ro storage.Storage, ar *model.ArtifactResult, sp *spool.Spool, reserve int64) (string, error) {
	f, err := sp.Create("fetch")
	if err != nil {
		return "", &copyError{skip: true, reason: "download: " + err.Error(), err: plugin.Permanent(err)}
	}
	name := f.Name()
	fail := func(err error) (string, error) {
		_ = sp.Remove(name)
		return "", err
	}
	g := &spaceGuard{w: f, sp: sp, reserve: reserve}
	m := &meter{w: g, hash: sha256.New(), limit: ar.Bytes}
	getErr := safeCall(func() error { return ro.Get(ctx, ar.Key, m) })
	closeErr := g.Close()
	switch {
	case m.overflow:
		return fail(&copyError{skip: true, reason: overflowReason(m.over, ar.Bytes), err: plugin.Permanent(errOverflow)})
	case g.err != nil:
		return fail(&copyError{err: g.err})
	case getErr != nil:
		return fail(&copyError{skip: true, reason: "download: " + getErr.Error(), err: getErr})
	case closeErr != nil:
		return fail(&copyError{skip: true, reason: "download: " + closeErr.Error(), err: closeErr})
	case m.n != ar.Bytes:
		return fail(&copyError{skip: true, reason: overflowReason(m.n, ar.Bytes),
			err: plugin.Permanent(errors.New(overflowReason(m.n, ar.Bytes)))})
	case m.sum() != ar.SHA256:
		reason := fmt.Sprintf("sha256 is %s, the manifest says %s", m.sum(), ar.SHA256)
		return fail(&copyError{skip: true, reason: reason, err: plugin.Permanent(errors.New(reason))})
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
	n, err := plugin.CopyContext(ctx, io.Discard, src)
	return source.ProbeReport{Bytes: n}, err
}

// downloadStream downloads one copy and decodes it as it flows into consume.
// Every attempt downloads again — there is no spool file to rewind — so a
// consumer retried after a transient failure sees the stream from its start.
func (e *Engine) downloadStream(ctx context.Context, sb *storageBinding, ar *model.ArtifactResult,
	stages []func(ctx context.Context, dst io.Writer, src io.Reader) error, consume streamProbe) (source.ProbeReport, *StorageSkip, error) {
	ro := readOnlyStorage{sb.inst}
	probe, err := attempt(ctx, e.restoreRetry, "get "+ar.Key, func(ctx context.Context, _ int) (source.ProbeReport, error) {
		return streamCopy(ctx, ro, ar, stages, consume)
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
func streamCopy(ctx context.Context, ro storage.Storage, ar *model.ArtifactResult,
	stages []func(ctx context.Context, dst io.Writer, src io.Reader) error, consume streamProbe) (source.ProbeReport, error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	m := &meter{w: &detachWriter{w: inW}, hash: sha256.New(), limit: ar.Bytes}

	gdone := make(chan struct{})
	var getErr error
	go func() {
		defer close(gdone)
		getErr = safeCall(func() error { return ro.Get(attemptCtx, ar.Key, m) })
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

	probe, consumerErr := safeValue(func() (source.ProbeReport, error) { return consume(attemptCtx, outR) })
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
		return source.ProbeReport{}, &copyError{err: plugin.Permanent(consumerErr)}
	case m.overflow:
		return source.ProbeReport{}, &copyError{skip: true, reason: overflowReason(m.over, ar.Bytes), err: plugin.Permanent(errOverflow)}
	case getErr != nil:
		return source.ProbeReport{}, &copyError{skip: true, reason: "download: " + getErr.Error(), err: getErr}
	case m.n != ar.Bytes:
		reason := overflowReason(m.n, ar.Bytes)
		return source.ProbeReport{}, &copyError{skip: true, reason: reason, err: plugin.Permanent(errors.New(reason))}
	case m.sum() != ar.SHA256:
		reason := fmt.Sprintf("sha256 is %s, the manifest says %s", m.sum(), ar.SHA256)
		return source.ProbeReport{}, &copyError{skip: true, reason: reason, err: plugin.Permanent(errors.New(reason))}
	case decodeErr != nil:
		return source.ProbeReport{}, &copyError{err: decodeErr}
	case consumerErr != nil:
		return source.ProbeReport{}, &copyError{err: consumerErr}
	}
	return probe, nil
}

// nearestOK names the newest other run of the same storage where the artifact is ok.
func nearestOK(sur survey, artifact, exceptRun string) string {
	best := ""
	var bestAt time.Time
	for id, sr := range sur.runs {
		if id == exceptRun || sr.manifest == nil {
			continue
		}
		if !artifactOK(sr.manifest, artifact) {
			continue
		}
		if best == "" || sr.manifest.StartedAt.After(bestAt) {
			best = id
			bestAt = sr.manifest.StartedAt
		}
	}
	if best == "" {
		return "no other run of this storage has it ok"
	}
	return "the nearest run where it is ok: " + best
}

// decodeStages resolves the manifest's chain against the current config: the
// instance must exist here and its type must match what the manifest recorded.
func (e *Engine) decodeStages(ctx context.Context, chain []model.ProcessorStep) ([]func(ctx context.Context, dst io.Writer, src io.Reader) error, error) {
	stages := make([]func(ctx context.Context, dst io.Writer, src io.Reader) error, 0, len(chain))
	for i := len(chain) - 1; i >= 0; i-- {
		step := chain[i]
		if step.Instance == "" {
			return nil, errors.New("fetch: the manifest's chain step has no instance name")
		}
		p, ok := e.processors[step.Instance]
		if !ok {
			return nil, fmt.Errorf("fetch: the manifest's chain names processor instance %q, which this config does not have", step.Instance)
		}
		if ct := e.cfg.Processors[step.Instance].Type; ct != step.Type {
			return nil, fmt.Errorf("fetch: processor instance %q is type %q here, %q in the manifest", step.Instance, ct, step.Type)
		}
		// The restore side's own preflight, before the first byte is fetched: a
		// processor that cannot decode says so here, with its own name, instead of
		// failing the stream halfway through.
		if d, ok := p.(processor.DecodeChecker); ok {
			if err := d.CheckDecode(ctx); err != nil {
				return nil, fmt.Errorf("fetch: processor %q cannot decode: %w", step.Instance, err)
			}
		}
		stages = append(stages, decodeStage(p))
	}
	return stages, nil
}

// StoredRuns lists the runs every storage holds, including runs without a manifest.
func (e *Engine) StoredRuns(ctx context.Context, req StoredRunsRequest) ([]StoredRunView, error) {
	storages, err := e.orderStorages(req.Storages)
	if err != nil {
		return nil, err
	}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		return nil, errors.New("stored runs: config has no layout section; a run survey needs layout.run_prefix")
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		return nil, err
	}
	var out []StoredRunView
	for _, sb := range storages {
		sur := e.surveyStorage(ctx, sb, prefix, e.restoreRetry, nil)
		if !sur.ok {
			return nil, fmt.Errorf("stored runs: storage %s: %w", sb.name, sur.err)
		}
		ids := sortedKeys(sur.runs)
		sort.Slice(ids, func(i, j int) bool {
			a, b := sur.runs[ids[i]].stored, sur.runs[ids[j]].stored
			if !a.At.Equal(b.At) {
				return a.At.After(b.At)
			}
			return a.RunID > b.RunID
		})
		for _, id := range ids {
			sr := sur.runs[id]
			view := StoredRunView{RunID: id, Kind: sr.stored.Kind, Storage: sb.name, HasManifest: sr.manifest != nil}
			if sr.manifest != nil {
				view.Report = *sr.manifest
			}
			out = append(out, view)
		}
	}
	return out, nil
}

// Apply fetches an artifact with decode, verifies it entirely, and hands the
// decoded bytes to the target source's Restorer. One attempt, no retry: an apply
// may be destructive. Everything an apply can refuse without the artifact — the
// target, its type, the confirmation, a foreign origin, a target that cannot
// restore — is refused before a byte is downloaded.
func (e *Engine) Apply(ctx context.Context, req ApplyRunRequest) (source.ApplyReport, error) {
	if req.Mode != source.ApplySideBySide && req.Mode != source.ApplyInPlace {
		return source.ApplyReport{}, errors.New("apply: Mode is required and must be side_by_side or in_place")
	}
	if req.Fetch.RawStorage != "" || req.Fetch.RawKey != "" {
		return source.ApplyReport{}, errors.New("apply: a raw by-key fetch cannot be applied; it carries no chain, source type or origin")
	}
	// The manifest says enough about the artifact to check the target before
	// the download: the run id, the source type and the origin are in it.
	// A discovered target is built for this call: it is closed when the call
	// returns, error paths included, never parked on the Engine.
	scope := &closerScope{}
	defer func() {
		if err := scope.close(); err != nil {
			e.event(human.Failure("target.close.failed", human.Text("reason", err.Error())))
		}
	}()
	plan, err := e.ResolveFetch(ctx, req.Fetch)
	if err != nil {
		return source.ApplyReport{}, err
	}
	target, typ, err := e.resolveTarget(ctx, req.Target, plan.Artifact, scope)
	if err != nil {
		return source.ApplyReport{}, err
	}
	traits, err := attempt(ctx, e.restoreRetry, "describe target "+req.Target, func(ctx context.Context, _ int) (source.SourceTraits, error) {
		return safeValue(func() (source.SourceTraits, error) { return target.Describe(ctx) })
	})
	if err != nil {
		return source.ApplyReport{}, fmt.Errorf("apply: describing the target: %w", err)
	}
	if plan.Source.Type != "" && typ != plan.Source.Type {
		return source.ApplyReport{}, fmt.Errorf("apply: the target is type %q, the manifest's source is type %q", typ, plan.Source.Type)
	}
	if req.Mode == source.ApplyInPlace {
		if req.Confirm == "" || req.Confirm != traits.Origin {
			return source.ApplyReport{}, fmt.Errorf("apply: in_place requires Confirm equal to the target origin %q verbatim", traits.Origin)
		}
	}
	if plan.Source.Origin != "" && traits.Origin != plan.Source.Origin && !req.AllowForeignOrigin {
		return source.ApplyReport{}, fmt.Errorf("apply: the manifest's origin %q differs from the target's %q; set AllowForeignOrigin to accept this deliberately", plan.Source.Origin, traits.Origin)
	}
	restorer, ok := target.(source.Restorer)
	if !ok {
		return source.ApplyReport{}, fmt.Errorf("apply: target type %q has no restorer; only fetch is available for it", typ)
	}
	prober, ok := target.(source.Prober)
	if req.Mode == source.ApplyInPlace && !ok {
		return source.ApplyReport{}, fmt.Errorf("apply: in_place needs a target that can probe the artifact before the first write, and type %q has no Prober", typ)
	}
	req.Fetch.Decode = false
	// The origin and the type were checked against the run the plan picked:
	// the download must stay on that run. Left to Newest, the fetch would fall
	// back to an older run whose recorded origin was never validated against
	// the target — so a copy that fails verification now refuses, naming the
	// run that was validated, instead of silently restoring another one's data.
	req.Fetch.RunID = plan.RunID
	req.Fetch.Newest = false
	// The decode into a second spool file needs the artifact's raw size kept
	// free before the first byte of the copy arrives.
	rep, ar, file, sp, err := e.fetchToFile(ctx, req.Fetch, plan.RawBytes)
	if err != nil {
		return source.ApplyReport{}, fmt.Errorf("apply: the validated run %s is not restorable: %w", plan.RunID, err)
	}
	defer func() { _ = sp.Remove(file) }()
	// The decode happens after the copy verified, into its own guarded spool
	// file: the restorer and the in-place probe both need a re-readable stream.
	decoded, err := e.decodeSpool(ctx, sp, file, ar)
	if err != nil {
		return source.ApplyReport{}, err
	}
	defer func() { _ = sp.Remove(decoded) }()
	src, err := sp.Open(decoded)
	if err != nil {
		return source.ApplyReport{}, err
	}
	defer func() { _ = src.Close() }()
	if req.Mode == source.ApplyInPlace {
		if _, err := safeValue(func() (source.ProbeReport, error) { return prober.Probe(ctx, src) }); err != nil {
			return source.ApplyReport{}, fmt.Errorf("apply: probe before an in-place write failed: %w", err)
		}
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return source.ApplyReport{}, err
		}
	}
	var options jsontext.Value
	if e.cfg.Restore != nil {
		options = e.cfg.Restore.Apply[typ]
	}
	return safeValue(func() (source.ApplyReport, error) {
		return restorer.Apply(ctx, source.ApplyRequest{Mode: req.Mode, Label: rep.RunID, Options: options}, src)
	})
}

// decodeSpool decodes a verified spool file into a second spool file the probe
// or the restorer reads. The second file is written under the reserve too: its
// size is the manifest's raw size, which a staged artifact can understate.
func (e *Engine) decodeSpool(ctx context.Context, sp *spool.Spool, file string, ar model.ArtifactResult) (string, error) {
	stages, err := e.decodeStages(ctx, ar.Chain)
	if err != nil {
		return "", err
	}
	dw, err := sp.Create("decoded")
	if err != nil {
		return "", err
	}
	name := dw.Name()
	fail := func(err error) (string, error) {
		_ = sp.Remove(name)
		return "", err
	}
	g := &spaceGuard{w: dw, sp: sp, reserve: e.restoreReserve}
	src, err := sp.Open(file)
	if err != nil {
		_ = g.Close()
		return fail(err)
	}
	derr := decodeInto(ctx, ar, stages, g, src)
	_ = src.Close()
	cerr := g.Close()
	if g.err != nil {
		return fail(g.err)
	}
	if derr != nil {
		return fail(derr)
	}
	if cerr != nil {
		return fail(cerr)
	}
	return name, nil
}

// resolveTarget builds the source an apply or a probe writes to: a static source
// instance by name, or "discovered" — the same discovery rules find the target,
// by the same artifact name.
func (e *Engine) resolveTarget(ctx context.Context, target, artifact string, scope *closerScope) (source.Source, string, error) {
	if target == "" {
		return nil, "", errors.New("apply: Target is required")
	}
	if target == "discovered" {
		for _, db := range e.discoveries {
			// The same loop as a run, with the narrower question: only a
			// retryable problem on the target this restore came to resolve
			// buys another pass. Another workload's transient fault leaves the
			// target as the pass found it, and when the budget is spent the
			// last attempt stands — a target that still carries a problem is
			// unavailable.
			found, err := e.discoverAll(ctx, e.restoreRetry, db.name, db.inst, targetProblem(artifact))
			if err != nil {
				return nil, "", fmt.Errorf("apply: discovery %s: %w", db.name, err)
			}
			for _, f := range found {
				// A unit artifact is named "<envelope>/<unit>", so an instance
				// whose envelope prefixes the target is the one that may hold
				// it — every other found object is left alone, unbuilt.
				if !targetUnder(f.Artifact.Name, artifact) {
					continue
				}
				if f.Excluded != "" {
					return nil, "", fmt.Errorf("apply: the discovered target for %q is excluded: %s", artifact, f.ExcludedReason)
				}
				if f.Problem != "" {
					return nil, "", fmt.Errorf("apply: the discovered target for %q is unavailable: %s", artifact, f.Problem)
				}
				inst, err := plugin.Build(ctx, e.reg, source.Kind, f.Type, f.Config, e.deps("source", f.Type, f.Name))
				if err != nil {
					return nil, "", err
				}
				scope.track(inst)
				units := e.expandUnits(ctx, e.restoreRetry, expandedUnit{
					instance: f.Name, srcType: f.Type, src: inst, artifact: f.Artifact,
				}, scope)
				var held []string
				for _, u := range units {
					held = append(held, u.artifact.Name)
					if u.artifact.Name != artifact {
						continue
					}
					if u.problem != nil {
						return nil, "", fmt.Errorf("apply: the discovered target for %q is unavailable: %w", artifact, u.problem)
					}
					return u.src, u.srcType, nil
				}
				if len(held) > 0 {
					// The instance that declares this artifact is alive and its
					// domain holds other artifacts: what the caller named is a
					// unit the server no longer has — a dropped database, or an
					// instance recreated without it. Creating it is the one step
					// a restore cannot do for itself.
					return nil, "", fmt.Errorf(
						"apply: %s holds no artifact %q (it holds: %s); a unit the server no longer has — a dropped database, a recreated instance — must exist empty and be discovered again before it can be restored into",
						f.Name, artifact, strings.Join(held, ", "))
				}
			}
		}
		return nil, "", fmt.Errorf("apply: no discovered source has artifact %q", artifact)
	}
	for _, sb := range e.sources {
		if sb.name == target {
			return sb.inst, sb.typ, nil
		}
	}
	names := make([]string, 0, len(e.sources))
	for _, sb := range e.sources {
		names = append(names, sb.name)
	}
	return nil, "", fmt.Errorf("apply: unknown target %q (known: %v, or \"discovered\")", target, names)
}

// Verify walks the requested depth for one artifact or every artifact of a run.
func (e *Engine) Verify(ctx context.Context, req VerifyRequest) ([]VerifyResult, error) {
	switch req.Depth {
	case "stored", "decoded", "probed":
	default:
		return nil, fmt.Errorf("verify: Depth must be stored, decoded or probed, got %q", req.Depth)
	}
	if req.Depth == "probed" && req.Target == "" {
		return nil, errors.New("verify: Depth probed needs a Target")
	}
	storages, err := e.orderStorages(req.Fetch.Storages)
	if err != nil {
		return nil, err
	}
	artifacts, err := e.verifyArtifacts(ctx, req, storages)
	if err != nil {
		return nil, err
	}
	var out []VerifyResult
	for _, name := range artifacts {
		out = append(out, e.verifyOne(ctx, req, storages, name))
	}
	return out, nil
}

func (e *Engine) verifyArtifacts(ctx context.Context, req VerifyRequest, storages []*storageBinding) ([]string, error) {
	if req.Fetch.Artifact != "" {
		return []string{req.Fetch.Artifact}, nil
	}
	if req.Fetch.RunID == "" {
		return nil, errors.New("verify: Artifact or RunID is required (Newest cannot enumerate)")
	}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		return nil, errors.New("verify: config has no layout section")
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		return nil, err
	}
	for _, sb := range storages {
		sur := e.surveyStorage(ctx, sb, prefix, e.restoreRetry, nil)
		if !sur.ok {
			continue
		}
		if sr := sur.runs[req.Fetch.RunID]; sr != nil && sr.manifest != nil {
			var names []string
			for _, a := range sr.manifest.Artifacts {
				names = append(names, a.Artifact.Name)
			}
			return names, nil
		}
	}
	return nil, fmt.Errorf("verify: no storage has a manifest for run %s", req.Fetch.RunID)
}

func (e *Engine) verifyOne(ctx context.Context, req VerifyRequest, storages []*storageBinding, artifact string) VerifyResult {
	if req.Depth == "stored" {
		res := e.verifyStored(ctx, req, storages, artifact)
		res.Depth = req.Depth
		return res
	}
	if req.Depth == "decoded" {
		return e.verifyStream(ctx, req, storages, artifact, "", discardProbe)
	}
	// A discovered target is built for this artifact of the walk: it is closed
	// when this verify call returns, error paths included.
	scope := &closerScope{}
	defer func() {
		if err := scope.close(); err != nil {
			e.event(human.Failure("target.close.failed", human.Text("reason", err.Error())))
		}
	}()
	res := VerifyResult{Artifact: artifact, Depth: req.Depth}
	target, typ, err := e.resolveTarget(ctx, req.Target, artifact, scope)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	prober, ok := target.(source.Prober)
	if !ok {
		res.Error = fmt.Sprintf("target type %q has no Prober", typ)
		return res
	}
	return e.verifyStream(ctx, req, storages, artifact, typ,
		func(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
			return safeValue(func() (source.ProbeReport, error) { return prober.Probe(ctx, src) })
		})
}

// verifyStream verifies one artifact without leaving the decoded plaintext on
// the engine's disk: the download is decoded as it streams into consume. The
// walk, the preflight and the verdicts are the download path's own.
func (e *Engine) verifyStream(ctx context.Context, req VerifyRequest, storages []*storageBinding, artifact, typ string, consume streamProbe) VerifyResult {
	res := VerifyResult{Artifact: artifact, Depth: req.Depth}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		res.Error = "config has no layout section"
		return res
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		res.Error = err.Error()
		return res
	}
	surveys := map[string]survey{}
	getSurvey := e.surveyFunc(ctx, prefix, surveys)
	sub := req.Fetch
	sub.Artifact = artifact
	sub.Decode = false
	take := func(ctx context.Context, sb *storageBinding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
		// The manifest's own record is the only thing the target's type may be
		// checked against, and it is checked before the copy's first byte.
		if typ != "" && ar.Source.Type != "" && typ != ar.Source.Type {
			return copyResult{}, nil, fmt.Errorf("the target is type %q, the manifest's source is type %q", typ, ar.Source.Type)
		}
		stages, err := e.decodeStages(ctx, ar.Chain)
		if err != nil {
			return copyResult{}, nil, err
		}
		probe, skip, err := e.downloadStream(ctx, sb, ar, stages, consume)
		return copyResult{ar: *ar, probe: probe}, skip, err
	}
	runID, sb, out, _, err := walkDownload(ctx, e, sub, storages, getSurvey, take)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.RunID = runID
	res.OK = true
	res.Storage = sb.name
	if req.Depth == "probed" {
		res.Bytes = out.probe.Bytes
	} else {
		res.Bytes = out.ar.RawBytes
	}
	// What an apply of this record would need on the restore spool: the stored
	// object plus its decoded copy, before the reserve.
	res.ApplySpoolBytes = out.ar.Bytes + out.ar.RawBytes
	return res
}

// verifyStored checks the object's presence and size on every storage of the
// walk, without downloading it. A stored check runs no fetch, so the newest-ok
// selector is resolved here the same way Fetch resolves it — from the manifests
// — and the candidates are tried newest first: a storage that does not have the
// newest run says so, and a storage that does still verifies.
func (e *Engine) verifyStored(ctx context.Context, req VerifyRequest, storages []*storageBinding, artifact string) VerifyResult {
	res := VerifyResult{Artifact: artifact}
	if e.cfg.Layout.RunPrefix.Text() == "" {
		res.Error = "config has no layout section"
		return res
	}
	prefix, err := runPrefixLiteral(e.cfg.Layout.RunPrefix.Text())
	if err != nil {
		res.Error = err.Error()
		return res
	}
	surveys := map[string]survey{}
	getSurvey := e.surveyFunc(ctx, prefix, surveys)
	selector := req.Fetch
	selector.Artifact = artifact
	candidates, err := e.fetchCandidates(ctx, selector, storages, getSurvey)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	var lastReasons []string
	for _, runID := range candidates {
		res.RunID = runID
		var reasons []string
		for _, sb := range storages {
			sur, ok := getSurvey(sb)
			if !ok {
				reasons = append(reasons, fmt.Sprintf("%s: survey failed", sb.name))
				continue
			}
			ar, reason := inspectRun(sur, runID, artifact)
			if ar == nil {
				reasons = append(reasons, fmt.Sprintf("%s: %s", sb.name, reason))
				continue
			}
			ro := readOnlyStorage{sb.inst}
			st, err := attempt(ctx, e.restoreRetry, "stat "+ar.Artifact.Name, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
				return safeValue(func() (model.ObjectInfo, error) { return ro.Stat(ctx, ar.Key) })
			})
			if err != nil {
				reasons = append(reasons, fmt.Sprintf("%s: %v", sb.name, err))
				continue
			}
			if st.Size != ar.Bytes {
				reasons = append(reasons, fmt.Sprintf("%s: size %d, manifest %d", sb.name, st.Size, ar.Bytes))
				continue
			}
			if st.SHA256 != "" && ar.SHA256 != "" && st.SHA256 != ar.SHA256 {
				reasons = append(reasons, fmt.Sprintf("%s: sha256 mismatch", sb.name))
				continue
			}
			res.OK, res.Storage, res.Bytes = true, sb.name, ar.Bytes
			return res
		}
		lastReasons = reasons
		if req.Fetch.RunID != "" {
			break
		}
	}
	res.Error = strings.Join(lastReasons, "; ")
	return res
}

func (e *Engine) storageByName(name string) *storageBinding {
	for _, sb := range e.storages {
		if sb.name == name {
			return sb
		}
	}
	return nil
}

func (e *Engine) storageNames() []string {
	names := make([]string, 0, len(e.storages))
	for _, sb := range e.storages {
		names = append(names, sb.name)
	}
	return names
}

func (e *Engine) orderStorages(names []string) ([]*storageBinding, error) {
	if len(names) == 0 {
		if e.cfg.Restore == nil {
			return nil, errors.New("restore: config has no restore section and no explicit storages")
		}
		names = e.cfg.Restore.StorageOrder
	}
	out := make([]*storageBinding, 0, len(names))
	for _, name := range names {
		sb := e.storageByName(name)
		if sb == nil {
			return nil, fmt.Errorf("unknown storage %q (known: %v)", name, e.storageNames())
		}
		out = append(out, sb)
	}
	return out, nil
}

// StorageOrder names the storages a fetch walks, in walk order: the request's
// own list when it gave one, else restore.storage_order — the same derivation
// the walk itself uses, exposed so a caller cannot name a different set.
func (e *Engine) StorageOrder(names []string) ([]string, error) {
	sbs, err := e.orderStorages(names)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(sbs))
	for _, sb := range sbs {
		out = append(out, sb.name)
	}
	return out, nil
}
