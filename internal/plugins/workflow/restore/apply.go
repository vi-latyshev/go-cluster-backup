package restore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
)

// Target is the source an apply or a probe writes into: the instance's type as
// the configuration declared it, and the instance itself.
type Target struct {
	Type   string
	Source source.Source
}

// Targets resolves the source an apply or a probed verification writes into: a
// configured instance by name, or the discovered instance that holds the
// artifact. The assembly owns construction; the workflow owns the checks. A
// non-nil closer releases whatever the resolution built and is closed when the
// call returns.
type Targets interface {
	ResolveTarget(ctx context.Context, name, artifact string) (Target, io.Closer, error)
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

// Apply fetches an artifact with decode, verifies it entirely, and hands the
// decoded bytes to the target source's Restorer. One attempt, no retry: an apply
// may be destructive. Everything an apply can refuse without the artifact — the
// target, its type, the confirmation, a foreign origin, a target that cannot
// restore — is refused before a byte is downloaded.
func (w *Workflow) Apply(ctx context.Context, req ApplyRunRequest) (source.ApplyReport, error) {
	if req.Mode != source.ApplySideBySide && req.Mode != source.ApplyInPlace {
		return source.ApplyReport{}, errors.New("apply: Mode is required and must be side_by_side or in_place")
	}
	if req.Fetch.RawStorage != "" || req.Fetch.RawKey != "" {
		return source.ApplyReport{}, errors.New("apply: a raw by-key fetch cannot be applied; it carries no chain, source type or origin")
	}
	plan, err := w.ResolveFetch(ctx, req.Fetch)
	if err != nil {
		return source.ApplyReport{}, err
	}
	target, typ, closer, err := w.resolveTarget(ctx, req.Target, plan.Artifact)
	if err != nil {
		return source.ApplyReport{}, err
	}
	if closer != nil {
		defer func() {
			if err := closer.Close(); err != nil {
				w.event(human.Failure("target.close.failed", human.Text("reason", err.Error())))
			}
		}()
	}
	traits, err := execution.Attempt(ctx, w.o.Policy, "describe target "+req.Target, func(ctx context.Context, _ int) (source.SourceTraits, error) {
		return execution.SafeValue(func() (source.SourceTraits, error) { return target.Describe(ctx) })
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
	rep, ar, file, sp, err := w.fetchToFile(ctx, req.Fetch, plan.RawBytes)
	if err != nil {
		return source.ApplyReport{}, fmt.Errorf("apply: the validated run %s is not restorable: %w", plan.RunID, err)
	}
	defer func() { _ = sp.Remove(file) }()
	// The decode happens after the copy verified, into its own guarded spool
	// file: the restorer and the in-place probe both need a re-readable stream.
	decoded, err := w.decodeSpool(ctx, sp, file, ar)
	if err != nil {
		return source.ApplyReport{}, err
	}
	defer func() { _ = sp.Remove(decoded) }()
	src, err := spoolOpen(sp, decoded)
	if err != nil {
		return source.ApplyReport{}, err
	}
	defer func() { _ = src.Close() }()
	if req.Mode == source.ApplyInPlace {
		if _, err := execution.SafeValue(func() (source.ProbeReport, error) { return prober.Probe(ctx, src) }); err != nil {
			return source.ApplyReport{}, fmt.Errorf("apply: probe before an in-place write failed: %w", err)
		}
		if _, err := fileSeek(src, 0, io.SeekStart); err != nil {
			return source.ApplyReport{}, err
		}
	}
	return execution.SafeValue(func() (source.ApplyReport, error) {
		return restorer.Apply(ctx, source.ApplyRequest{Mode: req.Mode, Label: rep.RunID, Options: w.o.ApplyOptions[typ]}, src)
	})
}

// resolveTarget asks the assembly for the source an apply or a probe writes to.
func (w *Workflow) resolveTarget(ctx context.Context, name, artifact string) (source.Source, string, io.Closer, error) {
	if name == "" {
		return nil, "", nil, errors.New("apply: Target is required")
	}
	if w.o.Targets == nil {
		return nil, "", nil, errors.New("apply: this config declares no targets")
	}
	t, closer, err := w.o.Targets.ResolveTarget(ctx, name, artifact)
	if err != nil {
		return nil, "", nil, err
	}
	return t.Source, t.Type, closer, nil
}

// decodeSpool decodes a verified spool file into a second spool file the probe
// or the restorer reads. The second file is written under the reserve too: its
// size is the manifest's raw size, which a staged artifact can understate.
func (w *Workflow) decodeSpool(ctx context.Context, sp *spool.Spool, file string, ar model.ArtifactResult) (string, error) {
	stages, err := w.decodeStages(ctx, ar.Chain)
	if err != nil {
		return "", err
	}
	dw, err := spoolCreate(sp, "decoded")
	if err != nil {
		return "", err
	}
	name := dw.Name()
	fail := func(err error) (string, error) {
		_ = sp.Remove(name)
		return "", err
	}
	g := &spaceGuard{w: dw, sp: sp, reserve: w.o.Reserve}
	src, err := spoolOpen(sp, file)
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

// Verify walks the requested depth for one artifact or every artifact of a run.
func (w *Workflow) Verify(ctx context.Context, req VerifyRequest) ([]VerifyResult, error) {
	switch req.Depth {
	case "stored", "decoded", "probed":
	default:
		return nil, fmt.Errorf("verify: Depth must be stored, decoded or probed, got %q", req.Depth)
	}
	if req.Depth == "probed" && req.Target == "" {
		return nil, errors.New("verify: Depth probed needs a Target")
	}
	storages, err := w.orderStorages(req.Fetch.Storages)
	if err != nil {
		return nil, err
	}
	artifacts, err := w.verifyArtifacts(ctx, req, storages)
	if err != nil {
		return nil, err
	}
	var out []VerifyResult
	for _, name := range artifacts {
		out = append(out, w.verifyOne(ctx, req, storages, name))
	}
	return out, nil
}

func (w *Workflow) verifyArtifacts(ctx context.Context, req VerifyRequest, storages []*binding) ([]string, error) {
	if req.Fetch.Artifact != "" {
		return []string{req.Fetch.Artifact}, nil
	}
	if req.Fetch.RunID == "" {
		return nil, errors.New("verify: Artifact or RunID is required (Newest cannot enumerate)")
	}
	prefix, err := w.prefix("verify")
	if err != nil {
		return nil, err
	}
	for _, sb := range storages {
		sur := w.inv.SurveyStorage(ctx, prefix, w.surveyStorage(sb))
		if !sur.OK {
			continue
		}
		if sr, ok := sur.Runs[req.Fetch.RunID]; ok && sr.Manifest != nil {
			var names []string
			for _, a := range sr.Manifest.Artifacts {
				names = append(names, a.Artifact.Name)
			}
			return names, nil
		}
	}
	return nil, fmt.Errorf("verify: no storage has a manifest for run %s", req.Fetch.RunID)
}

func (w *Workflow) verifyOne(ctx context.Context, req VerifyRequest, storages []*binding, artifact string) VerifyResult {
	if req.Depth == "stored" {
		res := w.verifyStored(ctx, req, storages, artifact)
		res.Depth = req.Depth
		return res
	}
	if req.Depth == "decoded" {
		return w.verifyStream(ctx, req, storages, artifact, "", discardProbe)
	}
	res := VerifyResult{Artifact: artifact, Depth: req.Depth}
	target, typ, closer, err := w.resolveTarget(ctx, req.Target, artifact)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if closer != nil {
		defer func() {
			if err := closer.Close(); err != nil {
				w.event(human.Failure("target.close.failed", human.Text("reason", err.Error())))
			}
		}()
	}
	prober, ok := target.(source.Prober)
	if !ok {
		res.Error = fmt.Sprintf("target type %q has no Prober", typ)
		return res
	}
	return w.verifyStream(ctx, req, storages, artifact, typ,
		func(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
			return execution.SafeValue(func() (source.ProbeReport, error) { return prober.Probe(ctx, src) })
		})
}

// verifyStream verifies one artifact without leaving the decoded plaintext on
// the workflow's disk: the download is decoded as it streams into consume. The
// walk, the preflight and the verdicts are the download path's own.
func (w *Workflow) verifyStream(ctx context.Context, req VerifyRequest, storages []*binding, artifact, typ string, consume streamProbe) VerifyResult {
	res := VerifyResult{Artifact: artifact, Depth: req.Depth}
	prefix, err := w.prefix("verify")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	surveys := map[string]inventory.Survey{}
	getSurvey := w.surveyFunc(ctx, prefix, surveys)
	sub := req.Fetch
	sub.Artifact = artifact
	sub.Decode = false
	take := func(ctx context.Context, sb *binding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
		// The manifest's own record is the only thing the target's type may be
		// checked against, and it is checked before the copy's first byte.
		if typ != "" && ar.Source.Type != "" && typ != ar.Source.Type {
			return copyResult{}, nil, fmt.Errorf("the target is type %q, the manifest's source is type %q", typ, ar.Source.Type)
		}
		stages, err := w.decodeStages(ctx, ar.Chain)
		if err != nil {
			return copyResult{}, nil, err
		}
		probe, skip, err := w.downloadStream(ctx, sb, ar, stages, consume)
		return copyResult{ar: *ar, probe: probe}, skip, err
	}
	runID, sb, out, _, err := walkDownload(ctx, w, sub, storages, getSurvey, take)
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
func (w *Workflow) verifyStored(ctx context.Context, req VerifyRequest, storages []*binding, artifact string) VerifyResult {
	res := VerifyResult{Artifact: artifact}
	prefix, err := w.prefix("verify")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	surveys := map[string]inventory.Survey{}
	getSurvey := w.surveyFunc(ctx, prefix, surveys)
	selector := req.Fetch
	selector.Artifact = artifact
	candidates, err := w.fetchCandidates(ctx, selector, storages, getSurvey)
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
			st, err := execution.Attempt(ctx, w.o.Policy, "stat "+ar.Artifact.Name, func(ctx context.Context, _ int) (model.ObjectInfo, error) {
				return execution.SafeValue(func() (model.ObjectInfo, error) { return sb.port.Stat(ctx, ar.Key) })
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
