// Package kubernetesconfigmap is the catalog plugin that keeps the run history
// in one key of one ConfigMap. The object itself is created by the deployment
// manifest, never by this plugin: an absent ConfigMap is an empty history for
// Load and Forget (a first run legitimately finds nothing), and a Save that
// needs it reports not_found — nothing here creates it.
//
// Every method makes one attempt, like every plugin: a stale resourceVersion
// comes back as a transient conflict and the engine's retry repeats the whole
// read-modify-write with a fresh version.
package kubernetesconfigmap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Config is the instance config of the catalog. Every field is required.
type Config struct {
	APIHostEnv     string         `json:"api_host_env" doc:"name of the environment variable holding the API server host"`
	APIPortEnv     string         `json:"api_port_env" doc:"name of the environment variable holding the API server port"`
	TokenFile      string         `json:"token_file" doc:"path of the ServiceAccount token file, re-read on every request"`
	CAFile         string         `json:"ca_file" doc:"path of the CA bundle the API server certificate is verified against"`
	RequestTimeout model.Duration `json:"request_timeout" doc:"timeout of one API request"`
	Namespace      string         `json:"namespace" required:"true" doc:"namespace of the catalog ConfigMap"`
	Name           string         `json:"name" required:"true" doc:"name of the catalog ConfigMap; the deployment manifest creates it"`
	Key            string         `json:"key" doc:"key of the ConfigMap holding the JSON run history"`
	MaxRuns        int            `json:"max_runs" doc:"newest runs kept; older entries are dropped on save"`
	// MaxHistoryBytes caps the JSON written into the key. The API server
	// refuses a ConfigMap object larger than 1 MiB (1048576 bytes, counting
	// every key and the metadata), so a value below that leaves room for the
	// object and turns a rejected write into a named error. max_runs is the
	// primary bound; this is the backstop.
	MaxHistoryBytes model.ByteSize `json:"max_history_bytes" doc:"upper bound of the JSON history written into the key; must be below the 1 MiB (1048576 bytes) ConfigMap object limit"`
	// LeaseKeySuffix is the second key of the ConfigMap the writer lease lives
	// under, appended to Key. A stored format: another process reads the same
	// key, so changing it makes every live claim invisible.
	LeaseKeySuffix string `json:"lease_key_suffix" doc:"suffix of the ConfigMap key the writer lease lives under, appended to key"`
}

// configMapObjectLimit is the API server's hard limit on one ConfigMap object,
// a Kubernetes fact rather than a knob: max_history_bytes must stay under it.
const configMapObjectLimit = 1 << 20

// Register adds the kubernetes_configmap catalog to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, catalog.Kind, plugin.Spec[Config, catalog.Catalog]{
		Type:      "kubernetes_configmap",
		Summary:   "the run history in one key of one ConfigMap",
		Stability: plugin.Stable,
		Example:   []byte(example),
		New:       New,
	})
}

func New(ctx context.Context, cfg Config, deps plugin.Deps) (catalog.Catalog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	log := deps.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Catalog{cfg: cfg, log: log.With("kind", "catalog", "type", "kubernetes_configmap", "instance", deps.Instance)}, nil
}

func (c *Config) Validate() error {
	var errs []error
	if c.APIHostEnv == "" {
		errs = append(errs, fmt.Errorf("/api_host_env is empty"))
	}
	if c.APIPortEnv == "" {
		errs = append(errs, fmt.Errorf("/api_port_env is empty"))
	}
	errs = append(errs, plugin.AbsolutePath(c.TokenFile, "/token_file")...)
	errs = append(errs, plugin.AbsolutePath(c.CAFile, "/ca_file")...)
	if c.RequestTimeout.Std() <= 0 {
		errs = append(errs, fmt.Errorf("/request_timeout must be positive, got %s", c.RequestTimeout))
	}
	if c.Namespace == "" {
		errs = append(errs, fmt.Errorf("/namespace is empty"))
	}
	if c.Name == "" {
		errs = append(errs, fmt.Errorf("/name is empty"))
	}
	if c.Key == "" {
		errs = append(errs, fmt.Errorf("/key is empty"))
	}
	if c.MaxRuns < 1 {
		errs = append(errs, fmt.Errorf("/max_runs must be at least 1, got %d", c.MaxRuns))
	}
	if c.LeaseKeySuffix == "" {
		errs = append(errs, fmt.Errorf("/lease_key_suffix is empty"))
	}
	if c.MaxHistoryBytes.Int64() <= 0 {
		errs = append(errs, fmt.Errorf("/max_history_bytes must be positive, got %d", c.MaxHistoryBytes.Int64()))
	} else if c.MaxHistoryBytes.Int64() >= configMapObjectLimit {
		errs = append(errs, fmt.Errorf("/max_history_bytes %d must stay below the %d-byte ConfigMap object limit", c.MaxHistoryBytes.Int64(), configMapObjectLimit))
	}
	return errors.Join(errs...)
}

type Catalog struct {
	cfg    Config
	log    *slog.Logger
	client *kubeapi.Client
}

// Check proves at preflight that the API answers, that the token and CA are
// usable and that the ConfigMap is readable. An absent ConfigMap passes: the
// first run legitimately finds it missing, and Save reports it if it is still
// missing then.
func (c *Catalog) Check(ctx context.Context) error {
	_, _, err := c.read(ctx)
	return err
}

// Save upserts by run id: a report saved as "running" is replaced by its own
// final outcome, and the history is trimmed to the newest max_runs entries and
// then to the byte bound, oldest first.
func (c *Catalog) Save(ctx context.Context, run model.RunReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, version, err := c.read(ctx)
	if err != nil {
		return err
	}
	if version == "" {
		return plugin.NotFound(fmt.Errorf("configmap %s/%s does not exist; the deployment manifest creates it", c.cfg.Namespace, c.cfg.Name))
	}
	runs = upsert(runs, run)
	sortNewestFirst(runs)
	runs = trimPerKind(runs, c.cfg.MaxRuns)
	c.log.Debug("saving catalog", "run", run.RunID, "outcome", string(run.Outcome), "runs", len(runs))
	return c.write(ctx, runs, version)
}

// Load returns the stored history, newest first by StartedAt.
func (c *Catalog) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runs, _, err := c.read(ctx)
	if err != nil {
		return nil, err
	}
	sortNewestFirst(runs)
	if req.Limit > 0 && len(runs) > req.Limit {
		runs = runs[:req.Limit]
	}
	return runs, nil
}

// Forget removes entries; ids that are not present are not an error, and an
// absent ConfigMap has nothing to forget.
func (c *Catalog) Forget(ctx context.Context, runIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, version, err := c.read(ctx)
	if err != nil {
		return err
	}
	if version == "" {
		return nil
	}
	drop := make(map[string]bool, len(runIDs))
	for _, id := range runIDs {
		drop[id] = true
	}
	kept := make([]model.RunReport, 0, len(runs))
	for _, run := range runs {
		if !drop[run.RunID] {
			kept = append(kept, run)
		}
	}
	// write trims from the end of the slice, so the history it receives is
	// newest first — the order every read returns it in.
	sortNewestFirst(kept)
	return c.write(ctx, kept, version)
}

// The lease lives in a second key of the same ConfigMap, named by the config's
// lease_key_suffix. The same object is deliberate: the deployment's RBAC grants
// get and patch on this one ConfigMap by name, so a separate object would need a
// new grant, and a merge patch leaves the run history's own key untouched. The
// resourceVersion precondition the catalog already uses is the same CAS that
// makes the lease atomic.
//
// There is no clock anywhere in the handover: an existing claim is taken over
// only when the request carries its exact TakeoverToken, which the engine sets
// after Alive proved the holder pod terminated. A claim that changed in the
// meantime does not match and is refused.

// Acquire takes the writer lease in one attempt.
func (c *Catalog) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Lease{}, err
	}
	client, err := c.clientFor()
	if err != nil {
		return catalog.Lease{}, err
	}
	cm, err := client.GetConfigMap(ctx, c.cfg.Namespace, c.cfg.Name)
	if err != nil {
		return catalog.Lease{}, err
	}
	if cm.Metadata.ResourceVersion == "" {
		return catalog.Lease{}, plugin.NotFound(fmt.Errorf("configmap %s/%s does not exist; the deployment manifest creates it", c.cfg.Namespace, c.cfg.Name))
	}
	held, err := parseLease(cm.Data[c.leaseKey()])
	if err != nil {
		return catalog.Lease{}, err
	}
	// A record with no holder is a released claim: free to take, but its
	// quarantine deadline is inherited, so a successor's early exit cannot make
	// a dead holder's in-flight operations look settled.
	free := held == nil || held.Holder == ""
	if !free && (req.TakeoverToken == "" || held.Token != req.TakeoverToken) {
		return *held, fmt.Errorf("held by run %s (%s): %w", held.Holder, held.Kind, catalog.ErrLeaseHeld)
	}
	quarantine := req.QuarantineUntil
	if held != nil && held.QuarantineUntil.After(quarantine) {
		quarantine = held.QuarantineUntil
	}
	lease := catalog.Lease{Holder: req.Holder, Kind: req.Kind, Token: newLeaseToken(),
		HolderNamespace: req.HolderNamespace, HolderPod: req.HolderPod, HolderUID: req.HolderUID,
		AcquiredAt: time.Now().UTC(), QuarantineUntil: quarantine}
	body, err := json.Marshal(lease)
	if err != nil {
		return catalog.Lease{}, plugin.Permanent(fmt.Errorf("encode lease: %w", err))
	}
	// The patch is conditional on the version just read: two runs racing here
	// both see the same claim, one patch wins, and the loser gets a conflict the
	// engine retries — its next read sees the winner's claim.
	if _, err := client.PatchConfigMapData(ctx, c.cfg.Namespace, c.cfg.Name, map[string]string{c.leaseKey(): string(body)}, cm.Metadata.ResourceVersion); err != nil {
		return catalog.Lease{}, err
	}
	return lease, nil
}

// HolderState reports whether the holder pod terminated, and whether the proof
// is the kubelet's own terminal phase. Only a pod found with the recorded UID in
// phase Succeeded or Failed is definitive; an absent pod or a different UID is
// NOT proof that the process stopped — a forced deletion removes the object
// while the process may still run — so it is reported as non-definitive and the
// engine requires the deployment's hard bound before acting on it. A running or
// pending pod, a partial identity and a failed read are all "not terminated".
func (c *Catalog) HolderState(ctx context.Context, lease catalog.Lease) (terminated, definitive bool, err error) {
	if lease.HolderNamespace == "" || lease.HolderPod == "" || lease.HolderUID == "" {
		return false, false, nil // incomplete identity: cannot tell
	}
	client, err := c.clientFor()
	if err != nil {
		return false, false, err
	}
	pod, err := client.GetPod(ctx, lease.HolderNamespace, lease.HolderPod)
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return true, false, nil // absent: needs the hard bound
		}
		return false, false, err
	}
	if pod.Metadata.UID != lease.HolderUID {
		return true, false, nil // the name now belongs to a different pod
	}
	switch pod.Status.Phase {
	case "Succeeded", "Failed":
		return true, true, nil // the kubelet recorded the terminal phase
	}
	return false, false, nil // Running, Pending, or anything else
}

// Release drops the claim if its token is still the holder's; a claim already
// gone or taken over is a no-op.
func (c *Catalog) Release(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	client, err := c.clientFor()
	if err != nil {
		return err
	}
	cm, err := client.GetConfigMap(ctx, c.cfg.Namespace, c.cfg.Name)
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return nil
		}
		return err
	}
	if cm.Metadata.ResourceVersion == "" {
		return nil
	}
	held, err := parseLease(cm.Data[c.leaseKey()])
	if err != nil {
		return err
	}
	if held == nil || held.Token != lease.Token {
		return nil // gone, or taken over by a run that outlived us
	}
	// The claim is dropped, the quarantine is not: it belongs to the record, not
	// to the holder that is leaving.
	cleared := catalog.Lease{QuarantineUntil: held.QuarantineUntil}
	// The claim in hand decoded from this very key, and the cleared record keeps
	// only its quarantine: a value that decoded always encodes again.
	body := must.Do(json.Marshal(cleared))
	_, err = client.PatchConfigMapData(ctx, c.cfg.Namespace, c.cfg.Name, map[string]string{c.leaseKey(): string(body)}, cm.Metadata.ResourceVersion)
	return err
}

// newLeaseToken is the claim's unique acquisition token: Release only drops the
// claim it carries this token for.
func newLeaseToken() string {
	var b [8]byte
	// crypto/rand.Read never returns an error: it crashes the process when the
	// system's random source fails, so there is nothing here to report.
	must.Do(rand.Read(b[:]))
	return hex.EncodeToString(b[:])
}

// leaseKey is the ConfigMap key the writer lease lives under.
func (c *Catalog) leaseKey() string { return c.cfg.Key + c.cfg.LeaseKeySuffix }

// parseLease reads the lease key: "" is no lease at all.
func parseLease(raw string) (*catalog.Lease, error) {
	if raw == "" {
		return nil, nil
	}
	var lease catalog.Lease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, plugin.Permanent(fmt.Errorf("the lease key is not a lease document: %w", err))
	}
	return &lease, nil
}

// trimPerKind keeps each kind's own window: max_runs applies per kind, so a
// flood of manual runs can never evict the scheduled history the nightly health
// is read from. The history arrives newest first and keeps that order.
func trimPerKind(runs []model.RunReport, maxRuns int) []model.RunReport {
	count := map[model.RunKind]int{}
	out := make([]model.RunReport, 0, len(runs))
	for _, r := range runs {
		kind := model.NormalizeKind(r.Kind)
		if count[kind] >= maxRuns {
			continue
		}
		count[kind]++
		out = append(out, r)
	}
	return out
}

// dropIndex is the byte-bound backstop's choice: the oldest manual entry first,
// then the oldest scheduled entry the nightly health does not need. The newest
// scheduled running record and the newest scheduled last-good record are
// protected, and so is the newest entry overall.
func dropIndex(runs []model.RunReport) int {
	protected := protectedScheduled(runs)
	for i := len(runs) - 1; i >= 1; i-- {
		if model.NormalizeKind(runs[i].Kind) == model.RunKindManual {
			return i
		}
	}
	for i := len(runs) - 1; i >= 1; i-- {
		if protected[i] {
			continue
		}
		if model.NormalizeKind(runs[i].Kind) == model.RunKindScheduled {
			return i
		}
	}
	return -1
}

// protectedScheduled names the scheduled entries byte trimming may not drop: the
// newest one that is still running, and the newest one that reported an outcome.
func protectedScheduled(runs []model.RunReport) map[int]bool {
	out := map[int]bool{}
	var running, lastGood bool
	for i, r := range runs { // newest first
		if model.NormalizeKind(r.Kind) != model.RunKindScheduled {
			continue
		}
		switch r.Outcome {
		case model.OutcomeRunning:
			if !running {
				out[i] = true
				running = true
			}
		case model.OutcomeDelivered, model.OutcomeDegraded:
			if !lastGood {
				out[i] = true
				lastGood = true
			}
		}
	}
	return out
}

// read returns the stored history and the ConfigMap's resource version. Absent
// and empty are told apart by the version: a ConfigMap this plugin can write to
// always has one, so an empty version means it is not there.
func (c *Catalog) read(ctx context.Context) ([]model.RunReport, string, error) {
	client, err := c.clientFor()
	if err != nil {
		return nil, "", err
	}
	cm, err := client.GetConfigMap(ctx, c.cfg.Namespace, c.cfg.Name)
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return nil, "", nil
		}
		return nil, "", err
	}
	raw := cm.Data[c.cfg.Key]
	if raw == "" {
		return nil, cm.Metadata.ResourceVersion, nil
	}
	var runs []model.RunReport
	if err := json.Unmarshal([]byte(raw), &runs); err != nil {
		return nil, "", plugin.Permanent(fmt.Errorf("configmap %s/%s key %s is not a JSON run history: %w", c.cfg.Namespace, c.cfg.Name, c.cfg.Key, err))
	}
	return runs, cm.Metadata.ResourceVersion, nil
}

func (c *Catalog) write(ctx context.Context, runs []model.RunReport, version string) error {
	if runs == nil {
		runs = []model.RunReport{}
	}
	// The byte bound is a backstop under max_runs, not a wall: the history is
	// newest first at every call deployment, so an over-bound history drops entries
	// until it fits — refusing instead would fail every later Save on an
	// unchanged, still oversized history. Extra manual entries go first; the
	// newest scheduled running and last-good records are never dropped, and
	// the newest overall entry always stays. What cannot be trimmed away is a
	// named error.
	var raw []byte
	for {
		var err error
		if raw, err = json.Marshal(runs); err != nil {
			return plugin.Permanent(fmt.Errorf("encode run history: %w", err))
		}
		if int64(len(raw)) <= c.cfg.MaxHistoryBytes.Int64() || len(runs) <= 1 {
			break
		}
		idx := dropIndex(runs)
		if idx < 0 {
			break
		}
		runs = slices.Delete(runs, idx, idx+1)
		c.log.Debug("dropped a run to fit max_history_bytes", "runs", len(runs))
	}
	if int64(len(raw)) > c.cfg.MaxHistoryBytes.Int64() {
		return plugin.Permanent(fmt.Errorf("the newest run alone is %d bytes, over the configured %d-byte limit; lower max_runs or max_history_bytes", len(raw), c.cfg.MaxHistoryBytes.Int64()))
	}
	client, err := c.clientFor()
	if err != nil {
		return err
	}
	_, err = client.PatchConfigMapData(ctx, c.cfg.Namespace, c.cfg.Name, map[string]string{c.cfg.Key: string(raw)}, version)
	return err
}

func (c *Catalog) clientFor() (*kubeapi.Client, error) {
	if c.client != nil {
		return c.client, nil
	}
	client, err := kubeapi.New(kubeapi.Options{
		HostEnv:   c.cfg.APIHostEnv,
		PortEnv:   c.cfg.APIPortEnv,
		TokenFile: c.cfg.TokenFile,
		CAFile:    c.cfg.CAFile,
		Timeout:   c.cfg.RequestTimeout.Std(),
	})
	if err != nil {
		// Missing environment, an unreadable CA: config, not a transient fault.
		return nil, plugin.Permanent(err)
	}
	c.client = client
	return c.client, nil
}

func upsert(runs []model.RunReport, run model.RunReport) []model.RunReport {
	for i, existing := range runs {
		if existing.RunID == run.RunID {
			runs[i] = run
			return runs
		}
	}
	return append(runs, run)
}

// sortNewestFirst orders by StartedAt descending; the run id breaks ties so the
// stored order is stable.
func sortNewestFirst(runs []model.RunReport) {
	sort.Slice(runs, func(i, j int) bool {
		if !runs[i].StartedAt.Equal(runs[j].StartedAt) {
			return runs[i].StartedAt.After(runs[j].StartedAt)
		}
		return runs[i].RunID < runs[j].RunID
	})
}

// PreviousJobFailure reads why the previous Job of this run's own CronJob failed
// before this tool could report. It walks the ownership chain the cluster
// records — the caller's pod to its Job, that Job to its CronJob, then the newest
// OTHER Job of the same CronJob — and reads that Job's Failed condition, its
// pods' container statuses and its own Warning events (the only record when no
// pod was ever created). An absent chain returns nil; a read it cannot make is
// returned as an error the engine surfaces and never acts on.
func (c *Catalog) PreviousJobFailure(ctx context.Context, namespace, pod string) (*model.PreviousFailure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, nil // not running as a pod: nothing to inspect
	}
	if namespace == "" {
		namespace = c.cfg.Namespace
	}
	client, err := c.clientFor()
	if err != nil {
		return nil, err
	}
	own, err := client.GetPod(ctx, namespace, pod)
	if err != nil {
		return nil, err
	}
	jobName := ownerName(own.Metadata.OwnerReferences, "Job")
	if jobName == "" {
		return nil, nil
	}
	job, err := client.GetJob(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	cronName := ownerName(job.Metadata.OwnerReferences, "CronJob")
	if cronName == "" {
		return nil, nil
	}
	jobs, err := client.ListJobs(ctx, namespace, kubeapi.ListRequest{PageSize: previousJobPageSize})
	if err != nil {
		return nil, err
	}
	previous := newestJobOf(jobs, cronName, jobName)
	if previous == nil || !jobFailed(*previous) {
		return nil, nil
	}
	pods, err := client.ListPodsIn(ctx, namespace, kubeapi.ListRequest{
		Selector: "job-name=" + previous.Metadata.Name, PageSize: previousJobPageSize,
	})
	if err != nil {
		return nil, err
	}
	failure := previousFailure(*previous, pods)
	// The Job's own Warning events, read whether or not any pod exists: a Pod
	// Security admission refusal (FailedCreate) creates no pod at all, so the
	// event is the only record of it. A read that fails is not fatal — the
	// container statuses already read still stand.
	failure.Events = jobWarningEvents(ctx, client, namespace, previous.Metadata.UID)
	return failure, nil
}

// previousJobPageSize bounds one listing page of the Job/pod walk. It is a fixed
// internal page size, not a deployment knob: the walk reads a handful of objects.
const previousJobPageSize = 100

// previousJobEventLimit caps how many Warning events are kept, newest first: an
// operator reads the first few, and a controller retrying a doomed Job can emit
// hundreds.
const previousJobEventLimit = 5

// jobWarningEvents reads the Warning events recorded against one Job, newest
// first and capped. The API server filters by involvedObject, so the listing
// carries the Job's own events alone. A read that fails returns nil: the events
// are the least certain part of this report and never worth losing the rest.
func jobWarningEvents(ctx context.Context, client *kubeapi.Client, namespace, jobUID string) []model.EventFailure {
	if jobUID == "" {
		return nil
	}
	events, err := client.ListEvents(ctx, namespace, kubeapi.ListRequest{
		FieldSelector: "involvedObject.uid=" + jobUID + ",involvedObject.kind=Job",
		PageSize:      previousJobPageSize,
	})
	if err != nil {
		return nil
	}
	warnings := make([]kubeapi.Event, 0, len(events))
	for _, e := range events {
		if e.Type == "Warning" {
			warnings = append(warnings, e)
		}
	}
	sort.SliceStable(warnings, func(i, j int) bool {
		return eventTime(warnings[i]).After(eventTime(warnings[j]))
	})
	if len(warnings) > previousJobEventLimit {
		warnings = warnings[:previousJobEventLimit]
	}
	out := make([]model.EventFailure, 0, len(warnings))
	for _, e := range warnings {
		out = append(out, model.EventFailure{Reason: e.Reason, Message: e.Message})
	}
	return out
}

// eventTime is when an Event was last seen: its own eventTime, then its
// deprecated lastTimestamp, then the object's creation time. A series writer sets
// one of the first two; an event read back from an old watch may carry neither.
func eventTime(e kubeapi.Event) time.Time {
	for _, s := range []string{e.EventTime, e.LastTimestamp, e.Metadata.CreationTimestamp} {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// ownerName returns the name of the first owner reference of the given kind.
func ownerName(refs []kubeapi.OwnerReference, kind string) string {
	for _, ref := range refs {
		if ref.Kind == kind {
			return ref.Name
		}
	}
	return ""
}

// newestJobOf returns the newest Job of cronName that is not exclude, by creation
// timestamp, or nil when there is none.
func newestJobOf(jobs []kubeapi.Job, cronName, exclude string) *kubeapi.Job {
	var newest *kubeapi.Job
	var newestAt time.Time
	for i := range jobs {
		j := &jobs[i]
		if j.Metadata.Name == exclude || ownerName(j.Metadata.OwnerReferences, "CronJob") != cronName {
			continue
		}
		at, err := time.Parse(time.RFC3339, j.Metadata.CreationTimestamp)
		if err != nil {
			continue
		}
		if newest == nil || at.After(newestAt) {
			newest, newestAt = j, at
		}
	}
	return newest
}

// jobFailed reports whether the Job carries a Failed condition that is true.
func jobFailed(job kubeapi.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == "Failed" && cond.Status == "True" {
			return true
		}
	}
	return false
}

// previousFailure renders the readable failure of one failed Job: its condition,
// and every pod container's terminal or waiting reason.
func previousFailure(job kubeapi.Job, pods []kubeapi.Pod) *model.PreviousFailure {
	out := &model.PreviousFailure{Job: job.Metadata.Name}
	for _, cond := range job.Status.Conditions {
		if cond.Type == "Failed" && cond.Status == "True" {
			out.Reason, out.Message = cond.Reason, cond.Message
		}
	}
	for _, pod := range pods {
		if ownerName(pod.Metadata.OwnerReferences, "Job") != job.Metadata.Name {
			continue
		}
		out.Containers = append(out.Containers, containerFailures(pod, pod.Status.InitContainerStatuses, true)...)
		out.Containers = append(out.Containers, containerFailures(pod, pod.Status.ContainerStatuses, false)...)
	}
	return out
}

// containerFailures renders the containers of one pod that explain its failure:
// one that ran and stopped with a failure, or one that never started. A
// container that completed cleanly is not an explanation and is left out.
func containerFailures(pod kubeapi.Pod, statuses []kubeapi.ContainerStatus, init bool) []model.ContainerFailure {
	var out []model.ContainerFailure
	for _, cs := range statuses {
		switch {
		case cs.State.Terminated != nil:
			t := cs.State.Terminated
			if t.ExitCode == 0 && t.Reason == "Completed" {
				continue
			}
			out = append(out, model.ContainerFailure{Pod: pod.Metadata.Name, Container: cs.Name, Init: init,
				Terminated: &model.ContainerTermination{Reason: t.Reason, ExitCode: t.ExitCode, Message: t.Message}})
		case cs.State.Waiting != nil:
			out = append(out, model.ContainerFailure{Pod: pod.Metadata.Name, Container: cs.Name, Init: init,
				Waiting: cs.State.Waiting.Reason})
		}
	}
	return out
}

const example = `{
  "api_host_env": "KUBERNETES_SERVICE_HOST",
  "api_port_env": "KUBERNETES_SERVICE_PORT",
  "token_file": "/var/run/secrets/kubernetes.io/serviceaccount/token",
  "ca_file": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
  "request_timeout": "30s",
  "namespace": "backup",
  "name": "cluster-backup-catalog",
  "key": "catalog.json",
  "lease_key_suffix": ".lease",
  "max_runs": 60,
  "max_history_bytes": 524288
}`
