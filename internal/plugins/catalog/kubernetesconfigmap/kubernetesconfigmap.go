// Package kubernetesconfigmap is the catalog plugin that keeps the writer lease
// in one key of one ConfigMap. The object itself is created by the deployment
// manifest, never by this plugin: Acquire, MarkUncertainty and ClearUncertainty
// against an absent ConfigMap report not_found, and Release is a no-op because
// no claim can exist without the object. Nothing else is stored: what earlier
// runs did is read from the storages themselves.
//
// Every method makes one attempt, like every plugin: a stale resourceVersion
// comes back as a transient conflict and execution's retry repeats the whole
// read-modify-write with a fresh version.
package kubernetesconfigmap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
)

// Config is the instance config of the catalog.
type Config struct {
	APIHostEnv     string         `json:"api_host_env" doc:"name of the environment variable holding the API server host"`
	APIPortEnv     string         `json:"api_port_env" doc:"name of the environment variable holding the API server port"`
	TokenFile      string         `json:"token_file" doc:"path of the ServiceAccount token file, re-read on every request"`
	CAFile         string         `json:"ca_file" doc:"path of the CA bundle the API server certificate is verified against"`
	RequestTimeout model.Duration `json:"request_timeout" doc:"timeout of one API request"`
	Namespace      string         `json:"namespace" required:"true" doc:"namespace of the catalog ConfigMap"`
	Name           string         `json:"name" required:"true" doc:"name of the catalog ConfigMap; the deployment manifest creates it"`
	// LeaseKey is a stored format: another process reads the same key, so
	// changing it makes every live claim invisible.
	LeaseKey string `json:"lease_key" doc:"key of the ConfigMap the writer lease JSON record lives under"`
}

func New(ctx context.Context, cfg Config, _ instances.Deps) (catalog.Catalog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Catalog{cfg: cfg}, nil
}

func (c *Config) Validate() error {
	var errs []error
	if c.APIHostEnv == "" {
		errs = append(errs, fmt.Errorf("/api_host_env is empty"))
	}
	if c.APIPortEnv == "" {
		errs = append(errs, fmt.Errorf("/api_port_env is empty"))
	}
	errs = append(errs, configuration.AbsolutePath(c.TokenFile, "/token_file")...)
	errs = append(errs, configuration.AbsolutePath(c.CAFile, "/ca_file")...)
	if c.RequestTimeout.Std() <= 0 {
		errs = append(errs, fmt.Errorf("/request_timeout must be positive, got %s", c.RequestTimeout))
	}
	if c.Namespace == "" {
		errs = append(errs, fmt.Errorf("/namespace is empty"))
	}
	if c.Name == "" {
		errs = append(errs, fmt.Errorf("/name is empty"))
	}
	if c.LeaseKey == "" {
		errs = append(errs, fmt.Errorf("/lease_key is empty"))
	}
	return errors.Join(errs...)
}

type Catalog struct {
	cfg    Config
	client *kubeapi.Client
}

// Check proves at preflight that the API answers, that the token and CA are
// usable and that the ConfigMap is readable. An absent ConfigMap passes: the
// deployment manifest creates it before the first run.
func (c *Catalog) Check(ctx context.Context) error {
	_, err := c.read(ctx)
	if err == nil {
		return nil
	}
	if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
		return nil
	}
	return err
}

// Acquire takes the writer lease in one attempt. A record with no holder is a
// released claim: free to take, and the uncertainty it carries is inherited
// unchanged. An existing claim is taken over only by a request carrying its
// exact token; when the record carries no uncertainty, the takeover arms one for
// the holder it replaces, whose provider operations may still be in flight.
func (c *Catalog) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Lease{}, err
	}
	cm, err := c.read(ctx)
	if err != nil {
		return catalog.Lease{}, err
	}
	held, err := parseLease(cm.Data[c.cfg.LeaseKey])
	if err != nil {
		return catalog.Lease{}, err
	}
	free := held == nil || held.Holder == ""
	if !free && (req.TakeoverToken == "" || held.Token != req.TakeoverToken) {
		return *held, fmt.Errorf("held by run %s (%s): %w", held.Holder, held.Kind, catalog.ErrLeaseHeld)
	}
	var uncertainty *catalog.Uncertainty
	if held != nil {
		uncertainty = held.Uncertainty
	}
	if !free && uncertainty == nil {
		uncertainty = &catalog.Uncertainty{RunIDs: []string{held.Holder}, Since: time.Now().UTC()}
	}
	lease := catalog.Lease{Holder: req.Holder, Kind: req.Kind, Token: newLeaseToken(),
		HolderNamespace: req.HolderNamespace, HolderPod: req.HolderPod, HolderUID: req.HolderUID,
		AcquiredAt: time.Now().UTC(), Uncertainty: uncertainty}
	body, err := json.Marshal(lease)
	if err != nil {
		// The identity strings come from the caller; one that is not valid
		// UTF-8 cannot be recorded and must not be claimed as taken.
		return catalog.Lease{}, pluginerrors.Permanent(fmt.Errorf("encode lease: %w", err))
	}
	// The patch is conditional on the version just read: two runs racing here
	// both see the same claim, one patch wins, and the loser gets a conflict the
	// execution retries — its next read sees the winner's claim.
	if err := c.patch(ctx, cm.Metadata.ResourceVersion, body); err != nil {
		return catalog.Lease{}, err
	}
	return lease, nil
}

// MarkUncertainty persists the uncertainty the caller carries, atomically with
// the claim's compare-and-swap: the caller decides the pending-run set, so it
// keeps every inherited run id and adds this holder's before the first
// asynchronous write. A lease handed without an uncertainty changes nothing —
// marking never clears a record — and a claim already released or taken over
// changes nothing either.
func (c *Catalog) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cm, err := c.read(ctx)
	if err != nil {
		return err
	}
	held, err := parseLease(cm.Data[c.cfg.LeaseKey])
	if err != nil {
		return err
	}
	if held == nil || held.Holder == "" || held.Token != lease.Token || lease.Uncertainty == nil {
		return nil // gone, taken over, or nothing to record
	}
	held.Uncertainty = lease.Uncertainty
	body, err := json.Marshal(held)
	if err != nil {
		// The run ids come from the caller; one that is not valid UTF-8 cannot
		// be recorded and must not be reported as marked.
		return pluginerrors.Permanent(fmt.Errorf("encode lease: %w", err))
	}
	return c.patch(ctx, cm.Metadata.ResourceVersion, body)
}

// ClearUncertainty removes the record's uncertainty while the claim is still
// the caller's, after a terminal proof and a fresh survey established that
// occupancy is known. A claim already released or taken over changes nothing.
func (c *Catalog) ClearUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cm, err := c.read(ctx)
	if err != nil {
		return err
	}
	held, err := parseLease(cm.Data[c.cfg.LeaseKey])
	if err != nil {
		return err
	}
	if held == nil || held.Holder == "" || held.Token != lease.Token {
		return nil // gone, or taken over by a run that outlived us
	}
	if held.Uncertainty == nil {
		return nil // already known
	}
	held.Uncertainty = nil
	return c.patch(ctx, cm.Metadata.ResourceVersion, must.Do(json.Marshal(held)))
}

// ObserveUncertainty records a full survey's inventory digest in the record's
// existing uncertainty, keeping its run and time. A record with no uncertainty,
// or a claim gone or taken over, changes nothing: a cleared uncertainty is never
// given a digest back.
func (c *Catalog) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cm, err := c.read(ctx)
	if err != nil {
		return err
	}
	held, err := parseLease(cm.Data[c.cfg.LeaseKey])
	if err != nil {
		return err
	}
	if held == nil || held.Holder == "" || held.Token != lease.Token || held.Uncertainty == nil {
		return nil
	}
	held.Uncertainty.Digest = digest
	body, err := json.Marshal(held)
	if err != nil {
		// The digest comes from the caller; one that is not valid UTF-8 cannot
		// be recorded and must not be reported as observed.
		return pluginerrors.Permanent(fmt.Errorf("encode lease: %w", err))
	}
	return c.patch(ctx, cm.Metadata.ResourceVersion, body)
}

// Release drops the claim if its token is still the holder's; a claim already
// gone or taken over is a no-op. The record's uncertainty is written back
// exactly as it stands: an uncertainty this run armed survives, and a
// successor's is never cleared.
func (c *Catalog) Release(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cm, err := c.read(ctx)
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			return nil // no object, no claim to drop
		}
		return err
	}
	held, err := parseLease(cm.Data[c.cfg.LeaseKey])
	if err != nil {
		return err
	}
	if held == nil || held.Holder == "" || held.Token != lease.Token {
		return nil
	}
	// The claim is dropped, the uncertainty is not: it belongs to the record,
	// not to the holder that is leaving. Only the record's own uncertainty is
	// carried over — a value that decoded always encodes again.
	cleared := catalog.Lease{Uncertainty: held.Uncertainty}
	return c.patch(ctx, cm.Metadata.ResourceVersion, must.Do(json.Marshal(cleared)))
}

// HolderState reports whether the holder pod terminated, and whether the proof
// is the kubelet's own terminal phase. Only a pod found with the recorded UID in
// phase Succeeded or Failed is definitive; an absent pod or a different UID is
// NOT proof that the process stopped — a forced deletion removes the object
// while the process may still run — so it is reported as non-definitive and the
// the protection lease requires the deployment's hard bound before acting on it. A running or
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
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
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

// read loads the ConfigMap carrying the lease record. An absent object, or one
// the API server answered without a version, is reported as not_found: nothing
// here creates it, and an unconditional patch is not a compare-and-swap.
func (c *Catalog) read(ctx context.Context) (kubeapi.ConfigMap, error) {
	client, err := c.clientFor()
	if err != nil {
		return kubeapi.ConfigMap{}, err
	}
	cm, err := client.GetConfigMap(ctx, c.cfg.Namespace, c.cfg.Name)
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			return kubeapi.ConfigMap{}, pluginerrors.NotFound(fmt.Errorf("configmap %s/%s does not exist; the deployment manifest creates it", c.cfg.Namespace, c.cfg.Name))
		}
		return kubeapi.ConfigMap{}, err
	}
	if cm.Metadata.ResourceVersion == "" {
		return kubeapi.ConfigMap{}, pluginerrors.NotFound(fmt.Errorf("configmap %s/%s answered without a resourceVersion; it is not a claim to write to", c.cfg.Namespace, c.cfg.Name))
	}
	return cm, nil
}

// patch writes the lease record under the configured key, conditional on the
// version just read. It is called only after read succeeded, so the client is
// built.
func (c *Catalog) patch(ctx context.Context, version string, body []byte) error {
	_, err := c.client.PatchConfigMapData(ctx, c.cfg.Namespace, c.cfg.Name, map[string]string{c.cfg.LeaseKey: string(body)}, version)
	return err
}

// newLeaseToken is the claim's unique acquisition token: Release,
// MarkUncertainty and ClearUncertainty only act on the claim they carry this
// token for.
func newLeaseToken() string {
	var b [8]byte
	// crypto/rand.Read never returns an error: it crashes the process when the
	// system's random source fails, so there is nothing here to report.
	must.Do(rand.Read(b[:]))
	return hex.EncodeToString(b[:])
}

// parseLease reads the lease key: "" is no lease at all. A record of any other
// shape — an unknown member, a missing field, an old quarantine — is refused
// rather than read as a different claim.
func parseLease(raw string) (*catalog.Lease, error) {
	if raw == "" {
		return nil, nil
	}
	var lease catalog.Lease
	if err := json.Unmarshal([]byte(raw), &lease, json.RejectUnknownMembers(true)); err != nil {
		return nil, pluginerrors.Permanent(fmt.Errorf("the lease key is not a lease document: %w", err))
	}
	return &lease, nil
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
		return nil, pluginerrors.Permanent(err)
	}
	c.client = client
	return c.client, nil
}

const example = `{
  "api_host_env": "KUBERNETES_SERVICE_HOST",
  "api_port_env": "KUBERNETES_SERVICE_PORT",
  "token_file": "/var/run/secrets/kubernetes.io/serviceaccount/token",
  "ca_file": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
  "request_timeout": "30s",
  "namespace": "backup",
  "name": "cluster-backup-catalog",
  "lease_key": "lease.json"
}`
