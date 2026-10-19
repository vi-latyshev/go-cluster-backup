// Package kubeapi is the minimal in-cluster Kubernetes REST client the backup
// plugins share: the host and port come from environment variables the config
// names, the CA bundle from a file the config names, and the ServiceAccount
// token is re-read from its file on every request — a projected token is
// rotated by kubelet underneath the process. The standard library only: the
// client-go dependency tree buys this service nothing.
//
// Every method makes one attempt, like every plugin, and classifies its error
// with the framework's markers: 404 is not_found, 401/403 and other 4xx are
// permanent, 5xx, timeouts and transport failures are transient, and a
// cancelled context is returned as itself.
package kubeapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// Options is what a plugin config carries for the connection; every field is
// required, and every error about a missing or unusable value names it.
type Options struct {
	// HostEnv and PortEnv are the NAMES of the environment variables holding
	// the API server's address, not the address itself.
	HostEnv   string
	PortEnv   string
	TokenFile string
	CAFile    string
	Timeout   time.Duration
}

// Client talks to the API server over HTTPS with the ServiceAccount token.
type Client struct {
	base      string
	tokenFile string
	http      *http.Client
}

// New reads the two environment variables and the CA bundle and returns a
// client; the token file is only opened when a request needs it, so a rotated
// token never goes stale. The errors are construction errors: the caller
// classifies them, they are config or environment problems, not retryable ones.
func New(opts Options) (*Client, error) {
	switch {
	case opts.HostEnv == "":
		return nil, fmt.Errorf("host environment variable name is empty")
	case opts.PortEnv == "":
		return nil, fmt.Errorf("port environment variable name is empty")
	case opts.TokenFile == "":
		return nil, fmt.Errorf("token file is empty")
	case opts.CAFile == "":
		return nil, fmt.Errorf("CA file is empty")
	case opts.Timeout <= 0:
		return nil, fmt.Errorf("timeout must be positive, got %s", opts.Timeout)
	}
	host := os.Getenv(opts.HostEnv)
	if host == "" {
		return nil, fmt.Errorf("environment variable %s is empty or unset", opts.HostEnv)
	}
	port := os.Getenv(opts.PortEnv)
	if port == "" {
		return nil, fmt.Errorf("environment variable %s is empty or unset", opts.PortEnv)
	}
	caPEM, err := os.ReadFile(opts.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %s holds no certificate", opts.CAFile)
	}
	return &Client{
		base:      "https://" + net.JoinHostPort(host, port),
		tokenFile: opts.TokenFile,
		http: &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			}},
			Timeout: opts.Timeout,
		},
	}, nil
}

// ObjectMeta is the metadata subset every object here needs.
type ObjectMeta struct {
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	Namespace       string            `json:"namespace"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
	// CreationTimestamp is the API server's RFC3339 creation time, used to order
	// the Jobs of one CronJob.
	CreationTimestamp string `json:"creationTimestamp"`
	// OwnerReferences is what ties a pod to its Job and a Job to its CronJob.
	OwnerReferences []OwnerReference `json:"ownerReferences"`
}

// OwnerReference names the object that owns this one: the kind and name are what
// a walk from a pod to its Job to its CronJob follows.
type OwnerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// EnvVar is one container environment variable. Value is a pointer so an
// explicit empty literal is told apart from an absent one; ValueFrom is kept as
// raw members so an unsupported source is reported by name instead of being
// dropped silently.
type EnvVar struct {
	Name      string                    `json:"name"`
	Value     *string                   `json:"value"`
	ValueFrom map[string]jsontext.Value `json:"valueFrom"`
}

// KeySelector is one secretKeyRef or configMapKeyRef.
type KeySelector struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type ContainerPort struct {
	Name          string `json:"name"`
	ContainerPort int    `json:"containerPort"`
}

// Container is the container subset the discovery rules read: identity, env and
// declared ports. EnvFrom is kept only so a lookup that misses the variable can
// say that whole-config imports exist and were not resolved.
type Container struct {
	Name    string           `json:"name"`
	Env     []EnvVar         `json:"env"`
	EnvFrom []jsontext.Value `json:"envFrom"`
	Ports   []ContainerPort  `json:"ports"`
}

// Volume is one volume of a pod spec, as far as the discovery rules need it:
// the name the pod knows it by and the claim it comes from. A volume carrying
// no claim — a configMap, a secret, an emptyDir — has a nil claim.
type Volume struct {
	Name                  string `json:"name"`
	PersistentVolumeClaim *struct {
		ClaimName string `json:"claimName"`
	} `json:"persistentVolumeClaim"`
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		Containers []Container `json:"containers"`
		Volumes    []Volume    `json:"volumes"`
	} `json:"spec"`
	Status struct {
		Phase                 string            `json:"phase"`
		Reason                string            `json:"reason"`
		Message               string            `json:"message"`
		InitContainerStatuses []ContainerStatus `json:"initContainerStatuses"`
		ContainerStatuses     []ContainerStatus `json:"containerStatuses"`
	} `json:"status"`
}

// ContainerStatus is one container's runtime state, as far as reading why a
// previous pod failed needs it: the last termination and any waiting reason.
type ContainerStatus struct {
	Name  string         `json:"name"`
	State ContainerState `json:"state"`
}

// ContainerState carries exactly one of the three a container can be in.
type ContainerState struct {
	Terminated *ContainerTerminated `json:"terminated"`
	Waiting    *ContainerWaiting    `json:"waiting"`
	Running    *struct{}            `json:"running"`
}

// ContainerTerminated is a container that ran and stopped: its exit code, the
// reason the kubelet recorded (OOMKilled, Error, Completed) and, when the pod
// sets terminationMessagePolicy FallbackToLogsOnError, the last log lines.
type ContainerTerminated struct {
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Signal   int    `json:"signal"`
}

// ContainerWaiting is a container that never started: the reason the kubelet
// recorded (ImagePullBackOff, CreateContainerConfigError) and its message.
type ContainerWaiting struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Job is a batch/v1 Job, as far as reading why a previous run failed needs it:
// its owner (the CronJob) and its conditions.
type Job struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   struct {
		Conditions []JobCondition `json:"conditions"`
	} `json:"status"`
}

// JobCondition is one condition of a Job: Complete, Failed or Suspended. Failed
// carries the reason a run never got to report (BackoffLimitExceeded,
// DeadlineExceeded, FailedCreate).
type JobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Event is a core/v1 Event, as far as reading why pod creation was rejected
// needs it: a Warning the Job controller recorded (FailedCreate for a Pod
// Security admission refusal) carries the reason and the message an operator
// needs, and it is the ONLY record when no Pod was ever created.
type Event struct {
	Metadata ObjectMeta `json:"metadata"`
	// Type is Normal or Warning.
	Type    string `json:"type"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Count   int    `json:"count"`
	// InvolvedObject names the object the event is about — the Job, for a
	// FailedCreate.
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"involvedObject"`
	// EventTime (micro) and LastTimestamp order one event's series; either may be
	// absent depending on the writer.
	EventTime     string `json:"eventTime"`
	LastTimestamp string `json:"lastTimestamp"`
}

// PVC is a persistent volume claim. Phase is the API server's own vocabulary —
// Pending, Bound, Lost — which no rule chooses: a Lost claim has lost its
// binding and with it its data.
type PVC struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		// VolumeName is the PersistentVolume the claim is bound to; empty while
		// the claim is Pending, because the volume is provisioned on first use.
		VolumeName string `json:"volumeName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// PV is a persistent volume: where a claim's data actually lives, which is how
// a rule turns a claim into the path the data is read from.
type PV struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     struct {
		// Local is a node-local volume: Path is a directory of that node's own
		// filesystem, on exactly one node.
		Local *struct {
			Path string `json:"path"`
		} `json:"local"`
		// HostPath is a directory of the node a pod runs on.
		HostPath *struct {
			Path string `json:"path"`
		} `json:"hostPath"`
	} `json:"spec"`
}

// Secret carries the data map exactly as the API returns it: values are
// base64-encoded, and Value decodes one entry.
type Secret struct {
	Metadata ObjectMeta        `json:"metadata"`
	Data     map[string]string `json:"data"`
}

// Value decodes one data entry; found is false when the key is absent.
func (s Secret) Value(key string) (value string, found bool, err error) {
	raw, ok := s.Data[key]
	if !ok {
		return "", false, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", true, fmt.Errorf("secret %s/%s: key %q is not valid base64: %w", s.Metadata.Namespace, s.Metadata.Name, key, err)
	}
	return string(decoded), true, nil
}

// ConfigMap is the subset the catalog plugin reads and writes. BinaryData is
// kept so a lookup that misses the key can report which unsupported store the
// value lives in, if it lives there.
type ConfigMap struct {
	Metadata   ObjectMeta        `json:"metadata"`
	Data       map[string]string `json:"data"`
	BinaryData map[string]string `json:"binaryData"`
}

// ListRequest is one listing: what to select and how many objects one page may
// carry. PageSize is required — the caller's config carries it — and a value
// below 1 is a configuration error, never a silent default. Selector is the
// label selector, "" selects everything.
//
// Phase is the required status.phase, "" does not filter, and only a pod list
// can honour it: the API server registers status.phase as a field selector for
// pods, and answers HTTP 400 for it on any other resource. A volume listing
// therefore carries every phase, and a request that asks for one is refused
// here rather than answered with a list the caller would misread (phaseRefused).
//
// FieldSelector is a raw field selector for the resources whose own fields a
// caller filters on — an Event's involvedObject.uid, for example. It is combined
// with Phase's selector when both are set.
type ListRequest struct {
	Selector      string
	Phase         string
	FieldSelector string
	PageSize      int
}

type listMeta struct {
	Continue string `json:"continue"`
}

type list[T any] struct {
	Metadata listMeta `json:"metadata"`
	Items    []T      `json:"items"`
}

// ListPods lists pods across all namespaces with the label selector and a
// status.phase field selector — the API server supports status.phase for pods
// — following the continue token to the end.
func (c *Client) ListPods(ctx context.Context, req ListRequest) ([]Pod, error) {
	return listAll[Pod](ctx, c, "/api/v1/pods", req, combineFieldSelector(phaseSelector(req.Phase), req.FieldSelector))
}

// ListPVCs lists persistent volume claims across all namespaces with the label
// selector, following the continue token to the end. Every phase comes back:
// the API server registers status.phase as a field selector for pods, not for
// claims, and answers HTTP 400 for it on this resource ("status.phase" is not a
// known field selector; only "metadata.name", "metadata.namespace" — confirmed
// against the live server), so no field selector is ever sent and no phase is
// dropped here: a claim in Pending has no volume yet, one in Lost has lost its
// binding, and the caller decides what each of them means.
func (c *Client) ListPVCs(ctx context.Context, req ListRequest) ([]PVC, error) {
	if err := phaseRefused("/api/v1/persistentvolumeclaims", req.Phase); err != nil {
		return nil, err
	}
	return listAll[PVC](ctx, c, "/api/v1/persistentvolumeclaims", req, "")
}

// ListPVs lists persistent volumes — a cluster-scoped resource, so no namespace
// is involved — with the label selector, following the continue token to the
// end. No field selector is sent.
func (c *Client) ListPVs(ctx context.Context, req ListRequest) ([]PV, error) {
	if err := phaseRefused("/api/v1/persistentvolumes", req.Phase); err != nil {
		return nil, err
	}
	return listAll[PV](ctx, c, "/api/v1/persistentvolumes", req, "")
}

// ListJobs lists the batch/v1 Jobs of one namespace, with the label selector,
// following the continue token to the end. No field selector is sent: the caller
// orders and filters by owner and creation time.
func (c *Client) ListJobs(ctx context.Context, namespace string, req ListRequest) ([]Job, error) {
	path := "/apis/batch/v1/namespaces/" + url.PathEscape(namespace) + "/jobs"
	if err := phaseRefused(path, req.Phase); err != nil {
		return nil, err
	}
	return listAll[Job](ctx, c, path, req, "")
}

// ListPodsIn lists the pods of one namespace, with the label selector and a
// status.phase field selector — the API server supports status.phase for pods —
// following the continue token to the end.
func (c *Client) ListPodsIn(ctx context.Context, namespace string, req ListRequest) ([]Pod, error) {
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods"
	return listAll[Pod](ctx, c, path, req, combineFieldSelector(phaseSelector(req.Phase), req.FieldSelector))
}

// ListEvents lists the core/v1 Events of one namespace with the label selector
// and the caller's field selector — an Event's involvedObject.uid is how a Job's
// own events are picked out — following the continue token to the end.
func (c *Client) ListEvents(ctx context.Context, namespace string, req ListRequest) ([]Event, error) {
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/events"
	if err := phaseRefused(path, req.Phase); err != nil {
		return nil, err
	}
	return listAll[Event](ctx, c, path, req, req.FieldSelector)
}

// combineFieldSelector joins the non-empty parts into one comma-separated field
// selector, which is how the API server takes several terms at once.
func combineFieldSelector(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, ",")
}

// GetJob fetches one Job by name.
func (c *Client) GetJob(ctx context.Context, namespace, name string) (Job, error) {
	var j Job
	path := "/apis/batch/v1/namespaces/" + url.PathEscape(namespace) + "/jobs/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &j); err != nil {
		return Job{}, err
	}
	return j, nil
}

// phaseRefused refuses a phase on a listing the API server cannot filter by
// phase. A silent answer would be worse than an error either way: a caller that
// asked for one phase and got every phase reads the others as absent objects.
func phaseRefused(path, phase string) error {
	if phase == "" {
		return nil
	}
	// Configuration, not the cluster: the same request gets the same answer.
	return plugin.Permanent(fmt.Errorf("%s %s: the API server has no status.phase field selector for this resource; list every phase and decide in the caller",
		http.MethodGet, path))
}

func phaseSelector(phase string) string {
	if phase == "" {
		return ""
	}
	return "status.phase=" + phase
}

func listAll[T any](ctx context.Context, c *Client, path string, req ListRequest, fieldSelector string) ([]T, error) {
	if req.PageSize < 1 {
		// Configuration, not the cluster: retrying an unchanged request
		// cannot fix it.
		return nil, plugin.Permanent(fmt.Errorf("%s %s: page size must be at least 1, got %d", http.MethodGet, path, req.PageSize))
	}
	var out []T
	token := ""
	for {
		q := url.Values{"limit": {strconv.Itoa(req.PageSize)}}
		if req.Selector != "" {
			q.Set("labelSelector", req.Selector)
		}
		if fieldSelector != "" {
			q.Set("fieldSelector", fieldSelector)
		}
		if token != "" {
			q.Set("continue", token)
		}
		var page list[T]
		if err := c.do(ctx, http.MethodGet, path, q, nil, &page); err != nil {
			return nil, err
		}
		out = append(out, page.Items...)
		if page.Metadata.Continue == "" {
			return out, nil
		}
		if page.Metadata.Continue == token {
			return nil, plugin.Transient(fmt.Errorf("%s %s: the continue token did not advance", http.MethodGet, path))
		}
		token = page.Metadata.Continue
	}
}

// GetSecret fetches one Secret by name.
func (c *Client) GetSecret(ctx context.Context, namespace, name string) (Secret, error) {
	var s Secret
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/secrets/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &s); err != nil {
		return Secret{}, err
	}
	return s, nil
}

// GetPod fetches one pod by name.
func (c *Client) GetPod(ctx context.Context, namespace, name string) (Pod, error) {
	var p Pod
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &p); err != nil {
		return Pod{}, err
	}
	return p, nil
}

// GetConfigMap fetches one ConfigMap by name.
func (c *Client) GetConfigMap(ctx context.Context, namespace, name string) (ConfigMap, error) {
	var cm ConfigMap
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/configmaps/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &cm); err != nil {
		return ConfigMap{}, err
	}
	return cm, nil
}

// configMapDataPatch is a JSON merge patch over one ConfigMap that names the
// data and the version it was read at and NOTHING else. A merge patch is
// defined by what it omits: every field it does not name — the annotations, the
// labels, the namespace, the resourceVersion of the stored object — is left
// exactly as it is. A PUT could not do this: it replaces the object, so a body
// without annotations deletes them, which is how a reconciler's
// `prune: disabled` annotation would be wiped by every Save.
type configMapDataPatch struct {
	Metadata struct {
		// ResourceVersion is the precondition, not a value: the API server
		// applies the patch only if the stored object is still at this version,
		// and answers 409 otherwise — which the plugin reports as transient and
		// the engine's retry re-reads and repeats.
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Data map[string]string `json:"data"`
}

// PatchConfigMapData replaces the data of one ConfigMap and touches nothing
// else, under optimistic concurrency: resourceVersion must be the one the
// caller read, and an empty one is refused (that would be an unconditional
// overwrite).
func (c *Client) PatchConfigMapData(ctx context.Context, namespace, name string, data map[string]string, resourceVersion string) (ConfigMap, error) {
	if resourceVersion == "" {
		return ConfigMap{}, plugin.Permanent(fmt.Errorf("configmap %s/%s: resourceVersion is empty; an unconditional overwrite is refused", namespace, name))
	}
	body := configMapDataPatch{Data: data}
	body.Metadata.ResourceVersion = resourceVersion
	var cm ConfigMap
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/configmaps/" + url.PathEscape(name)
	if err := c.doWith(ctx, http.MethodPatch, path, mergePatchContentType, nil, body, &cm); err != nil {
		return ConfigMap{}, err
	}
	return cm, nil
}

// mergePatchContentType is the JSON merge patch media type: the patch is a
// partial object, and only the members it carries are written.
const mergePatchContentType = "application/merge-patch+json"

func (c *Client) do(ctx context.Context, method, path string, q url.Values, body any, out any) error {
	return c.doWith(ctx, method, path, "application/json", q, body, out)
}

// doWith is do with an explicit request content type: a merge patch is not
// application/json, and the API server reads the media type to decide how to
// apply the body.
func (c *Client) doWith(ctx context.Context, method, path, contentType string, q url.Values, body any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	token, err := c.token()
	if err != nil {
		return err
	}
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return plugin.Permanent(fmt.Errorf("%s %s: encode request: %w", method, path, err))
		}
		reqBody = bytes.NewReader(b)
	}
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reqBody)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("%s %s: %w", method, path, err))
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return plugin.Transient(fmt.Errorf("%s %s: %w", method, path, err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return statusError(method, path, resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return plugin.Transient(fmt.Errorf("%s %s: read response: %w", method, path, err))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return plugin.Transient(fmt.Errorf("%s %s: decode response: %w", method, path, err))
	}
	return nil
}

// token re-reads the token file on every request: kubelet rotates a projected
// token underneath the process, and a cached copy would start failing with 401
// once it does. The value is used as a header and never logged.
func (c *Client) token() (string, error) {
	raw, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return "", plugin.Transient(fmt.Errorf("read token file %s: %w", c.tokenFile, err))
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", plugin.Permanent(fmt.Errorf("token file %s is empty", c.tokenFile))
	}
	return token, nil
}

// status is the API server's error body.
type status struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// statusError turns a non-2xx answer into a classified error: the server's own
// message when it sent one, the status line otherwise.
func statusError(method, path string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	message := strings.TrimSpace(string(raw))
	var st status
	if err := json.Unmarshal(raw, &st); err == nil && st.Message != "" {
		message = st.Message
	}
	err := fmt.Errorf("%s %s: HTTP %s: %s", method, path, resp.Status, message)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return plugin.NotFound(err)
	case resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests:
		return plugin.Transient(err)
	case resp.StatusCode >= 500:
		return plugin.Transient(err)
	default:
		// 401, 403 (RBAC), 422 and every other client error: retrying an
		// unchanged request gets the same answer.
		return plugin.Permanent(err)
	}
}
