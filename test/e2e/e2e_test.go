// Package e2e is the user-level guarantee: the shipped image and the shipped
// Helm chart, on a real k3s cluster (k3d), doing the real work. The unit suites
// prove the code; this proves the artifact a deployment installs.
//
// It is off unless GCB_E2E=1, so `go test ./...` stays fast and needs no
// cluster. Everything the run needs is in README.md; the suite creates the
// cluster itself, imports the images and never touches the caller's kubeconfig.
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
)

// env is the shared cluster fixture, nil when the suite is disabled.
var env *Env

func TestMain(m *testing.M) {
	if os.Getenv("GCB_E2E") != "1" {
		// Every test skips itself; nothing touches a cluster.
		os.Exit(m.Run())
	}
	e, err := setup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(1)
	}
	env = e
	code := m.Run()
	if err := env.teardown(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e teardown:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// Env is one k3d cluster plus the image under test.
type Env struct {
	Cluster    string
	Image      string // full ref, e.g. registry/x:tag
	Repo       string // repository half of Image
	Tag        string // tag half of Image
	Namespace  string
	Kubeconfig string
	CoverDir   string // host directory bind-mounted into the node, or ""
	ChartDir   string
	created    bool
	// homeKubeconfig is the fingerprint of the caller's ~/.kube/config taken
	// before anything runs. The suite must leave it byte-identical: k3d writes
	// there by default, and a test that reconfigures its runner's kubectl is a
	// test that breaks the next thing the runner does.
	homeKubeconfig string
}

// kubeconfigFingerprint identifies the caller's default kubeconfig: its SHA-256,
// or "absent" when there is none.
func kubeconfigFingerprint() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "unknown"
	}
	b, err := os.ReadFile(filepath.Join(home, ".kube", "config"))
	if err != nil {
		return "absent"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The k3s image the cluster runs. Pinned: the tool's primary target is k3s, and
// an unpinned node is an unpinned target.
const defaultK3sImage = "rancher/k3s:v1.37.1-k3s1"

// setup reads the environment and prepares the cluster. The suite creates the
// cluster itself when it is missing, so a local run and a CI run use the same
// flags.
func setup() (*Env, error) {
	e := &Env{
		Cluster:   envOr("GCB_E2E_CLUSTER", "gcb-e2e"),
		Image:     os.Getenv("GCB_E2E_IMAGE"),
		Namespace: envOr("GCB_E2E_NAMESPACE", "gcb-e2e"),
		CoverDir:  os.Getenv("GCB_E2E_COVERDIR"),
	}
	if e.Image == "" {
		return nil, fmt.Errorf("GCB_E2E_IMAGE is required: the image under test, present in the local docker daemon")
	}
	if e.CoverDir != "" {
		if !filepath.IsAbs(e.CoverDir) {
			return nil, fmt.Errorf("GCB_E2E_COVERDIR must be an absolute path, got %q", e.CoverDir)
		}
		// 0777: the pods write as uid 1000, and fsGroup is not applied to a
		// hostPath. The directory must exist before the cluster is created —
		// the node's bind mount names it — and must not be replaced after.
		if err := os.MkdirAll(e.CoverDir, 0o777); err != nil {
			return nil, fmt.Errorf("create coverage dir: %w", err)
		}
		if err := os.Chmod(e.CoverDir, 0o777); err != nil {
			return nil, fmt.Errorf("chmod coverage dir: %w", err)
		}
	}
	e.Repo, e.Tag = splitImage(e.Image)

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return nil, err
	}
	e.ChartDir = filepath.Join(root, "charts", "go-cluster-backup")
	if _, err := os.Stat(filepath.Join(e.ChartDir, "Chart.yaml")); err != nil {
		return nil, fmt.Errorf("chart not found at %s: %w", e.ChartDir, err)
	}

	// Taken before any k3d command: `k3d cluster create` updates and switches
	// the default kubeconfig unless told otherwise, and this suite must not
	// reconfigure its runner.
	e.homeKubeconfig = kubeconfigFingerprint()

	if !e.clusterExists() {
		if err := e.createCluster(); err != nil {
			return nil, err
		}
	}

	// The kubeconfig is a temp file, never the caller's: this suite must not be
	// able to touch any other context.
	kubeconfig := filepath.Join(os.TempDir(), "gcb-e2e-"+e.Cluster+".kubeconfig")
	out, err := e.k3d("kubeconfig", "get", e.Cluster)
	if err != nil {
		return nil, fmt.Errorf("k3d kubeconfig get: %w\n%s", err, out)
	}
	if err := os.WriteFile(kubeconfig, []byte(out), 0o600); err != nil {
		return nil, err
	}
	e.Kubeconfig = kubeconfig

	// Every image is pulled once into the local daemon and loaded into the node
	// together. Four in-cluster pulls cost minutes of registry latency per run
	// and fail on a throttled registry; one import is seconds and offline.
	// The dependencies come from a registry; the image under test is built
	// locally and must not be pulled.
	for _, image := range dependencyImages {
		if out, err := e.exec("", "docker", "pull", "-q", image); err != nil {
			return nil, fmt.Errorf("docker pull %s: %w\n%s", image, err, out)
		}
	}
	if out, err := e.k3d(append([]string{"image", "import"}, append(dependencyImages, "-c", e.Cluster)...)...); err != nil {
		return nil, fmt.Errorf("k3d image import (dependencies): %w\n%s", err, out)
	}
	// The image under test is imported on its own: a locally built image mixed
	// with registry references in one import is silently dropped.
	if out, err := e.k3d("image", "import", e.Image, "-c", e.Cluster); err != nil {
		return nil, fmt.Errorf("k3d image import %s: %w\n%s", e.Image, err, out)
	}
	if out, err := e.exec("", "kubectl", "get", "nodes"); err != nil {
		return nil, fmt.Errorf("cluster not reachable: %w\n%s", err, out)
	}
	return e, nil
}

// teardown removes the cluster the suite created and proves the caller's
// kubeconfig came through untouched.
func (e *Env) teardown() error {
	if e.created {
		_, _ = e.k3d("cluster", "delete", e.Cluster)
	}
	if e.Kubeconfig != "" {
		_ = os.Remove(e.Kubeconfig)
	}
	if after := kubeconfigFingerprint(); after != e.homeKubeconfig {
		return fmt.Errorf("the suite changed the caller's ~/.kube/config (before %s, after %s): k3d must run with --kubeconfig-update-default=false and --kubeconfig-switch-context=false", e.homeKubeconfig, after)
	}
	return nil
}

func (e *Env) clusterExists() bool {
	out, err := e.k3d("cluster", "list", "-o", "json")
	return err == nil && strings.Contains(out, `"name":"`+e.Cluster+`"`)
}

// createCluster brings up the one node every scenario runs on. The coverage
// directory is bind-mounted into it, which is the whole of the plumbing: the
// pods' hostPath volume and the host directory are then the same files.
func (e *Env) createCluster() error {
	image := envOr("GCB_E2E_K3S_IMAGE", defaultK3sImage)
	args := []string{"cluster", "create", e.Cluster, "--image", image, "--wait",
		// The caller's ~/.kube/config is not ours: the suite reads its own
		// kubeconfig from `k3d kubeconfig get` into a temp file.
		"--kubeconfig-update-default=false", "--kubeconfig-switch-context=false"}
	if e.CoverDir != "" {
		args = append(args, "--volume", e.CoverDir+":"+coverNodeDir+"@server:*")
	}
	if out, err := e.k3d(args...); err != nil {
		return fmt.Errorf("k3d cluster create: %w\n%s", err, out)
	}
	e.created = true
	return nil
}

// coverNodeDir is where the host coverage directory appears on the node, and
// the path the pods' hostPath volume names.
const coverNodeDir = "/var/lib/gocover"

// --- process helpers ------------------------------------------------------

func (e *Env) k3d(args ...string) (string, error) {
	return e.exec("", "k3d", args...)
}

// exec runs a command with this cluster's kubeconfig alone in KUBECONFIG. It is
// bounded, so a command that hangs (a kubectl exec into a process that never
// exits) fails the poll instead of wedging the suite.
func (e *Env) exec(stdin, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = extcmd.ReplaceEnv(os.Environ(), "KUBECONFIG="+e.Kubeconfig)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err := cmd.Run()
	return buf.String(), err
}

// kubectl runs kubectl and fails the test on a non-zero exit.
func (e *Env) kubectl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := e.exec("", "kubectl", args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// apply pipes a manifest to kubectl apply.
func (e *Env) apply(t *testing.T, manifest string) {
	t.Helper()
	if out, err := e.exec(manifest, "kubectl", "apply", "-f", "-"); err != nil {
		t.Fatalf("kubectl apply: %v\n%s", err, out)
	}
}

// helm runs helm and fails the test on a non-zero exit.
func (e *Env) helm(t *testing.T, args ...string) string {
	t.Helper()
	out, err := e.exec("", "helm", args...)
	if err != nil {
		t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// waitFor polls cond until it reports done or the deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		ok, detail := cond()
		if ok {
			return
		}
		last = detail
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %s", timeout, what, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// dump prints what a namespace holds: pods, events and the logs of every pod.
// Called before a fatal assertion so the failure is readable without a rerun.
func (e *Env) dump(t *testing.T, namespace string) {
	t.Helper()
	for _, args := range [][]string{
		{"get", "pods,jobs,configmaps", "-n", namespace, "-o", "wide"},
		{"get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
	} {
		if out, err := e.exec("", "kubectl", args...); err == nil {
			t.Logf("--- kubectl %s\n%s", strings.Join(args, " "), out)
		}
	}
	pods, err := e.exec("", "kubectl", "get", "pods", "-n", namespace, "-o", "name")
	if err != nil {
		return
	}
	for _, pod := range strings.Fields(pods) {
		pod = strings.TrimPrefix(pod, "pod/")
		if out, err := e.exec("", "kubectl", "logs", "-n", namespace, pod, "--all-containers", "--tail=60"); err == nil {
			t.Logf("--- logs %s\n%s", pod, out)
		}
	}
}

// --- small helpers --------------------------------------------------------

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// splitImage splits an image reference into repository and tag.
func splitImage(ref string) (repo, tag string) {
	if i := strings.LastIndex(ref, "@"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		return ref[:i], ref[i+1:]
	}
	return ref, "latest"
}

// mustRequireEnv skips the test when the suite is disabled.
func mustRequireEnv(t *testing.T) *Env {
	t.Helper()
	if env == nil {
		t.Skip("e2e suite disabled: set GCB_E2E=1 (see test/e2e/README.md)")
	}
	return env
}

// jobSucceeded waits for a Job to complete and fails on a failed one.
func (e *Env) jobSucceeded(t *testing.T, namespace, name string, timeout time.Duration) {
	t.Helper()
	waitFor(t, "job/"+name, timeout, func() (bool, string) {
		succeeded, failed := e.jobCounts(t, namespace, name)
		if failed > 0 {
			e.dump(t, namespace)
			t.Fatalf("job %s failed", name)
		}
		return succeeded > 0, fmt.Sprintf("succeeded=%d failed=%d", succeeded, failed)
	})
}

// jobCounts reads a Job's succeeded and failed pod counts. They are read one at
// a time: `1/` and `/1` are different shapes, and one scanf over them cannot
// tell a completed Job from a failed one.
func (e *Env) jobCounts(t *testing.T, namespace, name string) (succeeded, failed int) {
	t.Helper()
	return e.jobCount(namespace, name, ".status.succeeded"), e.jobCount(namespace, name, ".status.failed")
}

func (e *Env) jobCount(namespace, name, field string) int {
	out, err := e.exec("", "kubectl", "get", "job", name, "-n", namespace, "-o", "jsonpath={"+field+"}")
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(out))
	return n
}

// podLogs returns the logs of the pods of one Job.
func (e *Env) jobLogs(t *testing.T, namespace, job string) string {
	t.Helper()
	out, err := e.exec("", "kubectl", "logs", "-n", namespace, "-l", "job-name="+job,
		"--all-containers", "--tail=-1")
	if err != nil {
		t.Fatalf("logs of job %s: %v\n%s", job, err, out)
	}
	return out
}
