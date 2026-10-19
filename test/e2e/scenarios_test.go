package e2e

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestSuite drives one release through the whole lifecycle: install, a manual
// run, a restore, the previous-run report, the writer lease, and a rejected
// upgrade. It runs against its own namespace and shares the cluster with
// TestK3sState.
func TestSuite(t *testing.T) {
	t.Parallel()
	e := mustRequireEnv(t)
	ns := e.Namespace

	files := writeFiles(t, map[string]string{
		"config.yaml": e.toolConfig(ns, catalogName(suiteFullname)),
		"values.yaml": e.baseValues(ns, suiteFullname, nil, nil),
	})
	cfg, values := files["config.yaml"], files["values.yaml"]

	// Scenario a: install, and the config gate runs before anything else.
	t.Run("install", func(t *testing.T) {
		e.installNamespace(t, ns)
		e.installStorage(t, ns)
		e.installWorkloads(t, ns)
		if cm := e.coverageManifests(ns); cm != "" {
			e.apply(t, cm)
		}
		e.helm(t, e.helmArgs(ns, cfg, values)...)

		status := e.helm(t, "status", releaseName, "-n", ns)
		if !strings.Contains(status, "STATUS: deployed") {
			t.Fatalf("release not deployed:\n%s", status)
		}
		e.kubectl(t, "get", "cronjob", suiteFullname, suiteFullname+"-manual", suiteFullname+"-restore", "-n", ns)
		e.kubectl(t, "get", "configmap", catalogName(suiteFullname), suiteFullname+"-config", "-n", ns)

		// The hook Job is deleted on success (hook-delete-policy), so its
		// having run is proven by its events.
		waitFor(t, "the validate hook's events", time.Minute, func() (bool, string) {
			out, err := e.exec("", "kubectl", "get", "events", "-n", ns)
			if err != nil {
				return false, out
			}
			return strings.Contains(out, suiteFullname+"-validate"), "no event names " + suiteFullname + "-validate"
		})
	})

	// Scenario b: a manual run does the work, records itself, and notifies.
	var runID string
	t.Run("manual_run", func(t *testing.T) {
		job := "manual-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-manual", job, nil)
		e.jobSucceeded(t, ns, job, 5*time.Minute)

		line := e.waitForEcho(t, ns, "event=run_finished", "kind=manual", "outcome=delivered")
		runID = runField(line)
		if runID == "" {
			t.Fatalf("no run id in the notification: %q", line)
		}
		// The finished notification is sent before the last catalog save, so the
		// entry is polled for rather than read once.
		waitFor(t, "the catalog entry for run "+runID, time.Minute, func() (bool, string) {
			catalog := e.kubectl(t, "get", "configmap", catalogName(suiteFullname), "-n", ns,
				"-o", `jsonpath={.data.catalog\.json}`)
			return strings.Contains(catalog, runID) && strings.Contains(catalog, `"outcome":"delivered"`), catalog
		})
	})

	// Scenario c: the run is listed from the storage, and its artifact restores
	// into a database beside the original.
	t.Run("restore", func(t *testing.T) {
		artifact := ns + "/postgres/" + appDatabase
		list := writeFiles(t, map[string]string{"restore-list.yaml": restoreArgs(
			"restore", "list", "--config", "/etc/go-cluster-backup/config.yaml")})
		e.helm(t, e.helmArgs(ns, cfg, values, list["restore-list.yaml"])...)
		listJob := "restore-list-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-restore", listJob, nil)
		e.jobSucceeded(t, ns, listJob, 5*time.Minute)
		logs := e.jobLogs(t, ns, listJob)
		if !strings.Contains(logs, "run "+runID) || !strings.Contains(logs, "outcome=delivered") {
			t.Fatalf("restore list does not show run %s:\n%s", runID, logs)
		}

		apply := writeFiles(t, map[string]string{"restore-apply.yaml": restoreArgs(
			"restore", "apply", "--config", "/etc/go-cluster-backup/config.yaml",
			"--run", "newest-ok", "--artifact", artifact,
			"--target", "discovered", "--mode", "side-by-side")})
		e.helm(t, e.helmArgs(ns, cfg, values, apply["restore-apply.yaml"])...)
		applyJob := "restore-apply-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-restore", applyJob, nil)
		e.jobSucceeded(t, ns, applyJob, 5*time.Minute)

		restored := e.waitForRestoredDatabase(t, ns)
		rows := e.psql(t, ns, restored, "select count(*) from "+appTable)
		if rows != "42" {
			t.Fatalf("restored database %s holds %s rows, want 42", restored, rows)
		}
	})

	// Scenario e: a run that failed before the tool started is reported by the
	// next run of the same CronJob.
	t.Run("previous_run_failure", func(t *testing.T) {
		broken := "prev-fail-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-manual", broken, []string{"/bin/sh", "-c", "exit 7"})
		e.jobFailed(t, ns, broken, 3*time.Minute)

		after := "after-fail-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-manual", after, nil)
		e.jobSucceeded(t, ns, after, 5*time.Minute)

		e.waitForEcho(t, ns, "event=run_finished", "previous=failed", "exit=7", "job="+broken)
	})

	// Scenario f: a live claim blocks a second writer. The claim is written the
	// way the tool writes it — a holder the catalog cannot prove dead — so the
	// refusal is deterministic rather than a race against a run that finishes.
	t.Run("writer_lease", func(t *testing.T) {
		e.seedLease(t, ns, catalogName(suiteFullname))
		e.waitForLease(t, ns, catalogName(suiteFullname))

		loser := "loser-" + timestamp()
		e.createJobFrom(t, ns, suiteFullname+"-manual", loser, nil)
		e.jobFailed(t, ns, loser, 3*time.Minute)
		logs := e.jobLogs(t, ns, loser)
		if !strings.Contains(logs, "lease") {
			t.Fatalf("the refused run does not name the lease:\n%s", logs)
		}
		e.clearLease(t, ns, catalogName(suiteFullname))
	})

	// Scenario d: an invalid configuration never reaches the CronJobs; the
	// installed release stays as it was.
	t.Run("upgrade_rejected", func(t *testing.T) {
		bad := writeFiles(t, map[string]string{
			"bad-config.yaml": e.toolConfig(ns, catalogName(suiteFullname)) + "\nunknown_section: {nope: true}\n"})
		out, err := e.exec("", "helm", append(e.helmArgs(ns, bad["bad-config.yaml"], values), "--wait")...)
		if err == nil {
			t.Fatalf("helm upgrade with an invalid config succeeded:\n%s", out)
		}
		if !strings.Contains(out, "validate") {
			t.Logf("upgrade output did not mention validate:\n%s", out)
		}
		deployed := e.kubectl(t, "get", "configmap", suiteFullname+"-config", "-n", ns,
			"-o", `jsonpath={.data.config\.yaml}`)
		if strings.Contains(deployed, "unknown_section") {
			t.Fatalf("the rejected config reached the release:\n%s", deployed)
		}
		e.kubectl(t, "get", "cronjob", suiteFullname+"-manual", "-n", ns)
	})
}

// TestK3sState is scenario g: the stage initContainer snapshots the k3d node's
// own k3s datastore, and the run reads it back. It needs a privileged
// namespace (hostPath) and its own release, so it lives beside TestSuite.
func TestK3sState(t *testing.T) {
	t.Parallel()
	e := mustRequireEnv(t)
	ns := e.Namespace + "-k3s"

	files := writeFiles(t, map[string]string{
		"config.yaml": e.k3sConfig(ns, catalogName(k3sFullname)),
		"values.yaml": e.baseValues(ns, k3sFullname, nil, nil) + `
# The node mounts are writer-only: the restore and hook pods must not carry
# them. /gocover stays on the shared extras, so the writer, stage, restore and
# hook containers all write coverage.
writer:
  volumes:
    - {name: staged, emptyDir: {}}
    - {name: k3s-db, hostPath: {path: /var/lib/rancher/k3s/server/db, type: Directory}}
    - {name: k3s-token, hostPath: {path: /var/lib/rancher/k3s/server/token, type: File}}
  volumeMounts:
    - {name: staged, mountPath: /staged}
stage:
  enabled: true
  volumeMounts:
    # /staged comes from writer.volumes; repeating /gocover here would mount the
    # same path twice and the kubelet would refuse the pod.
    - {name: staged, mountPath: /staged}
    - {name: k3s-db, mountPath: /k3s-db}
    - {name: k3s-token, mountPath: /k3s-token, readOnly: true}
`,
	})
	e.installNamespace(t, ns)
	e.installStorage(t, ns)
	// hostPath volumes need a namespace below `baseline`.
	e.kubectl(t, "label", "namespace", ns, "--overwrite",
		"pod-security.kubernetes.io/enforce=privileged")
	if cm := e.coverageManifests(ns); cm != "" {
		e.apply(t, cm)
	}
	e.helm(t, e.helmArgs(ns, files["config.yaml"], files["values.yaml"])...)

	job := "k3s-" + timestamp()
	e.createJobFrom(t, ns, k3sFullname+"-manual", job, nil)
	e.jobSucceeded(t, ns, job, 5*time.Minute)

	list := writeFiles(t, map[string]string{"restore-list.yaml": restoreArgs(
		"restore", "list", "--config", "/etc/go-cluster-backup/config.yaml")})
	e.helm(t, e.helmArgs(ns, files["config.yaml"], files["values.yaml"], list["restore-list.yaml"])...)
	listJob := "k3s-list-" + timestamp()
	e.createJobFrom(t, ns, k3sFullname+"-restore", listJob, nil)
	e.jobSucceeded(t, ns, listJob, 5*time.Minute)
	logs := e.jobLogs(t, ns, listJob)
	// `restore list` names an artifact as --artifact takes it: the group is not
	// part of the name.
	if !strings.Contains(logs, "outcome=delivered") || !strings.Contains(logs, "artifacts: k3s-state") {
		t.Fatalf("the staged k3s artifact is not in the storage:\n%s", logs)
	}
}

// --- release plumbing -----------------------------------------------------

func (e *Env) helmArgs(ns, configPath, baseValues string, extra ...string) []string {
	args := []string{"upgrade", "--install", releaseName, e.ChartDir,
		"--namespace", ns, "--create-namespace",
		"-f", baseValues, "--set-file", "config=" + configPath}
	for _, f := range extra {
		args = append(args, "-f", f)
	}
	return args
}

// restoreArgs renders a values fragment that sets the restore CronJob's args.
func restoreArgs(args ...string) string {
	var b strings.Builder
	b.WriteString("restore:\n  args:\n")
	for _, a := range args {
		fmt.Fprintf(&b, "    - %q\n", a)
	}
	return b.String()
}

// createJobFrom starts a Job from a CronJob. When command is non-empty the
// writer container's command is replaced, which is how a run is made to fail
// before the tool starts.
func (e *Env) createJobFrom(t *testing.T, ns, cronjob, name string, command []string) {
	t.Helper()
	args := []string{"create", "job", name, "--from=cronjob/" + cronjob, "-n", ns}
	if command == nil {
		if out, err := e.exec("", "kubectl", args...); err != nil {
			e.dump(t, ns)
			t.Fatalf("create job %s: %v\n%s", name, err, out)
		}
		return
	}
	args = append(args, "--dry-run=client", "-o", "json")
	out, err := e.exec("", "kubectl", args...)
	if err != nil {
		t.Fatalf("render job %s: %v\n%s", name, err, out)
	}
	var job map[string]any
	if err := json.Unmarshal([]byte(out), &job); err != nil {
		t.Fatalf("decode job %s: %v\n%s", name, err, out)
	}
	containers := job["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	containers[0].(map[string]any)["command"] = command
	edited, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("encode job %s: %v", name, err)
	}
	e.apply(t, string(edited))
}

// jobFailed waits for a Job to have failed.
func (e *Env) jobFailed(t *testing.T, ns, name string, timeout time.Duration) {
	t.Helper()
	waitFor(t, "job/"+name+" failed", timeout, func() (bool, string) {
		succeeded, failed := e.jobCounts(t, ns, name)
		return failed > 0, fmt.Sprintf("succeeded=%d failed=%d", succeeded, failed)
	})
}

// seedLease writes the writer-lease record the tool itself writes, naming a
// holder the catalog cannot prove dead: the pod is absent and the claim is far
// inside run.lease_hard_bound. A manual run refuses it at once, which is the
// one behaviour under test — no race against a run that may finish first.
func (e *Env) seedLease(t *testing.T, ns, catalog string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"holder":           "e2e-holder",
		"kind":             "manual",
		"token":            "e2e",
		"holder_namespace": ns,
		"holder_pod":       "e2e-holder",
		"holder_uid":       "e2e",
		"acquired_at":      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode lease: %v", err)
	}
	patch := fmt.Sprintf(`{"data":{"catalog.json.lease":%q}}`, string(body))
	e.kubectl(t, "patch", "configmap", catalog, "-n", ns, "--type=merge", "-p", patch)
}

// clearLease drops the seeded claim, so a later run is not blocked by it.
func (e *Env) clearLease(t *testing.T, ns, catalog string) {
	t.Helper()
	_, _ = e.exec("", "kubectl", "patch", "configmap", catalog, "-n", ns, "--type=json",
		"-p", `[{"op":"remove","path":"/data/catalog.json.lease"}]`)
}

// waitForLease waits until the catalog ConfigMap carries a held writer lease.
func (e *Env) waitForLease(t *testing.T, ns, catalog string) {
	t.Helper()
	waitFor(t, "the writer lease to be taken", time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "get", "configmap", catalog, "-n", ns,
			"-o", `jsonpath={.data.catalog\.json\.lease}`)
		if err != nil {
			return false, out
		}
		return strings.Contains(out, `"holder"`), "lease: " + out
	})
}

// waitForRestoredDatabase returns the side-by-side database the restore made.
func (e *Env) waitForRestoredDatabase(t *testing.T, ns string) string {
	t.Helper()
	var name string
	waitFor(t, "the restored database", time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/postgres", "--",
			"psql", "-U", "postgres", "-d", "postgres", "-tAc",
			"select datname from pg_database where datname like '"+appDatabase+`_restore_%'`)
		if err != nil {
			return false, out
		}
		name = strings.TrimSpace(out)
		return name != "", "no side-by-side database yet"
	})
	return name
}

// psql runs one query and returns its trimmed output.
func (e *Env) psql(t *testing.T, ns, database, query string) string {
	t.Helper()
	out, err := e.exec("", "kubectl", "exec", "-n", ns, "deploy/postgres", "--",
		"psql", "-U", "postgres", "-d", database, "-tAc", query)
	if err != nil {
		t.Fatalf("psql -d %s: %v\n%s", database, err, out)
	}
	return strings.TrimSpace(out)
}

// --- notifications --------------------------------------------------------

// waitForEcho waits for one notification line carrying every want substring.
func (e *Env) waitForEcho(t *testing.T, ns string, want ...string) string {
	t.Helper()
	var found string
	waitFor(t, "notification "+strings.Join(want, " "), time.Minute, func() (bool, string) {
		out, err := e.exec("", "kubectl", "logs", "-n", ns, "deploy/echo", "--tail=-1")
		if err != nil {
			return false, out
		}
		for _, line := range strings.Split(out, "\n") {
			if containsAll(line, want) {
				found = line
				return true, ""
			}
		}
		return false, "not seen in the receiver's log yet"
	})
	return found
}

func containsAll(line string, want []string) bool {
	for _, w := range want {
		if !strings.Contains(line, w) {
			return false
		}
	}
	return true
}

var runFieldRe = regexp.MustCompile(`run=(\S+)`)

func runField(line string) string {
	if m := runFieldRe.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

func timestamp() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
}
