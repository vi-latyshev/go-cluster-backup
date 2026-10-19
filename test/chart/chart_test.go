// Package chart renders charts/go-cluster-backup with helm and asserts the
// properties a consumer depends on: the object names, the RBAC shape, the pod
// security context and the config-gate hook. It is a real `helm template` run,
// not a re-implementation of the templates.
package chart

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const (
	chartPath   = "charts/go-cluster-backup"
	releaseName = "test"
	// The fullname every generated object name derives from, for the release
	// name above and this chart's name.
	fullname = releaseName + "-go-cluster-backup"
	// The catalog ConfigMap name the example configuration states.
	catalogName = "go-cluster-backup-catalog"
)

// repoRoot is the module root, two directories above this package.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving the repository root: %v", err)
	}
	return root
}

// helmBinary returns the helm in PATH. A missing helm is a failure in CI and a
// loud skip locally: a chart test that silently does nothing is worse than none.
func helmBinary(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("helm is required in CI and was not found in PATH: %v", err)
		}
		t.Skipf("SKIPPED: helm not found in PATH, so this chart test did not run (set CI=1 to make this a failure): %v", err)
	}
	return path
}

// runHelm runs one helm command in the repository root and returns its stdout,
// or the error helm reported with its stderr.
func runHelm(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(helmBinary(t), args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("helm %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// render runs `helm template` and returns the manifest, or the error helm
// reported.
func render(t *testing.T, extraArgs ...string) (string, error) {
	t.Helper()
	root := repoRoot(t)
	args := append([]string{"template", releaseName, filepath.Join(root, chartPath)}, extraArgs...)
	return runHelm(t, root, args...)
}

// renderNotes renders the chart's NOTES. `helm template` does not render
// NOTES.txt, so this is a client-side dry run — the form that never contacts a
// cluster. Helm 3 spells the flag as a bare boolean, so a rejection of the
// value falls back to that.
func renderNotes(t *testing.T, extraArgs ...string) string {
	t.Helper()
	root := repoRoot(t)
	base := []string{"install", releaseName, filepath.Join(root, chartPath), "--namespace", "backup"}
	withDryRun := func(flag string) []string {
		args := append([]string{}, base...)
		args = append(args, flag)
		return append(args, extraArgs...)
	}
	manifest, err := runHelm(t, root, withDryRun("--dry-run=client")...)
	if err != nil {
		manifest, err = runHelm(t, root, withDryRun("--dry-run")...)
	}
	if err != nil {
		t.Fatalf("rendering NOTES: %v", err)
	}
	index := strings.Index(manifest, "\nNOTES:\n")
	if index < 0 {
		t.Fatalf("helm printed no NOTES section:\n%s", manifest)
	}
	return manifest[index:]
}

// renderExample renders the chart with one example's values and configuration.
func renderExample(t *testing.T, name string) string {
	t.Helper()
	root := repoRoot(t)
	manifest, err := render(t,
		"-f", filepath.Join(root, "examples", "values", name+".yaml"),
		"--set-file", "config="+filepath.Join(root, "examples", "config", name+".yaml"),
	)
	if err != nil {
		t.Fatalf("rendering the %s example: %v", name, err)
	}
	return manifest
}

// documents decodes every YAML document of a rendered manifest.
func documents(t *testing.T, manifest string) []map[string]any {
	t.Helper()
	var docs []map[string]any
	dec := yaml.NewDecoder(strings.NewReader(manifest))
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatalf("decoding a rendered document: %v", err)
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
}

func meta(doc map[string]any) map[string]any {
	m, _ := doc["metadata"].(map[string]any)
	return m
}

func name(doc map[string]any) string {
	s, _ := meta(doc)["name"].(string)
	return s
}

func kind(doc map[string]any) string {
	s, _ := doc["kind"].(string)
	return s
}

// find returns the one rendered object of a kind and name.
func find(t *testing.T, docs []map[string]any, kindName, objectName string) map[string]any {
	t.Helper()
	for _, doc := range docs {
		if kind(doc) == kindName && name(doc) == objectName {
			return doc
		}
	}
	t.Fatalf("no %s named %q in the rendered manifest", kindName, objectName)
	return nil
}

// cronJobs returns every rendered CronJob, whatever its name.
func cronJobs(t *testing.T, docs []map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, doc := range docs {
		if kind(doc) == "CronJob" {
			out = append(out, doc)
		}
	}
	if len(out) == 0 {
		t.Fatal("no CronJob in the rendered manifest")
	}
	return out
}

func at(t *testing.T, doc map[string]any, path ...string) map[string]any {
	t.Helper()
	cur := doc
	for _, key := range path {
		next, ok := cur[key].(map[string]any)
		if !ok {
			t.Fatalf("%s: no mapping at %q (path %v)", name(doc), key, path)
		}
		cur = next
	}
	return cur
}

// TestConfigIsRequired: an empty config must fail the render, not produce a
// CronJob that cannot start.
func TestConfigIsRequired(t *testing.T) {
	_, err := render(t)
	if err == nil {
		t.Fatal("helm template succeeded with no config value; an empty configuration must fail the render")
	}
	if !strings.Contains(err.Error(), "config is required") {
		t.Fatalf("the render failed for the wrong reason: %v", err)
	}
}

// TestCatalogConfigMapIsKeptAndOwnsNoData: the tool writes `data`, so the chart
// must never render it, and an uninstall must not take the run history away.
func TestCatalogConfigMapIsKeptAndOwnsNoData(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	cm := find(t, docs, "ConfigMap", catalogName)
	if _, ok := cm["data"]; ok {
		t.Error("the catalog ConfigMap declares data; the tool owns it and the chart must never write it")
	}
	annotations, _ := meta(cm)["annotations"].(map[string]any)
	if got, _ := annotations["helm.sh/resource-policy"].(string); got != "keep" {
		t.Errorf("the catalog ConfigMap annotation helm.sh/resource-policy is %q, want %q", got, "keep")
	}
}

// TestCatalogRoleGrantsOnlyGetAndPatchOnThatName: the catalog ConfigMap is the
// only object the tool writes, so the grant must be exactly that.
func TestCatalogRoleGrantsOnlyGetAndPatchOnThatName(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	role := find(t, docs, "Role", fullname+"-catalog")
	rules, ok := role["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("the catalog Role has %d rules, want exactly 1", len(rules))
	}
	rule, _ := rules[0].(map[string]any)
	if got := strings.Join(stringsOf(rule["verbs"]), ","); got != "get,patch" {
		t.Errorf("the catalog Role verbs are %q, want get,patch (patch, not update: the tool writes a merge patch)", got)
	}
	if got := strings.Join(stringsOf(rule["resources"]), ","); got != "configmaps" {
		t.Errorf("the catalog Role resources are %q, want configmaps", got)
	}
	if got := strings.Join(stringsOf(rule["resourceNames"]), ","); got != catalogName {
		t.Errorf("the catalog Role resourceNames are %q, want %q", got, catalogName)
	}
}

// TestNoClusterWideSecretsAccess: a ClusterRole may name secrets only when no
// ClusterRoleBinding reaches it, so the grant stays scoped by the per-namespace
// RoleBindings the consumer opts into.
func TestNoClusterWideSecretsAccess(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	bound := make(map[string]bool)
	for _, doc := range docs {
		if kind(doc) != "ClusterRoleBinding" {
			continue
		}
		ref, _ := doc["roleRef"].(map[string]any)
		bound[ref["name"].(string)] = true
	}
	for _, doc := range docs {
		if kind(doc) != "ClusterRole" || !grantsSecrets(doc) {
			continue
		}
		if bound[name(doc)] {
			t.Errorf("ClusterRole %s grants secrets and is bound by a ClusterRoleBinding: that is cluster-wide secrets access", name(doc))
		}
	}
	if len(bound) == 0 {
		t.Fatal("no ClusterRoleBinding was rendered; the discovery cannot list pods")
	}
}

func grantsSecrets(clusterRole map[string]any) bool {
	rules, _ := clusterRole["rules"].([]any)
	for _, raw := range rules {
		rule, _ := raw.(map[string]any)
		if contains(stringsOf(rule["resources"]), "secrets") {
			return true
		}
	}
	return false
}

// TestCronJobPodLabelsMatch: one NetworkPolicy must match every run, so all
// three pod templates carry the same labels.
func TestCronJobPodLabelsMatch(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	jobs := cronJobs(t, docs)
	if len(jobs) != 3 {
		t.Fatalf("rendered %d CronJobs, want 3 (scheduled, manual, restore)", len(jobs))
	}
	var want string
	for i, job := range jobs {
		labels := at(t, job, "spec", "jobTemplate", "spec", "template", "metadata")["labels"]
		got, err := yaml.Marshal(labels)
		if err != nil {
			t.Fatalf("encoding the pod labels of %s: %v", name(job), err)
		}
		if i == 0 {
			want = string(got)
			continue
		}
		if string(got) != want {
			t.Errorf("CronJob %s has pod labels\n%s\nwant the same as %s:\n%s", name(job), got, name(jobs[0]), want)
		}
	}
}

// TestEveryContainerSurfacesItsTerminationMessage: the previous-run report
// reads the last log lines from the terminated container's message, which only
// FallbackToLogsOnError fills.
func TestEveryContainerSurfacesItsTerminationMessage(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	checked := 0
	for _, doc := range docs {
		var specs []map[string]any
		switch kind(doc) {
		case "CronJob":
			specs = append(specs, at(t, doc, "spec", "jobTemplate", "spec", "template", "spec"))
		case "Job":
			specs = append(specs, at(t, doc, "spec", "template", "spec"))
		default:
			continue
		}
		for _, spec := range specs {
			for _, field := range []string{"containers", "initContainers"} {
				containers, _ := spec[field].([]any)
				for _, raw := range containers {
					container, _ := raw.(map[string]any)
					checked++
					if got, _ := container["terminationMessagePolicy"].(string); got != "FallbackToLogsOnError" {
						t.Errorf("%s %s container %v has terminationMessagePolicy %q, want FallbackToLogsOnError",
							kind(doc), name(doc), container["name"], got)
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no container was checked; the test found no pod spec")
	}
}

// TestMinimalExampleIsRestrictedAndTouchesNoHostPath: the default values pass
// the restricted Pod Security Standard and read nothing from the node.
func TestMinimalExampleIsRestrictedAndTouchesNoHostPath(t *testing.T) {
	manifest := renderExample(t, "minimal")
	if strings.Contains(manifest, "hostPath") {
		t.Error("the minimal example renders a hostPath volume")
	}
	docs := documents(t, manifest)
	for _, job := range cronJobs(t, docs) {
		pod := at(t, job, "spec", "jobTemplate", "spec", "template", "spec")
		security, _ := pod["securityContext"].(map[string]any)
		if security["runAsNonRoot"] != true {
			t.Errorf("CronJob %s: pod securityContext.runAsNonRoot is %v, want true", name(job), security["runAsNonRoot"])
		}
		seccomp, _ := security["seccompProfile"].(map[string]any)
		if seccomp["type"] != "RuntimeDefault" {
			t.Errorf("CronJob %s: seccompProfile.type is %v, want RuntimeDefault", name(job), seccomp["type"])
		}
		containers, _ := pod["containers"].([]any)
		for _, raw := range containers {
			container, _ := raw.(map[string]any)
			sc, _ := container["securityContext"].(map[string]any)
			if sc["allowPrivilegeEscalation"] != false || sc["readOnlyRootFilesystem"] != true {
				t.Errorf("CronJob %s container %v: securityContext is %v, want allowPrivilegeEscalation=false and readOnlyRootFilesystem=true",
					name(job), container["name"], sc)
			}
			capabilities, _ := sc["capabilities"].(map[string]any)
			if !contains(stringsOf(capabilities["drop"]), "ALL") {
				t.Errorf("CronJob %s container %v: capabilities.drop is %v, want ALL", name(job), container["name"], capabilities["drop"])
			}
		}
	}
}

// TestConfigGateHookRunsBeforeInstallAndUpgrade.
func TestConfigGateHookRunsBeforeInstallAndUpgrade(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	job := find(t, docs, "Job", fullname+"-validate")
	annotations, _ := meta(job)["annotations"].(map[string]any)
	hook, _ := annotations["helm.sh/hook"].(string)
	if !strings.Contains(hook, "pre-install") || !strings.Contains(hook, "pre-upgrade") {
		t.Errorf("the config-gate Job hook is %q, want it to run on pre-install and pre-upgrade", hook)
	}
	// The configuration the hook validates must be the new one, not the
	// release's own ConfigMap, which is applied after the hooks.
	spec := at(t, job, "spec", "template", "spec")
	volumes, _ := spec["volumes"].([]any)
	if len(volumes) != 1 {
		t.Fatalf("the config-gate Job mounts %d volumes, want exactly the new configuration", len(volumes))
	}
	volume, _ := volumes[0].(map[string]any)
	configMap, _ := volume["configMap"].(map[string]any)
	if configMap["name"] != fullname+"-validate" {
		t.Errorf("the config-gate Job mounts ConfigMap %v, want %s", configMap["name"], fullname+"-validate")
	}
}

// TestObjectNamesDeriveFromFullname: a consumer binds a NetworkPolicy or a
// RoleBinding to these names, so they must be stable and documented.
func TestObjectNamesDeriveFromFullname(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	names := make([]string, 0, len(docs))
	for _, doc := range docs {
		got := name(doc)
		names = append(names, got)
		if got == catalogName {
			// Named by the configuration, not by the chart.
			continue
		}
		if !strings.HasPrefix(got, fullname) {
			t.Errorf("%s %q does not derive from the release fullname %q", kind(doc), got, fullname)
		}
	}
	sort.Strings(names)
	for _, want := range []string{
		fullname,
		fullname + "-config",
		fullname + "-manual",
		fullname + "-restore",
		fullname + "-validate",
	} {
		if !contains(names, want) {
			t.Errorf("no object named %q was rendered; names are %v", want, names)
		}
	}
}

// TestLeaseHardBoundInvariant: the chart refuses to render a Job deadline above
// the config's run.lease_hard_bound, because the lease takeover assumes a writer
// cannot outlive that bound. The duration must be read with Go's grammar — a
// token scan that finds `5h` inside `1.5h` would read 1.5h as five hours and let
// a deployment through with a bound five times too large.
func TestLeaseHardBoundInvariant(t *testing.T) {
	cases := []struct {
		name           string
		leaseHardBound string
		wantErr        bool
	}{
		{"seconds above the deadline", "9000s", false},
		{"hours and minutes", "2h20m", false},
		{"exactly the deadline", "8400s", false},
		{"fractional hours above the deadline", "2.5h", false},
		{"fractional minutes above the deadline", "140.5m", false},
		// 1.5h is 5400s: below the 8400s deadline. A token scan reads it as 5h.
		{"fractional hours below the deadline", "1.5h", true},
		{"minutes and seconds below the deadline", "90m30s", true},
		{"a bare number", "9000", true},
		{"a fraction with no unit", "1.5", true},
		{"garbage", "banana", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := render(t,
				"--set", "activeDeadlineSeconds=8400",
				"--set-file", "config="+stubConfig(t, tc.leaseHardBound),
			)
			if tc.wantErr && err == nil {
				t.Fatalf("run.lease_hard_bound %q rendered clean, want a failure", tc.leaseHardBound)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("run.lease_hard_bound %q failed to render: %v", tc.leaseHardBound, err)
			}
		})
	}
}

// TestLeaseHardBoundGrammarMatchesGo: the chart's duration parser must accept
// exactly what time.ParseDuration accepts for a non-negative value, and reject
// everything else. A parser that disagrees in either direction is wrong: one
// that accepts more reads a bound as a duration the tool would refuse, one that
// accepts less refuses a configuration the tool itself accepts.
func TestLeaseHardBoundGrammarMatchesGo(t *testing.T) {
	const deadline = 100
	inputs := []string{
		"0", "+0", "0s", "0h0m", "1ns", "500us", "1µs", "1μs", "1ms", "999ms",
		"1s", "1.5s", ".5s", "1.s", "99s", "100s", "+100s", "100.5s", "0.5h",
		"1m40s", "2h", "3.h", "+3h", "1h2m3s", "1.5h",
		"-1h", "-0.5s", "1", "s", "1x", "1h30", "1e3s", "1h 30m", "abc",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			args := []string{
				"--set", fmt.Sprintf("activeDeadlineSeconds=%d", deadline),
				"--set-file", "config=" + stubConfig(t, input),
			}
			_, renderErr := render(t, args...)
			parsed, parseErr := time.ParseDuration(input)
			switch {
			case parseErr != nil || parsed < 0:
				if renderErr == nil {
					t.Fatalf("time.ParseDuration rejects %q and the chart rendered it", input)
				}
				if !strings.Contains(renderErr.Error(), "is not a non-negative duration") {
					t.Fatalf("time.ParseDuration rejects %q and the chart failed for another reason: %v", input, renderErr)
				}
			case parsed.Seconds() < deadline:
				if renderErr == nil || !strings.Contains(renderErr.Error(), "is below activeDeadlineSeconds") {
					t.Fatalf("time.ParseDuration reads %q as %s, below the %ds deadline, and the chart did not say so: %v",
						input, parsed, deadline, renderErr)
				}
			default:
				if renderErr != nil {
					t.Fatalf("time.ParseDuration reads %q as %s, above the %ds deadline, and the chart rejected it: %v",
						input, parsed, deadline, renderErr)
				}
			}
		})
	}
}

// stubConfig is the smallest document the render checks read.
func stubConfig(t *testing.T, leaseHardBound string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	// Quoted: `0` must stay a string, and `.5h` must not be read as a float.
	body := fmt.Sprintf("version: 1\nrun:\n  lease_hard_bound: %q\n", leaseHardBound)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the stub configuration: %v", err)
	}
	return path
}

// TestExampleInlineConfigsMatchMinimal: the flux and argocd examples carry a
// copy of examples/config/minimal.yaml, because an OCI chart source cannot read
// a file out of this repository. A copy that drifts from the document the tool
// actually validates would stop an install at the chart's config gate, so the
// equality is asserted, the copy is run through the real validator, and the
// example's own values are rendered through the chart — the install those two
// manifests describe.
func TestExampleInlineConfigsMatchMinimal(t *testing.T) {
	root := repoRoot(t)
	want, err := os.ReadFile(filepath.Join(root, "examples", "config", "minimal.yaml"))
	if err != nil {
		t.Fatalf("reading examples/config/minimal.yaml: %v", err)
	}
	examples := map[string]exampleValues{
		"examples/flux/helmrelease.yaml":   fluxExample(t, root),
		"examples/argocd/application.yaml": argocdExample(t, root),
	}
	validator := buildValidator(t, root)
	for path, example := range examples {
		if example.config != string(want) {
			t.Errorf("%s carries a configuration that differs from examples/config/minimal.yaml: %s",
				path, firstDifference(string(want), example.config))
		}
		valuesFile := filepath.Join(t.TempDir(), "values.yaml")
		if err := os.WriteFile(valuesFile, []byte(example.valuesYAML), 0o600); err != nil {
			t.Fatalf("writing the values of %s: %v", path, err)
		}
		if _, err := render(t, "-f", valuesFile); err != nil {
			t.Errorf("the values of %s do not render the chart: %v", path, err)
		}
		configFile := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(configFile, []byte(example.config), 0o600); err != nil {
			t.Fatalf("writing the configuration of %s: %v", path, err)
		}
		cmd := exec.Command(validator, "validate", "--config", configFile)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("go-cluster-backup validate rejected the configuration in %s: %v\n%s", path, err, out)
		}
	}
}

// exampleValues is one example's chart values and the configuration inside them.
type exampleValues struct {
	valuesYAML string
	config     string
}

func fluxExample(t *testing.T, root string) exampleValues {
	t.Helper()
	doc := loadYAML(t, filepath.Join(root, "examples", "flux", "helmrelease.yaml"))
	values, ok := nested(doc, "spec", "values").(map[string]any)
	if !ok {
		t.Fatal("examples/flux/helmrelease.yaml has no spec.values mapping")
	}
	config, ok := values["config"].(string)
	if !ok {
		t.Fatal("examples/flux/helmrelease.yaml has no spec.values.config string")
	}
	encoded, err := yaml.Marshal(values)
	if err != nil {
		t.Fatalf("encoding the values of examples/flux/helmrelease.yaml: %v", err)
	}
	return exampleValues{valuesYAML: string(encoded), config: config}
}

func argocdExample(t *testing.T, root string) exampleValues {
	t.Helper()
	doc := loadYAML(t, filepath.Join(root, "examples", "argocd", "application.yaml"))
	values, ok := nested(doc, "spec", "source", "helm", "values").(string)
	if !ok {
		t.Fatal("examples/argocd/application.yaml has no spec.source.helm.values string")
	}
	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(values), &parsed); err != nil {
		t.Fatalf("the Argo CD values block is not YAML: %v", err)
	}
	config, ok := parsed["config"].(string)
	if !ok {
		t.Fatal("the Argo CD values block has no config string")
	}
	return exampleValues{valuesYAML: values, config: config}
}

func loadYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("%s is not YAML: %v", path, err)
	}
	return doc
}

func nested(doc map[string]any, keys ...string) any {
	var current any = doc
	for _, key := range keys {
		mapping, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = mapping[key]
	}
	return current
}

func firstDifference(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var wantLine, gotLine string
		if i < len(wantLines) {
			wantLine = wantLines[i]
		}
		if i < len(gotLines) {
			gotLine = gotLines[i]
		}
		if wantLine != gotLine {
			return fmt.Sprintf("line %d: want %q, got %q", i+1, wantLine, gotLine)
		}
	}
	return "the documents are equal line by line"
}

var (
	validatorOnce sync.Once
	validatorPath string
	validatorErr  error
)

// buildValidator builds cmd/go-cluster-backup once per test run: the chart's
// config-gate hook runs exactly this binary, so the examples are checked with
// the real validator and not a re-implementation of it.
func buildValidator(t *testing.T, root string) string {
	t.Helper()
	validatorOnce.Do(func() {
		dir, err := os.MkdirTemp("", "go-cluster-backup-validate")
		if err != nil {
			validatorErr = err
			return
		}
		validatorPath = filepath.Join(dir, "go-cluster-backup")
		cmd := exec.Command("go", "build", "-o", validatorPath, "./cmd/go-cluster-backup")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			validatorErr = fmt.Errorf("building the validator: %w\n%s", err, out)
		}
	})
	if validatorErr != nil {
		t.Fatalf("%v", validatorErr)
	}
	return validatorPath
}

// TestReleaseNamespaceRoleReadsEvents: the previous-run report explains a Job
// that failed before the tool started by listing the Events of that Job's uid.
func TestReleaseNamespaceRoleReadsEvents(t *testing.T) {
	docs := documents(t, renderExample(t, "minimal"))
	role := find(t, docs, "Role", fullname)
	for _, want := range []struct{ apiGroup, resource, verb string }{
		{"", "pods", "get"},
		{"", "pods", "list"},
		{"batch", "jobs", "get"},
		{"batch", "jobs", "list"},
		{"", "events", "list"},
	} {
		if !grants(role, want.apiGroup, want.resource, want.verb) {
			t.Errorf("Role %s does not grant %s on %s in %q", fullname, want.verb, want.resource, want.apiGroup)
		}
	}
}

func grants(role map[string]any, apiGroup, resource, verb string) bool {
	rules, _ := role["rules"].([]any)
	for _, raw := range rules {
		rule, _ := raw.(map[string]any)
		if !contains(stringsOf(rule["resources"]), resource) {
			continue
		}
		if !contains(stringsOf(rule["apiGroups"]), apiGroup) {
			continue
		}
		if contains(stringsOf(rule["verbs"]), verb) {
			return true
		}
	}
	return false
}

// TestNotesPreflightRecipeFollowsTheRestoreArgs: a Job made from the restore
// CronJob runs whatever restore.args says, so offering it as the preflight is
// only safe while those args are the preflight. With an apply configured, NOTES
// must offer a one-off Job instead.
func TestNotesPreflightRecipeFollowsTheRestoreArgs(t *testing.T) {
	root := repoRoot(t)
	base := []string{
		"-f", filepath.Join(root, "examples", "values", "minimal.yaml"),
		"--set-file", "config=" + filepath.Join(root, "examples", "config", "minimal.yaml"),
	}
	// The exact preflight line, not the bare `create job --from=…-restore` the
	// restore section prints for a restore of its own.
	recipe := `create job --from=cronjob/` + fullname + `-restore "$job"`

	notes := renderNotes(t, base...)
	if !strings.Contains(notes, recipe) {
		t.Errorf("the default restore.args are a preflight, so NOTES should offer the restore CronJob:\n%s", notes)
	}

	unsafe := filepath.Join(t.TempDir(), "unsafe.yaml")
	values := "restore:\n  args: [\"restore\", \"apply\", \"--mode\", \"in-place\", \"--confirm\", \"x\"]\n"
	if err := os.WriteFile(unsafe, []byte(values), 0o600); err != nil {
		t.Fatalf("writing the unsafe values: %v", err)
	}
	args := append(append([]string{}, base...), "-f", unsafe)
	notes = renderNotes(t, args...)
	if strings.Contains(notes, recipe) {
		t.Errorf("restore.args is an apply, so NOTES must not offer the restore CronJob as the check:\n%s", notes)
	}

	// The replacement must be the restore pod itself, with the preflight args
	// forced in: a Job that mounted only the configuration would fail on a
	// healthy deployment, whose credentials live in the secret mounts and whose
	// discovery writes into the work volumes.
	jobYAML := notesHeredoc(t, notes)
	var job map[string]any
	if err := yaml.Unmarshal([]byte(jobYAML), &job); err != nil {
		t.Fatalf("the Job NOTES prints is not YAML: %v\n%s", err, jobYAML)
	}
	if kind(job) != "Job" {
		t.Fatalf("NOTES prints a %s, want a Job:\n%s", kind(job), jobYAML)
	}
	preflightPod := at(t, job, "spec", "template", "spec")

	manifest, err := render(t, args...)
	if err != nil {
		t.Fatalf("rendering the chart for its restore CronJob: %v", err)
	}
	restore := find(t, documents(t, manifest), "CronJob", fullname+"-restore")
	restorePod := at(t, restore, "spec", "jobTemplate", "spec", "template", "spec")

	if got, want := mustJSON(t, preflightPod["volumes"]), mustJSON(t, restorePod["volumes"]); got != want {
		t.Errorf("the preflight Job's volumes differ from the restore CronJob's:\n got %s\nwant %s", got, want)
	}
	mounts := containerVolumeMounts(t, preflightPod)
	if len(mounts) == 0 {
		t.Fatal("the preflight Job's container mounts nothing")
	}
	if got, want := mustJSON(t, mounts), mustJSON(t, containerVolumeMounts(t, restorePod)); got != want {
		t.Errorf("the preflight Job's volumeMounts differ from the restore CronJob's:\n got %s\nwant %s", got, want)
	}
	wantArgs := "validate --preflight --config /etc/go-cluster-backup/config.yaml"
	if got := strings.Join(stringsOf(firstContainer(t, preflightPod)["args"]), " "); got != wantArgs {
		t.Errorf("the preflight Job runs %q, want %q", got, wantArgs)
	}

	kubeconform(t, root, jobYAML)
}

// notesHeredoc returns the manifest between the `apply -f - <<EOF` line of
// NOTES and its terminating `EOF`.
func notesHeredoc(t *testing.T, notes string) string {
	t.Helper()
	lines := strings.Split(notes, "\n")
	start := -1
	for i, line := range lines {
		if strings.Contains(line, "apply -f - <<EOF") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("NOTES print no heredoc manifest:\n%s", notes)
	}
	for i := start; i < len(lines); i++ {
		if lines[i] == "EOF" {
			return strings.Join(lines[start:i], "\n") + "\n"
		}
	}
	t.Fatalf("the heredoc manifest in NOTES is never terminated:\n%s", notes)
	return ""
}

// kubeconform validates one rendered manifest with the tool the chart target
// uses.
func kubeconform(t *testing.T, root, manifest string) {
	t.Helper()
	cmd := exec.Command("go", "tool", "-modfile=tools/go.mod", "kubeconform",
		"-strict", "-summary", "-kubernetes-version", "1.31.0", "-")
	cmd.Dir = root
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the manifest is not valid for the Kubernetes API: %v\n%s", err, out)
	}
}

func firstContainer(t *testing.T, pod map[string]any) map[string]any {
	t.Helper()
	containers, ok := pod["containers"].([]any)
	if !ok || len(containers) == 0 {
		t.Fatal("the pod declares no container")
	}
	container, _ := containers[0].(map[string]any)
	return container
}

func containerVolumeMounts(t *testing.T, pod map[string]any) []any {
	t.Helper()
	mounts, _ := firstContainer(t, pod)["volumeMounts"].([]any)
	return mounts
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encoding a rendered value: %v", err)
	}
	return string(encoded)
}

// TestArgocdApplicationCreatesItsNamespace: the chart creates no Namespace, so
// the Application must create its destination namespace itself.
func TestArgocdApplicationCreatesItsNamespace(t *testing.T) {
	root := repoRoot(t)
	doc := loadYAML(t, filepath.Join(root, "examples", "argocd", "application.yaml"))
	options, ok := nested(doc, "spec", "syncPolicy", "syncOptions").([]any)
	if !ok {
		t.Fatal("examples/argocd/application.yaml has no spec.syncPolicy.syncOptions, and the chart creates no Namespace")
	}
	if !contains(stringsOf(options), "CreateNamespace=true") {
		t.Errorf("spec.syncPolicy.syncOptions is %v, want it to contain CreateNamespace=true", options)
	}
}

// TestEveryPodHonoursTheSharedExtras: a deployment adds volumes, mounts and
// environment to the chart once, and every pod that runs the tool gets them —
// the stage initContainer and the config-gate hook Job included. A container
// the extras skip is a container a deployment cannot configure or instrument.
func TestEveryPodHonoursTheSharedExtras(t *testing.T) {
	root := repoRoot(t)
	manifest, err := render(t,
		"-f", filepath.Join(root, "examples", "values", "minimal.yaml"),
		"-f", extrasValues(t),
		"--set-file", "config="+filepath.Join(root, "examples", "config", "minimal.yaml"),
	)
	if err != nil {
		t.Fatalf("rendering the chart with extras: %v", err)
	}
	docs := documents(t, manifest)

	targets := []struct {
		what string
		pod  map[string]any
	}{
		{"the scheduled writer pod", at(t, find(t, docs, "CronJob", fullname), "spec", "jobTemplate", "spec", "template", "spec")},
		{"the manual writer pod", at(t, find(t, docs, "CronJob", fullname+"-manual"), "spec", "jobTemplate", "spec", "template", "spec")},
		{"the restore pod", at(t, find(t, docs, "CronJob", fullname+"-restore"), "spec", "jobTemplate", "spec", "template", "spec")},
		{"the config-gate hook pod", at(t, find(t, docs, "Job", fullname+"-validate"), "spec", "template", "spec")},
	}
	initContainers := 0
	for _, target := range targets {
		if volumes := mustJSON(t, target.pod["volumes"]); !strings.Contains(volumes, `"name":"cover"`) {
			t.Errorf("%s does not carry the extra volume: %s", target.what, volumes)
		}
		for _, field := range []string{"containers", "initContainers"} {
			containers, _ := target.pod[field].([]any)
			for _, raw := range containers {
				container, _ := raw.(map[string]any)
				where := fmt.Sprintf("%s, %s %v", target.what, field, container["name"])
				if got := mustJSON(t, container["volumeMounts"]); !strings.Contains(got, `"/gocover"`) {
					t.Errorf("%s does not mount the extra volume: %s", where, got)
				}
				if got := mustJSON(t, container["env"]); !strings.Contains(got, `"GOCOVERDIR"`) {
					t.Errorf("%s does not carry the extra environment: %s", where, got)
				}
			}
		}
		initContainers += len(asList(target.pod["initContainers"]))
	}
	if initContainers == 0 {
		t.Error("no initContainer was rendered, so the stage extras are not proven")
	}
}

// extrasValues is a values file that turns on the stage initContainer and adds
// one volume, one mount and one environment variable to the shared surface.
func extrasValues(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "extras.yaml")
	body := `stage:
  enabled: true
extraVolumes:
  - name: cover
    hostPath:
      path: /tmp/gocover
      type: DirectoryOrCreate
extraVolumeMounts:
  - name: cover
    mountPath: /gocover
extraEnv:
  - name: GOCOVERDIR
    valueFrom:
      secretKeyRef:
        name: cover-config
        key: dir
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the extras values: %v", err)
	}
	return path
}

// TestEveryExamplePodHasUniqueMounts: the API server rejects a pod with a
// duplicate mountPath or volume name. The shared extras reach every container,
// so an example that also lists a mount the extras already carry renders a pod
// the cluster refuses.
func TestEveryExamplePodHasUniqueMounts(t *testing.T) {
	for _, example := range []string{"minimal", "k3s-single-node"} {
		for _, pod := range podSpecs(t, documents(t, renderExample(t, example))) {
			volumes := map[string]bool{}
			for _, raw := range asList(pod["volumes"]) {
				volume, _ := raw.(map[string]any)
				name, _ := volume["name"].(string)
				if volumes[name] {
					t.Errorf("%s: volume %q is declared twice in one pod", example, name)
				}
				volumes[name] = true
			}
			for _, field := range []string{"containers", "initContainers"} {
				for _, raw := range asList(pod[field]) {
					container, _ := raw.(map[string]any)
					mounts := map[string]bool{}
					for _, entry := range asList(container["volumeMounts"]) {
						mount, _ := entry.(map[string]any)
						path, _ := mount["mountPath"].(string)
						if mounts[path] {
							t.Errorf("%s: container %v mounts %q twice", example, container["name"], path)
						}
						mounts[path] = true
						if name, _ := mount["name"].(string); name != "" && !volumes[name] {
							t.Errorf("%s: container %v mounts volume %q, which the pod does not declare",
								example, container["name"], name)
						}
					}
				}
			}
		}
	}
}

// podSpecs returns every pod spec a rendered manifest contains.
func podSpecs(t *testing.T, docs []map[string]any) []map[string]any {
	t.Helper()
	var specs []map[string]any
	for _, doc := range docs {
		switch kind(doc) {
		case "CronJob":
			specs = append(specs, at(t, doc, "spec", "jobTemplate", "spec", "template", "spec"))
		case "Job":
			specs = append(specs, at(t, doc, "spec", "template", "spec"))
		}
	}
	if len(specs) == 0 {
		t.Fatal("the manifest rendered no pod")
	}
	return specs
}

// isolationValues gives the writer, the restore and the shared extras each a
// distinct hostPath volume, plus the scheduling a deployment sets once for every
// pod, and turns on the stage initContainer.
func isolationValues(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "isolation.yaml")
	body := `securityContext:
  runAsNonRoot: false
  runAsUser: 0
  runAsGroup: 0
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
podSecurityContext:
  runAsNonRoot: false
  runAsUser: 0
  runAsGroup: 0
  fsGroup: 0
  fsGroupChangePolicy: OnRootMismatch
  seccompProfile:
    type: RuntimeDefault
priorityClassName: high
nodeSelector:
  kubernetes.io/hostname: node-a
tolerations:
  - key: dedicated
    operator: Exists
    effect: NoSchedule
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: role
              operator: In
              values: [backup]
writer:
  volumes:
    - name: writer-node
      hostPath:
        path: /node/writer
        type: Directory
  volumeMounts:
    - name: writer-node
      mountPath: /writer-node
restore:
  volumes:
    - name: restore-node
      hostPath:
        path: /node/restore
        type: Directory
  volumeMounts:
    - name: restore-node
      mountPath: /restore-node
stage:
  enabled: true
  volumeMounts:
    - name: writer-node
      mountPath: /stage-writer
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the isolation values: %v", err)
	}
	return path
}

// renderIsolation renders the minimal example with the isolation values and the
// shared extras (the coverage volume and GOCOVERDIR, reused rather than
// duplicated).
func renderIsolation(t *testing.T) []map[string]any {
	t.Helper()
	root := repoRoot(t)
	manifest, err := render(t,
		"-f", filepath.Join(root, "examples", "values", "minimal.yaml"),
		"-f", isolationValues(t),
		"-f", extrasValues(t),
		"--set-file", "config="+filepath.Join(root, "examples", "config", "minimal.yaml"),
	)
	if err != nil {
		t.Fatalf("rendering the chart with the isolation values: %v", err)
	}
	return documents(t, manifest)
}

func volumeNames(t *testing.T, pod map[string]any) []string {
	t.Helper()
	var out []string
	for _, raw := range asList(pod["volumes"]) {
		volume, _ := raw.(map[string]any)
		if n, _ := volume["name"].(string); n != "" {
			out = append(out, n)
		}
	}
	return out
}

func containerMountPaths(t *testing.T, container map[string]any) []string {
	t.Helper()
	var out []string
	for _, raw := range asList(container["volumeMounts"]) {
		mount, _ := raw.(map[string]any)
		if path, _ := mount["mountPath"].(string); path != "" {
			out = append(out, path)
		}
	}
	return out
}

func containerByName(t *testing.T, pod map[string]any, field, containerName string) map[string]any {
	t.Helper()
	for _, raw := range asList(pod[field]) {
		container, _ := raw.(map[string]any)
		if container["name"] == containerName {
			return container
		}
	}
	t.Fatalf("the pod has no %s container named %q", field, containerName)
	return nil
}

func writerPod(t *testing.T, docs []map[string]any, name string) map[string]any {
	t.Helper()
	return at(t, find(t, docs, "CronJob", name), "spec", "jobTemplate", "spec", "template", "spec")
}

// TestWriterRestoreHookMountsAreIsolated: a node hostPath holding state or
// secrets must reach one pod alone. `writer.*` lands in the writer pods,
// `restore.*` in the restore pod, and only the innocuous shared extras reach
// every pod — a node path left in `extraVolumes` leaks into the restore and
// config-gate hook pods.
func TestWriterRestoreHookMountsAreIsolated(t *testing.T) {
	docs := renderIsolation(t)
	writerPods := map[string]map[string]any{
		"the scheduled writer pod": writerPod(t, docs, fullname),
		"the manual writer pod":    writerPod(t, docs, fullname+"-manual"),
	}
	restorePod := at(t, find(t, docs, "CronJob", fullname+"-restore"), "spec", "jobTemplate", "spec", "template", "spec")
	hookPod := at(t, find(t, docs, "Job", fullname+"-validate"), "spec", "template", "spec")

	for what, pod := range writerPods {
		volumes := volumeNames(t, pod)
		if !contains(volumes, "writer-node") {
			t.Errorf("%s declares %v, want writer.volumes", what, volumes)
		}
		if !contains(volumes, "cover") {
			t.Errorf("%s declares %v, want the shared extra volume", what, volumes)
		}
		if contains(volumes, "restore-node") {
			t.Errorf("%s declares a restore.* volume: %v", what, volumes)
		}
		mounts := containerMountPaths(t, containerByName(t, pod, "containers", "backup"))
		if !contains(mounts, "/writer-node") || !contains(mounts, "/gocover") {
			t.Errorf("%s mounts %v on its writer container, want /writer-node and /gocover", what, mounts)
		}
		if contains(mounts, "/restore-node") {
			t.Errorf("%s mounts a restore.* path on its writer container: %v", what, mounts)
		}
		// The stage initContainer shares the writer pod, so it may reference
		// writer.volumes through its own volumeMounts — and only through them.
		stage := containerMountPaths(t, containerByName(t, pod, "initContainers", "stage"))
		if !contains(stage, "/stage-writer") || !contains(stage, "/gocover") {
			t.Errorf("%s mounts %v on its stage container, want /stage-writer (a writer.volume) and /gocover", what, stage)
		}
		if contains(stage, "/writer-node") {
			t.Errorf("%s mounts the writer container's own writer.volumeMounts on the stage container: %v", what, stage)
		}
	}

	restoreVolumes := volumeNames(t, restorePod)
	if !contains(restoreVolumes, "restore-node") || !contains(restoreVolumes, "cover") {
		t.Errorf("the restore pod declares %v, want restore.volumes and the shared extra volume", restoreVolumes)
	}
	if contains(restoreVolumes, "writer-node") {
		t.Errorf("the restore pod declares a writer.* volume: %v", restoreVolumes)
	}
	restoreMounts := containerMountPaths(t, containerByName(t, restorePod, "containers", "restore"))
	if !contains(restoreMounts, "/restore-node") || !contains(restoreMounts, "/gocover") {
		t.Errorf("the restore container mounts %v, want /restore-node and /gocover", restoreMounts)
	}
	if contains(restoreMounts, "/writer-node") {
		t.Errorf("the restore container mounts a writer.* path: %v", restoreMounts)
	}

	hookVolumes := volumeNames(t, hookPod)
	if !contains(hookVolumes, "cover") {
		t.Errorf("the hook pod declares %v, want the shared extra volume", hookVolumes)
	}
	hookMounts := containerMountPaths(t, firstContainer(t, hookPod))
	if !contains(hookMounts, "/gocover") {
		t.Errorf("the hook container mounts %v, want the shared /gocover", hookMounts)
	}
	for _, leaked := range []string{"writer-node", "restore-node"} {
		if contains(hookVolumes, leaked) {
			t.Errorf("the hook pod declares the node volume %q; a node hostPath stays in writer.*/restore.*", leaked)
		}
	}
	for _, leaked := range []string{"/writer-node", "/restore-node"} {
		if contains(hookMounts, leaked) {
			t.Errorf("the hook container mounts %q; a node hostPath stays in writer.*/restore.*", leaked)
		}
	}
}

// TestRestoreAndHookDoNotInheritTheWritersSecurityContext: the writer runs as
// root in this fixture; the restore and hook containers carry their own
// restricted non-root securityContext, so a root writer never makes them root.
// The hook pod also carries the shared scheduling, which it used to drop.
func TestRestoreAndHookDoNotInheritTheWritersSecurityContext(t *testing.T) {
	docs := renderIsolation(t)

	writer := containerByName(t, writerPod(t, docs, fullname), "containers", "backup")
	if sc, _ := writer["securityContext"].(map[string]any); sc["runAsUser"] != 0 || sc["runAsNonRoot"] != false {
		t.Fatalf("the writer container securityContext is %v, want the root top-level one this test contrasts against", sc)
	}

	for _, tc := range []struct {
		what      string
		container map[string]any
	}{
		{"the restore container", containerByName(t, at(t, find(t, docs, "CronJob", fullname+"-restore"), "spec", "jobTemplate", "spec", "template", "spec"), "containers", "restore")},
		{"the hook container", firstContainer(t, at(t, find(t, docs, "Job", fullname+"-validate"), "spec", "template", "spec"))},
	} {
		sc, _ := tc.container["securityContext"].(map[string]any)
		if sc["runAsNonRoot"] != true || sc["runAsUser"] != 1000 || sc["runAsGroup"] != 1000 {
			t.Errorf("%s securityContext is %v, want the restricted non-root default, independent from the writer's", tc.what, sc)
		}
		if sc["allowPrivilegeEscalation"] != false || sc["readOnlyRootFilesystem"] != true {
			t.Errorf("%s securityContext is %v, want allowPrivilegeEscalation=false and readOnlyRootFilesystem=true", tc.what, sc)
		}
	}

	hookPod := at(t, find(t, docs, "Job", fullname+"-validate"), "spec", "template", "spec")
	for _, field := range []string{"nodeSelector", "tolerations", "affinity"} {
		if _, ok := hookPod[field]; !ok {
			t.Errorf("the hook pod has no %s, want the shared scheduling every pod carries", field)
		}
	}
	if got, _ := hookPod["priorityClassName"].(string); got != "high" {
		t.Errorf("the hook pod priorityClassName is %q, want the shared %q", got, "high")
	}
}

func podSecurityContext(t *testing.T, pod map[string]any) map[string]any {
	t.Helper()
	sc, ok := pod["securityContext"].(map[string]any)
	if !ok {
		t.Fatalf("the pod declares no pod-level securityContext")
	}
	return sc
}

// TestPodSecurityContextContract pins the pod-level half of the isolation, which
// the container tests do not reach. The writer pod carries the top-level
// `podSecurityContext` (root in this fixture); the config-gate hook pod carries
// its own restricted non-root one and never inherits the writer's; the restore
// pod inherits the top-level one while `restore.podSecurityContext` is empty,
// and a non-empty value replaces it wholesale.
func TestPodSecurityContextContract(t *testing.T) {
	docs := renderIsolation(t)

	writerPodContext := podSecurityContext(t, writerPod(t, docs, fullname))
	if writerPodContext["runAsUser"] != 0 || writerPodContext["fsGroup"] != 0 {
		t.Fatalf("the writer pod podSecurityContext is %v, want the root top-level one this test contrasts against", writerPodContext)
	}

	hookPodContext := podSecurityContext(t, at(t, find(t, docs, "Job", fullname+"-validate"), "spec", "template", "spec"))
	if hookPodContext["runAsNonRoot"] != true || hookPodContext["runAsUser"] != 1000 || hookPodContext["runAsGroup"] != 1000 {
		t.Errorf("the hook pod podSecurityContext is %v, want the restricted non-root default, independent from the writer's root one", hookPodContext)
	}
	if seccomp, _ := hookPodContext["seccompProfile"].(map[string]any); seccomp["type"] != "RuntimeDefault" {
		t.Errorf("the hook pod seccompProfile is %v, want RuntimeDefault", hookPodContext["seccompProfile"])
	}

	restorePodContext := podSecurityContext(t, at(t, find(t, docs, "CronJob", fullname+"-restore"), "spec", "jobTemplate", "spec", "template", "spec"))
	if got, want := mustJSON(t, restorePodContext), mustJSON(t, writerPodContext); got != want {
		t.Errorf("the restore pod podSecurityContext is %s, want the top-level one inherited while restore.podSecurityContext is empty (%s)", got, want)
	}

	// A non-empty restore.podSecurityContext replaces the inherited one whole.
	root := repoRoot(t)
	override := filepath.Join(t.TempDir(), "restore-context.yaml")
	body := `restore:
  podSecurityContext:
    runAsNonRoot: true
    runAsUser: 2000
    runAsGroup: 2000
    fsGroup: 2000
    fsGroupChangePolicy: OnRootMismatch
    seccompProfile: {type: RuntimeDefault}
`
	if err := os.WriteFile(override, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the restore pod context override: %v", err)
	}
	manifest, err := render(t,
		"-f", filepath.Join(root, "examples", "values", "minimal.yaml"),
		"-f", isolationValues(t),
		"-f", extrasValues(t),
		"-f", override,
		"--set-file", "config="+filepath.Join(root, "examples", "config", "minimal.yaml"),
	)
	if err != nil {
		t.Fatalf("rendering with the restore pod context override: %v", err)
	}
	restorePodContext = podSecurityContext(t, at(t, find(t, documents(t, manifest), "CronJob", fullname+"-restore"), "spec", "jobTemplate", "spec", "template", "spec"))
	if restorePodContext["runAsUser"] != 2000 || restorePodContext["fsGroup"] != 2000 {
		t.Errorf("the restore pod podSecurityContext is %v, want the full override (runAsUser 2000, fsGroup 2000), not the inherited top-level one", restorePodContext)
	}
}

// TestHookPodContextContractIsNotVacuous mutates the hook template to
// reintroduce the inheritance the contract above forbids, and proves the
// assertion fails on it: a contract that also passes on the bug it exists to
// catch proves nothing.
func TestHookPodContextContractIsNotVacuous(t *testing.T) {
	root := repoRoot(t)
	mutated := filepath.Join(t.TempDir(), "chart")
	if err := os.CopyFS(mutated, os.DirFS(filepath.Join(root, chartPath))); err != nil {
		t.Fatalf("copying the chart: %v", err)
	}
	hookPath := filepath.Join(mutated, "templates", "hook-validate.yaml")
	data, err := os.ReadFile(hookPath)
	if err != nil {
		t.Fatalf("reading the hook template: %v", err)
	}
	// The line the chart carried before the fix: the hook pod read the writer's
	// top-level context. (A `default`-based mutation would be a no-op, because
	// `hooks.podSecurityContext` is non-empty and `default` then returns it.)
	const inherited = "{{- toYaml .Values.podSecurityContext | nindent 8 }}"
	patched := strings.Replace(string(data), "{{- toYaml .Values.hooks.podSecurityContext | nindent 8 }}", inherited, 1)
	if patched == string(data) {
		t.Fatal("the hook pod securityContext line changed; update this mutation so the contract stays proven")
	}
	if err := os.WriteFile(hookPath, []byte(patched), 0o600); err != nil {
		t.Fatalf("writing the mutated hook template: %v", err)
	}
	manifest, err := runHelm(t, root, "template", releaseName, mutated,
		"-f", filepath.Join(root, "examples", "values", "minimal.yaml"),
		"-f", isolationValues(t),
		"-f", extrasValues(t),
		"--set-file", "config="+filepath.Join(root, "examples", "config", "minimal.yaml"),
	)
	if err != nil {
		t.Fatalf("rendering the mutated chart: %v", err)
	}
	hookPodContext := podSecurityContext(t, at(t, find(t, documents(t, manifest), "Job", fullname+"-validate"), "spec", "template", "spec"))
	if hookPodContext["runAsNonRoot"] == true && hookPodContext["runAsUser"] == 1000 {
		t.Fatalf("the mutated hook pod inherited the top-level root context yet still looks non-root, so the contract would pass on it: %v", hookPodContext)
	}
}

func asList(v any) []any {
	list, _ := v.([]any)
	return list
}

func stringsOf(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
