package kuberneteslabels_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery/discoverytest"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
)

const (
	testHostEnv = "GCB_TEST_HOST"
	testPortEnv = "GCB_TEST_PORT"
)

func registry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := kuberneteslabels.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// Fixtures are built as Go values and marshalled, so a test states only the
// parts of an object it cares about.

type envVar struct {
	Name      string         `json:"name"`
	Value     *string        `json:"value"`
	ValueFrom map[string]any `json:"valueFrom,omitempty"`
}

type containerPort struct {
	ContainerPort int `json:"containerPort"`
}

type container struct {
	Name    string          `json:"name"`
	Env     []envVar        `json:"env,omitempty"`
	EnvFrom []any           `json:"envFrom,omitempty"`
	Ports   []containerPort `json:"ports,omitempty"`
}

type volumeFixture struct {
	Name                  string `json:"name"`
	PersistentVolumeClaim struct {
		ClaimName string `json:"claimName"`
	} `json:"persistentVolumeClaim"`
}

type objectFixture struct {
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Containers []container     `json:"containers"`
		Volumes    []volumeFixture `json:"volumes,omitempty"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// claimFixture is a persistent volume claim: its metadata and phase, plus the
// volume it is bound to.
type claimFixture struct {
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		VolumeName string `json:"volumeName"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

func pod(namespace, name string, labels map[string]string, phase string, containers ...container) string {
	var p objectFixture
	p.Metadata.Name = name
	p.Metadata.Namespace = namespace
	p.Metadata.Labels = labels
	p.Spec.Containers = containers
	p.Status.Phase = phase
	return mustJSON(p)
}

func pvc(namespace, name string, labels map[string]string, phase string) string {
	return boundPVC(namespace, name, labels, phase, "")
}

// boundPVC is a claim bound to a named volume — what volumePath resolves.
func boundPVC(namespace, name string, labels map[string]string, phase, volumeName string) string {
	var p claimFixture
	p.Metadata.Name = name
	p.Metadata.Namespace = namespace
	p.Metadata.Labels = labels
	p.Spec.VolumeName = volumeName
	p.Status.Phase = phase
	return mustJSON(p)
}

// mountingPod is a pod whose volumes mount the named claims.
func mountingPod(namespace, name string, labels map[string]string, phase string, claims ...string) string {
	var p objectFixture
	p.Metadata.Name = name
	p.Metadata.Namespace = namespace
	p.Metadata.Labels = labels
	p.Status.Phase = phase
	for _, claim := range claims {
		var v volumeFixture
		v.Name = "volume-" + claim
		v.PersistentVolumeClaim.ClaimName = claim
		p.Spec.Volumes = append(p.Spec.Volumes, v)
	}
	return mustJSON(p)
}

// withCovers marks a pod rule as reading the data of the volumes its pod mounts.
func withCovers(r ruleFixture) ruleFixture {
	r.CoversVolumes = true
	return r
}

// withPhase is the rule with its phase replaced.
func (r ruleFixture) withPhase(phase string) ruleFixture {
	r.Phase = phase
	return r
}

func literal(name, value string) envVar {
	return envVar{Name: name, Value: &value}
}

// mustJSON marshals a fixture; it never fails for the shapes used here.
func mustJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

type artifactFixture struct {
	Group       string `json:"group"`
	Name        string `json:"name"`
	Importance  string `json:"importance"`
	Sensitivity string `json:"sensitivity"`
}

type sourceFixture struct {
	Type string `json:"type"`
	// Config is any JSON value: the plugin only requires it to be an object,
	// and a test needs to be able to send something that is not one.
	Config any `json:"config"`
}

// ruleFixture is one rule of either list: the two lists differ only in their
// resource and in whether a rule may cover volumes, so a fixture says which
// resource it is about and the config builder puts it in the right list.
type ruleFixture struct {
	Name          string
	Resource      string // "pods" or "persistentvolumeclaims"
	Selector      string
	Phase         string
	Artifact      artifactFixture
	Pipeline      []string
	Source        sourceFixture
	CoversVolumes bool
	MinMatches    int
}

// podRule renders as a pods entry: resource is the builder's business, and a
// pod rule may cover volumes.
func (r ruleFixture) podRule() map[string]any {
	return map[string]any{
		"name": r.Name, "selector": r.Selector, "phase": r.Phase, "artifact": r.Artifact,
		"pipeline": r.Pipeline, "source": r.Source, "covers_volumes": r.CoversVolumes,
		"min_matches": r.MinMatches,
	}
}

// volumeRule renders as a volumes entry: no covers_volumes, no resource.
func (r ruleFixture) volumeRule() map[string]any {
	return map[string]any{
		"name": r.Name, "selector": r.Selector, "phase": r.Phase, "artifact": r.Artifact,
		"pipeline": r.Pipeline, "source": r.Source, "min_matches": r.MinMatches,
	}
}

// undeclaredFixture is the setting behind the accounting.
type undeclaredFixture struct {
	Name       string `json:"name"`
	Importance string `json:"importance"`
}

type excludeFixture struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// undeclaredVolume is what the accounting reports an unaccounted claim as.
func undeclaredVolume() undeclaredFixture {
	return undeclaredFixture{Name: "undeclared", Importance: "required"}
}

type configFixture struct {
	APIHostEnv     string            `json:"api_host_env"`
	APIPortEnv     string            `json:"api_port_env"`
	TokenFile      string            `json:"token_file"`
	CAFile         string            `json:"ca_file"`
	RequestTimeout string            `json:"request_timeout"`
	SecretsDir     string            `json:"secrets_path"`
	ListPageSize   int               `json:"list_page_size"`
	Exclude        excludeFixture    `json:"exclude"`
	Rules          []ruleFixture     `json:"-"` // routed into the two lists below
	Undeclared     undeclaredFixture `json:"undeclared_volumes"`
}

// MarshalJSON is the fixture's one transformation: the rule list a fixture
// declared becomes the pods and volumes lists the config reads.
func (c configFixture) MarshalJSON() ([]byte, error) {
	type plain configFixture
	pods, volumes := []map[string]any{}, []map[string]any{}
	for _, r := range c.Rules {
		if r.Resource == "pods" {
			pods = append(pods, r.podRule())
			continue
		}
		volumes = append(volumes, r.volumeRule())
	}
	return json.Marshal(struct {
		plain
		Pods    []map[string]any `json:"pods"`
		Volumes []map[string]any `json:"volumes"`
	}{plain(c), pods, volumes})
}

// The marker the fixtures carry; a real deployment names its own.
const (
	testExcludeLabel = "backup.example/ignore"
	testExcludeValue = "true"
)

// simpleRule matches every object of its selector and renders without helpers.
func simpleRule(name, resource, selector, phase string) ruleFixture {
	return ruleFixture{
		Name: name, Resource: resource, Selector: selector, Phase: phase,
		Artifact: artifactFixture{Group: "group", Name: "{{ .Namespace }}/{{ .Name }}", Importance: "required", Sensitivity: "plain"},
		Pipeline: []string{"compress"},
		Source:   sourceFixture{Type: "files", Config: map[string]any{"root": "/volumes/{{ .Namespace }}/{{ .Name }}"}},
		// May be empty unless a test says otherwise; the minimum is the
		// subject of TestMinMatches.
		MinMatches: 0,
	}
}

func newDiscoverer(t *testing.T, srv *kubeapitest.Server, rules ...ruleFixture) discovery.Discoverer {
	t.Helper()
	return newDiscovererIn(t, srv, t.TempDir(), 500, rules...)
}

func newDiscovererIn(t *testing.T, srv *kubeapitest.Server, secretsPath string, pageSize int, rules ...ruleFixture) discovery.Discoverer {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: secretsPath, ListPageSize: pageSize,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      rules,
	}
	raw := mustJSON(cfg)
	d, err := plugin.Build(context.Background(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(raw), plugin.Deps{Instance: "workloads", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func discover(t *testing.T, d discovery.Discoverer) []discovery.Found {
	t.Helper()
	found, err := d.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// check runs preflight with the caller's test reporting.
func check(t *testing.T, d discovery.Discoverer) error {
	t.Helper()
	c, ok := d.(plugin.Checker)
	if !ok {
		t.Fatal("the discoverer does not implement plugin.Checker")
	}
	return c.Check(context.Background())
}

// problemOf runs Discover and returns the Problem of its only Found: the path a
// per-object render or resolve failure travels. Check deliberately stays silent
// about it — a run skips a discovery whose Check failed, so failing the check
// here would cost every other artifact of the discovery.
func problemOf(t *testing.T, d discovery.Discoverer) string {
	t.Helper()
	found, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover = %v, want the object reported as a Problem, not an aborted discovery", err)
	}
	if len(found) != 1 {
		t.Fatalf("found %d objects, want 1", len(found))
	}
	return found[0].Problem
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, registry(t))
}

func TestDiscoveryConformance(t *testing.T) {
	discoverytest.Run(t, func(t *testing.T) discovery.Discoverer {
		srv := kubeapitest.New(t)
		srv.AddPod(pod("example-prod", "postgres-0",
			map[string]string{"backup.example/enabled": "true", "backup.example/type": "database"},
			"Running", container{Name: "postgres", Ports: []containerPort{{ContainerPort: 5432}}}))
		srv.AddPVC(pvc("media", "media-media",
			map[string]string{"backup.example/enabled": "true", "backup.example/type": "pvc"}, "Bound"))
		return newDiscoverer(t, srv,
			simpleRule("databases", "pods", "backup.example/type=database", "Running"),
			simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound"))
	})
}

// TestOrderAndEnvelope: objects come out sorted by namespace and name whatever
// order the API returned them in, and every field of the envelope is rendered.
func TestOrderAndEnvelope(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("z-ns", "b-0", map[string]string{"backup.example/type": "database"}, "Running"))
	srv.AddPod(pod("a-ns", "b-0", map[string]string{"backup.example/type": "database"}, "Running"))
	srv.AddPod(pod("a-ns", "a-0", map[string]string{"backup.example/type": "database"}, "Running"))
	d := newDiscoverer(t, srv, simpleRule("databases", "pods", "backup.example/type=database", "Running"))

	found := discover(t, d)
	var ids []string
	for _, f := range found {
		ids = append(ids, f.Name)
	}
	want := []string{"databases/a-ns/a-0", "databases/a-ns/b-0", "databases/z-ns/b-0"}
	if strings.Join(ids, " ") != strings.Join(want, " ") {
		t.Fatalf("names = %v, want %v", ids, want)
	}
	f := found[0]
	if f.Type != "files" || !slices.Equal(f.Pipeline, []string{"compress"}) {
		t.Fatalf("found = %+v", f)
	}
	if f.Artifact.Group != "group" || f.Artifact.Name != "a-ns/a-0" ||
		f.Artifact.Importance != "required" || f.Artifact.Sensitivity != "plain" {
		t.Fatalf("artifact = %+v", f.Artifact)
	}
	if !strings.Contains(string(f.Config), "/volumes/a-ns/a-0") {
		t.Fatalf("config = %s", f.Config)
	}
}

// TestPagination: the page size is the config's, and the client follows the
// continue token, so a listing split across pages still yields every object.
func TestPagination(t *testing.T) {
	srv := kubeapitest.New(t)
	for _, name := range []string{"p-0", "p-1", "p-2", "p-3", "p-4"} {
		srv.AddPod(pod("ns", name, map[string]string{"backup.example/type": "database"}, "Running"))
	}
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	d := newDiscovererIn(t, srv, t.TempDir(), 2, rule)
	if found := discover(t, d); len(found) != 5 {
		t.Fatalf("paged listing returned %d sources, want 5", len(found))
	}
}

// TestEnvForms: a literal value, a configMapKeyRef through envValue and a
// secretKeyRef through envFile all resolve; the secret value never reaches the
// rendered config, only the 0600 file envFile wrote.
func TestEnvForms(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddSecret("ns", "pw-secret", map[string]string{"PW": "s3cret-value"})
	srv.AddConfigMap("ns", "conf", map[string]string{"DB": "cdb"})
	srv.AddPod(pod("ns", "db-0",
		map[string]string{"backup.example/type": "database", "backup.example/service-name": "postgres"},
		"Running", container{
			Name: "postgres",
			Env: []envVar{
				literal("LIT", "literal-value"),
				{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "pw-secret", "key": "PW"}}},
				{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": map[string]any{"name": "conf", "key": "DB"}}},
			},
			Ports: []containerPort{{ContainerPort: 5432}},
		}))

	rule := ruleFixture{
		Name: "postgres", Resource: "pods", Selector: "backup.example/type=database", Phase: "Running",
		Artifact: artifactFixture{
			Group: "databases", Name: `{{ .Namespace }}-{{ envValue 0 "DB" }}`,
			Importance: "required", Sensitivity: "plain",
		},
		Pipeline: []string{"compress"},
		Source: sourceFixture{Type: "postgres", Config: map[string]any{
			"address":  `{{ label "backup.example/service-name" }}:{{ port 0 0 }}`,
			"database": `{{ envValue 0 "DB" }}`,
			"literal":  `{{ envValue 0 "LIT" }}`,
			"password": map[string]any{"file": `{{ envFile 0 "PW" }}`},
			"nested":   map[string]any{"again": `{{ envValue 0 "LIT" }}`},
		}},
	}
	d := newDiscoverer(t, srv, rule)
	found := discover(t, d)
	if len(found) != 1 {
		t.Fatalf("found %d sources, want 1", len(found))
	}
	f := found[0]
	if f.Artifact.Name != "ns-cdb" {
		t.Fatalf("artifact name = %q", f.Artifact.Name)
	}
	cfg := string(f.Config)
	for _, want := range []string{"cdb", "literal-value", "postgres:5432"} {
		if !strings.Contains(cfg, want) {
			t.Fatalf("config %s does not carry %q", cfg, want)
		}
	}
	if strings.Contains(cfg, "s3cret-value") {
		t.Fatal("the secret value leaked into the rendered config")
	}
	var rendered struct {
		Password struct {
			File string `json:"file"`
		} `json:"password"`
	}
	if err := json.Unmarshal(f.Config, &rendered); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(rendered.Password.File)
	if err != nil {
		t.Fatalf("secret file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret file mode = %v, want 0600", info.Mode().Perm())
	}
	content, err := os.ReadFile(rendered.Password.File)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "s3cret-value" {
		t.Fatalf("secret file = %q", content)
	}
	if !strings.HasSuffix(rendered.Password.File, filepath.Join("ns", "secret-pw-secret", "PW")) {
		t.Fatalf("secret file path = %q", rendered.Password.File)
	}
}

// TestLiteralEnvFilesArePerPod: a literal value belongs to the pod spec that
// states it, so two pods of one namespace carrying different values for the
// same variable must write different files. A namespace-wide slot lets the last
// render overwrite the first pod's file, and that pod's backup then
// authenticates with the other pod's credential.
func TestLiteralEnvFilesArePerPod(t *testing.T) {
	srv := kubeapitest.New(t)
	passwords := map[string]string{"db-0": "first-password", "db-1": "second-password"}
	for name, password := range passwords {
		srv.AddPod(pod("ns", name, map[string]string{"backup.example/type": "database"}, "Running",
			container{Name: "postgres", Env: []envVar{literal("PW", password)}}))
	}
	rule := ruleFixture{
		Name: "postgres", Resource: "pods", Selector: "backup.example/type=database", Phase: "Running",
		Artifact: artifactFixture{Group: "databases", Name: "{{ .Namespace }}/{{ .Name }}", Importance: "required", Sensitivity: "plain"},
		Pipeline: []string{"compress"},
		Source: sourceFixture{Type: "postgres", Config: map[string]any{
			"address":  "db.example:5432",
			"database": "example",
			"username": "backup",
			"password": map[string]any{"file": `{{ envFile 0 "PW" }}`},
		}},
	}
	found := discover(t, newDiscoverer(t, srv, rule))
	if len(found) != 2 {
		t.Fatalf("found %d sources, want 2", len(found))
	}
	paths := map[string]string{}
	for _, f := range found {
		var rendered struct {
			Password struct {
				File string `json:"file"`
			} `json:"password"`
		}
		if err := json.Unmarshal(f.Config, &rendered); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(rendered.Password.File)
		if err != nil {
			t.Fatalf("%s: %v", f.Artifact.Name, err)
		}
		want, ok := passwords[strings.TrimPrefix(f.Artifact.Name, "ns/")]
		if !ok {
			t.Fatalf("unexpected artifact %q", f.Artifact.Name)
		}
		if string(content) != want {
			t.Fatalf("%s renders file %s holding %q, want its own %q",
				f.Artifact.Name, rendered.Password.File, content, want)
		}
		paths[rendered.Password.File] = want
	}
	if len(paths) != 2 {
		t.Fatalf("the two pods share one literal slot: %v", paths)
	}
}

// TestSecretDirectoryIsExactly0700: the per-namespace directory envFile creates
// under secrets_path must be exactly 0700 even when secrets_path is setgid — a
// tmpfs mounted with fsGroup, which is what kubelet hands the container — where
// Linux would otherwise give the created directory the inherited setgid bit.
func TestSecretDirectoryIsExactly0700(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddSecret("ns", "pw-secret", map[string]string{"PW": "s3cret-value"})
	srv.AddPod(pod("ns", "db-0",
		map[string]string{"backup.example/type": "database", "backup.example/service-name": "postgres"},
		"Running", container{
			Name: "postgres",
			Env: []envVar{
				{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "pw-secret", "key": "PW"}}},
			},
			Ports: []containerPort{{ContainerPort: 5432}},
		}))

	secretsDir := t.TempDir()
	if err := os.Chmod(secretsDir, 0o777|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	rule := ruleFixture{
		Name: "postgres", Resource: "pods", Selector: "backup.example/type=database", Phase: "Running",
		Artifact: artifactFixture{
			Group: "databases", Name: `{{ .Namespace }}-{{ .Name }}`,
			Importance: "required", Sensitivity: "plain",
		},
		Pipeline: []string{"compress"},
		Source: sourceFixture{Type: "postgres", Config: map[string]any{
			"address":  `{{ label "backup.example/service-name" }}:{{ port 0 0 }}`,
			"database": "db",
			"password": map[string]any{"file": `{{ envFile 0 "PW" }}`},
		}},
	}
	found := discover(t, newDiscovererIn(t, srv, secretsDir, 500, rule))
	if len(found) != 1 {
		t.Fatalf("found %d sources, want 1", len(found))
	}
	var rendered struct {
		Password struct {
			File string `json:"file"`
		} `json:"password"`
	}
	if err := json.Unmarshal(found[0].Config, &rendered); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Dir(rendered.Password.File))
	if err != nil {
		t.Fatalf("secret directory: %v", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 || info.Mode()&os.ModeSetgid != 0 {
		t.Fatalf("secret directory mode = %v, want drwx------ with no setgid", info.Mode())
	}
}

// TestLiteralEmptyValue: an explicit empty literal is a value, not a missing
// variable.
func TestLiteralEmptyValue(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{literal("PW", "")}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envValue 0 "PW" }}`}
	d := newDiscoverer(t, srv, rule)
	if found := discover(t, d); len(found) != 1 || !strings.Contains(string(found[0].Config), `"password":""`) {
		t.Fatalf("found %+v", found)
	}
}

// TestNumberPreserved: a number in the source config keeps its exact text —
// decoding to float64 would have turned it into an exponent.
func TestNumberPreserved(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"part_bytes": 5497558138880, "connect_attempts": 6}
	d := newDiscoverer(t, srv, rule)
	found := discover(t, d)
	var rendered struct {
		PartBytes       int64 `json:"part_bytes"`
		ConnectAttempts int   `json:"connect_attempts"`
	}
	if err := json.Unmarshal(found[0].Config, &rendered); err != nil {
		t.Fatalf("rendered config does not decode into ints: %v (%s)", err, found[0].Config)
	}
	if rendered.PartBytes != 5497558138880 || rendered.ConnectAttempts != 6 {
		t.Fatalf("rendered = %+v", rendered)
	}
}

// TestDeterminism: two runs over one unchanged cluster yield the same result,
// byte for byte.
func TestDeterminism(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("b", "db-1", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	srv.AddPod(pod("a", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"z": "{{ .Name }}", "a": 1}
	d := newDiscoverer(t, srv, rule)
	first := discover(t, d)
	second := discover(t, d)
	if len(first) != len(second) {
		t.Fatalf("lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Name != second[i].Name || string(first[i].Config) != string(second[i].Config) {
			t.Fatalf("run %d differs:\n%s\n%s", i, first[i].Config, second[i].Config)
		}
	}
}

func TestTwoContainersByIndex(t *testing.T) {
	srv := kubeapitest.New(t)
	pw := "hunter2"
	// The real example Postgres pod: container 0 is postgres, container 1
	// is pgbouncer, which has no env at all.
	srv.AddPod(pod("example-prod", "postgres-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{{Name: "PW", Value: &pw}}, Ports: []containerPort{{ContainerPort: 5432}}},
		container{Name: "pgbouncer", Ports: []containerPort{{ContainerPort: 6432}}}))

	postgres := simpleRule("postgres", "pods", "backup.example/type=database", "Running")
	postgres.Source.Config = map[string]any{"address": "db:{{ port 0 0 }}"}
	pgbouncer := simpleRule("pgbouncer", "pods", "backup.example/type=database", "Running")
	pgbouncer.Source.Config = map[string]any{"address": "db:{{ port 1 0 }}"}
	d := newDiscoverer(t, srv, postgres, pgbouncer)
	found := discover(t, d)
	if len(found) != 2 {
		t.Fatalf("found %d sources, want 2", len(found))
	}
	if !strings.Contains(string(found[0].Config), "db:5432") || !strings.Contains(string(found[1].Config), "db:6432") {
		t.Fatalf("ports by container index: %s / %s", found[0].Config, found[1].Config)
	}
}

// TestResolutionErrorsNameTheObject: a missing container, variable or port is
// named by Discover as one Found carrying Problem — the object keeps its
// identity, the discovery does not abort, and Check stays nil so the run is not
// denied every other artifact of the discovery.
func TestResolutionErrorsNameTheObject(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
		want   string
	}{
		{"missing variable", map[string]any{"password": `{{ envValue 1 "PW" }}`}, `container 1 (pgbouncer) has no environment variable "PW"`},
		{"missing container", map[string]any{"password": `{{ envValue 2 "PW" }}`}, "pod example-prod/postgres-0 has no container 2"},
		{"missing port", map[string]any{"address": "db:{{ port 1 1 }}"}, "container 1 (pgbouncer) has no port 1"},
		{"missing label", map[string]any{"name": `{{ label "backup.example/service-name" }}`}, `pod example-prod/postgres-0 has no label "backup.example/service-name"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			pw := "hunter2"
			srv.AddPod(pod("example-prod", "postgres-0", map[string]string{"backup.example/type": "database"}, "Running",
				container{Name: "postgres", Env: []envVar{{Name: "PW", Value: &pw}}, Ports: []containerPort{{ContainerPort: 5432}}},
				container{Name: "pgbouncer", Ports: []containerPort{{ContainerPort: 6432}}}))
			rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
			rule.Source.Config = tc.config
			d := newDiscoverer(t, srv, rule)
			found, err := d.Discover(context.Background())
			if err != nil {
				t.Fatalf("Discover = %v, want the object reported as a Problem, not an aborted discovery", err)
			}
			if len(found) != 1 || !strings.Contains(found[0].Problem, tc.want) {
				t.Fatalf("Discover = %+v, want one Found whose Problem contains %q", found, tc.want)
			}
			if found[0].Name != "databases/example-prod/postgres-0" {
				t.Fatalf("Name = %q, want the rule/namespace/object identity", found[0].Name)
			}
			if err := check(t, d); err != nil {
				t.Fatalf("Check = %v, want nil: a per-object problem is not a discovery-level failure", err)
			}
		})
	}
}

// TestPerObjectProblemIsScoped: one object whose source cannot be rendered is
// one Found with a Problem — identity and envelope kept, Type and Pipeline
// still set — while every other object of the same rule is returned normally
// and Check stays silent, because a run skips a discovery whose Check failed.
func TestPerObjectProblemIsScoped(t *testing.T) {
	srv := kubeapitest.New(t)
	labels := map[string]string{"backup.example/type": "database"}
	srv.AddPod(pod("ns", "db-broken", labels, "Running", container{Name: "postgres"}))
	srv.AddPod(pod("ns", "db-good", labels, "Running",
		container{Name: "postgres", Env: []envVar{literal("DB", "cdb")}, Ports: []containerPort{{ContainerPort: 5432}}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.MinMatches = 2
	rule.Source.Config = map[string]any{"database": `{{ envValue 0 "DB" }}`}
	d := newDiscoverer(t, srv, rule)

	found, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover = %v, want both objects returned", err)
	}
	if len(found) != 2 {
		t.Fatalf("found %d sources, want 2 (the broken one included)", len(found))
	}
	broken, good := found[0], found[1]
	if broken.Name != "databases/ns/db-broken" || !strings.Contains(broken.Problem, `has no environment variable "DB"`) {
		t.Fatalf("broken = %+v", broken)
	}
	if broken.Type != "files" || !slices.Equal(broken.Pipeline, []string{"compress"}) || broken.Artifact.Importance != "required" {
		t.Fatalf("broken envelope = %+v", broken)
	}
	if good.Problem != "" || !strings.Contains(string(good.Config), `"cdb"`) {
		t.Fatalf("good = %+v (config %s)", good, good.Config)
	}
	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want nil: the discovery's other objects depend on it passing", err)
	}
}

// TestArtifactNameFallbackKeepsIdentity: when the artifact-name template itself
// cannot render, the Found still carries an identity derived from the rule, the
// namespace and the object, the Problem says what fell back, and the source
// config — rendered independently — survives.
func TestArtifactNameFallbackKeepsIdentity(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Artifact.Name = `{{ label "not-a-label" }}/{{ .Name }}`
	d := newDiscoverer(t, srv, rule)

	found := discover(t, d)
	if len(found) != 1 {
		t.Fatalf("found %d sources", len(found))
	}
	f := found[0]
	if f.Name != "databases/ns/db-0" {
		t.Fatalf("Name = %q", f.Name)
	}
	if f.Artifact.Name != "ns/db-0" || f.Artifact.Group != "group" {
		t.Fatalf("artifact = %+v, want the namespace/object fallback name", f.Artifact)
	}
	if !strings.Contains(f.Problem, `has no label "not-a-label"`) || !strings.Contains(f.Problem, "falls back to") {
		t.Fatalf("Problem = %q", f.Problem)
	}
	if len(f.Config) == 0 {
		t.Fatal("the source config was dropped although only the artifact name failed")
	}
	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want nil: the problem rides the Found, not the check", err)
	}
}

// TestMinMatchesCountsProblemObjects: the count is about what the cluster
// labels, so an object that matched but carries a Problem satisfies
// min_matches — the failure is the object's Problem, never a bogus "too few
// matches".
func TestMinMatchesCountsProblemObjects(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.MinMatches = 1
	rule.Source.Config = map[string]any{"database": `{{ envValue 0 "DB" }}`}
	d := newDiscoverer(t, srv, rule)

	found, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover = %v, want the matched object counted and reported", err)
	}
	if len(found) != 1 || found[0].Problem == "" {
		t.Fatalf("found = %+v", found)
	}
	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want nil: the count is satisfied and the problem rides the Found", err)
	}
}

// TestUnsupportedFormsAreRefused: a rule that reads a fieldRef variable, or a
// variable that only exists through envFrom, is an error naming the object —
// not a silent skip.
func TestUnsupportedFormsAreRefused(t *testing.T) {
	run := func(t *testing.T, c container, want string) {
		t.Helper()
		srv := kubeapitest.New(t)
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", c))
		rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
		rule.Source.Config = map[string]any{"password": `{{ envValue 0 "PW" }}`}
		if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, want) {
			t.Fatalf("Problem = %q, want a message containing %q", p, want)
		}
	}
	t.Run("fieldRef", func(t *testing.T) {
		run(t, container{Name: "postgres", Env: []envVar{
			{Name: "PW", ValueFrom: map[string]any{"fieldRef": map[string]any{"fieldPath": "status.podIP"}}},
		}}, "uses valueFrom.fieldRef, which is not supported")
	})
	t.Run("envFrom", func(t *testing.T) {
		run(t, container{Name: "postgres", EnvFrom: []any{
			map[string]any{"secretRef": map[string]any{"name": "pw"}},
		}}, "envFrom imports are not resolved")
	})
}

// TestSecretErrors: an unreadable Secret is a permanent error naming the
// object, the variable and the Secret; a missing key is named too.
func TestSecretErrors(t *testing.T) {
	rule := func() ruleFixture {
		r := simpleRule("databases", "pods", "backup.example/type=database", "Running")
		r.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
		return r
	}
	ref := envVar{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "pw-secret", "key": "PW"}}}
	newWithRef := func(t *testing.T) discovery.Discoverer {
		srv := kubeapitest.New(t)
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
			container{Name: "postgres", Env: []envVar{ref}}))
		d := newDiscoverer(t, srv, rule())
		return d
	}

	t.Run("forbidden", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddSecret("ns", "pw-secret", map[string]string{"PW": "x"})
		srv.Deny("secrets/ns/pw-secret")
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
			container{Name: "postgres", Env: []envVar{ref}}))
		if p := problemOf(t, newDiscoverer(t, srv, rule())); !strings.Contains(p, "403") {
			t.Fatalf("Problem = %q, want a 403", p)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddSecret("ns", "pw-secret", map[string]string{"OTHER": "x"})
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
			container{Name: "postgres", Env: []envVar{ref}}))
		if p := problemOf(t, newDiscoverer(t, srv, rule())); !strings.Contains(p, `secret ns/pw-secret has no key "PW"`) {
			t.Fatalf("Problem = %q, want the missing key named", p)
		}
	})

	t.Run("missing secret", func(t *testing.T) {
		if p := problemOf(t, newWithRef(t)); !strings.Contains(p, "secret ns/pw-secret") {
			t.Fatalf("Problem = %q, want the missing secret named", p)
		}
	})
}

// TestNothingToBackUp: a rule that matches nothing is an empty result, not an
// error — the "no pods to back up" case the script had.
func TestNothingToBackUp(t *testing.T) {
	srv := kubeapitest.New(t)
	d := newDiscoverer(t, srv, simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want nil", err)
	}
	if found := discover(t, d); len(found) != 0 {
		t.Fatalf("found %d sources", len(found))
	}
}

// TestMinMatches: a rule that requires matches fails the run when the API
// answers with fewer. An empty list is not evidence of "nothing to back up",
// and the error names the rule, the resource, the selector and the count.
func TestMinMatches(t *testing.T) {
	cases := []struct {
		name   string
		rules  []ruleFixture
		seed   func(*kubeapitest.Server)
		want   string
		found  int
		failed bool
	}{
		{
			name: "pod rule, nothing matches",
			rules: []ruleFixture{func() ruleFixture {
				r := simpleRule("postgres", "pods", "backup.example/type=database", "Running")
				r.MinMatches = 1
				return r
			}()},
			want:   `rule "postgres": pods with selector "backup.example/type=database" matched 0 objects, want at least 1`,
			failed: true,
		},
		{
			name: "pod rule, fewer than required",
			rules: []ruleFixture{func() ruleFixture {
				r := simpleRule("postgres", "pods", "backup.example/type=database", "Running")
				r.MinMatches = 2
				return r
			}()},
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running"))
			},
			want:   "matched 1 objects, want at least 2",
			failed: true,
		},
		{
			name: "pvc rule, nothing matches",
			rules: []ruleFixture{func() ruleFixture {
				r := simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
				r.MinMatches = 1
				return r
			}()},
			want:   `rule "volumes": persistentvolumeclaims with selector "backup.example/type=pvc" matched 0 objects, want at least 1`,
			failed: true,
		},
		{
			name: "minimum met",
			rules: []ruleFixture{func() ruleFixture {
				r := simpleRule("postgres", "pods", "backup.example/type=database", "Running")
				r.MinMatches = 1
				return r
			}()},
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running"))
			},
			found: 1,
		},
		{
			name: "zero minimum allows an empty result",
			rules: []ruleFixture{
				simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound"),
			},
		},
		{
			// The count is the raw answer to the rule's own query, so an
			// excluded object still proves that this rule's labels exist.
			name: "an excluded object still counts",
			rules: []ruleFixture{func() ruleFixture {
				r := simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
				r.MinMatches = 1
				return r
			}()},
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", map[string]string{"backup.example/type": "pvc", testExcludeLabel: testExcludeValue}, "Bound"))
			},
			found: 1,
		},
		{
			// The count is the raw answer to the rule's own query, so an object
			// another rule claimed still proves that this rule's labels exist.
			name: "a claimed object still counts",
			rules: []ruleFixture{
				withCovers(func() ruleFixture {
					r := simpleRule("postgres", "pods", "backup.example/type=database", "Running")
					r.MinMatches = 1
					return r
				}()),
				func() ruleFixture {
					r := simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
					r.MinMatches = 1
					return r
				}(),
			},
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(mountingPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", "data-db-0"))
				srv.AddPVC(pvc("ns", "data-db-0", map[string]string{"backup.example/type": "pvc"}, "Bound"))
			},
			found: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			if tc.seed != nil {
				tc.seed(srv)
			}
			d := newDiscoverer(t, srv, tc.rules...)
			_, discoverErr := d.Discover(context.Background())
			checkErr := check(t, d)
			if !tc.failed {
				if discoverErr != nil || checkErr != nil {
					t.Fatalf("Discover = %v, Check = %v", discoverErr, checkErr)
				}
				if found := discover(t, d); len(found) != tc.found {
					t.Fatalf("found %d sources, want %d", len(found), tc.found)
				}
				return
			}
			for name, err := range map[string]error{"Discover": discoverErr, "Check": checkErr} {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("%s = %v, want a message containing %q", name, err, tc.want)
				}
				if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
					t.Fatalf("%s class = %q, want permanent", name, class)
				}
			}
		})
	}
}

// TestPhaseFilter: a pod in another phase is not discovered — the phase is the
// server-side filter the script's field selector was.
func TestPhaseFilter(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Succeeded", container{Name: "postgres"}))
	srv.AddPod(pod("ns", "db-1", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	d := newDiscoverer(t, srv, simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	found := discover(t, d)
	if len(found) != 1 || found[0].Artifact.Name != "ns/db-1" {
		t.Fatalf("found %+v", found)
	}

	// A claim carries its own phase and the API server has no field selector
	// for it, so the rule's phase is applied to a listing that carries every
	// one: a claim in another phase is not discovered, and the listing may not
	// quietly become the rule's filter either way.
	srv.AddPVC(pvc("ns", "media-pending", map[string]string{"backup.example/type": "pvc"}, "Pending"))
	srv.AddPVC(pvc("ns", "media-bound", map[string]string{"backup.example/type": "pvc"}, "Bound"))
	claims := discover(t, newDiscoverer(t, srv,
		simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")))
	if len(claims) != 1 || claims[0].Artifact.Name != "ns/media-bound" {
		t.Fatalf("found %+v", claims)
	}
}

func TestCancelledContext(t *testing.T) {
	srv := kubeapitest.New(t)
	d := newDiscoverer(t, srv, simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Discover(ctx); err == nil {
		t.Fatal("Discover returned success on a cancelled context")
	}
	c, _ := d.(plugin.Checker)
	if err := c.Check(ctx); err == nil {
		t.Fatal("Check returned success on a cancelled context")
	}
}

// TestConfigValidation: malformed rules are refused at decode, offline.
func TestConfigValidation(t *testing.T) {
	base := func(mutate func(*configFixture)) jsontext.Value {
		cfg := configFixture{
			APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
			TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
			RequestTimeout: "10s", SecretsDir: "/tmp/discovered", ListPageSize: 500,
			Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
			Undeclared: undeclaredVolume(),
			Rules:      []ruleFixture{simpleRule("databases", "pods", "x=y", "Running")},
		}
		mutate(&cfg)
		return jsontext.Value(mustJSON(cfg))
	}
	cases := map[string]struct {
		raw  jsontext.Value
		want string
	}{
		"duplicate rule names": {base(func(c *configFixture) { c.Rules = append(c.Rules, c.Rules[0]) }), "/pods/1/name"},
		"bad importance":       {base(func(c *configFixture) { c.Rules[0].Artifact.Importance = "maybe" }), "/pods/0/artifact/importance"},
		"relative secrets dir": {base(func(c *configFixture) { c.SecretsDir = "tmp/discovered" }), "/secrets_path"},
		"empty token file":     {base(func(c *configFixture) { c.TokenFile = "" }), "/token_file is empty"},
		"source config array":  {base(func(c *configFixture) { c.Rules[0].Source.Config = []any{"x"} }), "must be a JSON object"},
		"zero page size":       {base(func(c *configFixture) { c.ListPageSize = 0 }), "/list_page_size"},
		"negative min_matches": {base(func(c *configFixture) { c.Rules[0].MinMatches = -1 }), "/pods/0/min_matches"},
		"broken template":      {base(func(c *configFixture) { c.Rules[0].Source.Config = map[string]any{"x": "{{ .Name"} }), "template"},
		// The marker names and value are the config's, and both are required:
		// an empty one is a marker nothing carries.
		"empty marker label": {base(func(c *configFixture) { c.Exclude.Label = "" }), "/exclude/label is empty"},
		"empty marker value": {base(func(c *configFixture) { c.Exclude.Value = "" }), "/exclude/value is empty"},
		// The accounting is a setting, not a rule: the checks that guarded the
		// shape of such a rule are structural — a volume rule has no
		// covers_volumes, neither list carries a resource — and what the setting
		// itself states is checked here.
		"empty undeclared name":      {base(func(c *configFixture) { c.Undeclared.Name = "" }), "/undeclared_volumes/name is empty"},
		"bad undeclared importance":  {base(func(c *configFixture) { c.Undeclared.Importance = "maybe" }), "/undeclared_volumes/importance"},
		"empty pod and volume lists": {base(func(c *configFixture) { c.Rules = nil }), "/pods and /volumes are both empty"},
		"empty phase":                {base(func(c *configFixture) { c.Rules[0].Phase = "" }), "/pods/0/phase is empty"},
		// A phase the resource does not have is a rule that matches nothing,
		// forever, and nothing at run time would ever say so.
		"a phase no pod has": {base(func(c *configFixture) { c.Rules[0].Phase = "Bund" }), `/pods/0/phase "Bund" is not a phase of pods`},
		"a pod phase on a claim": {base(func(c *configFixture) {
			c.Rules = []ruleFixture{simpleRule("volumes", "persistentvolumeclaims", "x=y", "Running")}
		}), `/volumes/0/phase "Running" is not a phase of persistentvolumeclaims`},
		"the accounting name taken by a rule": {base(func(c *configFixture) {
			c.Undeclared.Name = c.Rules[0].Name
		}), "/undeclared_volumes/name"},
		"empty volume rule name": {base(func(c *configFixture) {
			c.Rules = []ruleFixture{simpleRule("", "persistentvolumeclaims", "x=y", "Bound")}
		}), "/volumes/0/name is empty"},
		// The two connection names are the deployment's: an empty one is a
		// discovery that cannot reach an API server at all.
		"empty host environment name": {base(func(c *configFixture) { c.APIHostEnv = "" }), "/api_host_env is empty"},
		"empty port environment name": {base(func(c *configFixture) { c.APIPortEnv = "" }), "/api_port_env is empty"},
		// A timeout of zero is a request that is cancelled before it is made.
		"zero request timeout": {base(func(c *configFixture) { c.RequestTimeout = "0s" }), "/request_timeout must be positive"},
		// An empty pipeline element is a step nobody named; an empty pipeline is
		// written as [].
		"empty pipeline element": {base(func(c *configFixture) {
			c.Rules[0].Pipeline = []string{"compress", ""}
		}), "/pods/0/pipeline/1 is empty"},
		// The source type is what the registry builds the source from.
		"empty source type": {base(func(c *configFixture) {
			c.Rules[0].Source.Type = ""
		}), "/pods/0/source/type is empty"},
		// Every artifact field is required of a rule: an empty group or name is
		// an artifact nothing can be found by.
		"empty artifact group": {base(func(c *configFixture) {
			c.Rules[0].Artifact.Group = ""
		}), "/pods/0/artifact/group is empty"},
		"empty artifact name": {base(func(c *configFixture) {
			c.Rules[0].Artifact.Name = ""
		}), "/pods/0/artifact/name is empty"},
		"bad sensitivity": {base(func(c *configFixture) {
			c.Rules[0].Artifact.Sensitivity = "maybe"
		}), "/pods/0/artifact/sensitivity"},
		// A template inside an array of the source config is parsed like any
		// other, so its syntax error is reported by the rule that carries it.
		"broken template inside an array": {base(func(c *configFixture) {
			c.Rules[0].Source.Config = map[string]any{"list": []any{"fixed", "{{ .Name"}}
		}), "template"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := registry(t).Check("discovery", "kubernetes_labels", tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Check = %v, want a message containing %q", err, tc.want)
			}
		})
	}
}

// TestSourceConfigShapes: arrays, literals and nulls of a source config are
// copied, and only the strings are templated.
func TestSourceConfigShapes(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{
		"list":   []any{"fixed", "{{ .Name }}", 5, true, nil},
		"nested": map[string]any{"array": []any{map[string]any{"x": "{{ .Namespace }}"}}},
	}
	d := newDiscoverer(t, srv, rule)
	found := discover(t, d)
	var rendered struct {
		List []any `json:"list"`
	}
	if err := json.Unmarshal(found[0].Config, &rendered); err != nil {
		t.Fatal(err)
	}
	if len(rendered.List) != 5 || rendered.List[0] != "fixed" || rendered.List[1] != "db-0" || rendered.List[4] != nil {
		t.Fatalf("list = %+v", rendered.List)
	}
	if !strings.Contains(string(found[0].Config), `{"array":[{"x":"ns"}]}`) {
		t.Fatalf("config = %s", found[0].Config)
	}
}

// TestConfigMapFailures: every way a configMapKeyRef can fail is named.
func TestConfigMapFailures(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*kubeapitest.Server)
		env    envVar
		want   string
	}{
		{
			"missing configmap",
			func(*kubeapitest.Server) {},
			envVar{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": map[string]any{"name": "conf", "key": "DB"}}},
			"configmap ns/conf",
		},
		{
			"missing key",
			func(srv *kubeapitest.Server) { srv.AddConfigMap("ns", "conf", map[string]string{"OTHER": "x"}) },
			envVar{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": map[string]any{"name": "conf", "key": "DB"}}},
			`configmap ns/conf has no key "DB"`,
		},
		{
			"binary data",
			func(srv *kubeapitest.Server) { srv.AddConfigMapBinary("ns", "conf", nil, map[string]string{"DB": "x"}) },
			envVar{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": map[string]any{"name": "conf", "key": "DB"}}},
			`configmap ns/conf key "DB" holds binaryData, which is not supported`,
		},
		{
			"incomplete ref",
			func(srv *kubeapitest.Server) { srv.AddConfigMap("ns", "conf", map[string]string{"DB": "x"}) },
			envVar{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": map[string]any{"name": "conf"}}},
			"incomplete configMapKeyRef",
		},
		{
			"two sources",
			func(srv *kubeapitest.Server) {
				srv.AddConfigMap("ns", "conf", map[string]string{"DB": "x"})
				srv.AddSecret("ns", "pw", map[string]string{"DB": "x"})
			},
			envVar{Name: "DB", ValueFrom: map[string]any{
				"configMapKeyRef": map[string]any{"name": "conf", "key": "DB"},
				"secretKeyRef":    map[string]any{"name": "pw", "key": "DB"},
			}},
			"want exactly one",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			tc.mutate(srv)
			srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
				container{Name: "postgres", Env: []envVar{tc.env}}))
			rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
			rule.Source.Config = map[string]any{"database": `{{ envValue 0 "DB" }}`}
			if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, tc.want) {
				t.Fatalf("Problem = %q, want a message containing %q", p, tc.want)
			}
		})
	}
}

// TestSecretFileFailures: an unwritable secrets path and an impossible object
// namespace are named errors, never a silent half-rendered config.
func TestSecretFileFailures(t *testing.T) {
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
	ref := envVar{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "pw", "key": "PW"}}}

	t.Run("secrets path is a file", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddSecret("ns", "pw", map[string]string{"PW": "x"})
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres", Env: []envVar{ref}}))
		blocker := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		d := newDiscovererIn(t, srv, blocker, 500, rule)
		found, err := d.Discover(context.Background())
		if err != nil {
			t.Fatalf("Discover = %v, want the problem scoped to its object", err)
		}
		if len(found) != 1 || !strings.Contains(found[0].Problem, blocker) {
			t.Fatalf("found = %+v, want a Problem naming the unwritable path", found)
		}
		if found[0].ProblemRetryable {
			t.Fatal("an unusable secrets path is permanent: another attempt answers the same")
		}
		if err := check(t, d); err != nil {
			t.Fatalf("Check = %v, want nil: the object's problem stays scoped", err)
		}
	})

	t.Run("namespace escapes", func(t *testing.T) {
		srv := kubeapitest.New(t)
		// A literal needs no Secret, so the namespace reaches the file path
		// check directly.
		srv.AddPod(pod("..", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
			container{Name: "postgres", Env: []envVar{literal("PW", "x")}}))
		if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "escapes the secrets directory") {
			t.Fatalf("Problem = %q, want the escaping namespace refused", p)
		}
	})
}

// TestPortOnAPVC: the port function is a pod function; a rule that uses it on
// another resource is refused by name.
func TestPortOnAPVC(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPVC(pvc("ns", "media", map[string]string{"backup.example/type": "pvc"}, "Bound"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
	rule.Source.Config = map[string]any{"address": "x:{{ port 0 0 }}"}
	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "persistentvolumeclaim ns/media declares no containers, so it declares no ports") {
		t.Fatalf("Problem = %q", p)
	}
}

// TestNewHonoursCancelledContext: construction is cancellation-aware like every
// plugin call.
func TestNewHonoursCancelledContext(t *testing.T) {
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
		RequestTimeout: "10s", SecretsDir: "/tmp/discovered", ListPageSize: 500,
		Rules: []ruleFixture{simpleRule("databases", "pods", "x=y", "Running")},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := plugin.Build(ctx, registry(t), discovery.Kind, "kubernetes_labels", jsontext.Value(mustJSON(cfg)),
		plugin.Deps{Instance: "workloads", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("New returned success on a cancelled context")
	}
}

// TestBrokenAPI: a failing API is a classified transient error — never the
// "too few matches" verdict — and the same discoverer recovers once the fake
// answers again.
func TestBrokenAPI(t *testing.T) {
	srv := kubeapitest.New(t)
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.MinMatches = 1
	d := newDiscoverer(t, srv, rule)
	srv.Fail("pods", 500)
	err := check(t, d)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("Check = %v, want a 500", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if strings.Contains(err.Error(), "want at least") {
		t.Fatalf("an API failure was reported as too few matches: %v", err)
	}
	srv.Fail("pods", 0)
	if _, err := d.Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "want at least 1") {
		t.Fatalf("after the fake recovered, Discover = %v, want the count verdict", err)
	}
}

// TestPerObjectReadClassification: an error the API client classed transient
// marks its Found ProblemRetryable — the engine may re-run Discover while its
// budget lasts — while every other class is final. Either way the problem stays
// with its one artifact: Discover itself returns no per-object error and Check
// stays nil, so the discovery's other artifacts are never denied.
func TestPerObjectReadClassification(t *testing.T) {
	secretRule := func() ruleFixture {
		r := simpleRule("databases", "pods", "backup.example/type=database", "Running")
		r.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
		return r
	}
	ref := envVar{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "cred", "key": "PW"}}}
	cases := []struct {
		name          string
		prepare       func(*kubeapitest.Server)
		wantRetryable bool
		want          string
	}{
		{
			"transient 500 on the secret read",
			func(srv *kubeapitest.Server) { srv.Fail("secrets/ns/cred", 500) },
			true, "500",
		},
		{
			"permanent 403 on the secret read",
			func(srv *kubeapitest.Server) {
				srv.AddSecret("ns", "cred", map[string]string{"PW": "x"})
				srv.Deny("secrets/ns/cred")
			},
			false, "403",
		},
		{
			"not_found on the secret read",
			func(*kubeapitest.Server) {},
			false, "secret ns/cred",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			tc.prepare(srv)
			srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
				container{Name: "postgres", Env: []envVar{ref}}))
			d := newDiscoverer(t, srv, secretRule())
			found, err := d.Discover(context.Background())
			if err != nil {
				t.Fatalf("Discover = %v, want a Found.Problem instead", err)
			}
			if len(found) != 1 || !strings.Contains(found[0].Problem, tc.want) {
				t.Fatalf("found = %+v, want a Problem containing %q", found, tc.want)
			}
			if found[0].ProblemRetryable != tc.wantRetryable {
				t.Fatalf("ProblemRetryable = %v, want %v", found[0].ProblemRetryable, tc.wantRetryable)
			}
			if err := check(t, d); err != nil {
				t.Fatalf("Check = %v, want nil: a per-object problem stays scoped", err)
			}
		})
	}

	t.Run("unclassified missing label", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
		rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
		rule.Source.Config = map[string]any{"password": `{{ label "nothing" }}`}
		d := newDiscoverer(t, srv, rule)
		found, err := d.Discover(context.Background())
		if err != nil || len(found) != 1 || !strings.Contains(found[0].Problem, `has no label "nothing"`) {
			t.Fatalf("Discover = %+v, %v, want the unclassified problem as a Found.Problem", found, err)
		}
		if err := check(t, d); err != nil {
			t.Fatalf("Check = %v, want nil: an unclassified problem is data, not a fault to retry", err)
		}
	})
}

// TestCancellationDuringARenderStopsTheDiscovery: a context cancelled while one
// object's read is in flight is the discovery's answer — the read's failure is
// not that object's Problem, and the pass does not carry on rendering with a
// dead context. The fake counts the request and then holds it, so the poll can
// cancel strictly inside the read whatever the machine's speed.
func TestCancellationDuringARenderStopsTheDiscovery(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Hold = map[string]time.Duration{"secrets/ns/pw": 500 * time.Millisecond}
	srv.AddSecret("ns", "pw", map[string]string{"PW": "x"})
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "pw", "key": "PW"}}}}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
	d := newDiscoverer(t, srv, rule)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := srv.Calls("secrets/ns/pw")
	go func() {
		for srv.Calls("secrets/ns/pw") < base+1 {
			time.Sleep(2 * time.Millisecond)
		}
		cancel()
	}()

	found, err := d.Discover(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Discover = %+v, %v, want the cancellation", found, err)
	}
	if found != nil {
		t.Fatalf("found = %+v, want nil: the cancelled pass is not a result", found)
	}
}

// TestUndeclaredClaimNamesThePodThatMountsIt: the claim nobody declared is best
// explained by the pod that mounts it — including a pod no rule matches and one
// whose phase no rule asks for, which is exactly the pod an operator is looking
// for when a claim is reported. The run lists every pod for that reason, and the
// phase travels with the name.
func TestUndeclaredClaimNamesThePodThatMountsIt(t *testing.T) {
	srv := kubeapitest.New(t)
	// A rule over a *different* workload: it never sees the finished pod below.
	d := newDiscoverer(t, srv, simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	srv.AddPod(pod("ns", "postgres-0", map[string]string{"backup.example/type": "database"}, "Running"))
	srv.AddPVC(boundPVC("ns", "declared", map[string]string{"backup.example/type": "database"}, "Bound", "pv-declared"))
	// The claim below is mounted by a pod that finished, carries no label a rule
	// asks for, and matches nothing.
	srv.AddPod(mountingPod("ns", "batch-1", map[string]string{"backup.example/kind": "job"}, "Succeeded", "orphan"))
	srv.AddPVC(boundPVC("ns", "orphan", map[string]string{"backup.example/kind": "pvc"}, "Bound", "pv-orphan"))

	found := discover(t, d)
	var orphan *discovery.Found
	for i := range found {
		if found[i].Artifact.Name == "ns/orphan" {
			orphan = &found[i]
		}
	}
	if orphan == nil {
		t.Fatalf("the undeclared claim was not reported: %+v", found)
	}
	for _, want := range []string{"is backed up by no rule", "no rule's source covers the pod that mounts it, ns/batch-1 (Succeeded)"} {
		if !strings.Contains(orphan.Problem, want) {
			t.Errorf("the report %q does not name %q", orphan.Problem, want)
		}
	}
}

// TestExclusionGroupsNameWhatWouldHaveProducedTheObject: the group a decision's
// record carries is the artifact group of the rule that would have produced the
// object — the covering pod's own group for a claim that pod's dump covers, the
// volume rule's group for a claim that rule would have rendered — and is empty
// for a claim no rule declared: the accounting setting's own name is a failed
// artifact's name, never the group of a decision.
func TestExclusionGroupsNameWhatWouldHaveProducedTheObject(t *testing.T) {
	srv := kubeapitest.New(t)
	podRule := withCovers(simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	podRule.Artifact.Group = "pod-dumps"
	volumeRule := simpleRule("declared", "persistentvolumeclaims", "backup.example/type=database", "Bound")
	volumeRule.Artifact.Group = "volume-dumps"
	d := newDiscoverer(t, srv, podRule, volumeRule)

	// A claim the pod's own dump covers, mounted by the pod that dumps it.
	srv.AddPod(mountingPod("ns", "postgres-0", map[string]string{"backup.example/type": "database"}, "Running", "covered"))
	srv.AddPVC(boundPVC("ns", "covered", map[string]string{"backup.example/type": "database"}, "Bound", "pv-covered"))
	// A claim the volume rule would have rendered, excluded by its own marker.
	srv.AddPVC(boundPVC("ns", "marked", map[string]string{"backup.example/type": "database", testExcludeLabel: testExcludeValue}, "Bound", "pv-marked"))
	// A claim no rule declared, excluded by its own marker.
	srv.AddPVC(boundPVC("ns", "orphan", map[string]string{testExcludeLabel: testExcludeValue}, "Bound", "pv-orphan"))

	byName := map[string]discovery.Found{}
	for _, f := range discover(t, d) {
		if f.Excluded != "" {
			byName[f.Artifact.Name] = f
		}
	}
	want := map[string]struct {
		kind  model.ExclusionKind
		group string
	}{
		"ns/covered": {model.ExclusionCovered, "pod-dumps"},
		"ns/marked":  {model.ExclusionIgnored, "volume-dumps"},
		"ns/orphan":  {model.ExclusionIgnored, ""},
	}
	if len(byName) != len(want) {
		t.Fatalf("exclusions = %+v, want one per decision", byName)
	}
	for name, w := range want {
		f, ok := byName[name]
		if !ok {
			t.Errorf("%s is not recorded as an exclusion", name)
			continue
		}
		if f.Excluded != w.kind || f.ExcludedGroup != w.group {
			t.Errorf("%s: kind=%q group=%q, want kind=%q group=%q", name, f.Excluded, f.ExcludedGroup, w.kind, w.group)
		}
	}
}
