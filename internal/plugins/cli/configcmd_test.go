package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v4"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// defaultsYAML is the harness's own defaults document: what the two test
// source fields may be left out of. The generic document is the world's other
// layer — the real plugins are the real ones — so this one only adds the test's.
const defaultsYAML = `plugins:
  config:
    source/synthetic:
      origin: test:default
`

// TestConfigDefaultsPrintsEveryDocument: the embedded documents as they are,
// comments included, one YAML stream separated by `---` — the service's own
// half of a configuration, which a deployment reads to know what it may omit.
func TestConfigDefaultsPrintsEveryDocument(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "config", "defaults")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "go-cluster-backup — the service's defaults") {
		t.Errorf("the module's own document is not printed:\n%s", stdout)
	}
	if !strings.Contains(stdout, "\n---\n") {
		t.Errorf("the two documents are not separated by ---:\n%s", stdout)
	}
	if !strings.Contains(stdout, "source/synthetic") {
		t.Errorf("the harness's own document is not printed:\n%s", stdout)
	}
	if !strings.Contains(stdout, "# one of the two lists must be non-empty") {
		t.Errorf("the comments of the document are not printed:\n%s", stdout)
	}
}

// TestConfigEffectiveMarksWhatIsDefaulted: the file as the assembly reads it,
// every defaulted value carrying `# default`, and `--output json` naming the
// pointers — nested rule sources included.
func TestConfigEffectiveMarksWhatIsDefaulted(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	// A configuration stating neither the source's origin (the harness's
	// default), the artifact's importance (the envelope's), the processor's
	// concurrency nor the run's finish_allowance.
	file := mustWrite(t, dir, "backup.yaml", `run:
  spool_dir: /spool
  spool_reserve_bytes: 0
  max_artifact_bytes: 1073741824
  deadline: 1m
  lease_hard_bound: 8400s
layout:
  run_prefix: "runs/{{ .Run.ID }}"
  artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"
  manifest_key: manifest.json
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: hi, dir: ""}
processors:
  squeeze: {type: zstd, config: {level: fastest, window_bytes: 1024, decode_window_bytes: 1024}}
`)
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", file)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	for _, want := range []string{
		"concurrency: 1 # default",
		"origin: test:default # default",
		"importance: required # default",
		"finish_allowance: 2m # default",
		"id_layout: 20060102T150405Z # default",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the effective configuration does not carry %q:\n%s", want, stdout)
		}
	}
	// A value the file states is never marked.
	if strings.Contains(stdout, "payload: hi # default") {
		t.Errorf("a value of the file is marked as defaulted:\n%s", stdout)
	}

	code, stdout, stderr = w.main(context.Background(), "config", "effective", "--config", file, "--output", "json")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	var doc struct {
		Config    map[string]any `json:"config"`
		Defaulted []string       `json:"defaulted"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("the JSON output does not read as a document: %v\n%s", err, stdout)
	}
	found := map[string]bool{}
	for _, p := range doc.Defaulted {
		found[p] = true
	}
	for _, want := range []string{
		"/sources/thing/config/origin",
		"/sources/thing/artifact/importance",
		"/run/finish_allowance",
		"/processors/squeeze/config/concurrency",
	} {
		if !found[want] {
			t.Errorf("the pointer list does not name %s: %v", want, doc.Defaulted)
		}
	}
	// The merged document is in the output, not only the pointers.
	if _, ok := doc.Config["run"]; !ok {
		t.Errorf("the merged configuration is missing from the JSON output:\n%s", stdout)
	}
}

// TestPluginsDescribeCarriesTheClassification: the description names, per
// field, what the deployment states and what the distribution's documents fill — the
// same two sources the check at startup reads.
func TestPluginsDescribeCarriesTheClassification(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "plugins", "describe", "source/postgres")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	for _, want := range []string{
		`address string [required]`,
		`format string [default: "custom"]`,
		`password configuration.Secret [secret] [required]`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the description does not carry %q:\n%s", want, stdout)
		}
	}

	// A default of the harness's own document, and a field the deployment
	// states. The harness's synthetic source is a test double with no compiled
	// descriptor in the loader, so this half reads the factory registry — the
	// view a build without a loader still answers from.
	var direct, directErr strings.Builder
	code = Main(context.Background(),
		Options{Program: "backup", Factories: w.factories(), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"plugins", "describe", "source/synthetic"}, &direct, &directErr)
	stdout = direct.String()
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, directErr.String())
	}
	for _, want := range []string{
		`payload string [required]`,
		`origin string [default: "test:default"]`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the description does not carry %q:\n%s", want, stdout)
		}
	}
}

// TestSchemaCarriesDefaultsAndRequired: the schema's required list and default
// keys come from the same classification, so a generator can read them.
func TestSchemaCarriesDefaultsAndRequired(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "schema")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("the schema does not read as JSON: %v", err)
	}
	if !strings.Contains(stdout, `"default": "custom"`) {
		t.Errorf("the schema carries no default of the postgres format:\n%s", stdout)
	}
	// The configuration has no format version: this build reads the current
	// schema only, and a `version:` member is refused by the strict decoder.
	if props, ok := doc["properties"].(map[string]any); ok {
		if _, declared := props["version"]; declared {
			t.Error("the schema still declares a format version")
		}
	}
	if !strings.Contains(stdout, `"default": "20060102T150405Z"`) {
		t.Errorf("the schema carries no default of the run id layout:\n%s", stdout)
	}
	if strings.Contains(stdout, `"default": "db:5432"`) {
		t.Error("the schema invents a default for a required field")
	}
}

// TestConfigEffectiveMarksAListElement: a `name[]` block fills the elements of
// a list the file declares — one element at a time, and the marking follows the
// element's own pointer, which is the one place a reader cannot see a default
// from the file alone.
func TestConfigEffectiveMarksAListElement(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	file := mustWrite(t, dir, "rules.yaml", `discoveries:
  workloads:
    type: kubernetes_labels
    config:
      api_host_env: KUBERNETES_SERVICE_HOST
      api_port_env: KUBERNETES_SERVICE_PORT
      token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      request_timeout: 30s
      secrets_path: /tmp/discovered
      list_page_size: 500
      exclude: {label: backup.example/ignore, value: "true"}
      # The accounting always runs: the setting names what a claim nobody
      # declared is reported as.
      undeclared_volumes: {importance: required}
      pods:
        - name: postgres
          selector: "backup.example/engine=postgres"
          phase: Running
          covers_volumes: true
          artifact: {group: databases, name: '{{ .Namespace }}/{{ label "backup.example/service-name" }}', importance: required, sensitivity: plain}
          pipeline: [squeeze]
          source: {type: postgres, config: {address: "db:5432", database: app, username: app, password: {file: /run/secrets/db}, all_databases: false}}
      volumes:
        - name: volumes
          selector: "backup.example/engine=files"
          phase: Bound
          artifact: {group: volumes, name: "{{ .Namespace }}/{{ .Name }}", importance: required, sensitivity: plain}
          pipeline: [squeeze]
          source: {type: files, config: {root: "{{ volumePath }}"}}
processors:
  squeeze: {type: zstd, config: {level: fastest, window_bytes: 1024, decode_window_bytes: 1024}}
`)
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", file)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	// Both lists' elements take what their own block states: the pods rule is
	// overridden by the file, the volume rule takes the block's 0.
	if n := strings.Count(stdout, "min_matches: 0 # default"); n != 2 {
		t.Errorf("the list elements carry %d marked min_matches, want both of them:\n%s", n, stdout)
	}

	code, stdout, stderr = w.main(context.Background(), "config", "effective", "--config", file, "--output", "json")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	var doc struct {
		Defaulted []string `json:"defaulted"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("the JSON output does not read as a document: %v", err)
	}
	for _, want := range []string{
		"/discoveries/workloads/config/pods/0/min_matches",
		"/discoveries/workloads/config/volumes/0/min_matches",
		// The rule's own source config, reached through the plugin's declaration
		// of where it lives.
		"/discoveries/workloads/config/volumes/0/source/config/ignore_vanished",
	} {
		if !slices.Contains(doc.Defaulted, want) {
			t.Errorf("the pointer list does not name %s: %v", want, doc.Defaulted)
		}
	}
}

// marker is the comment that names a defaulted value in `config effective`.
const marker = "default"

// escapePointerToken encodes one RFC 6901 reference token: `~` becomes `~0` and
// `/` becomes `~1`, so a member name carrying a slash stays one segment of the
// pointer that names it.
func escapePointerToken(token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	return strings.ReplaceAll(token, "/", "~1")
}

// markedPaths reads a rendered effective configuration back and returns the
// JSON pointer of every entry that carries the marker, in the order the file
// presents them. It is the reader's half of a round trip: the paths say where a
// reader of the file finds the marker, never where the renderer meant to put it.
//
// A marker on a mapping entry is read from either the key or the value node —
// the emitter chooses, and both name the same entry — while a marker on a
// sequence element or on the document itself is reported at its own path, where
// nothing has any business marking anything.
func markedPaths(t *testing.T, rendered []byte) []string {
	t.Helper()
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		t.Fatalf("the rendered configuration does not read back: %v\n%s", err, rendered)
	}
	var out []string
	carries := func(n *yaml.Node) bool {
		return slices.Contains(strings.Fields(n.LineComment), marker) ||
			slices.Contains(strings.Fields(n.HeadComment), marker) ||
			slices.Contains(strings.Fields(n.FootComment), marker)
	}
	visit := func(n *yaml.Node, path string) {
		if carries(n) {
			out = append(out, path)
		}
	}
	var walk func(n *yaml.Node, path string)
	walk = func(n *yaml.Node, path string) {
		switch n.Kind {
		case yaml.DocumentNode:
			visit(n, path)
			for _, c := range n.Content {
				walk(c, path)
			}
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				key, value := n.Content[i], n.Content[i+1]
				entry := path + "/" + escapePointerToken(key.Value)
				visit(key, entry)
				visit(value, entry)
				walk(value, entry)
			}
		case yaml.SequenceNode:
			for i, item := range n.Content {
				entry := fmt.Sprintf("%s/%d", path, i)
				visit(item, entry)
				walk(item, entry)
			}
		}
	}
	walk(&root, "")
	return out
}

// TestMarkedYAMLMarksEveryValueKind is the renderer's round trip over the kinds
// a configuration holds: the pointer list names a value, the rendered document
// carries the marker on that value's own line, and reading the document back
// yields exactly the pointers that went in — so a marker on a neighbour's line,
// a marker dropped because its value has no line, and a marker on a value
// nobody defaulted all fail here. A document that does not read back at all (an
// emptied collection marked where the emitter then breaks the line) fails here
// too.
func TestMarkedYAMLMarksEveryValueKind(t *testing.T) {
	cases := map[string]struct {
		doc      string
		pointers []string
	}{
		"a scalar": {
			doc:      `{"a": 1, "b": "stated"}`,
			pointers: []string{"/a"},
		},
		"a scalar of every kind": {
			doc:      `{"s": "x", "i": 3, "b": true, "f": 1.5, "nil": null, "t": "30s"}`,
			pointers: []string{"/s", "/i", "/b", "/f", "/nil", "/t"},
		},
		"a multi-line string": {
			doc:      `{"before": "one\ntwo\n", "after": "three\nfour\n", "tail": 1}`,
			pointers: []string{"/before", "/after"},
		},
		"a sequence": {
			doc:      `{"seq": [200, 201, 202], "after": "stated"}`,
			pointers: []string{"/seq"},
		},
		"an empty sequence": {
			doc:      `{"seq": [], "after": "stated"}`,
			pointers: []string{"/seq"},
		},
		"a sequence of mappings": {
			doc:      `{"rules": [{"a": 1}, {"b": 2}], "after": "stated"}`,
			pointers: []string{"/rules"},
		},
		"a sequence inside a mapping inside a sequence": {
			doc:      `{"outer": [{"inner": [1, 2]}]}`,
			pointers: []string{"/outer/0/inner"},
		},
		"a map created whole": {
			doc:      `{"empty": {}, "full": {"x": 1}, "after": "stated"}`,
			pointers: []string{"/empty", "/full/x"},
		},
		"a value inside a list element": {
			doc:      `{"pods": [{"min_matches": 0, "name": "stated"}, {"min_matches": 0}], "after": 1}`,
			pointers: []string{"/pods/0/min_matches", "/pods/1/min_matches"},
		},
		"a key whose defaulted neighbour is a sequence": {
			doc:      `{"seq": [1, 2], "url": "https://example.net", "next": 3}`,
			pointers: []string{"/seq"},
		},
		"a key after a multi-line value": {
			doc:      `{"text": "one\ntwo\nthree\n", "url": "stated"}`,
			pointers: []string{"/url"},
		},
		"nothing defaulted": {
			doc:      `{"a": [1, 2], "b": {"c": "d"}}`,
			pointers: nil,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rendered, err := markedYAML(jsontext.Value(tc.doc), tc.pointers)
			if err != nil {
				t.Fatal(err)
			}
			got := slices.Sorted(slices.Values(markedPaths(t, rendered)))
			want := slices.Sorted(slices.Values(tc.pointers))
			if !slices.Equal(got, want) {
				t.Errorf("the marked entries are %v, want %v\n%s", got, want, rendered)
			}
		})
	}
}

// TestConfigEffectiveMarkersRoundTrip runs the same round trip over a real
// configuration of the real plugins: a discovery whose rules carry a nested
// source config, a notifier that leaves a whole sequence and a whole map to the
// documents, a storage, an encryption processor. `--output json` names what was
// defaulted, `--output yaml` must mark exactly those entries, and reading the
// file back is what proves it — the two outputs are compared, never a fixed
// expectation a misplaced marker could satisfy.
func TestConfigEffectiveMarkersRoundTrip(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	file := mustWrite(t, dir, "backup.yaml", `run:
  id_layout: fixedrun
  spool_dir: /spool
  spool_reserve_bytes: 0
  max_artifact_bytes: 1GiB
  deadline: 1m
  lease_hard_bound: 8400s
discoveries:
  workloads:
    type: kubernetes_labels
    config:
      secrets_path: /run/discovered
      exclude: {label: backup.example/ignore, value: "true"}
      undeclared_volumes: {importance: required}
      pods:
        - name: postgres
          selector: backup.example/engine=postgres
          covers_volumes: true
          artifact: {group: databases, name: "{{ .Namespace }}/db", sensitivity: plain}
          pipeline: [zstd]
          source: {type: files, config: {root: /volumes/postgres}}
      volumes:
        - name: volumes
          selector: backup.example/engine=files
          artifact: {group: volumes, name: "{{ .Namespace }}/{{ .Name }}", sensitivity: plain}
          pipeline: [zstd]
          source: {type: files, config: {root: "{{ volumePath }}"}}
processors:
  zstd: {type: zstd, config: {level: fastest, window_bytes: 1024, decode_window_bytes: 1024}}
  seal:
    type: age
    config: {recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], identities: []}
storages:
  offsite:
    type: s3
    time_budget: 30m
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 14GB
    config: {endpoint: "https://s3.example.net", region: ru-central-1, bucket: b, prefix: p, addressing: path, access_key_id: {file: /k}, secret_access_key: {file: /s}}
notifiers:
  ops:
    type: webhook
    config:
      url: https://notify.example.net/backup
      events: {run_finished: "Backup {{ .Run.Outcome }}"}
      payload: "title: {{ json .Title }}"
`)
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", file)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	code, jsonOut, stderr := w.main(context.Background(), "config", "effective", "--config", file, "--output", "json")
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	var doc struct {
		Defaulted []string `json:"defaulted"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &doc); err != nil {
		t.Fatalf("the JSON output does not read as a document: %v", err)
	}
	// The three shapes the merge produces beyond a plain scalar: a whole
	// sequence, a whole map and the nested source config of a rendered rule.
	for _, want := range []string{
		"/notifiers/ops/config/success_status",
		"/notifiers/ops/config/headers",
		"/discoveries/workloads/config/pods/0/source/config/ignore_vanished",
	} {
		if !slices.Contains(doc.Defaulted, want) {
			t.Fatalf("the pointer list does not name %s: %v", want, doc.Defaulted)
		}
	}
	got := slices.Sorted(slices.Values(markedPaths(t, []byte(stdout))))
	want := slices.Sorted(slices.Values(doc.Defaulted))
	if !slices.Equal(got, want) {
		t.Errorf("the marked entries are %v, want %v\n%s", got, want, stdout)
	}
	if len(want) < 10 {
		t.Errorf("the configuration exercised only %d defaults: %v", len(want), want)
	}
}

// TestYAMLJSONRefusesAValueJSONCannotCarry: the conversion is the whole YAML
// layer, and a value with no JSON spelling is refused there.
func TestYAMLJSONRefusesAValueJSONCannotCarry(t *testing.T) {
	// A YAML float has no JSON spelling for its two special values.
	if _, err := configuration.YAMLJSON([]byte("a: .nan\n")); err == nil {
		t.Error("a not-a-number was converted")
	}
	if _, err := configuration.YAMLJSON([]byte("a: .inf\n")); err == nil {
		t.Error("an infinity was converted")
	}
	if _, err := configuration.YAMLJSON([]byte("\tnot: [yaml")); err == nil {
		t.Fatal("malformed YAML was converted")
	}
	raw, err := configuration.YAMLJSON([]byte("a: 1\n"))
	if err != nil {
		t.Fatalf("a plain document failed: %v", err)
	}
	if string(raw) != `{"a":1}` {
		t.Errorf("YAMLJSON = %s, want %s", raw, `{"a":1}`)
	}
}

// TestReadConfigNamesTheFileItCouldNotConvert: the file's own path is what a
// reader needs, not the YAML parser's line number alone.
func TestReadConfigNamesTheFileItCouldNotConvert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("\tnot: [yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := configuration.ReadConfig(path); err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("ReadConfig error = %v, want it to name %s", err, path)
	}
}

// TestConfigDefaultsAddsTheFinalNewline: a document that does not end with one
// still prints as its own line, so two documents never run together.
func TestConfigDefaultsAddsTheFinalNewline(t *testing.T) {
	var stdout, stderr strings.Builder
	opts := Options{Program: "backup", Defaults: [][]byte{[]byte("a: 1"), []byte("b: 2\n")}}
	if code := configDefaults(opts, nil, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if got, want := stdout.String(), "a: 1\n---\nb: 2\n"; got != want {
		t.Errorf("config defaults = %q, want %q", got, want)
	}
}

// TestMarkedYAMLRefusesADocumentItCannotParse: the merged configuration is JSON
// the assembly wrote, and a document it cannot read back is reported, not guessed.
func TestMarkedYAMLRefusesADocumentItCannotParse(t *testing.T) {
	if _, err := markedYAML(jsontext.Value("{not json"), nil); err == nil {
		t.Fatal("an unparsable document was rendered")
	}
}

// TestMarkedYAMLMarksEveryPointer: every pointer names the node it marks, a
// pointer that names nothing is skipped, and an empty segment is not a step.
func TestMarkedYAMLMarksEveryPointer(t *testing.T) {
	doc := jsontext.Value(`{"a":1,"b":[1,2],"c":{"d":true,"e":"x"},"f":{"d/e":1},"g":{"h~i":2}}`)
	out, err := markedYAML(doc, []string{"/a", "/b", "/c/d", "/absent", "/c//e", "/f/d~1e", "/g/h~0i"})
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(out), "# default"); n != 6 {
		t.Errorf("marked %d values, want 6:\n%s", n, out)
	}
	var back map[string]any
	if err := yaml.Unmarshal(out, &back); err != nil {
		t.Fatalf("the marked document does not read back: %v\n%s", err, out)
	}
	if back["a"] != 1 {
		t.Errorf("the marked document lost a value:\n%s", out)
	}
}

// TestMarkDefaultSkipsAnEmptyDocument: a document node with no content has no
// child to walk into, so the mark is dropped rather than panicking.
func TestMarkDefaultSkipsAnEmptyDocument(t *testing.T) {
	var empty yaml.Node
	empty.Kind = yaml.DocumentNode
	markDefault(&empty, []string{"a"})
	if empty.LineComment != "" {
		t.Errorf("an empty document was marked")
	}
}

// TestConfigEffectiveRefusesABrokenOutput: the merged file is a report, but a
// stream that cannot carry it ends the command.
func TestConfigEffectiveRefusesABrokenOutput(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, "storages:\n"+exchangeYAML(root)+faultyYAML))
	for _, output := range []string{"yaml", "json"} {
		t.Run(output, func(t *testing.T) {
			var stderr strings.Builder
			code := Main(context.Background(),
				Options{Program: "backup", Factories: w.factories(), Loader: harnessLoader(t), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
				[]string{"config", "effective", "--config", cfg, "--output", output}, failWriter{}, &stderr)
			if code != exitFailure || !strings.Contains(stderr.String(), "stream is broken") {
				t.Fatalf("exit %d, stderr %s", code, stderr.String())
			}
		})
	}
}

// TestConfigEffectiveKeepsACandidateAndReportsDiagnostics: a file whose merge
// succeeds but whose selection does not is still printed — the document that
// must be repaired is the one the reader gets — while every diagnostic goes to
// stderr and the command exits non-zero.
func TestConfigEffectiveKeepsACandidateAndReportsDiagnostics(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `plugins:
  disabled: [source/ghost]
storages:
`+exchangeYAML(root)+faultyYAML))
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\nstdout: %s", code, exitFailure, stdout)
	}
	if !strings.Contains(stdout, "spool_dir:") {
		t.Errorf("the mergeable candidate is not printed:\n%s", stdout)
	}
	if !strings.Contains(stderr, `unknown plugin "source/ghost"`) {
		t.Errorf("the diagnostic is not on stderr: %s", stderr)
	}
}

// TestConfigEffectiveKeepsACandidateWhenTheAssemblyRefuses: a file that merges
// and decodes but whose references do not hold up still prints the candidate —
// it is the document the reader must repair — and every reference problem goes
// to stderr with a non-zero exit.
func TestConfigEffectiveKeepsACandidateWhenTheAssemblyRefuses(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
storages:
`+exchangeYAML(root)+faultyYAML))
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stdout, "ghost_processor") {
		t.Errorf("the candidate lost the reference the reader must see:\n%s", stdout)
	}
	if !strings.Contains(stderr, `unknown processor instance "ghost_processor"`) {
		t.Errorf("the reference failure is not on stderr: %s", stderr)
	}
}

// TestConfigEffectiveRefusesAFileItCannotRead: a path the YAML layer cannot
// read has no candidate to print, and the failure names the file.
func TestConfigEffectiveRefusesAFileItCannotRead(t *testing.T) {
	w := newWorld()
	path := filepath.Join(t.TempDir(), "absent.yaml")
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", path)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d", code, exitFailure)
	}
	if stdout != "" {
		t.Errorf("an unreadable file still printed a candidate:\n%s", stdout)
	}
	if !strings.Contains(stderr, path) {
		t.Errorf("the failure does not name the file: %s", stderr)
	}
}

// TestConfigEffectiveDefaultsNothing: the file as the assembly reads it, fed back
// as a file, states every value — so the second pass reports an empty list of
// defaulted pointers, which is what a machine compares.
func TestConfigEffectiveDefaultsNothing(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))

	code, merged, stderr := w.main(context.Background(), "config", "effective", "--config", cfg)
	if code != 0 {
		t.Fatalf("config effective: exit %d\nstderr: %s", code, stderr)
	}
	if !strings.Contains(merged, "# default") {
		t.Fatalf("the first pass defaulted nothing, so the second proves nothing:\n%s", merged)
	}
	stated := mustWrite(t, dir, "stated.yaml", merged)

	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", stated, "--output", "json")
	if code != 0 {
		t.Fatalf("config effective on the merged file: exit %d\nstderr: %s", code, stderr)
	}
	if !strings.Contains(stdout, `"defaulted": []`) {
		t.Errorf("a file that states everything still reports defaulted values:\n%s", stdout)
	}
}

// ruleConfig is the typed config of the source a rule points at: the defaults
// merge can only place a leaf into a type it knows.
type ruleConfig struct {
	Timeout string `json:"timeout"`
}

// ruleDiscoveryConfig carries the raw rule list the pure hook reads.
type ruleDiscoveryConfig struct {
	Rules []jsontext.Value `json:"rules"`
}

// withUnplaceableRule registers the world plus a discovery whose pure rule hook
// names a source config path the document does not carry, and a typed source
// the defaults document fills: the merge cannot place the defaulted leaf and
// reports it, which is what `config effective` must survive with no candidate.
func (w *world) withUnplaceableRule(r *instances.Registry) error {
	if err := w.register(r); err != nil {
		return err
	}
	if err := instances.Register(r, source.Kind, instances.FactorySpec[ruleConfig, source.Source]{
		Type:    "typed",
		Example: []byte(`{"timeout":"30s"}`),
		New: func(context.Context, ruleConfig, instances.Deps) (source.Source, error) {
			return &syntheticSource{}, nil
		},
	}); err != nil {
		return err
	}
	return instances.Register(r, discovery.Kind, instances.FactorySpec[ruleDiscoveryConfig, discovery.Discoverer]{
		Type:    "ruled",
		Example: []byte(`{"rules":[]}`),
		Rules: func(jsontext.Value) ([]configuration.RuleSource, error) {
			return []configuration.RuleSource{{
				Path: "/rules/5", ConfigPath: "/rules/5/source/config", Type: "typed",
				Config: jsontext.Value(`{}`), Pipeline: []string{},
				Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
			}}, nil
		},
		New: func(context.Context, ruleDiscoveryConfig, instances.Deps) (discovery.Discoverer, error) {
			return &fakeDiscovery{}, nil
		},
	})
}

// unplaceableRuleFactories is the factory registry of the world plus the rule
// that cannot land, the shape Options.Factories takes.
func (w *world) unplaceableRuleFactories() *instances.Registry {
	r := instances.NewRegistry()
	if err := w.withUnplaceableRule(r); err != nil {
		panic(err)
	}
	return r
}

// TestConfigEffectiveRefusesAnUnplaceableRule: a rule hook that names a config
// path the document cannot hold is a semantic merge problem — the candidate
// stays printable (M11) and the problem refuses the operation with a non-zero
// exit. The path here points past the rule list, so the merge cannot create it.
func TestConfigEffectiveRefusesAnUnplaceableRule(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	file := mustWrite(t, dir, "rules.yaml", `discoveries:
  workloads:
    type: ruled
    config:
      rules:
        - source:
            type: typed
`)
	doc := []byte(`plugins:
  config:
    source/synthetic:
      origin: test:default
    source/typed:
      timeout: 30s
`)
	lr := harnessLoader(t)
	for _, id := range []string{"discovery/ruled", "source/typed"} {
		if err := loader.Add(lr, loader.Spec{ID: id, Summary: id}, nil); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr strings.Builder
	code := Main(context.Background(),
		Options{Program: "backup", Factories: w.unplaceableRuleFactories(), Loader: lr, Defaults: [][]byte{defaults.Defaults, doc}},
		[]string{"config", "effective", "--config", file}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d\nstdout: %s", code, exitFailure, stdout.String())
	}
	if !strings.Contains(stdout.String(), "discoveries:") || !strings.Contains(stdout.String(), "type: ruled") {
		t.Errorf("the mergeable candidate is not printed:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "/discoveries/workloads/config/rules/5/source/config: no such element") {
		t.Errorf("the unplaceable rule is not reported at its path: %s", stderr.String())
	}
}
