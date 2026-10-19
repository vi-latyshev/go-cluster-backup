package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/cli"
	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// TestExamplesValidate keeps the shipped examples honest: every file under
// examples/config is a configuration the strict decoder accepts, so the
// documentation cannot drift from the schema.
func TestExamplesValidate(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "config", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no example configuration was found")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := cli.Main(context.Background(), cli.Options{
				Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{defaults.Defaults},
			}, []string{"validate", "--config", path}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("validate %s: exit %d\nstdout: %s\nstderr: %s", path, code, stdout.String(), stderr.String())
			}
		})
	}
}

// TestExamplesResolveAManualRun: a shipped example must be able to start the
// manual CronJob the chart enables by default. The engine refuses a manual run
// on a storage with no `manual` retention partition before it does anything
// else, so a config that only declares `scheduled` retention fails here — a
// later failure (missing client binaries, no cluster reachable) is not this
// test's concern.
func TestExamplesResolveAManualRun(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "config", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no example configuration was found")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			cli.Main(context.Background(), cli.Options{
				Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{defaults.Defaults},
			}, []string{"run", "--config", path, "--kind", "manual", "--dry-run"}, &stdout, &stderr)
			if strings.Contains(stderr.String(), "retention/manual") {
				t.Fatalf("the example cannot start a manual run:\n%s", stderr.String())
			}
		})
	}
}

// TestExampleConfigValidates keeps the reference configuration honest: the file
// the README points at must pass the strict decoder of the distribution this
// command builds, secret files absent and all.
func TestExampleConfigValidates(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := cli.Main(context.Background(),
		cli.Options{Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{defaults.Defaults}},
		[]string{"validate", "--config", filepath.Join("testdata", "config.yaml")},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("validate: exit %d\nstdout: %s\nstderr: %s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "valid") {
		t.Fatalf("validate did not answer:\n%s", stdout.String())
	}
}

// instance declares one configured plugin: the reference config is checked for
// one instance of every registered type.
type instance struct {
	Type string `yaml:"type"`
}

// referenceConfig is the reference file's instance surface: one map per kind.
type referenceConfig struct {
	Discoveries map[string]instance `yaml:"discoveries"`
	Sources     map[string]instance `yaml:"sources"`
	Processors  map[string]instance `yaml:"processors"`
	Storages    map[string]storage  `yaml:"storages"`
	Notifiers   map[string]instance `yaml:"notifiers"`
	Catalog     *instance           `yaml:"catalog"`
	Stage       *struct {
		Sources map[string]instance `yaml:"sources"`
	} `yaml:"stage"`
}

// storage is one storage entry: its own type, and the retention policy inline,
// one per run kind.
type storage struct {
	Type      string              `yaml:"type"`
	Retention map[string]instance `yaml:"retention"`
}

// TestExampleConfigDocumentsEveryPlugin keeps the reference file the
// documentation of the configuration surface: a plugin the distribution
// registers but the file never instances is a plugin the file does not describe.
func TestExampleConfigDocumentsEveryPlugin(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc referenceConfig
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	present := map[string]map[string]bool{}
	add := func(kind string, instances map[string]instance) {
		if present[kind] == nil {
			present[kind] = map[string]bool{}
		}
		for _, inst := range instances {
			present[kind][inst.Type] = true
		}
	}
	add("discovery", doc.Discoveries)
	add("source", doc.Sources)
	add("processor", doc.Processors)
	present["storage"] = map[string]bool{}
	for _, st := range doc.Storages {
		present["storage"][st.Type] = true
	}
	add("notifier", doc.Notifiers)
	if doc.Stage != nil {
		add("source", doc.Stage.Sources)
	}
	present["catalog"] = map[string]bool{}
	if doc.Catalog != nil {
		present["catalog"][doc.Catalog.Type] = true
	}
	// The retention policies of the reference config: each storage carries a
	// policy per run kind, and each type present at least once.
	present["retention"] = map[string]bool{}
	for _, st := range doc.Storages {
		for _, rt := range st.Retention {
			present["retention"][rt.Type] = true
		}
	}

	reg := plugin.NewRegistry()
	if err := cli.Generic(reg); err != nil {
		t.Fatal(err)
	}
	for _, d := range reg.Descriptors() {
		if !present[d.Kind][d.Type] {
			t.Errorf("the reference config has no %s instance of type %q", d.Kind, d.Type)
		}
	}
}

// TestDistributionResolvesEveryPlugin: the k3s_state source is a first-party
// plugin like any other — resolved by this distribution's own registration.
func TestDistributionResolvesEveryPlugin(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := cli.Main(context.Background(), cli.Options{
		Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{defaults.Defaults},
	}, []string{"plugins", "describe", "source/k3s_state"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("describe source/k3s_state: exit %d\nstderr: %s", code, stderr.String())
	}
	for _, want := range []string{"sqlite3", "scratch_dir", "datastore"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("the description does not carry %q:\n%s", want, stdout.String())
		}
	}
}

// TestDefaultsAreCheckedBeforeAnyCommand: the distribution's document is checked
// against its registry before any command runs — a document that no longer
// classifies a field fails there, not one night later. One key given an
// undecodable value is the mutant this test catches.
func TestDefaultsAreCheckedBeforeAnyCommand(t *testing.T) {
	broken := bytes.Replace(defaults.Defaults, []byte("connect_attempts: 6"), []byte("connect_attempts: six"), 1)
	if bytes.Equal(broken, defaults.Defaults) {
		t.Fatal("the document no longer carries the line this mutant rewrites")
	}
	var stdout, stderr bytes.Buffer
	code := cli.Main(context.Background(), cli.Options{
		Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{broken},
	}, []string{"plugins"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("a broken defaults document passed the check:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "/plugins/source/postgres/connect_attempts") {
		t.Errorf("the failure does not name the broken key:\n%s", stderr.String())
	}
}

// TestDefaultsArePrintedAsTheyAreEmbedded: `config defaults` answers with the
// document the defaults package embeds, comments included.
func TestDefaultsArePrintedAsTheyAre(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := cli.Main(context.Background(), cli.Options{
		Program: "go-cluster-backup", Register: cli.Generic, Defaults: [][]byte{defaults.Defaults},
	}, []string{"config", "defaults"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "go-cluster-backup — the service's defaults") {
		t.Errorf("the document's own header is missing:\n%s", stdout.String())
	}
}

// TestRunWiresTheDistribution drives run — main's whole body — without a process
// exiting: the distribution's own registry and defaults answer a command through
// the streams it was handed.
func TestRunWiresTheDistribution(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"config", "defaults"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run: exit %d\nstderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "go-cluster-backup — the service's defaults") {
		t.Errorf("run did not answer with the embedded document:\n%s", stdout.String())
	}
}

// TestMainRunsACommand covers main itself — the one os.Exit call — by running the
// test binary again as the real program: the helper process rewrites argv to the
// command under test and calls main, which exits with the command's code. The
// child inherits GOCOVERDIR, so its statements land in the same profile.
func TestMainRunsACommand(t *testing.T) {
	if os.Getenv("GOCB_MAIN_HELPER") == "1" {
		os.Args = []string{"go-cluster-backup", "config", "defaults"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainRunsACommand$")
	cmd.Env = append(os.Environ(), "GOCB_MAIN_HELPER=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("the helper process failed: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "go-cluster-backup — the service's defaults") {
		t.Errorf("main did not run the command:\n%s", stdout.String())
	}
}
