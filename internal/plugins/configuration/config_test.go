package configuration

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func validRetryConfig() RetryConfig {
	return RetryConfig{
		Attempts:     1,
		InitialDelay: model.Duration(time.Millisecond),
		MaxDelay:     model.Duration(time.Millisecond),
		Jitter:       0,
	}
}

func validRunConfig() *RunConfig {
	return &RunConfig{
		IDLayout:            "20060102T150405Z",
		SpoolDir:            "/spool",
		SpoolReserveBytes:   0,
		MaxArtifactBytes:    1,
		Deadline:            model.Duration(time.Second),
		FinishAllowance:     model.Duration(time.Second),
		MinVerifiedStorages: 1,
		StorageParallelism:  1,
		UnclassifiedError:   pluginerrors.ClassTransient,
		Retry:               validRetryConfig(),
		LeaseWait:           model.Duration(0),
		LeasePoll:           model.Duration(time.Millisecond),
		LeaseHardBound:      model.Duration(time.Second),
	}
}

func validArtifactConfig() ArtifactConfig {
	return ArtifactConfig{
		Group:       "g",
		Name:        "n",
		Importance:  model.ImportanceRequired,
		Sensitivity: model.SensitivityPlain,
	}
}

func baseConfig() *Config {
	return &Config{Run: validRunConfig(), Layout: LayoutConfig{ManualIDSuffix: "-manual"}}
}

func TestConfigValidateRejectsEveryField(t *testing.T) {
	withRestore := func(mutate func(*RestoreConfig)) func(*Config) {
		return func(c *Config) {
			c.Restore = &RestoreConfig{SpoolDir: "/r", StorageOrder: []string{"s"}, UnclassifiedError: pluginerrors.ClassTransient, Retry: validRetryConfig()}
			mutate(c.Restore)
		}
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"id_layout", func(c *Config) { c.Run.IDLayout = "" }, "/run/id_layout is empty"},
		{"spool_dir", func(c *Config) { c.Run.SpoolDir = "" }, "/run/spool_dir is empty"},
		{"spool_reserve_bytes", func(c *Config) { c.Run.SpoolReserveBytes = -1 }, "spool_reserve_bytes is negative"},
		{"max_artifact_bytes", func(c *Config) { c.Run.MaxArtifactBytes = 0 }, "max_artifact_bytes must be at least 1"},
		{"deadline", func(c *Config) { c.Run.Deadline = 0 }, "deadline must be positive"},
		{"finish_allowance", func(c *Config) { c.Run.FinishAllowance = 0 }, "finish_allowance must be positive"},
		{"min_verified_storages", func(c *Config) { c.Run.MinVerifiedStorages = 0 }, "min_verified_storages must be at least 1"},
		{"run exit codes", func(c *Config) { c.Run.DegradedExitCode = -1 }, "exit codes must not be negative"},
		{"storage_parallelism", func(c *Config) { c.Run.StorageParallelism = 0 }, "storage_parallelism must be at least 1"},
		{"unclassified_error", func(c *Config) { c.Run.UnclassifiedError = "maybe" }, "unclassified_error must be"},
		{"lease_wait", func(c *Config) { c.Run.LeaseWait = model.Duration(-1) }, "lease_wait must not be negative"},
		{"lease_poll", func(c *Config) { c.Run.LeasePoll = 0 }, "lease_poll must be positive"},
		{"lease_hard_bound", func(c *Config) { c.Run.LeaseHardBound = 0 }, "lease_hard_bound must be positive"},
		{"manual_id_suffix empty", func(c *Config) { c.Layout.ManualIDSuffix = "" }, "manual_id_suffix is empty"},
		{"manual_id_suffix slash", func(c *Config) { c.Layout.ManualIDSuffix = "a/b" }, "must not carry"},
		{"manual_id_suffix dotdot", func(c *Config) { c.Layout.ManualIDSuffix = "a..b" }, "must not carry"},
		{"restore spool_dir", withRestore(func(r *RestoreConfig) { r.SpoolDir = "" }), "/restore/spool_dir is empty"},
		{"restore spool_reserve_bytes", withRestore(func(r *RestoreConfig) { r.SpoolReserveBytes = -1 }), "/restore/spool_reserve_bytes is negative"},
		{"restore storage_order", withRestore(func(r *RestoreConfig) { r.StorageOrder = nil }), "/restore/storage_order is empty"},
		{"restore unknown storage", withRestore(func(r *RestoreConfig) { r.StorageOrder = []string{"absent"} }), "unknown storage"},
		{"restore unclassified_error", withRestore(func(r *RestoreConfig) { r.UnclassifiedError = "maybe" }), "/restore/unclassified_error must be"},
		{"restore retry", withRestore(func(r *RestoreConfig) { r.Retry.Attempts = 0 }), "/restore/retry/attempts must be at least 1"},
		{"stage dir", func(c *Config) { c.Stage = &StageConfig{Dir: ""} }, "/stage/dir is empty"},
		{"stage exit codes", func(c *Config) { c.Stage = &StageConfig{Dir: "/d", DegradedExitCode: -1} }, "/stage exit codes must not be negative"},
		{"storage max_total_bytes", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", MaxTotalBytes: -1, TimeBudget: model.Duration(time.Second)}}
		}, "max_total_bytes must not be negative"},
		{"storage call_parallelism", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", TimeBudget: model.Duration(time.Second)}}
		}, "call_parallelism must be at least 1"},
		{"retention type empty", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", TimeBudget: model.Duration(time.Second), Retention: map[string]*Instance{"scheduled": nil}}}
		}, "/type is empty"},
		{"retention unknown kind", func(c *Config) {
			c.Storages = map[string]StorageInstance{"s": {Type: "fake", TimeBudget: model.Duration(time.Second), Retention: map[string]*Instance{"weekly": {Type: "keep_last"}}}}
		}, "unknown run kind"},
		{"pipeline empty ref", func(c *Config) {
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: validArtifactConfig(), Pipeline: []string{""}}}
		}, "pipeline/0 is empty"},
		{"source rate_limit", func(c *Config) {
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: validArtifactConfig(), Pipeline: []string{}, RateLimit: -1}}
		}, "rate_limit must not be negative"},
		{"artifact group", func(c *Config) {
			a := validArtifactConfig()
			a.Group = ""
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "/group is empty"},
		{"artifact name", func(c *Config) {
			a := validArtifactConfig()
			a.Name = ""
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "/name is empty"},
		{"artifact importance", func(c *Config) {
			a := validArtifactConfig()
			a.Importance = "maybe"
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "importance must be"},
		{"artifact sensitivity", func(c *Config) {
			a := validArtifactConfig()
			a.Sensitivity = "weird"
			c.Sources = map[string]SourceInstance{"s": {Type: "fake", Artifact: a, Pipeline: []string{}}}
		}, "sensitivity must be"},
		{"run retry initial_delay", func(c *Config) { c.Run.Retry.InitialDelay = 0 }, "initial_delay must be positive"},
		{"run retry max_delay", func(c *Config) {
			c.Run.Retry.InitialDelay = model.Duration(2 * time.Millisecond)
			c.Run.Retry.MaxDelay = model.Duration(time.Millisecond)
		}, "max_delay must not be smaller"},
		{"run retry jitter", func(c *Config) { c.Run.Retry.Jitter = 1 }, "jitter must be in [0, 1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate accepted a broken config (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q lacks %q", err.Error(), tc.want)
			}
		})
	}
}

func TestConfigValidateAcceptsACompleteRunConfig(t *testing.T) {
	if err := baseConfig().Validate(); err != nil {
		t.Fatalf("a complete config was refused: %v", err)
	}
	// A config with no run and no restore section is the restore-only shape: it
	// carries no run knobs and is still valid.
	only := &Config{Layout: LayoutConfig{ManualIDSuffix: "-manual"}}
	if err := only.Validate(); err != nil {
		t.Fatalf("a run-less config was refused: %v", err)
	}
}

// TestConfigPluginsValidation covers the plugins root's own structure the
// decoder can judge without the compiled set: the settings keys and their
// nulls. The selection lists are CheckSelection's (TestR9_M20).
func TestConfigPluginsValidation(t *testing.T) {
	cases := []struct {
		name    string
		plugins *Plugins
		want    string
	}{
		{"a malformed settings key", &Plugins{Config: map[string]jsontext.Value{"source/": jsontext.Value(`{}`)}}, "/plugins/config/source/: not a plugin ID"},
		{"a null settings block", &Plugins{Config: map[string]jsontext.Value{"source/x": jsontext.Value(`null`)}}, "is null"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Plugins = c.plugins
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Validate = %v, want %q", err, c.want)
			}
		})
	}
}

// TestParseIDAndValidID: the one spelling of a factory identity.
func TestParseIDAndValidID(t *testing.T) {
	cases := []struct {
		id   string
		kind Kind
		typ  string
		ok   bool
		// valid is ValidID's answer: a known kind, or a stable provider name.
		valid bool
	}{
		{id: "source/postgres", kind: KindSource, typ: "postgres", ok: true, valid: true},
		{id: "source/a/b", ok: false, valid: false},
		{id: "source/", ok: false, valid: false},
		{id: "/postgres", ok: false, valid: false},
		{id: "unknown/postgres", kind: "unknown", typ: "postgres", ok: true, valid: false},
		{id: "reporting", ok: false, valid: true},
		{id: "", ok: false, valid: false},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			kind, typ, ok := ParseID(c.id)
			if ok != c.ok || kind != c.kind || typ != c.typ {
				t.Fatalf("ParseID(%q) = (%q, %q, %v)", c.id, kind, typ, ok)
			}
			if got := ValidID(c.id); got != c.valid {
				t.Errorf("ValidID(%q) = %v, want %v", c.id, got, c.valid)
			}
		})
	}
	if got := ID(KindSource, "postgres"); got != "source/postgres" {
		t.Errorf("ID = %q", got)
	}
}

// TestSelectAppliesBundlesThenDisabled: the compiled set starts available, every
// listed bundle applies in order, then the disabled list; an unknown or repeated
// name is an error and never leaves a plugin on by accident.
func TestSelectAppliesBundlesThenDisabled(t *testing.T) {
	compiled := []string{"source/a", "source/b", "processor/x"}
	bundles := []Bundle{
		{Name: "minimal", Disables: []string{"processor/x"}},
		{Name: "no-b", Disables: []string{"source/b"}},
	}
	plugins := &Plugins{Bundles: []string{"minimal"}, Disabled: []string{"source/a"}}
	active, errs := plugins.Select(compiled, bundles)
	if len(errs) != 0 {
		t.Fatalf("Select = %v, want no errors", errs)
	}
	if active.Enabled("source/a") || !active.Enabled("source/b") || active.Enabled("processor/x") {
		t.Errorf("the selection resolved wrong: %v", active)
	}

	// A zero selection — no plugins root at all — enables everything.
	var none *Plugins
	active, errs = none.Select(compiled, bundles)
	if len(errs) != 0 || !active.Enabled("source/a") {
		t.Fatalf("a nil plugins root = (%v, %v), want everything enabled", active, errs)
	}
	if !(Active{}).Enabled("anything") {
		t.Error("the zero Active does not enable everything")
	}
}

// TestSelectRefusesBrokenSelections: every way a selection can go wrong is an
// error, and the active set still carries every compiled ID.
func TestSelectRefusesBrokenSelections(t *testing.T) {
	compiled := []string{"source/a"}
	cases := []struct {
		name    string
		plugins *Plugins
		bundles []Bundle
		want    string
	}{
		{"a bundle declared twice by the build", &Plugins{}, []Bundle{{Name: "x"}, {Name: "x"}}, "declared twice"},
		{"a compiled bundle disabling nothing compiled", &Plugins{}, []Bundle{{Name: "x", Disables: []string{"ghost"}}}, "not a compiled plugin"},
		{"an unknown bundle", &Plugins{Bundles: []string{"ghost"}}, nil, "unknown bundle"},
		{"a bundle listed twice", &Plugins{Bundles: []string{"x", "x"}}, []Bundle{{Name: "x"}}, "listed twice"},
		{"an unknown disabled id", &Plugins{Disabled: []string{"ghost"}}, nil, "unknown plugin"},
		{"a disabled id listed twice", &Plugins{Disabled: []string{"source/a", "source/a"}}, nil, "listed twice"},
	}
	off := map[string]bool{"a disabled id listed twice": true}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			active, errs := c.plugins.Select(compiled, c.bundles)
			if !anyErrHas(errs, c.want) {
				t.Fatalf("Select = %v, want %q", errs, c.want)
			}
			if active.Enabled("source/a") == off[c.name] {
				t.Errorf("the invalid selection resolved %v: %v", c.name, active)
			}
		})
	}
}

func TestActiveEnabledAndActiveFrom(t *testing.T) {
	active := ActiveFrom([]string{"source/a"})
	if !active.Enabled("source/a") || active.Enabled("source/b") {
		t.Fatalf("ActiveFrom = %v", active)
	}
	var zero Active
	if !zero.Enabled("whatever") {
		t.Error("the zero Active does not enable everything")
	}
}

// TestRequiredRolesAndCheckRoles: a present run, stage or restore section needs
// its role, and the check is offline — it reads the compiled providers and the
// selection, never a running command.
func TestRequiredRolesAndCheckRoles(t *testing.T) {
	roles := Roles{
		RoleBackup:  {"workflow/backup"},
		RoleStage:   {"workflow/stage"},
		RoleRestore: {"workflow/restore"},
	}
	cfg := &Config{
		Run:     &RunConfig{},
		Stage:   &StageConfig{},
		Restore: &RestoreConfig{},
	}
	if got := strings.Join(roleNames(cfg.RequiredRoles()), ","); got != "backup-job,stage-job,restore-job" {
		t.Fatalf("RequiredRoles = %s", got)
	}
	if errs := cfg.CheckRoles(Active{}, roles); len(errs) != 0 {
		t.Fatalf("CheckRoles with every provider active = %v", errs)
	}
	// No section, no role.
	if errs := (&Config{}).CheckRoles(Active{}, nil); len(errs) != 0 {
		t.Fatalf("CheckRoles of a section-less config = %v", errs)
	}
	// A selected-off provider leaves its role unavailable, and the error names
	// the section that needs it.
	active := ActiveFrom([]string{"workflow/stage"})
	errs := cfg.CheckRoles(active, roles)
	if len(errs) != 2 {
		t.Fatalf("CheckRoles = %v, want the backup and restore roles refused", errs)
	}
	for _, want := range []string{`/run: the section needs the "backup-job" role`, `/restore: the section needs the "restore-job" role`} {
		if !anyErrHas(errs, want) {
			t.Errorf("CheckRoles does not report %q: %v", want, errs)
		}
	}
	// A role with no compiled provider at all is unavailable too.
	if errs := (&Config{Run: &RunConfig{}}).CheckRoles(Active{}, nil); len(errs) != 1 {
		t.Fatalf("CheckRoles without providers = %v, want the role refused", errs)
	}
	// The stage section names itself in its refusal too.
	if errs := (&Config{Stage: &StageConfig{}}).CheckRoles(ActiveFrom(nil), roles); !anyErrHas(errs, `/stage: the section needs the "stage-job" role`) {
		t.Fatalf("CheckRoles of a stage-only config = %v", errs)
	}
}

func roleNames(roles []Role) []string {
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		out = append(out, string(r))
	}
	return out
}

// TestConfigDecodeRefusesTheDeletedVersion: the versioned file shape is gone —
// a document that still carries it is refused with its pointer, and the unified
// plugins root decodes.
func TestConfigDecodeRefusesTheDeletedVersion(t *testing.T) {
	raw := jsontext.Value(`{"version":1,"layout":{"manual_id_suffix":"-manual"}}`)
	var cfg Config
	if err := Decode(raw, &cfg); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("Decode accepted the deleted version field: %v", err)
	}
	raw = jsontext.Value(`{"plugins":{"bundles":[],"disabled":[],"config":{"processor/zstd":{"concurrency":1}}},"layout":{"run_prefix":"runs/{{ .Run.ID }}","artifact_key":"a","manifest_key":"m","manual_id_suffix":"-manual"}}`)
	if err := Decode(raw, &cfg); err != nil {
		t.Fatalf("Decode refused the unified plugins root: %v", err)
	}
	if cfg.Plugins == nil || len(cfg.Plugins.Config) != 1 {
		t.Fatalf("the plugins root did not decode: %+v", cfg.Plugins)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the decoded config does not validate: %v", err)
	}
}

// TestCheckRulesReportsEveryFailure: the offline rule check reads the pure hook
// and reports an extraction failure, an unknown and a disabled source type, a
// malformed envelope, a null nested value, and a routing name that does not
// exist — all in one pass.
func TestCheckRulesReportsEveryFailure(t *testing.T) {
	schema := testKit()
	cfg := &Config{
		Processors: map[string]Instance{"pack": {Type: "testkit"}},
		Discoveries: map[string]Instance{
			"broken": {Type: "brokenhook", Config: jsontext.Value(`{"rules":[]}`)},
			"d":      {Type: "testkit", Config: jsontext.Value(`{"rules":[{"name":"r","source":{"type":"ghost","config":{}},"pipeline":["absent"],"min_matches":0}]}`)},
			"nulled": {Type: "testkit", Config: jsontext.Value(`{"rules":[{"name":"r","source":{"type":"testkit","config":{"timeout":null}},"pipeline":["pack"],"min_matches":0}]}`)},
		},
	}
	schema.configs["discovery/brokenhook"] = reflect.TypeFor[ruleConfig]()
	schema.hooks["discovery/brokenhook"] = failingHook
	schema.configs["discovery/envelope"] = reflect.TypeFor[ruleConfig]()
	schema.hooks["discovery/envelope"] = func(jsontext.Value) ([]RuleSource, error) {
		return []RuleSource{{
			Path: "/rules/0", ConfigPath: "/rules/0/source/config",
			Type: "testkit", Config: jsontext.Value(`{"address":"x","timeout":"30s"}`),
			Pipeline: []string{""}, Importance: "maybe", Sensitivity: "weird", RateLimit: -1,
		}}, nil
	}
	cfg.Discoveries["envelope"] = Instance{Type: "envelope", Config: jsontext.Value(`{"rules":[]}`)}
	errs := CheckRules(schema, Active{}, cfg)
	for _, want := range []string{
		"/discoveries/broken/config: reading the rules",
		"unknown source type \"ghost\"",
		"unknown processor \"absent\"",
		"is null",
		"importance must be",
		"sensitivity must be",
		"rate_limit must not be negative",
		"pipeline/0 is empty",
	} {
		if !anyErrHas(errs, want) {
			t.Errorf("CheckRules does not report %q:\n%v", want, errs)
		}
	}

	// A rule's nested source config is opaque bytes to the strict decoder: its shape
	// errors carry their own pointers, and a null member is refused.
	cfg.Discoveries["shape"] = Instance{Type: "testkit", Config: jsontext.Value(
		`{"rules":[{"name":"r","source":{"type":"testkit","config":{"adress":"x","timeout":null}},"pipeline":["pack"],"min_matches":0}]}`)}
	// A discovery name carrying a slash is one pointer member: its paths are
	// escaped the RFC 6901 way.
	cfg.Discoveries["sl/ash"] = Instance{Type: "testkit", Config: jsontext.Value(
		`{"rules":[{"name":"r","source":{"type":"testkit","config":{}},"pipeline":["pack"],"min_matches":0}]}`)}
	errs = CheckRules(schema, Active{}, cfg)
	for _, want := range []string{
		"missing field /discoveries/shape/rules/0/source/config/address",
		"unknown field /discoveries/shape/rules/0/source/config/adress",
		"/discoveries/shape/rules/0/source/config/timeout is null",
		"missing field /discoveries/sl~1ash/rules/0/source/config/address",
	} {
		if !anyErrHas(errs, want) {
			t.Errorf("CheckRules does not report %q:\n%v", want, errs)
		}
	}

	// A source type the selection leaves off can never produce one.
	disabled := []string{"source/testkit"}
	active, errs := (&Plugins{Disabled: disabled}).Select([]string{"discovery/testkit", "source/testkit", "processor/testkit"}, nil)
	if len(errs) != 0 {
		t.Fatalf("Select: %v", errs)
	}
	cfg.Discoveries = map[string]Instance{"d": {Type: "testkit", Config: jsontext.Value(`{"rules":[{"name":"r","source":{"type":"testkit","config":{"address":"x"}},"pipeline":["pack"],"min_matches":0}]}`)}}
	if got := CheckRules(schema, active, cfg); !anyErrHas(got, "disabled by the plugin selection") {
		t.Errorf("CheckRules accepted a disabled rule source: %v", got)
	}

	// A schema without the hook has no rules to report at all.
	if got, errs := ExtractRules(noHookSchema{schema}, cfg.Discoveries); got != nil || errs != nil {
		t.Errorf("ExtractRules without the hook = (%v, %v), want nothing", got, errs)
	}
}

// TestExtractRulesSkipsAnEmptyType: an instance without a type contributes
// nothing; the strict decoder names it.
func TestExtractRulesSkipsAnEmptyType(t *testing.T) {
	rules, errs := ExtractRules(testKit(), map[string]Instance{"d": {Config: jsontext.Value(`{}`)}})
	if errs != nil || len(rules) != 0 {
		t.Fatalf("ExtractRules = (%v, %v), want nothing", rules, errs)
	}
}
