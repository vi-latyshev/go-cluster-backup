package kuberneteslabels_test

import (
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// exampleConfig is the reference config this factory registers, read from the
// compiled descriptor: the hook runs on exactly the bytes a deployment starts
// from.
func exampleConfig(t *testing.T) jsontext.Value {
	t.Helper()
	for _, d := range registry(t).Descriptors() {
		if d.Type == "kubernetes_labels" {
			return d.Example
		}
	}
	t.Fatal("kubernetes_labels registered no example")
	return nil
}

// TestRuleSourcesExposeEveryRule: the pure hook hands offline validation each
// rule as the file spells it — paths, source type, the unrendered config, the
// routing — from raw bytes, with no discovery built and no cluster touched.
func TestRuleSourcesExposeEveryRule(t *testing.T) {
	rules, err := kuberneteslabels.RuleSources(exampleConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	// The last rule is the undeclared accounting: it renders no source, so its
	// type is empty and its config is empty — its routing is what validation
	// still checks. A pod rule's paths name its own list.
	wantTypes := []string{"postgres", "redis", "files", ""}
	wantPaths := []string{"/pods/0", "/pods/1", "/volumes/0", "/undeclared_volumes"}
	wantSensitivity := []model.Sensitivity{model.SensitivityPlain, model.SensitivityPlain, model.SensitivityPlain, model.SensitivityPlain}
	if len(rules) != len(wantTypes) {
		t.Fatalf("%d rules, want %d: %+v", len(rules), len(wantTypes), rules)
	}
	for i, want := range wantTypes {
		rs := rules[i]
		if rs.Path != wantPaths[i] {
			t.Fatalf("rule %d path: %+v", i, rs)
		}
		if want == "" && rs.ConfigPath != "" {
			t.Fatalf("the accounting rule has no source config to report: %+v", rs)
		}
		if want != "" && rs.ConfigPath != wantPaths[i]+"/source/config" {
			t.Fatalf("rule %d config path: %+v", i, rs)
		}
		if rs.Type != want || rs.Sensitivity != wantSensitivity[i] {
			t.Fatalf("rule %d routing: %+v", i, rs)
		}
		if want == "" {
			// The accounting declares no pipeline: it reports failures, it does
			// not route data.
			if len(rs.Pipeline) != 0 {
				t.Fatalf("the accounting rule declares a pipeline: %+v", rs)
			}
			continue
		}
		if !slices.Equal(rs.Pipeline, []string{"compress"}) {
			t.Fatalf("rule %d pipeline: %+v", i, rs)
		}
		if rs.Importance != model.ImportanceRequired && rs.Importance != model.ImportanceBestEffort {
			t.Fatalf("rule %d importance %q", i, rs.Importance)
		}
	}
	if rules[1].Importance != model.ImportanceBestEffort {
		t.Fatalf("rule 1 importance %q, want best_effort", rules[1].Importance)
	}
	// The config is the file's own bytes: templates still unrendered, nothing
	// decoded into the source's config type.
	if !strings.Contains(string(rules[0].Config), "{{ envValue 0") ||
		!strings.Contains(string(rules[0].Config), "{{ port 0 0 }}") {
		t.Fatalf("rule 0 config is not the unrendered source config: %s", rules[0].Config)
	}
}

// TestRuleSourcesRefuseMalformedConfig: the hook reads raw bytes, so bytes that
// are not this plugin's config are refused by the strict decoder instead of
// yielding a partial rule set.
func TestRuleSourcesRefuseMalformedConfig(t *testing.T) {
	if _, err := kuberneteslabels.RuleSources(jsontext.Value(`{"pods": []}`)); err == nil {
		t.Fatal("RuleSources accepted a config missing every required field")
	}
	if _, err := kuberneteslabels.RuleSources(jsontext.Value(`{`)); err == nil {
		t.Fatal("RuleSources accepted bytes that are not JSON")
	}
}

// TestRuleSourcesThroughTheConfigurationSchema: a discovery declared in the one
// config document reaches the merge and offline validation with no plugin
// constructed and no cluster touched.
func TestRuleSourcesThroughTheConfigurationSchema(t *testing.T) {
	rules, errs := configuration.ExtractRules(registry(t), map[string]configuration.Instance{
		"workloads": {Type: "kubernetes_labels", Config: exampleConfig(t)},
	})
	if len(errs) != 0 {
		t.Fatalf("ExtractRules = %v, want no failures", errs)
	}
	if got := len(rules["workloads"]); got != 4 {
		t.Fatalf("ExtractRules read %d rules, want 4: %+v", got, rules["workloads"])
	}
}

// TestRuleSourcesCarryTheRuleRateLimit: the pure hook exposes a rule's
// rate_limit beside its routing, so offline validation sees the same envelope
// runtime renders.
func TestRuleSourcesCarryTheRuleRateLimit(t *testing.T) {
	raw := jsontext.Value(`{
	  "api_host_env":"H","api_port_env":"P","token_file":"/t","ca_file":"/c",
	  "request_timeout":"1s","secrets_path":"/s","list_page_size":10,
	  "exclude":{"label":"l","value":"v"},
	  "undeclared_volumes":{"name":"u","importance":"required"},
	  "node_env":"","node_scope":false,"writer_nodes":[],
	  "pods":[{"name":"r","selector":"","phase":"Running","artifact":{"group":"g","name":"n","importance":"required","sensitivity":"plain"},
	    "pipeline":[],"source":{"type":"files","config":{"root":"/r"}},"covers_volumes":false,"min_matches":0,"rate_limit":1048576}],
	  "volumes":[]
	}`)
	rules, err := kuberneteslabels.RuleSources(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 {
		t.Fatalf("rules = %+v, want the pod rule and the accounting", rules)
	}
	if rules[0].RateLimit != 1<<20 {
		t.Fatalf("rule rate limit = %d, want 1048576", rules[0].RateLimit)
	}
	if rules[1].RateLimit != 0 {
		t.Fatalf("the accounting rule carries a rate limit: %+v", rules[1])
	}
	if !slices.Equal(rules[0].Pipeline, []string{}) {
		t.Fatalf("pipeline = %v, want the empty list as written", rules[0].Pipeline)
	}
}
