package kuberneteslabels_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

func builtReporter(t *testing.T) discovery.RuleSourceReporter {
	t.Helper()
	r := registry(t)
	var example []byte
	for _, d := range r.Descriptors() {
		if d.Type == "kubernetes_labels" {
			example = d.Example
		}
	}
	if example == nil {
		t.Fatal("kubernetes_labels registered no example")
	}
	inst, err := plugin.Build(context.Background(), r, discovery.Kind, "kubernetes_labels",
		jsontext.Value(example), plugin.Deps{Instance: "workloads"})
	if err != nil {
		t.Fatal(err)
	}
	reporter, ok := inst.(discovery.RuleSourceReporter)
	if !ok {
		t.Fatal("kubernetes_labels does not expose its rules")
	}
	return reporter
}

// TestRuleSourcesExposeEveryRule: the discovery hands offline validation each
// rule as the file spells it — paths, source type, the unrendered config, the
// routing — without touching a cluster.
func TestRuleSourcesExposeEveryRule(t *testing.T) {
	rules, err := builtReporter(t).RuleSources(context.Background())
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

func TestRuleSourcesReturnOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := builtReporter(t).RuleSources(ctx); err == nil {
		t.Fatal("RuleSources ignored a cancelled context")
	}
}
