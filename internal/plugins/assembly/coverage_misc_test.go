package assembly

import (
	"testing"
)

// TestBuildReporterInstallsAValidReporter: a complete reporting block builds the
// reporter the run's previous-run record comes from, and the assembly keeps it.
func TestBuildReporterInstallsAValidReporter(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{
		"reporting": map[string]any{
			"api_host_env": "KUBERNETES_SERVICE_HOST", "api_port_env": "KUBERNETES_SERVICE_PORT",
			"token_file":      "/var/run/secrets/kubernetes.io/serviceaccount/token",
			"ca_file":         "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
			"request_timeout": "30s", "walk_timeout": "30s", "namespace_env": "POD_NAMESPACE", "pod_env": "POD_NAME",
		},
	}}
	e := w.newAssembly(t, cfg)
	if e.reporter == nil {
		t.Fatal("a valid reporting block built no reporter")
	}
}

// TestInstanceTypeNamesTheDeclaredType: the binary lookup asks a registry which
// programs one instance's plugin type declares, so the projection names the type
// each instance was declared with; a stage source on a run-only config and a
// kind no scope lists have none.
func TestInstanceTypeNamesTheDeclaredType(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"d": instance("fake")}
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	e := w.newAssembly(t, cfg)

	cases := []struct{ kind, name, want string }{
		{"processor", "zstd3", "fake"},
		{"source", "vpn", "fake"},
		{"storage", "primary", "fake"},
		{"discovery", "d", "fake"},
		{"notifier", "ops", "fake"},
		{"catalog", "catalog", "fake"},
		{"stage_source", "snapshot", ""},
		{"unknown", "x", ""},
	}
	for _, tc := range cases {
		if got := e.instanceType(tc.kind, tc.name); got != tc.want {
			t.Errorf("instanceType(%q, %q) = %q, want %q", tc.kind, tc.name, got, tc.want)
		}
	}
}
