package reporting_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
)

func validConfig() reporting.Config {
	return reporting.Config{
		APIHostEnv: "GCB_TEST_HOST", APIPortEnv: "GCB_TEST_PORT",
		TokenFile: "/var/run/secrets/token", CAFile: "/var/run/secrets/ca.crt",
		RequestTimeout: model.Duration(30 * time.Second),
		NamespaceEnv:   "GCB_TEST_NAMESPACE", PodEnv: "GCB_TEST_POD",
	}
}

// TestConfigValidate: the complete config passes, and every field that cannot
// work is named.
func TestConfigValidate(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the complete config was refused: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*reporting.Config)
		want   string
	}{
		{name: "host env", mutate: func(c *reporting.Config) { c.APIHostEnv = "" }, want: "/api_host_env is empty"},
		{name: "port env", mutate: func(c *reporting.Config) { c.APIPortEnv = "" }, want: "/api_port_env is empty"},
		{name: "empty token file", mutate: func(c *reporting.Config) { c.TokenFile = "" }, want: "/token_file is empty"},
		{name: "relative token file", mutate: func(c *reporting.Config) { c.TokenFile = "token" }, want: "/token_file \"token\" must be absolute"},
		{name: "empty CA file", mutate: func(c *reporting.Config) { c.CAFile = "" }, want: "/ca_file is empty"},
		{name: "relative CA file", mutate: func(c *reporting.Config) { c.CAFile = "ca.crt" }, want: "/ca_file \"ca.crt\" must be absolute"},
		{name: "timeout", mutate: func(c *reporting.Config) { c.RequestTimeout = 0 }, want: "/request_timeout must be positive"},
		{name: "namespace env", mutate: func(c *reporting.Config) { c.NamespaceEnv = "" }, want: "/namespace_env is empty"},
		{name: "pod env", mutate: func(c *reporting.Config) { c.PodEnv = "" }, want: "/pod_env is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestNewRefusesAnInvalidConfig: a config that cannot work never builds a
// reporter.
func TestNewRefusesAnInvalidConfig(t *testing.T) {
	if _, err := reporting.New(context.Background(), reporting.Config{}); err == nil {
		t.Fatal("an empty config built a reporter")
	}
}

// TestNewRefusesACancelledContext: a cancelled start is refused before any work.
func TestNewRefusesACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reporting.New(ctx, validConfig()); err == nil {
		t.Fatal("a cancelled context built a reporter")
	}
}

// TestNewBuildsNoClient: building a reporter reads nothing — an unreachable API
// server is the first inspection's problem, not the constructor's.
func TestNewBuildsNoClient(t *testing.T) {
	t.Setenv(testNamespaceEnv, testNamespace)
	t.Setenv(testPodEnv, "backup-now")
	cfg := validConfig()
	cfg.TokenFile, cfg.CAFile = "/nonexistent-token", "/nonexistent-ca"
	if _, err := reporting.New(context.Background(), cfg); err != nil {
		t.Fatalf("the constructor touched the cluster's files: %v", err)
	}
}

// TestApplyPublishesTheProviderSchema: the composition root's registration maps
// the stable ID to this config type, which is what the compiled schema's
// provider walk judges /plugins/config/reporting against.
func TestApplyPublishesTheProviderSchema(t *testing.T) {
	factories := instances.NewRegistry()
	r := loader.New()
	if err := loader.Add(r, reporting.Spec, func(lr *loader.Registry) error {
		return reporting.Apply(lr, factories)
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := factories.ProviderConfigType(reporting.Spec.ID)
	if !ok || got != reflect.TypeFor[reporting.Config]() {
		t.Fatalf("provider config type = %v, %v; want reporting.Config", got, ok)
	}
	var schema configuration.ProviderSchema = factories
	if _, ok := schema.ProviderConfigType(reporting.Spec.ID); !ok {
		t.Fatalf("the registry no longer satisfies configuration.ProviderSchema for %q", reporting.Spec.ID)
	}
}
