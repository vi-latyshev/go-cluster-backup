package lifecycle

import (
	"math"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func modelDuration(d time.Duration) model.Duration { return model.Duration(d) }

// TestM3_NegativeBudgetIsRefused encodes the reviewer's probe: join=-10m made the
// sum non-positive and RequiredGrace answered zero, which the grace gate read as
// "no deadline". The negative budget must be refused instead.
func TestM3_NegativeBudgetIsRefused(t *testing.T) {
	_, err := RequiredGrace(GraceInputs{
		Join: -10 * time.Minute, Release: 2 * time.Minute,
		AbortTotal: 2 * time.Minute, Close: 30 * time.Second, Margin: 30 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "join must be positive") {
		t.Fatalf("RequiredGrace with join=-10m = %v, want a refusal naming join", err)
	}
}

// TestM3_BudgetRangesAreEnforced: every cleanup budget has an allowed range and
// a violation is an error, not a bound the caller could treat as unbounded.
func TestM3_BudgetRangesAreEnforced(t *testing.T) {
	valid := GraceInputs{Join: time.Minute, Release: 2 * time.Minute,
		AbortTotal: 2 * time.Minute, Close: 30 * time.Second, Margin: 30 * time.Second}
	cases := []struct {
		name string
		drop func(*GraceInputs)
		want string
	}{
		{"a negative preStop", func(in *GraceInputs) { in.PreStop = -time.Second }, "pre_stop must not be negative"},
		{"a zero join", func(in *GraceInputs) { in.Join = 0 }, "join must be positive"},
		{"a negative join", func(in *GraceInputs) { in.Join = -time.Minute }, "join must be positive"},
		{"a negative release", func(in *GraceInputs) { in.Release = -time.Second }, "release must not be negative"},
		{"a zero abort_total", func(in *GraceInputs) { in.AbortTotal = 0 }, "abort_total must be positive"},
		{"a zero close", func(in *GraceInputs) { in.Close = 0 }, "close must be positive"},
		{"a negative margin", func(in *GraceInputs) { in.Margin = -time.Second }, "margin must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := valid
			tc.drop(&in)
			if _, err := RequiredGrace(in); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RequiredGrace(%+v) = %v, want a refusal naming %q", in, err, tc.want)
			}
		})
	}
}

// TestM3_OverflowIsRefused: checked addition never wraps into a small bound.
func TestM3_OverflowIsRefused(t *testing.T) {
	over := time.Duration(math.MaxInt64)
	_, err := RequiredGrace(GraceInputs{
		Join: over - 1, Release: over - 1, AbortTotal: time.Nanosecond, Close: time.Nanosecond,
	})
	if err == nil || !strings.Contains(err.Error(), "overflows time.Duration") {
		t.Fatalf("RequiredGrace at the duration ceiling = %v, want an overflow refusal", err)
	}
	// The gate can also overflow while rounding the (valid) sum up to seconds.
	_, err = RequiredGrace(GraceInputs{
		Join: over - 10, AbortTotal: time.Nanosecond, Close: time.Nanosecond,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be rounded up") {
		t.Fatalf("RequiredGrace near the ceiling = %v, want a rounding refusal", err)
	}
}

// TestM3_RoundsUpToASecond: the chart field is in whole seconds, and a fraction
// of a second of cleanup must not be rounded away.
func TestM3_RoundsUpToASecond(t *testing.T) {
	cases := []struct {
		name string
		in   GraceInputs
		want time.Duration
	}{
		{"an exact sum", GraceInputs{Join: time.Second, AbortTotal: time.Second,
			Close: time.Second}, 3 * time.Second},
		{"a fractional sum", GraceInputs{Join: 1500 * time.Millisecond,
			AbortTotal: time.Nanosecond, Close: time.Nanosecond}, 2 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RequiredGrace(tc.in)
			if err != nil {
				t.Fatalf("RequiredGrace(%+v): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("RequiredGrace(%+v) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

// TestM3_EffectiveDefaults pins the number the chart ships: no preStop hook,
// join 1m, release 2m (run.finish_allowance), abort_total 2m, close 30s, margin
// 30s.
func TestM3_EffectiveDefaults(t *testing.T) {
	got, err := RequiredGrace(GraceInputs{
		Join:       time.Minute,
		Release:    2 * time.Minute,
		AbortTotal: 2 * time.Minute,
		Close:      30 * time.Second,
		Margin:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("RequiredGrace of the effective defaults: %v", err)
	}
	if want := 6 * time.Minute; got != want {
		t.Fatalf("RequiredGrace of the effective defaults = %s, want %s", got, want)
	}
}

// TestM3_ConfigValidateRefusesInvalidBudgets: the effective block is validated
// at decode time, before any runtime cleanup decision or unbounded fallback.
func TestM3_ConfigValidateRefusesInvalidBudgets(t *testing.T) {
	valid := Config{
		Join:       modelDuration(time.Minute),
		AbortTotal: modelDuration(2 * time.Minute),
		Close:      modelDuration(30 * time.Second),
		Margin:     modelDuration(30 * time.Second),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("a valid config = %v", err)
	}
	cases := []struct {
		name string
		drop func(*Config)
		want string
	}{
		{"a zero join", func(c *Config) { c.Join = 0 }, "join must be positive"},
		{"a negative join", func(c *Config) { c.Join = modelDuration(-time.Minute) }, "join must be positive"},
		{"a zero abort_total", func(c *Config) { c.AbortTotal = 0 }, "abort_total must be positive"},
		{"a zero close", func(c *Config) { c.Close = 0 }, "close must be positive"},
		{"a negative margin", func(c *Config) { c.Margin = modelDuration(-time.Second) }, "margin must not be negative"},
		{"an overflowing sum", func(c *Config) {
			c.Join = math.MaxInt64
			c.AbortTotal = math.MaxInt64
		}, "overflows time.Duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.drop(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate(%+v) = %v, want a refusal naming %q", cfg, err, tc.want)
			}
		})
	}
}

// TestM3_DecodeRefusesAnInvalidEffectiveBlock: configuration.Decode runs the
// Validator, so the invalid block never reaches a runtime cleanup decision.
func TestM3_DecodeRefusesAnInvalidEffectiveBlock(t *testing.T) {
	raw := jsontext.Value(`{"join": "-10m", "abort_total": "2m", "close": "30s", "margin": "30s"}`)
	var cfg Config
	err := configuration.Decode(raw, &cfg)
	if err == nil || !strings.Contains(err.Error(), "join must be positive") {
		t.Fatalf("Decode of join=-10m = %v, want a refusal naming join", err)
	}
}

// TestConfigDecodesTheLifecycleBudgets: the plugin's config is the shape the
// defaults document merges under plugins.config.lifecycle.
func TestConfigDecodesTheLifecycleBudgets(t *testing.T) {
	raw := jsontext.Value(`{"join": "1m", "abort_total": "2m", "close": "30s", "margin": "30s"}`)
	var cfg Config
	if err := configuration.Decode(raw, &cfg); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cfg.Join.Std() != time.Minute || cfg.AbortTotal.Std() != 2*time.Minute ||
		cfg.Close.Std() != 30*time.Second || cfg.Margin.Std() != 30*time.Second {
		t.Fatalf("decoded budgets = %+v", cfg)
	}
}
