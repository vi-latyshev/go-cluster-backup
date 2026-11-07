// Package lifecycle owns the lifetime of one invocation: the instances it built,
// the order they close in, the cancel sequence and the one cleanup deadline every
// shutdown step shares.
//
// Construction ownership, not the Requires graph, holds a client: the operation
// that built an instance joins its writers and releases the writer lease itself,
// and the role closes the scope only after the operation returns. The Requires
// graph orders availability; it never decides when a client may be closed.
package lifecycle

import (
	"math"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Spec is the compiled plugin identity. A library plugin contributes no kind and
// no role: every application role imports it directly.
var Spec = loader.Spec{
	ID:      "lifecycle",
	Summary: "invocation scope, construction-order close and the one cleanup deadline",
}

// Apply publishes this plugin's settings type under its stable ID, so the
// compiled schema walks `plugins.config.lifecycle` like every factory block.
func Apply(_ *loader.Registry, factories *instances.Registry) error {
	return instances.RegisterProvider[Config](factories, Spec.ID)
}

// Config is the cleanup budgets. They live under plugins.config.lifecycle in the
// central defaults and may be overridden per deployment; the chart grace is
// derived from the effective values, never from the implicit kubelet default.
type Config struct {
	// Join bounds the wait for every request goroutine after admission closes.
	Join model.Duration `json:"join" doc:"bound on joining the invocation's request goroutines"`
	// AbortTotal bounds ALL aborts of this run's own upload IDs together, never
	// one upload at a time.
	AbortTotal model.Duration `json:"abort_total" doc:"one budget over every abort of this run's own upload IDs"`
	// Close bounds closing the invocation's clients.
	Close model.Duration `json:"close" doc:"bound on closing the invocation's clients"`
	// Margin is the safety slack the chart adds above the computed cleanup bound.
	Margin model.Duration `json:"margin" doc:"safety slack above the computed cleanup bound"`
}

// Validate refuses cleanup budgets a bounded deadline cannot be derived from. It
// runs on the effective, defaults-merged block through configuration.Decode, so
// an invalid configuration is refused before any runtime cleanup decision.
func (c Config) Validate() error {
	for _, budget := range []struct {
		name     string
		value    time.Duration
		positive bool
	}{
		{"join", c.Join.Std(), true},
		{"abort_total", c.AbortTotal.Std(), true},
		{"close", c.Close.Std(), true},
		{"margin", c.Margin.Std(), false},
	} {
		if err := checkBudget(budget.name, budget.value, budget.positive); err != nil {
			return err
		}
	}
	sum := c.Join.Std()
	for _, v := range []time.Duration{c.AbortTotal.Std(), c.Close.Std(), c.Margin.Std()} {
		if sum > math.MaxInt64-v {
			return errGraceOverflow
		}
		sum += v
	}
	return nil
}
