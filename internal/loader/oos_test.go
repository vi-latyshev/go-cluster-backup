package loader_test

import (
	"context"
	"slices"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// TestOOS_HooksForSkipsADisabledPluginsHooks: a plugin the deployment turned
// off must not run its role lifecycle — start or stop — while the active ones
// still do, in order.
func TestOOS_HooksForSkipsADisabledPluginsHooks(t *testing.T) {
	var events []string
	hook := func(owner, phase string) func(context.Context) error {
		return func(context.Context) error {
			events = append(events, owner+"-"+phase)
			return nil
		}
	}
	register := func(r *loader.Registry) error {
		addPlugin(t, r, loader.Spec{ID: "remote"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: hook("remote", "start"),
				Stop:  hook("remote", "stop"),
			})
		})
		addPlugin(t, r, loader.Spec{ID: "keeper"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: hook("keeper", "start"),
				Stop:  hook("keeper", "stop"),
			})
		})
		withSelection(t, r, "config", "remote")
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 0 {
		t.Fatalf("Main = %d, stderr %q, want a successful role", code, stderr)
	}
	if want := []string{"keeper-start", "keeper-stop"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want only the active plugin's hooks %v", events, want)
	}
}
