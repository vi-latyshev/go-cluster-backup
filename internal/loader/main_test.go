package loader_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

func addPlugin(t *testing.T, r *loader.Registry, spec loader.Spec, apply func(*loader.Registry) error) {
	t.Helper()
	if err := loader.Add(r, spec, apply); err != nil {
		t.Fatalf("Add(%q) = %v", spec.ID, err)
	}
}

// addSelectable registers a plugin with an empty apply: it contributes nothing
// yet stays selectable, unlike a library.
func addSelectable(t *testing.T, r *loader.Registry, spec loader.Spec) {
	t.Helper()
	addPlugin(t, r, spec, func(*loader.Registry) error { return nil })
}

func addRole(t *testing.T, r *loader.Registry, plugin, role string, def bool, run func(context.Context, loader.Session) int) {
	t.Helper()
	addPlugin(t, r, loader.Spec{ID: plugin}, func(r *loader.Registry) error {
		return loader.Role(r, loader.RoleSpec{
			Name:     role,
			Default:  def,
			Run:      run,
			Disabled: func(loader.Session) int { return 2 },
		})
	})
}

func addHostRole(t *testing.T, r *loader.Registry) {
	t.Helper()
	addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int { return 0 })
}

func assertContains(t *testing.T, problems []string, want string) {
	t.Helper()
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return
		}
	}
	t.Fatalf("%q does not contain %q", problems, want)
}

func runMain(t *testing.T, options loader.Options) (int, string) {
	t.Helper()
	var stderr strings.Builder
	options.Stderr = &stderr
	code := loader.Main(t.Context(), options)
	return code, stderr.String()
}

func TestMainReportsRegistrationFailure(t *testing.T) {
	code, stderr := runMain(t, loader.Options{
		Register: func(*loader.Registry) error { return errors.New("bad wiring") },
	})
	if code != 1 || !strings.Contains(stderr, "registration failed: bad wiring") {
		t.Fatalf("Main = %d, stderr %q, want a registration refusal", code, stderr)
	}
}

func TestMainReportsRegistrationFailureWithoutStderr(t *testing.T) {
	code := loader.Main(t.Context(), loader.Options{
		Register: func(*loader.Registry) error { return errors.New("bad wiring") },
	})
	if code != 1 {
		t.Fatalf("Main = %d, want 1 without a stderr stream", code)
	}
}

func TestMainNeedsADefaultRole(t *testing.T) {
	for _, register := range []func(*loader.Registry) error{
		nil,
		func(r *loader.Registry) error {
			return loader.Add(r, loader.Spec{ID: "library"}, nil)
		},
	} {
		code, stderr := runMain(t, loader.Options{Register: register})
		if code != 1 || !strings.Contains(stderr, "no default role is registered") {
			t.Fatalf("Main = %d, stderr %q, want the missing default role", code, stderr)
		}
	}
}

func TestMainPicksTheRoleByFirstArgument(t *testing.T) {
	var picked string
	var args []string
	var streamsOK bool
	run := func(who string) func(context.Context, loader.Session) int {
		return func(_ context.Context, session loader.Session) int {
			picked = who
			args = session.Args
			streamsOK = session.Registry != nil && session.Stdin != nil &&
				session.Stdout != nil && session.Stderr != nil
			return 0
		}
	}
	register := func(r *loader.Registry) error {
		addRole(t, r, "host", "host", true, run("host"))
		addRole(t, r, "worker", "worker", false, run("worker"))
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"worker", "x", "y"}})
	if code != 0 || picked != "worker" || !slices.Equal(args, []string{"x", "y"}) || !streamsOK {
		t.Fatalf("Main = %d, role %q, args %v, streams ok %v, stderr %q", code, picked, args, streamsOK, stderr)
	}
}

func TestMainFallsBackToTheDefaultRole(t *testing.T) {
	var picked string
	var args []string
	run := func(who string) func(context.Context, loader.Session) int {
		return func(_ context.Context, session loader.Session) int {
			picked = who
			args = session.Args
			return 0
		}
	}
	register := func(r *loader.Registry) error {
		addRole(t, r, "host", "host", true, run("host"))
		addRole(t, r, "worker", "worker", false, run("worker"))
		return nil
	}
	code, _ := runMain(t, loader.Options{Register: register, Args: []string{"command", "--flag"}})
	if code != 0 || picked != "host" || !slices.Equal(args, []string{"command", "--flag"}) {
		t.Fatalf("Main = %d, role %q, args %v, want the default role with every argument", code, picked, args)
	}
	picked = ""
	code, _ = runMain(t, loader.Options{Register: register})
	if code != 0 || picked != "host" || args != nil {
		t.Fatalf("Main without arguments = %d, role %q, args %v, want the bare default role", code, picked, args)
	}
}

func TestMainDisabledOwnerRefusesInsteadOfFallingBack(t *testing.T) {
	hostRan := false
	register := func(r *loader.Registry) error {
		addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int {
			hostRan = true
			return 0
		})
		addRole(t, r, "worker", "worker", false, func(context.Context, loader.Session) int { return 0 })
		withSelection(t, r, "config", "worker")
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"worker"}})
	if code != 2 || hostRan {
		t.Fatalf("Main = %d, default role ran %v, stderr %q, want the disabled role's own refusal", code, hostRan, stderr)
	}
}

func TestMainRefusesAnOperationalRoleOnProblems(t *testing.T) {
	ran := false
	register := func(r *loader.Registry) error {
		addSelectable(t, r, loader.Spec{ID: "core", Floor: true})
		withSelection(t, r, "config", "core")
		addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int {
			ran = true
			return 0
		})
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 1 || ran || !strings.Contains(stderr, `selection: floor plugin "core" cannot be disabled`) {
		t.Fatalf("Main = %d, ran %v, stderr %q, want an offline refusal", code, ran, stderr)
	}
}

func TestMainRunsADiagnosticRoleDespiteProblems(t *testing.T) {
	seen := false
	register := func(r *loader.Registry) error {
		addSelectable(t, r, loader.Spec{ID: "core", Floor: true})
		withSelection(t, r, "config", "core")
		addPlugin(t, r, loader.Spec{ID: "host"}, func(r *loader.Registry) error {
			return loader.Role(r, loader.RoleSpec{
				Name:       "host",
				Default:    true,
				Diagnostic: true,
				Run: func(_ context.Context, session loader.Session) int {
					seen = len(loader.Health(session.Registry)) > 0
					return 0
				},
				Disabled: func(loader.Session) int { return 2 },
			})
		})
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 0 || !seen {
		t.Fatalf("Main = %d, health seen %v, stderr %q, want the diagnostic role to report", code, seen, stderr)
	}
}

func TestMainRefusesAnUnreadableSelection(t *testing.T) {
	register := func(r *loader.Registry) error {
		addPlugin(t, r, loader.Spec{ID: "config"}, func(r *loader.Registry) error {
			return loader.Provide[loader.Selector](r, loader.Selection, brokenSelector{})
		})
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 1 || !strings.Contains(stderr, "selection: selection source is unreadable") {
		t.Fatalf("Main = %d, stderr %q, want an operational refusal", code, stderr)
	}
}

func TestMainRefusesAnAmbiguousSelection(t *testing.T) {
	register := func(r *loader.Registry) error {
		withSelection(t, r, "one")
		withSelection(t, r, "two")
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 1 || !strings.Contains(stderr, "selection: 2 providers, want one") {
		t.Fatalf("Main = %d, stderr %q, want an ambiguity refusal", code, stderr)
	}
}

func TestMainRunsLifecycleInDependencyOrder(t *testing.T) {
	var events []string
	hook := func(owner, phase string) func(context.Context) error {
		return func(context.Context) error {
			events = append(events, owner+"-"+phase)
			return nil
		}
	}
	register := func(r *loader.Registry) error {
		addPlugin(t, r, loader.Spec{ID: "first"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start:   hook("first", "start"),
				Quiesce: hook("first", "quiesce"),
				Stop:    hook("first", "stop"),
			})
		})
		addPlugin(t, r, loader.Spec{ID: "second", Requires: []string{"first"}}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: hook("second", "start"),
				Stop:  hook("second", "stop"),
			})
		})
		addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int {
			events = append(events, "run")
			return 0
		})
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	want := []string{"first-start", "second-start", "run", "first-quiesce", "second-stop", "first-stop"}
	if code != 0 || !slices.Equal(events, want) {
		t.Fatalf("Main = %d, events %v, stderr %q, want %v", code, events, stderr, want)
	}
}

func TestMainStopsStartedHooksWhenStartFails(t *testing.T) {
	var events []string
	register := func(r *loader.Registry) error {
		addPlugin(t, r, loader.Spec{ID: "first"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: func(context.Context) error {
					events = append(events, "first-start")
					return nil
				},
				Stop: func(context.Context) error {
					events = append(events, "first-stop")
					return errors.New("stop failed")
				},
			})
		})
		addPlugin(t, r, loader.Spec{ID: "second", Requires: []string{"first"}}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: func(context.Context) error { return errors.New("boom") },
				Stop: func(context.Context) error {
					events = append(events, "second-stop")
					return nil
				},
			})
		})
		addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int {
			events = append(events, "run")
			return 0
		})
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 1 || !slices.Equal(events, []string{"first-start", "first-stop"}) {
		t.Fatalf("Main = %d, events %v, stderr %q, want the started hook stopped and no run", code, events, stderr)
	}
	if !strings.Contains(stderr, `plugin "second" failed to start: boom`) ||
		!strings.Contains(stderr, `plugin "first" failed to stop: stop failed`) {
		t.Fatalf("stderr %q, want both lifecycle failures", stderr)
	}
}

func TestMainShutdownFailureChangesOnlyASuccessfulCode(t *testing.T) {
	register := func(code int) func(*loader.Registry) error {
		return func(r *loader.Registry) error {
			addPlugin(t, r, loader.Spec{ID: "owner"}, func(r *loader.Registry) error {
				return loader.OnRole(r, "host", loader.Lifecycle{
					Quiesce: func(context.Context) error { return errors.New("quiesce failed") },
					Stop:    func(context.Context) error { return errors.New("stop failed") },
				})
			})
			addRole(t, r, "host", "host", true, func(context.Context, loader.Session) int { return code })
			return nil
		}
	}
	got, stderr := runMain(t, loader.Options{Register: register(0), Args: []string{"host"}})
	if got != 1 || !strings.Contains(stderr, `plugin "owner" failed to quiesce: quiesce failed`) ||
		!strings.Contains(stderr, `plugin "owner" failed to stop: stop failed`) {
		t.Fatalf("Main = %d, stderr %q, want a failed shutdown to fail a successful role", got, stderr)
	}
	got, _ = runMain(t, loader.Options{Register: register(2), Args: []string{"host"}})
	if got != 2 {
		t.Fatalf("Main = %d, want the role's own non-zero code kept", got)
	}
}

func TestMainBudgetsEachLifecyclePhase(t *testing.T) {
	withBudget, withoutBudget := false, false
	register := func(r *loader.Registry) error {
		addPlugin(t, r, loader.Spec{ID: "timed"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Budget: time.Minute,
				Start: func(ctx context.Context) error {
					_, withBudget = ctx.Deadline()
					return nil
				},
			})
		})
		addPlugin(t, r, loader.Spec{ID: "plain"}, func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{
				Start: func(ctx context.Context) error {
					_, bounded := ctx.Deadline()
					withoutBudget = !bounded
					return nil
				},
			})
		})
		addHostRole(t, r)
		return nil
	}
	code, stderr := runMain(t, loader.Options{Register: register, Args: []string{"host"}})
	if code != 0 || !withBudget || !withoutBudget {
		t.Fatalf("Main = %d, budgeted %v, unbudgeted %v, stderr %q", code, withBudget, withoutBudget, stderr)
	}
}

func TestMainNormalizesNilStreams(t *testing.T) {
	streamsOK := false
	register := func(r *loader.Registry) error {
		addRole(t, r, "host", "host", true, func(_ context.Context, session loader.Session) int {
			streamsOK = session.Registry != nil && session.Stdin != nil &&
				session.Stdout != nil && session.Stderr != nil
			return 0
		})
		return nil
	}
	code := loader.Main(t.Context(), loader.Options{Register: register, Args: []string{"host"}})
	if code != 0 || !streamsOK {
		t.Fatalf("Main = %d, streams ok %v, want normalized streams", code, streamsOK)
	}
}
