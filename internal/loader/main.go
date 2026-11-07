package loader

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// Options is one process invocation: the composition root, the process
// arguments without the program name, and the process streams.
type Options struct {
	Register func(*Registry) error
	Args     []string
	Stdin    io.Reader
	Stdout   io.Writer
	Stderr   io.Writer
}

// Main registers the compiled plugins, applies the selection, picks the role
// named by Args[0] — the default role when no role matches — starts that role's
// lifecycle, runs it and stops the lifecycle. It never reads a signal, never
// exits the process, and never falls back to the default role when the selected
// role's owner is disabled: that role's own refusal is the outcome.
func Main(ctx context.Context, o Options) int {
	r := New()
	if o.Register != nil {
		if err := o.Register(r); err != nil {
			o.fail("loader: registration failed: %v", err)
			return 1
		}
	}
	selector, problem := selectionProvider(r)
	if problem != "" {
		r.selectionProblems = append(r.selectionProblems, problem)
	}
	if selector != nil {
		disabled, err := selector.Disabled(ctx)
		if err != nil {
			r.selectionProblems = append(r.selectionProblems, fmt.Sprintf("selection: %v", err))
		} else {
			ApplySelection(r, disabled)
		}
	}
	role, args := pickRole(r, o.Args)
	if role == nil {
		o.fail("loader: no default role is registered")
		return 1
	}
	session := o.session(r, args)
	if problems := Health(r); len(problems) > 0 && !role.spec.Diagnostic {
		for _, problem := range problems {
			o.fail("loader: %s", problem)
		}
		return 1
	}
	if !r.isActive(role.owner) {
		return role.spec.Disabled(session)
	}
	return runRole(ctx, r.hooksFor(role.spec.Name), role.spec.Run, session)
}

func pickRole(r *Registry, args []string) (*roleEntry, []string) {
	if len(args) > 0 {
		if role, ok := r.roles[args[0]]; ok {
			return role, args[1:]
		}
	}
	return r.def, args
}

func (o Options) session(r *Registry, args []string) Session {
	stdin := o.Stdin
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	stdout := o.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	stderr := o.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	return Session{Registry: r, Args: args, Stdin: stdin, Stdout: stdout, Stderr: stderr}
}

func (o Options) fail(format string, args ...any) {
	if o.Stderr == nil {
		return
	}
	_, _ = fmt.Fprintf(o.Stderr, format+"\n", args...)
}

// runRole starts the hooks of the role in order, runs the role and shuts the
// started hooks down. A failed start stops what already started; a failed
// shutdown turns a successful role into a failure but never hides a role's own
// non-zero code.
func runRole(ctx context.Context, hooks []lifecycleEntry, run func(context.Context, Session) int, session Session) int {
	started := 0
	for _, hook := range hooks {
		if err := phase(ctx, hook.lifecycle.Budget, hook.lifecycle.Start); err != nil {
			session.logf("loader: plugin %q failed to start: %v", hook.owner, err)
			abortStart(ctx, hooks[:started], session)
			return 1
		}
		started++
	}
	code := run(ctx, session)
	if shutdown(ctx, hooks[:started], session) && code == 0 {
		return 1
	}
	return code
}

func abortStart(ctx context.Context, started []lifecycleEntry, session Session) {
	for _, hook := range slices.Backward(started) {
		if err := phase(ctx, hook.lifecycle.Budget, hook.lifecycle.Stop); err != nil {
			session.logf("loader: plugin %q failed to stop: %v", hook.owner, err)
		}
	}
}

// shutdown quiesces forward and stops in reverse, reporting every failure.
func shutdown(ctx context.Context, hooks []lifecycleEntry, session Session) bool {
	failed := false
	for _, hook := range hooks {
		if err := phase(ctx, hook.lifecycle.Budget, hook.lifecycle.Quiesce); err != nil {
			session.logf("loader: plugin %q failed to quiesce: %v", hook.owner, err)
			failed = true
		}
	}
	for _, hook := range slices.Backward(hooks) {
		if err := phase(ctx, hook.lifecycle.Budget, hook.lifecycle.Stop); err != nil {
			session.logf("loader: plugin %q failed to stop: %v", hook.owner, err)
			failed = true
		}
	}
	return failed
}

// phase runs one lifecycle phase within its budget; a nil phase is a no-op.
func phase(ctx context.Context, budget time.Duration, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	return fn(ctx)
}
