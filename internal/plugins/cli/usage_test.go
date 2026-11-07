package cli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all/components"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
)

// failWriter fails every write: one command's checked output ends the command
// instead of being retried line by line.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("the stream is broken") }

// distribution is the compiled factory registry: the generated composition
// root, registered once into a fresh registry.
func distribution() *instances.Registry {
	r := instances.NewRegistry()
	if err := components.Compose(loader.New(), r); err != nil {
		panic(err)
	}
	return r
}

// TestMainRejectsABrokenDistribution: options without a factory registry or a
// defaults document that does not hold up fail before any command runs.
func TestMainRejectsABrokenDistribution(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"the factory registry is missing",
			Options{Program: "backup"},
			"Factories"},
		{"the defaults document is not YAML",
			Options{Program: "backup", Factories: distribution(), Defaults: [][]byte{[]byte("\tnot: [yaml")}},
			"defaults document 1"},
		{"the defaults document names an unregistered type",
			Options{Program: "backup", Factories: instances.NewRegistry(), Defaults: [][]byte{[]byte("plugins:\n  source/absent:\n    key: v\n")}},
			"defaults"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			if code := Main(context.Background(), c.opts, []string{"plugins"}, &stdout, &stderr); code != exitFailure {
				t.Fatalf("exit %d, want %d", code, exitFailure)
			}
			if !strings.Contains(stderr.String(), c.want) {
				t.Errorf("the failure does not name %q: %s", c.want, stderr.String())
			}
		})
	}
}

// TestMainHelpAndNoCommand: help goes to stdout and is a zero exit; no command
// at all is a usage error carrying the same text on stderr.
func TestMainHelpAndNoCommand(t *testing.T) {
	w := newWorld()
	code, stdout, stderr := w.main(context.Background(), "help")
	if code != exitOK || !strings.Contains(stdout, "usage: backup COMMAND") {
		t.Fatalf("help: exit %d, stdout %s, stderr %s", code, stdout, stderr)
	}
	for _, arg := range []string{"-h", "--help"} {
		if code, stdout, _ := w.main(context.Background(), arg); code != exitOK || !strings.Contains(stdout, "usage: backup COMMAND") {
			t.Errorf("%s: exit %d, stdout %s", arg, code, stdout)
		}
	}
	code, stdout, stderr = w.main(context.Background())
	if code != exitUsage || !strings.Contains(stderr, "usage: backup COMMAND") {
		t.Fatalf("no command: exit %d, stdout %s, stderr %s", code, stdout, stderr)
	}
}

// TestEveryCommandAnswersHelpAndRejectsABadFlag: -h and an unknown flag are the
// flag set's own two answers, and every command carries both.
func TestEveryCommandAnswersHelpAndRejectsABadFlag(t *testing.T) {
	commands := [][]string{
		{"run"}, {"stage"}, {"validate"}, {"plugins"}, {"schema"}, {"doctor"},
		{"config", "defaults"}, {"config", "effective"}, {"plugins", "describe"},
		{"restore", "list"}, {"restore", "fetch"}, {"restore", "apply"}, {"restore", "verify"},
	}
	for _, command := range commands {
		name := strings.Join(command, " ")
		t.Run(name, func(t *testing.T) {
			w := newWorld()
			code, _, _ := w.main(context.Background(), append(append([]string{}, command...), "-h")...)
			if code != exitOK {
				t.Errorf("%s -h: exit %d, want %d", name, code, exitOK)
			}
			code, _, stderr := w.main(context.Background(), append(append([]string{}, command...), "--no-such-flag")...)
			if code != exitUsage {
				t.Errorf("%s --no-such-flag: exit %d, want %d", name, code, exitUsage)
			}
			if !strings.Contains(stderr, "no-such-flag") {
				t.Errorf("%s: the parse failure is not reported: %s", name, stderr)
			}
		})
	}
}

// TestCommandUsageErrors: every argument a command cannot work without is a
// usage error naming it, and no command answers with a default it does not have.
func TestCommandUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"config without a subcommand", []string{"config"}, "subcommand is required"},
		{"config with an unknown subcommand", []string{"config", "bogus"}, "unknown subcommand"},
		{"config defaults with an argument", []string{"config", "defaults", "extra"}, "no arguments"},
		{"config effective without a file", []string{"config", "effective"}, "--config is required"},
		{"config effective with a bad output", []string{"config", "effective", "--config", "c.yaml", "--output", "xml"}, "--output"},
		{"config effective with a bad log level", []string{"config", "effective", "--config", "c.yaml", "--log-level", "loud"}, "log level"},
		{"run with a bad kind", []string{"run", "--config", "c.yaml", "--kind", "nightly"}, "--kind"},
		{"run with a bad log level", []string{"run", "--config", "c.yaml", "--kind", "scheduled", "--log-level", "loud"}, "log level"},
		{"stage with a bad log level", []string{"stage", "--config", "c.yaml", "--log-level", "loud"}, "log level"},
		{"validate with a bad log level", []string{"validate", "--config", "c.yaml", "--log-level", "loud"}, "log level"},
		{"validate --instance without --preflight", []string{"validate", "--config", "c.yaml", "--instance", "source/thing"}, "--instance requires --preflight"},
		{"plugins describe without a KIND/TYPE", []string{"plugins", "describe", "postgres"}, "KIND/TYPE"},
		{"plugins describe with a nested type", []string{"plugins", "describe", "source/a/b"}, "KIND/TYPE"},
		{"restore without a subcommand", []string{"restore"}, "one of list, fetch, apply, verify"},
		{"restore list without a config", []string{"restore", "list"}, "--config is required"},
		{"restore list with a bad output", []string{"restore", "list", "--config", "c.yaml", "--output", "xml"}, "--output"},
		{"restore list with a bad log level", []string{"restore", "list", "--config", "c.yaml", "--log-level", "loud"}, "log level"},
		{"restore fetch without a config", []string{"restore", "fetch", "--run", "r1", "--artifact", "a", "--out", "-"}, "--config is required"},
		{"restore fetch without --out", []string{"restore", "fetch", "--config", "c.yaml", "--run", "r1", "--artifact", "a"}, "--out is required"},
		{"restore fetch by key with no storage", []string{"restore", "fetch", "--config", "c.yaml", "--key", "k", "--out", "-"}, "exactly one --storage"},
		{"restore fetch by key with a run", []string{"restore", "fetch", "--config", "c.yaml", "--key", "k", "--out", "-", "--storage", "s", "--run", "r1"}, "exclude each other"},
		{"restore fetch by key with an artifact", []string{"restore", "fetch", "--config", "c.yaml", "--key", "k", "--out", "-", "--storage", "s", "--artifact", "a"}, "exclude each other"},
		{"restore fetch by key with --raw", []string{"restore", "fetch", "--config", "c.yaml", "--key", "k", "--out", "-", "--storage", "s", "--raw"}, "never decodes"},
		{"restore fetch without --run", []string{"restore", "fetch", "--config", "c.yaml", "--artifact", "a", "--out", "-"}, "--run is required"},
		{"restore fetch without --artifact", []string{"restore", "fetch", "--config", "c.yaml", "--run", "r1", "--out", "-"}, "--artifact is required"},
		{"restore fetch with a bad log level", []string{"restore", "fetch", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--out", "-", "--log-level", "loud"}, "log level"},
		{"restore apply without a config", []string{"restore", "apply", "--run", "r1", "--artifact", "a", "--target", "t", "--mode", "side-by-side"}, "--config is required"},
		{"restore apply without --run", []string{"restore", "apply", "--config", "c.yaml", "--artifact", "a", "--target", "t", "--mode", "side-by-side"}, "--run is required"},
		{"restore apply without --artifact", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--target", "t", "--mode", "side-by-side"}, "--artifact is required"},
		{"restore apply without --target", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--mode", "side-by-side"}, "--target is required"},
		{"restore apply without --mode", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--target", "t"}, "--mode is required"},
		{"restore apply with a bad --mode", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--target", "t", "--mode", "beside"}, "--mode"},
		{"restore apply in-place without --confirm", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--target", "t", "--mode", "in-place"}, "--confirm is required"},
		{"restore apply with a bad log level", []string{"restore", "apply", "--config", "c.yaml", "--run", "r1", "--artifact", "a", "--target", "t", "--mode", "side-by-side", "--log-level", "loud"}, "log level"},
		{"restore verify without a config", []string{"restore", "verify", "--run", "r1", "--depth", "stored"}, "--config is required"},
		{"restore verify without --run", []string{"restore", "verify", "--config", "c.yaml", "--depth", "stored"}, "--run is required"},
		{"restore verify without --depth", []string{"restore", "verify", "--config", "c.yaml", "--run", "r1"}, "--depth is required"},
		{"restore verify with a bad --depth", []string{"restore", "verify", "--config", "c.yaml", "--run", "r1", "--depth", "deep"}, "--depth"},
		{"restore verify probed without --target", []string{"restore", "verify", "--config", "c.yaml", "--run", "r1", "--depth", "probed"}, "--target is required"},
		{"restore verify with a bad log level", []string{"restore", "verify", "--config", "c.yaml", "--run", "r1", "--depth", "stored", "--log-level", "loud"}, "log level"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld()
			code, _, stderr := w.main(context.Background(), c.args...)
			if code != exitUsage {
				t.Fatalf("exit %d, want %d\nstderr: %s", code, exitUsage, stderr)
			}
			if !strings.Contains(stderr, c.want) {
				t.Fatalf("stderr does not name %q: %s", c.want, stderr)
			}
		})
	}
}

// TestCommandsFailOnAMissingConfigFile: a file that cannot be read is an
// operational failure, never a usage error, and every command reports it.
func TestCommandsFailOnAMissingConfigFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	commands := [][]string{
		{"config", "effective", "--config", missing},
		{"run", "--config", missing, "--kind", "scheduled"},
		{"stage", "--config", missing},
		{"validate", "--config", missing},
		{"restore", "list", "--config", missing},
		{"restore", "fetch", "--config", missing, "--run", "r1", "--artifact", "a", "--out", "-"},
		{"restore", "apply", "--config", missing, "--run", "r1", "--artifact", "a", "--target", "t", "--mode", "side-by-side"},
		{"restore", "verify", "--config", missing, "--run", "r1", "--depth", "stored"},
	}
	for _, command := range commands {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			w := newWorld()
			code, _, stderr := w.main(context.Background(), command...)
			if code != exitFailure {
				t.Fatalf("exit %d, want %d\nstderr: %s", code, exitFailure, stderr)
			}
			if !strings.Contains(stderr, "absent.yaml") {
				t.Fatalf("the failure does not name the file: %s", stderr)
			}
		})
	}
}

// TestMainRefusesABrokenOutput: a report describing finished work cannot change
// the outcome, but a checked output that cannot be written ends the command.
func TestMainRefusesABrokenOutput(t *testing.T) {
	commands := [][]string{
		{"config", "defaults"},
		{"schema"},
		{"plugins"},
		{"plugins", "describe", "source/postgres"},
		{"plugins", "describe", "source/k3s_state"},
	}
	for _, command := range commands {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			var stderr strings.Builder
			code := Main(context.Background(),
				Options{Program: "backup", Factories: distribution(), Defaults: [][]byte{defaults.Defaults}},
				command, failWriter{}, &stderr)
			if code != exitFailure {
				t.Fatalf("exit %d, want %d\nstderr: %s", code, exitFailure, stderr.String())
			}
			if !strings.Contains(stderr.String(), "stream is broken") {
				t.Errorf("the failure is not reported: %s", stderr.String())
			}
		})
	}
}

// failingCloser is a plugin whose Close fails: closing is reported but never
// changes a command's outcome.
type failingCloser struct{ err error }

func (c failingCloser) Close(context.Context) error { return c.err }

// TestCloseAssemblyReportsAFailure: a close failure is a warning on the command's
// own log, not a second exit code; an invocation whose join did not complete
// refuses to close and that refusal is diagnostic too.
func TestCloseAssemblyReportsAFailure(t *testing.T) {
	var buf strings.Builder
	log := slog.New(human.NewHandler(&buf, slog.LevelDebug))
	closeAssembly(context.Background(), failingCloser{errors.New("close failed")}, log)
	if !strings.Contains(buf.String(), "closing plugins") || !strings.Contains(buf.String(), "close failed") {
		t.Errorf("the close failure is not reported: %s", buf.String())
	}
	buf.Reset()
	closeAssembly(context.Background(), failingCloser{lifecycle.ErrIncompleteJoin}, log)
	if !strings.Contains(buf.String(), "closing plugins") || !strings.Contains(buf.String(), lifecycle.ErrIncompleteJoin.Error()) {
		t.Errorf("the incomplete-join refusal is not reported: %s", buf.String())
	}
	buf.Reset()
	closeAssembly(context.Background(), closingOK{}, slog.New(human.NewHandler(&buf, slog.LevelInfo)))
	if buf.Len() != 0 {
		t.Errorf("a clean close wrote a line: %s", buf.String())
	}
}

type closingOK struct{}

func (closingOK) Close(context.Context) error { return nil }

// hostOptions is the distribution the role tests register: the real generic
// plugins and the embedded defaults, so a role's command answers like production.
func hostOptions() Options {
	return Options{Program: "backup", Factories: distribution(), Defaults: [][]byte{defaults.Defaults}}
}

// selectorFunc is a loader.Selection provider a test drives.
type selectorFunc func(context.Context) ([]string, error)

func (f selectorFunc) Disabled(ctx context.Context) ([]string, error) { return f(ctx) }

// TestHostRolesStayDisjointFromCommands: the role tokens are process
// entrypoints, never commands, and each role runs the command it names.
func TestHostRolesStayDisjointFromCommands(t *testing.T) {
	commands := []string{"run", "stage", "validate", "plugins", "schema", "config", "doctor", "restore", "help"}
	roles := hostRoles(hostOptions())
	seen := map[string]bool{}
	defaults := 0
	for _, role := range roles {
		if slices.Contains(commands, role.Name) {
			t.Errorf("role %q collides with a command name", role.Name)
		}
		if seen[role.Name] {
			t.Errorf("role %q is registered twice", role.Name)
		}
		seen[role.Name] = true
		if role.Default {
			defaults++
			if role.Name != RoleCLI {
				t.Errorf("the default role is %q, want %q", role.Name, RoleCLI)
			}
		}
		if role.Diagnostic != (role.Name == RoleCLI || role.Name == RoleConfigCheck) {
			t.Errorf("role %q has diagnostic=%v", role.Name, role.Diagnostic)
		}
		if role.Run == nil || role.Disabled == nil {
			t.Errorf("role %q lacks its Run or Disabled outcome", role.Name)
		}
	}
	if defaults != 1 {
		t.Errorf("%d default roles, want exactly the cli role", defaults)
	}
	// Every role's command completion reaches its command: the flag surface
	// answers --help before a configuration is read, and restore names its own
	// subcommands.
	cases := map[string]struct {
		args []string
		code int
		want string
	}{
		RoleCLI:         {[]string{"--help"}, exitOK, "usage: backup"},
		RoleBackupJob:   {[]string{"--help"}, exitOK, "run"},
		RoleStageJob:    {[]string{"--help"}, exitOK, "stage"},
		RoleRestoreJob:  {[]string{"list"}, exitUsage, "backup restore list: --config is required"},
		RoleConfigCheck: {[]string{"--help"}, exitOK, "validate"},
	}
	for _, role := range roles {
		tc := cases[role.Name]
		var stdout, stderr bytes.Buffer
		code := role.Run(context.Background(), loader.Session{Args: tc.args, Stdout: &stdout, Stderr: &stderr})
		if code != tc.code {
			t.Errorf("role %q %v: exit %d, want %d\nstdout: %s\nstderr: %s", role.Name, tc.args, code, tc.code, stdout.String(), stderr.String())
			continue
		}
		if out := stdout.String() + stderr.String(); !strings.Contains(out, tc.want) {
			t.Errorf("role %q %v did not reach its command (%q missing):\n%s", role.Name, tc.args, tc.want, out)
		}
	}
}

// TestAddHostRejectsADuplicateRole: a second registration of one role token is
// refused, so the process never has two runtimes behind one entrypoint.
func TestAddHostRejectsADuplicateRole(t *testing.T) {
	r := loader.New()
	role := loader.RoleSpec{Name: RoleCLI, Run: func(context.Context, loader.Session) int { return exitOK },
		Disabled: func(loader.Session) int { return exitFailure }}
	if err := loader.Add(r, loader.Spec{ID: "test.duplicate"}, func(lr *loader.Registry) error {
		return loader.Role(lr, role)
	}); err != nil {
		t.Fatal(err)
	}
	if err := AddHost(r, hostOptions()); err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("a duplicate role was not refused: %v", err)
	}
}

// TestDisabledRoleRefuses: the host is Floor, so a deployment cannot disable it —
// the selection is refused, an operational role fails closed, and the diagnostic
// cli role still answers so the operator can read the problem. The role's own
// Disabled outcome stays a refusal naming the role, never a fallback to the
// default role's command surface.
func TestDisabledRoleRefuses(t *testing.T) {
	register := func(r *loader.Registry) error {
		if err := AddHost(r, hostOptions()); err != nil {
			return err
		}
		return loader.Add(r, loader.Spec{ID: "test.selection"}, func(lr *loader.Registry) error {
			return loader.Provide[loader.Selector](lr, loader.Selection, selectorFunc(func(context.Context) ([]string, error) {
				return []string{Spec.ID}, nil
			}))
		})
	}
	var stdout, stderr bytes.Buffer
	code := loader.Main(context.Background(), loader.Options{
		Register: register, Args: []string{RoleBackupJob, "--config", "ignored.yaml"},
		Stdout: &stdout, Stderr: &stderr,
	})
	if code != exitFailure {
		t.Fatalf("a refused disable exited %d, want %d\nstdout: %s\nstderr: %s", code, exitFailure, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), `floor plugin "cli" cannot be disabled`) {
		t.Errorf("the refusal does not name the Floor plugin:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("an operational role ran the command layer on a refused selection:\n%s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = loader.Main(context.Background(), loader.Options{
		Register: register, Args: []string{RoleCLI, "config", "defaults"},
		Stdout: &stdout, Stderr: &stderr,
	})
	if code != exitOK {
		t.Fatalf("the diagnostic role exited %d, want %d\nstderr: %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "go-cluster-backup — the service's defaults") {
		t.Errorf("the diagnostic role did not answer through the command layer:\n%s", stdout.String())
	}
	for _, role := range hostRoles(hostOptions()) {
		var refusal strings.Builder
		if code := role.Disabled(loader.Session{Stderr: &refusal}); code != exitFailure {
			t.Errorf("role %q Disabled = %d, want %d", role.Name, code, exitFailure)
		}
		if !strings.Contains(refusal.String(), role.Name) {
			t.Errorf("role %q Disabled does not name the role: %s", role.Name, refusal.String())
		}
	}
}

// TestApplyRegistersRolesOnlyInsideTheComposedPlugin: production has one
// registration path — the generated root's loader.Add(Spec, Apply). Apply adds
// the roles to the plugin being added and refuses to register outside Add.
func TestApplyRegistersRolesOnlyInsideTheComposedPlugin(t *testing.T) {
	factories := distribution()
	lr := loader.New()
	if err := loader.Add(lr, Spec, func(lr *loader.Registry) error {
		return Apply(lr, factories)
	}); err != nil {
		t.Fatalf("Apply inside the composed plugin: %v", err)
	}
	if !loader.Active(lr, Spec.ID) {
		t.Fatalf("the host plugin is not registered")
	}
	if err := Apply(loader.New(), factories); err == nil || !strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("Apply outside the composed plugin = %v, want a refusal", err)
	}
}

// TestOperationalRoleFailsClosedOnHealth: a health problem refuses an
// operational role before its Run, while the diagnostic cli role still answers
// so an operator can read the problem.
func TestOperationalRoleFailsClosedOnHealth(t *testing.T) {
	register := func(r *loader.Registry) error {
		if err := AddHost(r, hostOptions()); err != nil {
			return err
		}
		return loader.Add(r, loader.Spec{ID: "test.broken", Requires: []string{"test.missing"}}, nil)
	}
	var stdout, stderr bytes.Buffer
	code := loader.Main(context.Background(), loader.Options{
		Register: register, Args: []string{RoleBackupJob},
		Stdout: &stdout, Stderr: &stderr,
	})
	if code != exitFailure {
		t.Fatalf("an operational role with a health problem exited %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "depends on unregistered plugin") {
		t.Errorf("the refusal does not report the health problem:\n%s", stderr.String())
	}
	if strings.Contains(stdout.String(), "usage:") {
		t.Errorf("the operational role ran the command layer:\n%s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = loader.Main(context.Background(), loader.Options{
		Register: register, Args: []string{RoleCLI, "config", "defaults"},
		Stdout: &stdout, Stderr: &stderr,
	})
	if code != exitOK {
		t.Fatalf("the diagnostic role exited %d, want %d\nstderr: %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "go-cluster-backup — the service's defaults") {
		t.Errorf("the diagnostic role did not answer through the command layer:\n%s", stdout.String())
	}
}
