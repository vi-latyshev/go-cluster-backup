package cli

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// failWriter fails every write: one command's checked output ends the command
// instead of being retried line by line.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("the stream is broken") }

// noPlugins is a distribution that registers nothing: the flag surface of a
// command is reachable without the whole first-party registry behind it.
func noPlugins(*plugin.Registry) error { return nil }

// TestMainRejectsABrokenDistribution: a distribution that cannot build its
// registry or its defaults document fails before any command runs.
func TestMainRejectsABrokenDistribution(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"registering plugins fails",
			Options{Program: "backup", Register: func(*plugin.Registry) error { return errors.New("boom") }},
			"registering plugins"},
		{"the defaults document is not YAML",
			Options{Program: "backup", Register: Generic, Defaults: [][]byte{[]byte("\tnot: [yaml")}},
			"defaults document 1"},
		{"the defaults document names an unregistered type",
			Options{Program: "backup", Register: noPlugins, Defaults: [][]byte{[]byte("plugins:\n  source/absent:\n    key: v\n")}},
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
				Options{Program: "backup", Register: Generic, Defaults: [][]byte{defaults.Defaults}},
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
type failingCloser struct{}

func (failingCloser) Close() error { return errors.New("close failed") }

// TestCloseEngineReportsAFailure: a close failure is a warning on the command's
// own log, not a second exit code.
func TestCloseEngineReportsAFailure(t *testing.T) {
	var buf strings.Builder
	closeEngine(failingCloser{}, slog.New(human.NewHandler(&buf, slog.LevelDebug)))
	if !strings.Contains(buf.String(), "closing plugins") || !strings.Contains(buf.String(), "close failed") {
		t.Errorf("the close failure is not reported: %s", buf.String())
	}
	buf.Reset()
	closeEngine(closingOK{}, slog.New(human.NewHandler(&buf, slog.LevelInfo)))
	if buf.Len() != 0 {
		t.Errorf("a clean close wrote a line: %s", buf.String())
	}
}

type closingOK struct{}

func (closingOK) Close() error { return nil }
