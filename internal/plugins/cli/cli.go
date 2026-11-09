// Package cli is the command layer: it parses arguments, converts the YAML
// configuration file into the JSON bytes the assembly decodes, drives the assembly
// facade and renders the reports.
//
// It owns no deployment knowledge: a label, a URL, a namespace — every name a
// deployment uses lives in its configuration file, never here. Main returns the
// process exit code and never calls os.Exit; log/slog is the only logger, usage
// and errors go to stderr, and a report on stdout stays machine-readable; signal
// handling belongs to main.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// Options describe one build of the CLI: its program name, its plugins and its
// defaults.
type Options struct {
	// Program is the name the usage text and error prefixes use.
	Program string
	// Loader is the compiled plugin graph the process runs under, when the
	// composition root threaded one through the loader: selection, the
	// dependency graph and the compiled schema all read it. A build without it
	// still has the factory registry, but no selection and no graph to judge.
	Loader *loader.Registry
	// Factories is the composition root's one factory registry: every command
	// reads the types registered there, and the command layer never builds a
	// second one.
	Factories *instances.Registry
	// Defaults are the defaults documents, in layer order — the service's own
	// first (defaults.Defaults), then any a build adds. They are YAML with
	// comments, which `config defaults` prints; the command layer converts them
	// and checks them against the registry before any command runs.
	Defaults [][]byte
}

// Command-layer exit codes. A run or a stage uses the codes of its own config
// instead (run.degraded_exit_code, run.failed_exit_code and their stage twins);
// these cover every path that fails before an outcome exists.
const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

// Role names are the process entrypoints of the CLI host. They are disjoint from
// every command name: a role token selects the entrypoint, while a command name
// stays the first argument of the default cli role.
const (
	RoleCLI         = "cli"
	RoleBackupJob   = "backup-job"
	RoleStageJob    = "stage-job"
	RoleRestoreJob  = "restore-job"
	RoleConfigCheck = "config-check"
)

// hostRoles is one process entrypoint per container: the default cli role and
// its diagnostics, and one operational role per Job command.
func hostRoles(opts Options) []loader.RoleSpec {
	return []loader.RoleSpec{
		hostRole(opts, RoleCLI, "", true, true),
		hostRole(opts, RoleBackupJob, "run", false, false),
		hostRole(opts, RoleStageJob, "stage", false, false),
		hostRole(opts, RoleRestoreJob, "restore", false, false),
		hostRole(opts, RoleConfigCheck, "validate", false, true),
	}
}

// hostRole is one role: its run completes the argv with command, and its owner
// being disabled is a refusal — never a fallback to the default role.
func hostRole(opts Options, name, command string, isDefault, diagnostic bool) loader.RoleSpec {
	return loader.RoleSpec{
		Name:       name,
		Default:    isDefault,
		Diagnostic: diagnostic,
		Run: func(ctx context.Context, s loader.Session) int {
			args := s.Args
			if command != "" {
				args = append([]string{command}, args...)
			}
			return Main(ctx, opts, args, s.Stdout, s.Stderr)
		},
		Disabled: func(s loader.Session) int {
			writef(s.Stderr, "%s: role %q is disabled: the cli plugin is not active\n", opts.Program, name)
			return exitFailure
		},
	}
}

// Main parses args, runs one command and returns the process exit code — the
// only way this layer ends, so the mains have exactly one os.Exit call each.
// Cancellation arrives in ctx: every command returns as soon as the assembly does,
// and a cancelled command is never a zero exit.
func Main(ctx context.Context, opts Options, args []string, stdout, stderr io.Writer) int {
	if opts.Program == "" || opts.Factories == nil {
		writef(stderr, "cli: Options needs Program and Factories\n")
		return exitFailure
	}
	reg := opts.Factories
	// The distribution's defaults are checked for every command, before any of
	// them reads a configuration: a document that does not hold up — a typo, a
	// bad value, a default for a field the deployment states, a field with neither —
	// is a property of the distribution, not of one invocation.
	docs, err := configuration.DefaultsDocuments(opts.Defaults)
	if err != nil {
		writef(stderr, "%s: %v\n", opts.Program, err)
		return exitFailure
	}
	defs, err := configuration.LoadDefaults(reg, docs)
	if err != nil {
		writef(stderr, "%s: %v\n", opts.Program, err)
		return exitFailure
	}
	if len(args) == 0 {
		usage(stderr, opts.Program)
		return exitUsage
	}
	command, rest := args[0], args[1:]
	switch command {
	case "run":
		return runCommand(ctx, opts, reg, defs, rest, stdout, stderr)
	case "stage":
		return stageCommand(ctx, opts, reg, defs, rest, stdout, stderr)
	case "validate":
		return validateCommand(ctx, opts, reg, defs, rest, stdout, stderr)
	case "plugins":
		return pluginsCommand(opts, reg, defs, rest, stdout, stderr)
	case "schema":
		return schemaCommand(opts, reg, defs, rest, stdout, stderr)
	case "config":
		return configCommand(ctx, opts, reg, defs, rest, stdout, stderr)
	case "doctor":
		return doctorCommand(opts, reg, defs, rest, stdout, stderr)
	case "restore":
		return restoreCommand(ctx, opts, defs, rest, stdout, stderr)
	case "help", "-h", "--help":
		usage(stdout, opts.Program)
		return exitOK
	default:
		writef(stderr, "%s: unknown command %q\n", opts.Program, command)
		usage(stderr, opts.Program)
		return exitUsage
	}
}

// writef writes usage, diagnostics and a report of work already done — never a
// command's result. A command whose output a consumer parses writes it checked
// instead (the listings, the schema), and a report describing finished work
// cannot rewrite the outcome: the exit code is the report's, by contract.
func writef(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

// errWriter writes one command's checked output, remembering the first failure
// so a broken stream ends the command instead of being retried line by line.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

// usage writes the command list.
func usage(w io.Writer, program string) {
	writef(w, `usage: %[1]s COMMAND [FLAGS]

Commands:
  run        run one backup: discover sources, dump, upload to every storage
             (--kind scheduled|manual selects the run's identity)
  stage      produce the sealed artifacts the run side reads back (stage config)
  validate   check a configuration file offline (--preflight also runs plugin checks)
  plugins    list the registered plugins; "plugins describe KIND/TYPE" documents one,
             "plugins check" validates the compiled and selected build
  schema     print the JSON Schema of the configuration file
  config     print the service's defaults, or one file as the assembly reads it
  doctor     verify every external binary the registered plugins require
  restore    list runs, fetch an artifact, apply one, verify one
  help       this text

A run or a stage exits with the degraded/failed code from its own config, a
usage error with 2, anything else that fails with 1. See README.md.
`, program)
}

// newFlagSet builds one command's flag set: parsing errors and usage go to
// stderr, a parse error ends the command with the usage exit code.
func newFlagSet(name string, stderr io.Writer, usage func()) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = usage
	return fs
}

// parseFlags runs one command's flag parsing and reports whether it already
// answered the command (an invalid argument, or -h): a true result is returned
// with the exit code to use.
func parseFlags(fs *flag.FlagSet, args []string) (int, bool) {
	err := fs.Parse(args)
	switch {
	case err == nil:
		return exitOK, false
	case errors.Is(err, flag.ErrHelp):
		return exitOK, true
	default:
		return exitUsage, true
	}
}

// usageError reports absent required flags or invalid values, naming every one of
// them; it is the single exit path of every argument error.
func usageError(stderr io.Writer, program, command string, problems ...string) int {
	writef(stderr, "%s %s: %s\n", program, command, strings.Join(problems, "; "))
	return exitUsage
}

// failure reports an operational error. Every error of a joined group is printed
// — errors.Join keeps them all in one string, one per line.
func failure(stderr io.Writer, program, command string, err error) int {
	writef(stderr, "%s %s: %v\n", program, command, err)
	return exitFailure
}

// closeAssembly closes every plugin the assembly built, within the invocation's own
// cleanup context. A close failure is reported but never changes the command's
// outcome: the report already decided it. An invocation whose join did not
// complete refuses to close — its clients may still be in use — and that refusal
// is diagnostic too.
func closeAssembly(ctx context.Context, e interface{ Close(context.Context) error }, log *slog.Logger) {
	if err := e.Close(ctx); err != nil {
		// The reason is part of the line, not a debug attribute: a refused close
		// is exactly the line an operator reads at the default level.
		log.Warn("closing plugins: " + err.Error())
	}
}
