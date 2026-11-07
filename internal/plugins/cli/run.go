package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// runCommand runs one backup. The report decides the exit code: the backup workflow fills
// it from run.degraded_exit_code and run.failed_exit_code, and a delivered run is
// 0. An error here means the run could not start at all — a config or preflight
// failure — and exits 1, the command layer's own code.
func runCommand(ctx context.Context, opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("run", stderr, func() {
		writef(stderr, "usage: %s run --config FILE --kind scheduled|manual [--dry-run] [--log-level LEVEL]\n", opts.Program)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	kind := fs.String("kind", "", "the run's identity: scheduled or manual (required, no default)")
	dryRun := fs.Bool("dry-run", false, "resolve discovery, pipelines and the secret invariant, write nothing")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	var problems []string
	if *cfgPath == "" {
		problems = append(problems, "--config is required")
	}
	if !model.RunKind(*kind).Valid() {
		problems = append(problems, fmt.Sprintf("--kind %q must be %q or %q", *kind, model.RunKindScheduled, model.RunKindManual))
	}
	if len(problems) > 0 {
		return usageError(stderr, opts.Program, "run", problems...)
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, "run", err.Error())
	}
	e, err := openAssembly(ctx, opts, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, "run", err)
	}
	defer closeAssembly(ctx, e, log)
	rep, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKind(*kind), DryRun: *dryRun, ConfigPath: *cfgPath})
	if err != nil {
		return failure(stderr, opts.Program, "run", err)
	}
	printRunReport(stdout, rep, *level == "debug")
	return rep.ExitCode
}

// stageCommand produces the sealed artifacts the run side reads back with the
// staged source. Stage has no run id: the file name is the identity, and the
// stage config carries its own exit codes.
func stageCommand(ctx context.Context, opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("stage", stderr, func() {
		writef(stderr, "usage: %s stage --config FILE [--log-level LEVEL]\n", opts.Program)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if *cfgPath == "" {
		return usageError(stderr, opts.Program, "stage", "--config is required")
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, "stage", err.Error())
	}
	e, err := openAssembly(ctx, opts, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, "stage", err)
	}
	defer closeAssembly(ctx, e, log)
	rep, err := e.Stage(ctx, assembly.StageRequest{ConfigPath: *cfgPath})
	if err != nil {
		return failure(stderr, opts.Program, "stage", err)
	}
	printRunReport(stdout, rep, *level == "debug")
	return rep.ExitCode
}
