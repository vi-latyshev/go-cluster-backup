package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
)

// validateCommand checks a configuration file without touching the network:
// structure, types, templates, and every reference between instances. It must
// succeed on a config whose secret files do not exist on this machine — a
// secret is a reference, and only a plugin Check reads it. Every error is
// printed, each with its JSON-pointer path, never only the first.
//
// --preflight adds the planning preflight and prints one line per instance —
// the declared ones, the sources a discovery finds and the objects it excludes,
// each built and checked the way a run would build it: `ok`, `excluded (kind)`
// with the reason, or `FAILED` with the reason (credentials, reachability, a
// mount, a required binary PATH lacks, a literal the plugin rejects). An
// excluded object builds nothing: the decision is the whole result, and the
// summary counts it apart from the instances that were checked. It is not the
// post-rollout check in full: a scoped run against a pod that cannot see the
// cluster — the restore pod — checks credentials, storages and mounts, and
// nothing a discovery would: no pod/PVC/PV listing, no discovered Secret, no
// database. A full run is what covers those. A failure fails this command,
// never the run: the check is scoped to that instance. A storage is out for the
// run only when its failure is permanent; a transient one costs a retry.
//
// --instance KIND/NAME, repeatable, restricts the preflight to the named
// instances — the check for a pod that cannot reach every instance of the file
// (an identity file, a mount) without keeping a second, trimmed copy of the
// configuration. A named discovery brings the sources it finds with it; the
// summary counts only what was checked and names every configured instance the
// selection left out, so a scoped run is never read as a full one.
func validateCommand(ctx context.Context, opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("validate", stderr, func() {
		writef(stderr, "usage: %s validate --config FILE [--preflight [--instance KIND/NAME]...] [--termination-grace-seconds SECONDS] [--log-level LEVEL]\n", opts.Program)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	preflight := fs.Bool("preflight", false, "check every declared instance and every discovered source: credentials, reachability, required binaries; a failure is scoped to that instance, never the run")
	grace := fs.Int("termination-grace-seconds", 0, "the chart's terminationGracePeriodSeconds; refuse when the cleanup bound the effective configuration requires exceeds it. An explicitly passed value is always checked; omitting the flag skips the check")
	var instances stringList
	fs.Var(&instances, "instance", "with --preflight: check only this KIND/NAME, repeatable; a named discovery brings the sources it finds. Without it every declared instance is checked")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if *cfgPath == "" {
		return usageError(stderr, opts.Program, "validate", "--config is required")
	}
	// An explicit value is the chart's promise and is always checked; only an
	// absent flag skips the check. A literal zero is a grace no cleanup fits
	// under, never "no deadline to compare".
	graceGiven := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "termination-grace-seconds" {
			graceGiven = true
		}
	})
	if graceGiven && *grace < 0 {
		return usageError(stderr, opts.Program, "validate", "--termination-grace-seconds must not be negative")
	}
	var graceFlag *int
	if graceGiven {
		graceFlag = grace
	}
	if len(instances) > 0 && !*preflight {
		return usageError(stderr, opts.Program, "validate", "--instance requires --preflight")
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, "validate", err.Error())
	}
	// One aggregated answer: the configuration's decode and reference errors,
	// and the diagnostics only the compiled set can judge — a selection naming
	// an unknown plugin, a rule whose source the selection disables. The assembly
	// built here is what --preflight reuses, so every plugin is constructed once.
	e, problems := validateConfiguration(ctx, opts, reg, defs, *cfgPath, log, graceFlag)
	if e != nil {
		defer closeAssembly(ctx, e, log)
	}
	if len(problems) > 0 {
		return failure(stderr, opts.Program, "validate", errors.Join(problems...))
	}
	if !*preflight {
		writef(stdout, "%s: valid\n", *cfgPath)
		return exitOK
	}
	// The selection is resolved before a single check runs: an unknown name is
	// a usage error with the valid list, not a failed row to decode, and not a
	// single Check on an instance the caller meant to leave alone.
	req, skipped, err := preflightSelection(ctx, e, instances)
	if err != nil {
		return usageError(stderr, opts.Program, "validate", err.Error())
	}
	writef(stdout, "%s: valid\n", *cfgPath)
	// One line per checked instance, failures included: this is the check an
	// operator runs after a rollout, before the first nightly run, to see which
	// credentials and which endpoints answer.
	results := e.PreflightResults(ctx, req)
	failed, excluded := 0, 0
	for _, r := range results {
		switch {
		case r.Excluded != "":
			// A decision, not a fault: the object was seen and deliberately
			// carries no artifact, and its line says which decision it is.
			excluded++
			writef(stdout, "  %s/%s: excluded (%s): %s\n", r.Kind, r.Instance, r.Excluded, r.ExcludedReason)
		case r.OK:
			writef(stdout, "  %s/%s: ok\n", r.Kind, r.Instance)
		default:
			failed++
			writef(stdout, "  %s/%s: FAILED: %s\n", r.Kind, r.Instance, r.Error)
		}
	}
	checked := len(results) - excluded
	summary := fmt.Sprintf("%d of %d instances", failed, checked)
	if len(instances) > 0 {
		summary = fmt.Sprintf("%d of %d checked instances", failed, checked)
	}
	writef(stdout, "preflight: %s failed\n", summary)
	if excluded > 0 {
		writef(stdout, "preflight: %d objects excluded\n", excluded)
	}
	if len(skipped) > 0 {
		writef(stdout, "preflight: %d configured instances not checked: %s\n", len(skipped), strings.Join(skipped, ", "))
	}
	if failed > 0 {
		return failure(stderr, opts.Program, "validate", fmt.Errorf("preflight: %s failed", summary))
	}
	return exitOK
}

// validateConfiguration runs a configuration's offline checks once and returns
// every problem they found: the assembly's aggregated answer — merge, decode,
// selection, roles and every pure reference problem — and the diagnostics only
// the compiled set can judge on a configuration that built. The assembly is
// returned when it could be built, so --preflight reuses it instead of
// constructing every plugin a second time; the assembly itself never short-
// circuits one error class at the expense of another.
func validateConfiguration(ctx context.Context, opts Options, reg *instances.Registry, defs *configuration.Defaults, path string, log *slog.Logger, graceSeconds *int) (*assembly.Assembly, []error) {
	// The build's own problems come first: a floor, Requires or cycle problem is
	// the same answer `plugins check` and `doctor` give, and validate reads it
	// from the same validator rather than deciding it again.
	problems := validator{lr: opts.Loader}.problems()
	e, err := openAssembly(ctx, opts, defs, path, log)
	if err != nil {
		problems = append(problems, err)
	}
	// The chart-grace gate reads the effective configuration, not the built
	// assembly: when the assembly refuses, the pure merge still yields the
	// document it needs. Its merge problems are already in the assembly's
	// aggregated answer and are not repeated here.
	var effective jsontext.Value
	if e != nil {
		effective, _ = e.EffectiveConfig()
	} else if raw, err := configuration.ReadConfig(path); err == nil {
		if m, _, _ := defs.Merge(reg, raw); len(m) > 0 {
			effective = m
		}
	}
	if len(effective) > 0 {
		var cfg configuration.Config
		if err := configuration.Decode(effective, &cfg); err == nil {
			if e != nil {
				problems = append(problems, configDiagnostics(opts, reg, &cfg)...)
			}
			problems = append(problems, graceProblems(&cfg, graceSeconds)...)
		}
	}
	return e, problems
}

// graceProblems checks one effective configuration against the termination
// grace the chart sets. The cleanup bound is the release window the run owns
// plus the lifecycle budgets a deployment may override; a chart grace shorter
// than that bound kills the process mid-cleanup, leaving the writer lease and
// the abort ledger to uncertainty. A grace of 0 means the flag was not given
// and the check is skipped.
func graceProblems(cfg *configuration.Config, seconds *int) []error {
	if seconds == nil {
		return nil
	}
	var in lifecycle.GraceInputs
	if cfg.Run != nil {
		in.Release = cfg.Run.FinishAllowance.Std()
	}
	if cfg.Plugins != nil {
		if raw := cfg.Plugins.Config[lifecycle.Spec.ID]; len(raw) > 0 {
			var lc lifecycle.Config
			if err := configuration.Decode(raw, &lc); err != nil {
				return []error{fmt.Errorf("/plugins/config/%s: %w", lifecycle.Spec.ID, err)}
			}
			in.Join = lc.Join.Std()
			in.AbortTotal = lc.AbortTotal.Std()
			in.Close = lc.Close.Std()
			in.Margin = lc.Margin.Std()
		}
	}
	if need, err := lifecycle.RequiredGrace(in); err != nil {
		// A budget outside its range, or a sum that does not fit, is refused
		// here: a zero bound would otherwise be read as "no deadline".
		return []error{err}
	} else if need > time.Duration(*seconds)*time.Second {
		return []error{fmt.Errorf("--termination-grace-seconds %d is below the cleanup bound %s this configuration requires", *seconds, need)}
	}
	return nil
}

// preflightSelection maps --instance names onto the assembly's declared instances
// before any check runs. Names empty means the whole preflight. It returns the
// request and the configured instances the request leaves out, in the assembly's
// stable order; a name that is not a declared instance is an error naming every
// valid KIND/NAME.
func preflightSelection(ctx context.Context, e *assembly.Assembly, names []string) (planning.Request, []string, error) {
	if len(names) == 0 {
		return planning.Request{}, nil, nil
	}
	targets := e.PreflightTargets(ctx)
	valid := make([]string, 0, len(targets))
	present := make(map[string]bool, len(targets))
	for _, t := range targets {
		key := t.Kind + "/" + t.Name
		valid = append(valid, key)
		present[key] = true
	}
	var selection []planning.Target
	var unknown []string
	chosen := make(map[string]bool, len(names))
	for _, name := range names {
		if !present[name] {
			unknown = append(unknown, name)
			continue
		}
		kind, instance, _ := strings.Cut(name, "/")
		selection = append(selection, planning.Target{Kind: kind, Name: instance})
		chosen[name] = true
	}
	if len(unknown) > 0 {
		return planning.Request{}, nil, fmt.Errorf("unknown instance %s (valid: %s)",
			strings.Join(unknown, ", "), strings.Join(valid, ", "))
	}
	var skipped []string
	for _, key := range valid {
		if !chosen[key] {
			skipped = append(skipped, key)
		}
	}
	return planning.Request{Instances: selection}, skipped, nil
}
