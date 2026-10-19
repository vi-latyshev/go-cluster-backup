package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// validateCommand checks a configuration file without touching the network:
// structure, types, templates, and every reference between instances. It must
// succeed on a config whose secret files do not exist on this machine — a
// secret is a reference, and only a plugin Check reads it. Every error is
// printed, each with its JSON-pointer path, never only the first.
//
// --preflight adds the engine's preflight and prints one line per instance —
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
// never the run: the engine scopes it to that instance. A storage is out for the
// run only when its failure is permanent; a transient one costs a retry.
//
// --instance KIND/NAME, repeatable, restricts the preflight to the named
// instances — the check for a pod that cannot reach every instance of the file
// (an identity file, a mount) without keeping a second, trimmed copy of the
// configuration. A named discovery brings the sources it finds with it; the
// summary counts only what was checked and names every configured instance the
// selection left out, so a scoped run is never read as a full one.
func validateCommand(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("validate", stderr, func() {
		writef(stderr, "usage: %s validate --config FILE [--preflight [--instance KIND/NAME]...] [--log-level LEVEL]\n", opts.Program)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	preflight := fs.Bool("preflight", false, "check every declared instance and every discovered source: credentials, reachability, required binaries; a failure is scoped to that instance, never the run")
	var instances stringList
	fs.Var(&instances, "instance", "with --preflight: check only this KIND/NAME, repeatable; a named discovery brings the sources it finds. Without it every declared instance is checked")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if *cfgPath == "" {
		return usageError(stderr, opts.Program, "validate", "--config is required")
	}
	if len(instances) > 0 && !*preflight {
		return usageError(stderr, opts.Program, "validate", "--instance requires --preflight")
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, "validate", err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, "validate", err)
	}
	defer closeEngine(e, log)
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

// preflightSelection maps --instance names onto the engine's declared instances
// before any check runs. Names empty means the whole preflight. It returns the
// request and the configured instances the request leaves out, in the engine's
// stable order; a name that is not a declared instance is an error naming every
// valid KIND/NAME.
func preflightSelection(ctx context.Context, e *engine.Engine, names []string) (engine.PreflightRequest, []string, error) {
	if len(names) == 0 {
		return engine.PreflightRequest{}, nil, nil
	}
	targets := e.PreflightTargets(ctx)
	valid := make([]string, 0, len(targets))
	present := make(map[string]bool, len(targets))
	for _, t := range targets {
		key := t.Kind + "/" + t.Name
		valid = append(valid, key)
		present[key] = true
	}
	var selection []engine.PreflightTarget
	var unknown []string
	chosen := make(map[string]bool, len(names))
	for _, name := range names {
		if !present[name] {
			unknown = append(unknown, name)
			continue
		}
		kind, instance, _ := strings.Cut(name, "/")
		selection = append(selection, engine.PreflightTarget{Kind: kind, Name: instance})
		chosen[name] = true
	}
	if len(unknown) > 0 {
		return engine.PreflightRequest{}, nil, fmt.Errorf("unknown instance %s (valid: %s)",
			strings.Join(unknown, ", "), strings.Join(valid, ", "))
	}
	var skipped []string
	for _, key := range valid {
		if !chosen[key] {
			skipped = append(skipped, key)
		}
	}
	return engine.PreflightRequest{Instances: selection}, skipped, nil
}
