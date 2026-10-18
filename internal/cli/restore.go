package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// newestOK is the one selector value that is not a run id: the newest run whose
// copy of the artifact passes verification — per artifact, not per run.
const newestOK = "newest-ok"

// The two file operations of one fetch whose failure belongs to the machine, not
// to the request: a chmod and a close on a file this process just created. They
// are variables so a test can inject the failure and prove the cleanup that
// follows — a stale .partial removed, never published — which no ordinary file
// system will produce on demand.
var (
	chmod     = (*os.File).Chmod
	closeFile = (*os.File).Close
)

// restoreCommand dispatches the four restore subcommands. Nothing destructive is
// reachable by default: apply demands its mode, an in-place apply demands the
// target origin as confirmation, and every selector is explicit.
func restoreCommand(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return usageError(stderr, opts.Program, "restore", "one of list, fetch, apply, verify is required")
	}
	switch args[0] {
	case "list":
		return restoreList(ctx, opts, reg, defs, args[1:], stdout, stderr)
	case "fetch":
		return restoreFetch(ctx, opts, reg, defs, args[1:], stdout, stderr)
	case "apply":
		return restoreApply(ctx, opts, reg, defs, args[1:], stdout, stderr)
	case "verify":
		return restoreVerify(ctx, opts, reg, defs, args[1:], stdout, stderr)
	default:
		return usageError(stderr, opts.Program, "restore",
			fmt.Sprintf("unknown subcommand %q (known: list, fetch, apply, verify)", args[0]))
	}
}

// stringList collects one repeatable flag's values in the order given.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// runSelector maps the --run flag onto the fetch selector: the literal
// "newest-ok" asks the engine for the newest verified copy, anything else is an
// exact run id.
func runSelector(runFlag string) (runID string, newest bool) {
	if runFlag == newestOK {
		return "", true
	}
	return runFlag, false
}

// manifestArtifactNames lists the names one manifest holds, in the form
// --artifact takes: the artifact's own name, which may carry slashes; the group
// is a key namespace, never part of the name.
func manifestArtifactNames(rep model.RunReport) []string {
	names := make([]string, 0, len(rep.Artifacts))
	for _, a := range rep.Artifacts {
		names = append(names, a.Artifact.Name)
	}
	return names
}

// runManifestNames reads one run's artifact names from the first manifest a
// storage exposes for it — the same manifests `restore list` prints — one
// storage at a time, because a storage that is down must not withhold the
// answer. The run id is the engine's own selection; nothing here selects a run.
func runManifestNames(ctx context.Context, e *engine.Engine, holds []engine.StorageHold, runID string) ([]string, bool) {
	for _, hold := range holds {
		views, err := e.StoredRuns(ctx, engine.StoredRunsRequest{Storages: []string{hold.Storage}})
		if err != nil {
			continue
		}
		for _, v := range views {
			if v.RunID == runID && v.HasManifest {
				return manifestArtifactNames(v.Report), true
			}
		}
	}
	return nil, false
}

// artifactNameList renders names for one error message, bounded: a run holding
// hundreds of artifacts must not bury the answer.
func artifactNameList(names []string) string {
	const limit = 20
	if len(names) == 0 {
		return "none"
	}
	if len(names) > limit {
		return fmt.Sprintf("%s, … (%d more)", strings.Join(names[:limit], ", "), len(names)-limit)
	}
	return strings.Join(names, ", ")
}

// storageNames is the set of storages a hint may read: exactly the ones the
// fetch walk would, asked of the engine's own derivation — the request's
// list, else restore.storage_order — so a storage declared but left out of the
// order can never have its run named by a hint. An order the engine refuses
// (an unknown name, a config without a restore section) reads nothing: the
// caller's own error already says what is wrong.
func storageNames(e *engine.Engine, explicit []string) []string {
	names, err := e.StorageOrder(explicit)
	if err != nil {
		return nil
	}
	return names
}

// newestManifestNames names the newest run any one storage can read a manifest
// for — the latest start, its id and what it holds — surveying one storage at a
// time, because StoredRuns is all-or-nothing: a storage that is down must not
// withhold what the others answered. The id it names is evidence read back from
// a storage, never a selection the CLI made, and the hint stands only when no
// storage could be read at all.
func newestManifestNames(ctx context.Context, e *engine.Engine, storages []string) (string, []string, bool) {
	newest := ""
	var at time.Time
	var names []string
	for _, storage := range storages {
		views, err := e.StoredRuns(ctx, engine.StoredRunsRequest{Storages: []string{storage}})
		if err != nil {
			continue
		}
		for _, v := range views {
			if !v.HasManifest {
				continue
			}
			if newest == "" || v.Report.StartedAt.After(at) {
				newest, at, names = v.RunID, v.Report.StartedAt, manifestArtifactNames(v.Report)
			}
		}
	}
	if newest == "" {
		return "", nil, false
	}
	return newest, names, true
}

// unknownArtifactError is the honest answer when the engine's selection found no
// run holding the requested name. With a run selected — an exact --run — the
// plan names it and the answer is what that run does hold: the engine's
// per-storage reason ("the artifact is absent from the run") reads as if a copy
// were missing, while the truth is a name the run never had. With no run
// selected — newest-ok found nothing — the engine's sentence stays true and the
// answer appends the newest run a storage can read, and what it holds. It
// returns nil when the name is not the problem, so a caller's own path reports
// that failure as it always has.
func unknownArtifactError(ctx context.Context, e *engine.Engine, req engine.FetchRequest, plan engine.FetchPlan, err error) error {
	if req.RawStorage != "" {
		return nil
	}
	if plan.RunID != "" {
		names, ok := runManifestNames(ctx, e, plan.Storages, plan.RunID)
		if !ok || slices.Contains(names, req.Artifact) {
			return nil
		}
		return fmt.Errorf("run %s has no artifact named %q; it holds: %s",
			plan.RunID, req.Artifact, artifactNameList(names))
	}
	runID, names, ok := newestManifestNames(ctx, e, storageNames(e, req.Storages))
	if !ok || slices.Contains(names, req.Artifact) {
		return nil
	}
	return fmt.Errorf("%w; the newest run with a manifest, %s, holds: %s", err, runID, artifactNameList(names))
}

// resolveArtifact resolves one artifact request for a command that then uses the
// plan; a name the selected run does not hold comes back as the honest answer.
func resolveArtifact(ctx context.Context, e *engine.Engine, req engine.FetchRequest) (engine.FetchPlan, error) {
	plan, err := e.ResolveFetch(ctx, req)
	if err == nil {
		return plan, nil
	}
	if named := unknownArtifactError(ctx, e, req, plan, err); named != nil {
		return plan, named
	}
	return plan, err
}

// unknownArtifactName reports only a name the selected run does not hold, and
// nil for every other outcome: verify's rows keep their own error for a
// reachability problem or a missing object.
func unknownArtifactName(ctx context.Context, e *engine.Engine, req engine.FetchRequest) error {
	plan, err := e.ResolveFetch(ctx, req)
	if err == nil {
		return nil
	}
	return unknownArtifactError(ctx, e, req, plan, err)
}

// restoreList lists the runs every storage holds, including runs without a
// manifest: an unfinished run is the interesting one on this listing.
func restoreList(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	const command = "restore list"
	fs := newFlagSet(command, stderr, func() {
		writef(stderr, "usage: %s %s --config FILE [--storage NAME]... [--run ID] [--output text|json] [--log-level LEVEL]\n",
			opts.Program, command)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	var storages stringList
	fs.Var(&storages, "storage", "storage to list, repeatable (default: restore.storage_order)")
	runID := fs.String("run", "", "show only this run id")
	output := fs.String("output", "text", "text or json")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	var problems []string
	if *cfgPath == "" {
		problems = append(problems, "--config is required")
	}
	if *output != "text" && *output != "json" {
		problems = append(problems, fmt.Sprintf("--output %q must be text or json", *output))
	}
	if len(problems) > 0 {
		return usageError(stderr, opts.Program, command, problems...)
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, command, err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	defer closeEngine(e, log)
	views, err := e.StoredRuns(ctx, engine.StoredRunsRequest{Storages: storages})
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	if *runID != "" {
		views = filterViews(views, *runID)
	}
	if *output == "json" {
		return writeViewsJSON(stdout, stderr, opts.Program, command, views)
	}
	return writeViewsText(stdout, stderr, opts.Program, command, views)
}

func filterViews(views []engine.StoredRunView, runID string) []engine.StoredRunView {
	var out []engine.StoredRunView
	for _, v := range views {
		if v.RunID == runID {
			out = append(out, v)
		}
	}
	return out
}

// storedRunJSON is the JSON form of one stored run. The manifest is the run
// report as it was written to the storage, schema version included.
type storedRunJSON struct {
	RunID       string           `json:"run_id"`
	Kind        model.RunKind    `json:"kind"`
	Storage     string           `json:"storage"`
	HasManifest bool             `json:"has_manifest"`
	Report      *model.RunReport `json:"report"`
}

func writeViewsJSON(stdout, stderr io.Writer, program, command string, views []engine.StoredRunView) int {
	out := make([]storedRunJSON, 0, len(views))
	for _, v := range views {
		row := storedRunJSON{RunID: v.RunID, Kind: v.Kind, Storage: v.Storage, HasManifest: v.HasManifest}
		if v.HasManifest {
			report := v.Report
			row.Report = &report
		}
		out = append(out, row)
	}
	// Every field is a typed Go value from a decoded manifest: there is nothing
	// in a row JSON cannot carry.
	b := must.Do(json.Marshal(out))
	if _, err := fmt.Fprintf(stdout, "%s\n", b); err != nil {
		return failure(stderr, program, command, err)
	}
	return exitOK
}

func writeViewsText(stdout, stderr io.Writer, program, command string, views []engine.StoredRunView) int {
	w := &errWriter{w: stdout}
	for _, v := range views {
		if v.HasManifest {
			r := v.Report
			w.printf("run %s storage=%s kind=%s manifest=yes outcome=%s started=%s duration=%s artifacts=%d\n",
				v.RunID, v.Storage, v.Kind, r.Outcome, r.StartedAt.UTC().Format(time.RFC3339),
				human.Dur(r.Duration.Std()), len(r.Artifacts))
			// The names, exactly as --artifact takes them: an operator copies
			// one from here instead of guessing the group's part in a key.
			if names := manifestArtifactNames(r); len(names) > 0 {
				w.printf("  artifacts: %s\n", strings.Join(names, ", "))
			}
			continue
		}
		w.printf("run %s storage=%s kind=%s manifest=no\n", v.RunID, v.Storage, v.Kind)
	}
	if w.err != nil {
		return failure(stderr, program, command, w.err)
	}
	return exitOK
}

// restoreFetch downloads one artifact (or one object by key) and publishes it to
// --out. Without --raw the processor chain is reversed, so the bytes at the
// destination are the artifact's plaintext.
func restoreFetch(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	const command = "restore fetch"
	fs := newFlagSet(command, stderr, func() {
		writef(stderr, `usage: %[1]s %[2]s --config FILE (--run ID | --run %[3]s) --artifact NAME --out PATH|- [--storage NAME]... [--raw] [--plaintext-to-file]
       %[1]s %[2]s --config FILE --storage NAME --key KEY --out PATH|-
NAME is the artifact's name as "restore list" prints it: the group is not part
of it, and a name may itself contain "/".
`, opts.Program, command, newestOK)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	runFlag := fs.String("run", "", "run id, or "+strconv.Quote(newestOK)+" for the newest run whose copy passes verification")
	artifact := fs.String("artifact", "", `artifact name exactly as "restore list" prints it: the group is not part of it and a name may contain "/", e.g. app/postgres`)
	out := fs.String("out", "", "file to write, or - for stdout (required)")
	var storages stringList
	fs.Var(&storages, "storage", "storage to try, in order, repeatable (default: restore.storage_order)")
	raw := fs.Bool("raw", false, "write the stored bytes without reversing the processor chain")
	key := fs.String("key", "", "fetch one object by key with no manifest; needs exactly one --storage")
	plaintext := fs.Bool("plaintext-to-file", false, "acknowledge writing a decoded secret artifact to a file")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	problems := fetchFlagProblems(*cfgPath, *out, *key, *runFlag, *artifact, *raw, len(storages))
	if len(problems) > 0 {
		return usageError(stderr, opts.Program, command, problems...)
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, command, err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	defer closeEngine(e, log)
	req := engine.FetchRequest{Storages: storages}
	if *key != "" {
		req = engine.FetchRequest{RawStorage: storages[0], RawKey: *key}
	} else {
		req.RunID, req.Newest = runSelector(*runFlag)
		req.Artifact = *artifact
		req.Decode = !*raw
	}
	return writeFetched(ctx, e, req, *out, *plaintext, stdout, stderr, opts.Program, command)
}

// fetchFlagProblems validates the two fetch forms: by artifact needs a run and
// an artifact, by key needs exactly one storage and excludes the manifest
// selectors — none of them has a default.
func fetchFlagProblems(cfgPath, out, key, runFlag, artifact string, raw bool, storageCount int) []string {
	var problems []string
	if cfgPath == "" {
		problems = append(problems, "--config is required")
	}
	if out == "" {
		problems = append(problems, "--out is required")
	}
	if key != "" {
		if storageCount != 1 {
			problems = append(problems, "--key needs exactly one --storage")
		}
		if runFlag != "" {
			problems = append(problems, "--key and --run exclude each other")
		}
		if artifact != "" {
			problems = append(problems, "--key and --artifact exclude each other")
		}
		if raw {
			problems = append(problems, "--key and --raw exclude each other: a by-key fetch never decodes")
		}
		return problems
	}
	if runFlag == "" {
		problems = append(problems, "--run is required")
	}
	if artifact == "" {
		problems = append(problems, "--artifact is required")
	}
	return problems
}

// writeFetched resolves what the request would fetch, applies the plaintext
// rule, and only then downloads and publishes. The refusal is decided before a
// byte moves: a refused request neither downloads the object nor leaves any
// plaintext of it on the disk. A file destination is written as <out>.partial
// and renamed once the engine verified the bytes, so a failure never leaves a
// half-written file at <out>.
func writeFetched(ctx context.Context, e *engine.Engine, req engine.FetchRequest, out string, plaintextAllowed bool,
	stdout, stderr io.Writer, program, command string) int {
	plan, err := resolveArtifact(ctx, e, req)
	if err != nil {
		return failure(stderr, program, command, err)
	}
	// With the artifact on stdout the plan is the only place the run, the
	// storage and the size can be named; with a file destination it also
	// precedes a transfer that may take minutes.
	report := stdout
	if out == "-" {
		report = stderr
	}
	printFetchPlan(report, plan)
	if out != "-" && req.Decode && plan.Sensitivity == model.SensitivitySecret && !plaintextAllowed {
		return failure(stderr, program, command, fmt.Errorf(
			"%s is a secret artifact: writing its decoded plaintext to %s is refused without --plaintext-to-file; pass the flag, or stream it with --out -",
			plan.Artifact, out))
	}
	if out == "-" {
		// The artifact owns stdout; its report goes to stderr so a pipeline
		// can consume the bytes untouched.
		rep, err := e.Fetch(ctx, req, stdout)
		if err != nil {
			return failure(stderr, program, command, err)
		}
		printFetchReport(stderr, rep)
		return exitOK
	}
	partial := out + ".partial"
	// 0600 for every fetch output, whatever the artifact's label says: a
	// database dump is as sensitive as a secret one. O_CREATE's mode applies
	// only to a file that did not exist, so the chmod also narrows a stale
	// .partial from an earlier attempt — the published file inherits this mode
	// through the rename.
	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return failure(stderr, program, command, err)
	}
	if err := chmod(f, 0o600); err != nil {
		_ = closeFile(f)
		_ = os.Remove(partial)
		return failure(stderr, program, command, err)
	}
	rep, fetchErr := e.Fetch(ctx, req, f)
	closeErr := closeFile(f)
	if fetchErr != nil {
		_ = os.Remove(partial)
		return failure(stderr, program, command, fetchErr)
	}
	if closeErr != nil {
		_ = os.Remove(partial)
		return failure(stderr, program, command, closeErr)
	}
	if err := os.Rename(partial, out); err != nil {
		_ = os.Remove(partial)
		return failure(stderr, program, command, err)
	}
	printFetchReport(stdout, rep)
	return exitOK
}

// restoreApply writes one fetched and verified artifact to a target. The mode
// has no default, and an in-place apply demands the target origin verbatim: the
// command layer refuses an in-place call that carries no confirmation at all,
// and the engine — before downloading a byte — refuses a confirmation that does
// not match the target exactly, the rest of the target checks with it.
func restoreApply(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	const command = "restore apply"
	fs := newFlagSet(command, stderr, func() {
		writef(stderr, `usage: %[1]s %[2]s --config FILE (--run ID | --run %[3]s) --artifact NAME --target discovered|NAME --mode side-by-side|in-place [--confirm ORIGIN] [--allow-foreign-origin] [--log-level LEVEL]
NAME is the artifact's name as "restore list" prints it: the group is not part
of it, and a name may itself contain "/".
`, opts.Program, command, newestOK)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	runFlag := fs.String("run", "", "run id, or "+strconv.Quote(newestOK))
	artifact := fs.String("artifact", "", `artifact name to apply, exactly as "restore list" prints it (the group is not part of it)`)
	target := fs.String("target", "", `source instance name, or "discovered"`)
	mode := fs.String("mode", "", "side-by-side or in-place (required, no default)")
	confirm := fs.String("confirm", "", "with in-place: the target origin verbatim, as the manifest recorded it")
	foreign := fs.Bool("allow-foreign-origin", false, "accept a target whose origin differs from the manifest's")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	var problems []string
	if *cfgPath == "" {
		problems = append(problems, "--config is required")
	}
	if *runFlag == "" {
		problems = append(problems, "--run is required")
	}
	if *artifact == "" {
		problems = append(problems, "--artifact is required")
	}
	if *target == "" {
		problems = append(problems, "--target is required")
	}
	var applyMode source.ApplyMode
	switch *mode {
	case "side-by-side":
		applyMode = source.ApplySideBySide
	case "in-place":
		applyMode = source.ApplyInPlace
	case "":
		problems = append(problems, "--mode is required: side-by-side or in-place")
	default:
		problems = append(problems, fmt.Sprintf("--mode %q must be side-by-side or in-place", *mode))
	}
	if *mode == "in-place" && *confirm == "" {
		problems = append(problems, "--confirm is required with --mode in-place: its value must be the target origin verbatim")
	}
	if len(problems) > 0 {
		return usageError(stderr, opts.Program, command, problems...)
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, command, err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	defer closeEngine(e, log)
	runID, newest := runSelector(*runFlag)
	fetchReq := engine.FetchRequest{RunID: runID, Newest: newest, Artifact: *artifact}
	rep, err := e.Apply(ctx, engine.ApplyRunRequest{
		Fetch:              fetchReq,
		Target:             *target,
		Mode:               applyMode,
		Confirm:            *confirm,
		AllowForeignOrigin: *foreign,
	})
	if err != nil {
		// The apply resolves the artifact before it touches anything, so a name
		// the run does not hold is asked about only now: the happy path surveys
		// the storages once, inside the engine.
		if named := unknownArtifactName(ctx, e, fetchReq); named != nil {
			return failure(stderr, opts.Program, command, named)
		}
		return failure(stderr, opts.Program, command, err)
	}
	previous := rep.Previous
	if previous == "" {
		previous = "-"
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", human.Line(time.Now(),
		"✓ applied to %s (previous copy: %s, %s)", rep.Target, previous, human.Size(rep.Bytes))); err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	return exitOK
}

// restoreVerify proves an artifact is readable at one of three depths without a
// target: stored checks the object on every storage, decoded reverses the
// processor chain, probed asks a named source's Prober to read it. It exits 1
// when any artifact failed — this is the command the runbook schedules by hand
// after a rollout.
func restoreVerify(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	const command = "restore verify"
	fs := newFlagSet(command, stderr, func() {
		writef(stderr, `usage: %[1]s %[2]s --config FILE (--run ID | --run %[3]s) [--artifact NAME] --depth stored|decoded|probed [--target NAME|discovered] [--log-level LEVEL]
NAME is the artifact's name as "restore list" prints it: the group is not part
of it, and a name may itself contain "/".
`, opts.Program, command, newestOK)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	runFlag := fs.String("run", "", "run id, or "+strconv.Quote(newestOK))
	artifact := fs.String("artifact", "", `artifact name to verify, exactly as "restore list" prints it (the group is not part of it); empty verifies every artifact of the run (needs an exact run id)`)
	depth := fs.String("depth", "", "stored, decoded or probed (required)")
	target := fs.String("target", "", `with probed: the source instance name or "discovered" whose Prober runs`)
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	var problems []string
	if *cfgPath == "" {
		problems = append(problems, "--config is required")
	}
	if *runFlag == "" {
		problems = append(problems, "--run is required")
	}
	switch *depth {
	case "stored", "decoded", "probed":
	case "":
		problems = append(problems, "--depth is required: stored, decoded or probed")
	default:
		problems = append(problems, fmt.Sprintf("--depth %q must be stored, decoded or probed", *depth))
	}
	if *depth == "probed" && *target == "" {
		problems = append(problems, "--target is required with --depth probed: the source whose Prober runs")
	}
	if len(problems) > 0 {
		return usageError(stderr, opts.Program, command, problems...)
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, command, err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	defer closeEngine(e, log)
	runID, newest := runSelector(*runFlag)
	// A named artifact the selected run does not hold is answered here, before
	// the verification rows exist; every other failure stays that row's own.
	if *artifact != "" {
		if err := unknownArtifactName(ctx, e, engine.FetchRequest{RunID: runID, Newest: newest, Artifact: *artifact}); err != nil {
			return failure(stderr, opts.Program, command, err)
		}
	}
	results, err := e.Verify(ctx, engine.VerifyRequest{
		Fetch:  engine.FetchRequest{RunID: runID, Newest: newest, Artifact: *artifact},
		Depth:  *depth,
		Target: *target,
	})
	if err != nil {
		return failure(stderr, opts.Program, command, err)
	}
	w := &errWriter{w: stdout}
	now := time.Now()
	failed := false
	for _, r := range results {
		if r.OK {
			w.printf("%s\n", human.Line(now, "✓ %s %s ok on %s (%s) run=%s",
				r.Artifact, r.Depth, r.Storage, human.Size(r.Bytes), r.RunID))
			continue
		}
		failed = true
		w.printf("%s\n", human.Line(now, "✗ %s %s: %s", r.Artifact, r.Depth, r.Error))
	}
	if w.err != nil {
		return failure(stderr, opts.Program, command, w.err)
	}
	if failed {
		return exitFailure
	}
	return exitOK
}
