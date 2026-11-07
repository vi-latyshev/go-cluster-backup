package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sort"
	"strings"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
)

// doctorCommand verifies the build's own health and that every external binary
// any compiled plugin's Spec declares in Binaries is on PATH: the image build
// runs it as its last step, so an image whose plugin graph no longer holds up,
// or missing a client, cannot be built. It prints the shared validator's answer
// once — the same floor, Requires, cycle, selection and kind checks
// `plugins check` and `validate` read — then builds every type's own complete
// example under its defaults, which is where an enum or a range a plugin
// validates itself stops an image. It names every missing binary and exits
// non-zero; a binary nobody requires is not its business.
func doctorCommand(opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	program := opts.Program
	fs := newFlagSet("doctor", stderr, func() {
		writef(stderr, "usage: %s doctor\n", program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	// The command layer loaded and checked these documents before dispatching
	// here; this line exists so the image build's log carries the answer in its
	// own line, not to ask the question a second time.
	writef(stdout, "defaults: ok (%d documents)\n", defs.Documents())
	var problems []error
	problems = append(problems, validator{lr: opts.Loader}.problems()...)
	// Every registered type's defaults, in the envelope of its own complete
	// example, built: an enum or a range a plugin validates itself is a value
	// the startup check can only read as a string, so this is where a bad one in
	// a defaults document stops an image from being built rather than failing
	// the night it is first used.
	var buildErrs []error
	// ports is every storage example doctor built: the read-only surface of a
	// constructed destination, never the raw instance behind it.
	var ports []guard.Port
	for _, d := range descriptors(opts, reg) {
		if len(d.Example) == 0 {
			continue
		}
		// The pair comes from the registry doctor is walking: its config type is
		// registered, so the merge cannot fail to find it.
		merged := must.Do(defs.DefaultsConfig(reg, d.Kind, d.Type, d.Example))
		deps := instances.Deps{Instance: d.Type, Log: slog.New(slog.DiscardHandler)}
		if d.Kind == storage.Kind.Name() {
			// A storage has exactly one production construction path, guarded
			// from the factory to the read port; it carries the effective
			// call_parallelism the assembly would read from the instance
			// envelope. Doctor keeps the port for its answer and closes the
			// destination through the storage boundary once: the raw instance
			// never leaves it.
			dest, err := guard.Build(context.Background(), reg, d.Type, merged, storageInput(defs), deps)
			if err != nil {
				buildErrs = append(buildErrs, fmt.Errorf("%s: its own example does not hold up with the defaults merged under it: %w", d.ID, err))
				continue
			}
			ports = append(ports, dest.Port())
			if err := dest.Close(); err != nil {
				buildErrs = append(buildErrs, fmt.Errorf("%s: closing its example: %w", d.ID, err))
			}
			continue
		}
		inst, err := reg.BuildAny(context.Background(), d.Kind, d.Type, merged, deps)
		if err != nil {
			buildErrs = append(buildErrs, fmt.Errorf("%s: its own example does not hold up with the defaults merged under it: %w", d.ID, err))
			continue
		}
		if closer, ok := inst.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	writef(stdout, "storage examples: %d built\n", len(ports))
	problems = append(problems, buildErrs...)
	var binaries []string
	seen := map[string]bool{}
	for _, d := range descriptors(opts, reg) {
		for _, bin := range d.Binaries {
			if !seen[bin] {
				seen[bin] = true
				binaries = append(binaries, bin)
			}
		}
	}
	sort.Strings(binaries)
	out := &errWriter{w: stdout}
	var missing []string
	for _, bin := range binaries {
		path, err := exec.LookPath(bin)
		if err != nil {
			missing = append(missing, bin)
			out.printf("%s: missing\n", bin)
			continue
		}
		out.printf("%s: %s\n", bin, path)
	}
	if out.err != nil {
		problems = append(problems, out.err)
	}
	if len(missing) > 0 {
		problems = append(problems, fmt.Errorf("missing binaries: %s", strings.Join(missing, ", ")))
	}
	if err := errors.Join(problems...); err != nil {
		return failure(stderr, program, "doctor", err)
	}
	return exitOK
}

// storageInput is the construction input the assembly hands a storage: the
// effective call_parallelism of the storage instance envelope, which the
// defaults document owns. The startup check proved the document's shape, so a
// value that does not decode cannot reach this line; the fallback keeps a
// document that states nothing at the plugin's one call at a time.
func storageInput(defs *configuration.Defaults) storage.BuildInput {
	in := storage.BuildInput{CallParallelism: 1}
	block, _ := configuration.ObjectOf(defs.InstanceBlock(storage.Kind.Name()))
	if raw, ok := block["call_parallelism"]; ok {
		var n int
		if err := json.Unmarshal(raw, &n); err == nil && n > 0 {
			in.CallParallelism = n
		}
	}
	return in
}
