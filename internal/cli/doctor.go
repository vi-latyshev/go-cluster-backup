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

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// doctorCommand verifies that every external binary any registered plugin's
// Spec declares in Requires is on PATH and that the distribution's defaults
// documents still hold up against its registry — the image build runs it as its
// last step, so an image missing a client, or carrying a document that no
// longer classifies every field, cannot be built. It names every missing binary
// and exits non-zero; a binary nobody requires is not its business.
func doctorCommand(program string, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
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
	// Every registered type's defaults, in the envelope of its own complete
	// example, built: an enum or a range a plugin validates itself is a value
	// the startup check can only read as a string, so this is where a bad one in
	// a defaults document stops an image from being built rather than failing
	// the night it is first used.
	var buildErrs []error
	for _, d := range reg.Descriptors() {
		if len(d.Example) == 0 {
			continue
		}
		// The pair comes from the registry doctor is walking: its config type is
		// registered, so the merge cannot fail to find it.
		merged := must.Do(defs.DefaultsConfig(reg, d.Kind, d.Type, d.Example))
		inst, err := reg.BuildAny(context.Background(), d.Kind, d.Type, merged,
			plugin.Deps{Instance: d.Type, Log: slog.New(slog.DiscardHandler)})
		if err != nil {
			buildErrs = append(buildErrs, fmt.Errorf("%s/%s: its own example does not hold up with the defaults merged under it: %w", d.Kind, d.Type, err))
			continue
		}
		if closer, ok := inst.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	if err := errors.Join(buildErrs...); err != nil {
		return failure(stderr, program, "doctor", err)
	}
	var binaries []string
	seen := map[string]bool{}
	for _, d := range reg.Descriptors() {
		for _, bin := range d.Requires {
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
		return failure(stderr, program, "doctor", out.err)
	}
	if len(missing) > 0 {
		return failure(stderr, program, "doctor", fmt.Errorf("missing binaries: %s", strings.Join(missing, ", ")))
	}
	return exitOK
}
