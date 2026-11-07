package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// pluginsCommand answers the questions a compiled build raises before a
// deployment is touched: "plugins" lists every compiled plugin type, "plugins
// check" runs the build's one offline validator, and "plugins describe
// KIND/TYPE" documents one type. The fields and the example come from the
// loader's compiled descriptors, the defaults from the distribution's
// documents, so a new plugin documents itself.
func pluginsCommand(opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "check" {
		return pluginsCheck(opts, args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "describe" {
		return pluginsDescribe(opts, reg, defs, args[1:], stdout, stderr)
	}
	fs := newFlagSet("plugins", stderr, func() {
		writef(stderr, "usage: %s plugins [check | describe KIND/TYPE]\n", opts.Program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	for _, d := range descriptors(opts, reg) {
		binaries := ""
		if len(d.Binaries) > 0 {
			binaries = "binaries: " + strings.Join(d.Binaries, ", ")
		}
		// The tabwriter remembers its first write failure and Flush reports it.
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", d.ID, d.Stability, binaries, d.Summary)
	}
	if err := tw.Flush(); err != nil {
		return failure(stderr, opts.Program, "plugins", err)
	}
	return exitOK
}

// pluginsCheck runs the build's one offline validator: every loader kind's own
// Validate, the floor rule, the effective Requires edges, the dependency cycle
// and the selection the distribution applied. It constructs nothing and
// performs no I/O; the same validator is what `doctor` and `validate` read, so
// a build is never judged by two different sets of rules.
func pluginsCheck(opts Options, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("plugins check", stderr, func() {
		writef(stderr, "usage: %s plugins check\n", opts.Program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(stderr, opts.Program, "plugins check", "it takes no arguments")
	}
	if err := errors.Join(validator{lr: opts.Loader}.problems()...); err != nil {
		return failure(stderr, opts.Program, "plugins check", err)
	}
	if opts.Loader != nil {
		writef(stdout, "plugins: ok (%d compiled)\n", len(loader.Plugins(opts.Loader)))
		return exitOK
	}
	writef(stdout, "plugins: ok\n")
	return exitOK
}

func pluginsDescribe(opts Options, reg *instances.Registry, defs *configuration.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("plugins describe", stderr, func() {
		writef(stderr, "usage: %s plugins describe KIND/TYPE\n", opts.Program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if fs.NArg() != 1 {
		return usageError(stderr, opts.Program, "plugins describe", "exactly one KIND/TYPE argument is required")
	}
	name := fs.Arg(0)
	kind, typ, ok := strings.Cut(name, "/")
	if !ok || kind == "" || typ == "" || strings.Contains(typ, "/") {
		return usageError(stderr, opts.Program, "plugins describe",
			fmt.Sprintf("%q must be KIND/TYPE, e.g. source/postgres (known: %s)", name, knownTypes(opts, reg)))
	}
	for _, d := range descriptors(opts, reg) {
		if d.Kind == kind && d.Type == typ {
			return writeDescriptor(stdout, stderr, opts.Program, defs, d)
		}
	}
	return usageError(stderr, opts.Program, "plugins describe",
		fmt.Sprintf("unknown plugin %q (known: %s)", name, knownTypes(opts, reg)))
}

func writeDescriptor(stdout, stderr io.Writer, program string, defs *configuration.Defaults, d instances.Descriptor) int {
	out := &errWriter{w: stdout}
	out.printf("%s (%s)\n%s\n", d.ID, d.Stability, d.Summary)
	if len(d.Binaries) > 0 {
		out.printf("binaries: %s\n", strings.Join(d.Binaries, ", "))
	}
	out.printf("\nfields:\n")
	writeFields(out, d.Fields, defs.PluginBlock(d.Kind, d.Type), 1)
	if len(d.Example) > 0 {
		pretty := jsontext.Value(d.Example).Clone()
		if err := pretty.Indent(jsontext.WithIndent("  ")); err != nil {
			return failure(stderr, program, "plugins describe", fmt.Errorf("%s: example: %w", d.ID, err))
		}
		out.printf("\nexample (a complete instance config, never applied to anything):\n%s\n", pretty)
	}
	if out.err != nil {
		return failure(stderr, program, "plugins describe", out.err)
	}
	return exitOK
}

// writeFields prints one config type's fields, nested structs indented under
// their parents. A field carries its classification: `[required]` when the deployment
// states it, `[default: …]` when the defaults document does — a struct field
// carries none of its own, its children are the ones classified.
func writeFields(out *errWriter, fields []instances.Field, block jsontext.Value, depth int) {
	indent := strings.Repeat("  ", depth)
	obj, _ := configuration.ObjectOf(block)
	for _, f := range fields {
		marks := make([]string, 0, 2)
		if f.Secret {
			marks = append(marks, "[secret]")
		}
		val, has := obj[f.Name]
		switch {
		case has && len(f.Fields) > 0:
			// The struct itself may be omitted; its children say what it holds.
		case has:
			marks = append(marks, "[default: "+string(val)+"]")
		default:
			// A field the document does not fill is one the deployment must
			// state: the startup check accepted a document that classifies every
			// field as required or defaulted.
			marks = append(marks, "[required]")
		}
		out.printf("%s%s %s %s %s\n", indent, f.Name, f.Type, strings.Join(marks, " "), f.Doc)
		writeFields(out, f.Fields, val, depth+1)
	}
}

func knownTypes(opts Options, reg *instances.Registry) string {
	var names []string
	for _, d := range descriptors(opts, reg) {
		names = append(names, d.ID)
	}
	return strings.Join(names, ", ")
}

// validator is the one offline validation of a build: the compiled plugin graph
// and the selection applied to it. Every diagnostic command reads it through
// this type, so the floor rule, the Requires edges, the dependency cycle and
// every kind's own Validate run once per command, never once per command path.
type validator struct {
	// lr is the compiled registry this process runs under. A build whose
	// composition root has not threaded it has no graph to judge: the factory
	// registry still answers the documentation and construction questions.
	lr *loader.Registry
}

// problems returns every intrinsic problem of the compiled and selected
// registry, each as its own error so a command prints them all in one pass:
// the selection it refused, a floor plugin leaning on an optional one, a broken
// or cyclic dependency, and a kind whose Validate rejects its own view.
func (v validator) problems() []error {
	if v.lr == nil {
		return nil
	}
	out := make([]error, 0, 4)
	for _, p := range loader.Health(v.lr) {
		out = append(out, errors.New(p))
	}
	return out
}

// descriptors is the descriptor view every documentation command reads: the
// loader's compiled schema when the composition root threaded it — so a plugin
// the selection disables keeps its schema, its fields and its example — and the
// factory registry otherwise.
func descriptors(opts Options, reg *instances.Registry) []instances.Descriptor {
	if opts.Loader != nil {
		return loader.All(opts.Loader, instances.Descriptors)
	}
	return reg.Descriptors()
}

// configDiagnostics aggregates the configuration's own offline diagnostics:
// the effective selection resolved by the one plugin-owned resolver against the
// loader's compiled set and bundle table, every present section's role, and
// every nested discovery rule that names a source the selection disabled. They
// are returned together with the caller's decode and reference errors, never
// instead of them, so one `validate` reports the whole file. A missing loader
// registry is a failure, never a fallback to "everything active".
func configDiagnostics(opts Options, reg *instances.Registry, cfg *configuration.Config) []error {
	active, errs := configuration.Resolve(cfg, opts.Loader)
	errs = append(errs, cfg.CheckRoles(active, configuration.RolesFrom(opts.Loader))...)
	errs = append(errs, configuration.CheckRules(reg, active, cfg)...)
	return errs
}
