package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// pluginsCommand lists every registered plugin type, or documents the one named
// by "plugins describe KIND/TYPE": its fields, each marked required or with the
// default a defaults document gives it, and its complete example instance
// config. The fields and the example come from the registry's descriptors, the
// defaults from the distribution's documents, so a new plugin documents itself.
func pluginsCommand(program string, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "describe" {
		return pluginsDescribe(program, reg, defs, args[1:], stdout, stderr)
	}
	fs := newFlagSet("plugins", stderr, func() {
		writef(stderr, "usage: %s plugins [describe KIND/TYPE]\n", program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	for _, d := range reg.Descriptors() {
		requires := ""
		if len(d.Requires) > 0 {
			requires = "requires: " + strings.Join(d.Requires, ", ")
		}
		// The tabwriter remembers its first write failure and Flush reports it.
		_, _ = fmt.Fprintf(tw, "%s/%s\t%s\t%s\t%s\n", d.Kind, d.Type, d.Stability, requires, d.Summary)
	}
	if err := tw.Flush(); err != nil {
		return failure(stderr, program, "plugins", err)
	}
	return exitOK
}

func pluginsDescribe(program string, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("plugins describe", stderr, func() {
		writef(stderr, "usage: %s plugins describe KIND/TYPE\n", program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if fs.NArg() != 1 {
		return usageError(stderr, program, "plugins describe", "exactly one KIND/TYPE argument is required")
	}
	name := fs.Arg(0)
	kind, typ, ok := strings.Cut(name, "/")
	if !ok || kind == "" || typ == "" || strings.Contains(typ, "/") {
		return usageError(stderr, program, "plugins describe",
			fmt.Sprintf("%q must be KIND/TYPE, e.g. source/postgres (known: %s)", name, knownTypes(reg)))
	}
	for _, d := range reg.Descriptors() {
		if d.Kind == kind && d.Type == typ {
			return writeDescriptor(stdout, stderr, program, defs, d)
		}
	}
	return usageError(stderr, program, "plugins describe",
		fmt.Sprintf("unknown plugin %q (known: %s)", name, knownTypes(reg)))
}

func writeDescriptor(stdout, stderr io.Writer, program string, defs *engine.Defaults, d plugin.Descriptor) int {
	out := &errWriter{w: stdout}
	out.printf("%s/%s (%s)\n%s\n", d.Kind, d.Type, d.Stability, d.Summary)
	if len(d.Requires) > 0 {
		out.printf("requires: %s\n", strings.Join(d.Requires, ", "))
	}
	out.printf("\nfields:\n")
	writeFields(out, d.Fields, defs.PluginBlock(d.Kind, d.Type), 1)
	if len(d.Example) > 0 {
		pretty := jsontext.Value(d.Example).Clone()
		if err := pretty.Indent(jsontext.WithIndent("  ")); err != nil {
			return failure(stderr, program, "plugins describe", fmt.Errorf("%s/%s: example: %w", d.Kind, d.Type, err))
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
func writeFields(out *errWriter, fields []plugin.Field, block jsontext.Value, depth int) {
	indent := strings.Repeat("  ", depth)
	obj, _ := plugin.ObjectOf(block)
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

func knownTypes(reg *plugin.Registry) string {
	var names []string
	for _, d := range reg.Descriptors() {
		names = append(names, d.Kind+"/"+d.Type)
	}
	return strings.Join(names, ", ")
}
