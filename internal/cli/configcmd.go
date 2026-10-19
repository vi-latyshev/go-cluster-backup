package cli

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// configCommand answers the two questions a configuration raises before it ever
// runs: what the service's own defaults are, and what one file becomes once
// they are merged under it.
func configCommand(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return usageError(stderr, opts.Program, "config", "a subcommand is required: defaults or effective")
	}
	switch args[0] {
	case "defaults":
		return configDefaults(opts, args[1:], stdout, stderr)
	case "effective":
		return configEffective(ctx, opts, reg, defs, args[1:], stdout, stderr)
	default:
		return usageError(stderr, opts.Program, "config", fmt.Sprintf("unknown subcommand %q (known: defaults, effective)", args[0]))
	}
}

// configDefaults prints the distribution's defaults documents exactly as they
// are embedded — comments included — as one YAML stream separated by `---`.
// Nothing is merged and nothing is decoded: this is the service's own half of
// every configuration, and a deployment reads it to know what it may omit.
func configDefaults(opts Options, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("config defaults", stderr, func() {
		writef(stderr, "usage: %s config defaults\n", opts.Program)
	})
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(stderr, opts.Program, "config defaults", "it takes no arguments")
	}
	out := &errWriter{w: stdout}
	for i, doc := range opts.Defaults {
		if i > 0 {
			out.printf("---\n")
		}
		out.printf("%s", doc)
		if len(doc) > 0 && doc[len(doc)-1] != '\n' {
			out.printf("\n")
		}
	}
	if out.err != nil {
		return failure(stderr, opts.Program, "config defaults", out.err)
	}
	return exitOK
}

// configEffective prints the file as the engine reads it: the distribution's
// defaults merged under it, rule sources included. In YAML every value that
// came from a default carries a `# default` comment; `--output json` prints the
// merged configuration with the list of the JSON pointers that were defaulted,
// which is what a machine compares.
//
// Keys come out sorted: the merge re-encodes the document, and a sorted
// document is the one a diff between two versions of a file can be read in.
func configEffective(ctx context.Context, opts Options, reg *plugin.Registry, defs *engine.Defaults, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("config effective", stderr, func() {
		writef(stderr, "usage: %s config effective --config FILE [--output yaml|json] [--log-level LEVEL]\n", opts.Program)
	})
	cfgPath := fs.String("config", "", "path of the YAML configuration file (required)")
	output := fs.String("output", "yaml", "yaml marks every defaulted value with a # default comment; json prints the merged file with the list of defaulted pointers")
	level := fs.String("log-level", "info", "diagnostic log level: debug, info, warn or error")
	if code, done := parseFlags(fs, args); done {
		return code
	}
	if *cfgPath == "" {
		return usageError(stderr, opts.Program, "config effective", "--config is required")
	}
	if *output != "yaml" && *output != "json" {
		return usageError(stderr, opts.Program, "config effective", fmt.Sprintf("--output %q must be yaml or json", *output))
	}
	log, err := loggerFor(stderr, *level)
	if err != nil {
		return usageError(stderr, opts.Program, "config effective", err.Error())
	}
	e, err := openEngine(ctx, reg, defs, *cfgPath, log)
	if err != nil {
		return failure(stderr, opts.Program, "config effective", err)
	}
	defer closeEngine(e, log)
	merged, pointers := e.EffectiveConfig()
	out := &errWriter{w: stdout}
	if *output == "json" {
		doc := struct {
			Config    jsontext.Value `json:"config"`
			Defaulted []string       `json:"defaulted"`
		}{Config: merged, Defaulted: pointers}
		if pointers == nil {
			doc.Defaulted = []string{}
		}
		// The document is the engine's own re-encoding plus a list of strings:
		// there is no value in it JSON cannot carry.
		out.printf("%s\n", must.Do(json.Marshal(doc, jsontext.WithIndent("  "))))
	} else {
		out.printf("%s", must.Do(markedYAML(merged, pointers)))
	}
	if out.err != nil {
		return failure(stderr, opts.Program, "config effective", out.err)
	}
	return exitOK
}

// markedYAML renders a merged configuration as YAML with a `# default` comment
// on every value that came from a defaults document.
func markedYAML(doc jsontext.Value, pointers []string) ([]byte, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(doc, &root); err != nil {
		return nil, fmt.Errorf("rendering the merged configuration: %w", err)
	}
	// JSON is a flow-style YAML document, and the parser keeps the style: drop
	// it so the merged file prints the way a configuration file is written and
	// read — one key per line.
	blockStyle(&root)
	for _, pointer := range pointers {
		markDefault(&root, strings.Split(strings.TrimPrefix(pointer, "/"), "/"))
	}
	return yaml.Marshal(&root)
}

// blockStyle clears every node's style, leaving the emitter to decide quoting,
// with one exception: a string that renders over several lines — a message
// template, a key template — asks for a literal block, so that a reader of the
// file reads the text instead of a line of escapes. It is a preference, not a
// guarantee: the emitter keeps its own rules and quotes a value it will not
// write as a block, a line beginning with a space after a line break being one
// such case.
func blockStyle(node *yaml.Node) {
	node.Style = 0
	if node.Kind == yaml.ScalarNode && strings.Contains(node.Value, "\n") {
		node.Style = yaml.LiteralStyle
	}
	for _, child := range node.Content {
		blockStyle(child)
	}
}

// markDefault walks one JSON pointer through a YAML node tree and marks the
// node it names. A pointer that names nothing is skipped: both sides come from
// the same merge, so a miss would take a re-encoding the engine does not do,
// and a missing comment is not worth failing a reader's command.
//
// The marker is a line comment on the node the pointer names, which is the only
// place it can be read from the file afterwards — the pointer list, never a
// line's position, is what says which value is a default, so a value that
// renders over several lines cannot shift the mark onto its neighbour. One kind
// has no line to carry a comment: a sequence in block style, whose mark the
// emitter then drops or moves to whatever line comes next. A marked sequence is
// therefore written in flow style, which is how a short list of a configuration
// reads anyway.
func markDefault(node *yaml.Node, path []string) {
	cur := node
	for _, seg := range path {
		if seg == "" {
			continue
		}
		if cur.Kind == yaml.DocumentNode {
			if len(cur.Content) == 0 {
				return
			}
			cur = cur.Content[0]
		}
		var next *yaml.Node
		switch cur.Kind {
		case yaml.MappingNode:
			for i := 0; i+1 < len(cur.Content); i += 2 {
				if cur.Content[i].Value == seg {
					next = cur.Content[i+1]
					break
				}
			}
		case yaml.SequenceNode:
			if i, err := strconv.Atoi(seg); err == nil && i >= 0 && i < len(cur.Content) {
				next = cur.Content[i]
			}
		}
		if next == nil {
			return
		}
		cur = next
	}
	if cur.Kind == yaml.SequenceNode {
		cur.Style = yaml.FlowStyle
	}
	cur.LineComment = "default"
}
