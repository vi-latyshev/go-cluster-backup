// Command gen writes the compiled distribution's composition root and Floor
// manifest from the plugin packages themselves: every package under
// internal/plugins that declares the loader Spec literal is a plugin, and the
// import graph is the production import graph between those packages.
//
// The output is tracked, so a build needs no generator run; `-check` proves the
// tracked files still match their sources and runs in `make lint`.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Import-path suffixes and file names the generator owns. The composition root
// is not a plugin: it is the generated package that names every plugin, and its
// hostless half is the same set without the process hosts.
const (
	pluginsPath    = "internal/plugins"
	allPath        = "internal/plugins/all"
	componentsPath = "internal/plugins/all/components"
	generatedName  = "zz_generated.go"
	floorName      = "floor.json"
)

// hostDirs are the process hosts: plugins that only dispatch roles. The
// generator is repository tooling, so it knows their directories; the loader
// never does.
var hostDirs = map[string]bool{"internal/plugins/cli": true}

// isHost reports whether one plugin is a process host.
func isHost(p *plugin) bool { return hostDirs[p.dir] }

// osExit is the one unprovokable exit seam: a test overrides it to observe the
// exit code without ending the test process.
var osExit = os.Exit

func main() {
	osExit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's whole body: flag parsing, generation and the exit code, with
// its streams passed in so a test exercises the wiring without a process exit.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("gen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "module root holding go.mod and internal/plugins")
	check := flags.Bool("check", false, "verify the tracked outputs instead of writing them")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "gen: unexpected arguments: %v\n", flags.Args())
		return 2
	}
	if err := generate(*root, *check); err != nil {
		_, _ = fmt.Fprintf(stderr, "gen: %v\n", err)
		return 1
	}
	if *check {
		_, _ = fmt.Fprintln(stdout, "gen: generated files are current")
	} else {
		_, _ = fmt.Fprintln(stdout, "gen: generated files written")
	}
	return 0
}

// generate renders every output and either writes it or, in check mode, proves
// the tracked file already holds exactly these bytes.
func generate(root string, check bool) error {
	outputs, err := render(root)
	if err != nil {
		return err
	}
	for _, out := range outputs {
		if check {
			if err := checkCurrent(out); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(out.path), 0o755); err != nil {
			return fmt.Errorf("create %s: %w", filepath.Dir(out.path), err)
		}
		if err := os.WriteFile(out.path, out.data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", out.path, err)
		}
	}
	return nil
}

// checkCurrent reports a missing or differing generated file. A read error
// other than absence is not staleness and is returned as it is.
func checkCurrent(out output) error {
	current, err := os.ReadFile(out.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%s is missing: run 'make generate'", out.path)
	case err != nil:
		return fmt.Errorf("read %s: %w", out.path, err)
	case !bytes.Equal(current, out.data):
		return fmt.Errorf("%s is stale: run 'make generate'", out.path)
	}
	return nil
}
