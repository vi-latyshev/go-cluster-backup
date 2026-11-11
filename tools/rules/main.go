// Command rules enforces the plugin runtime's structural invariants that the
// compiler cannot see: the generated dependency graph matches the production
// imports, every runtime Requires edge is valid, a factory id is derived rather
// than spelled, the loader names no feature, a role never shadows a command, the
// raw storage boundary stays behind its owner and guard, the mutation boundary
// has no public bypass, no package initializer does eager work, and every plugin
// package declares its loader.Spec literal in plugin.go.
//
// The generated graph and Floor manifest are already compared byte for byte by
// tools/gen; this command checks the semantics behind them, so a diagnostic
// names the package and the edge instead of only "the file is stale".
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

// osExit is the one unprovokable exit seam: a test overrides it to observe the
// exit code without ending the test process.
var osExit = os.Exit

func main() {
	osExit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's whole body: flag parsing, the checks and the exit code, with its
// streams passed in so a test exercises the wiring without a process exit.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rules", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "module root holding go.mod and internal/plugins")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "rules: unexpected arguments: %v\n", flags.Args())
		return 2
	}
	findings, err := check(*root)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "rules: %v\n", err)
		return 1
	}
	if len(findings) > 0 {
		for _, finding := range findings {
			_, _ = fmt.Fprintln(stderr, finding)
		}
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "rules: all checks passed")
	return 0
}

// check runs every rule over the module at root and returns the sorted union of
// their findings. An error means the tree could not be read well enough to judge
// it; findings mean it was read and at least one invariant is broken.
func check(root string) ([]string, error) {
	scanned, err := scan(root)
	if err != nil {
		return nil, err
	}
	graph, err := scanned.generated()
	if err != nil {
		return nil, err
	}
	var findings []string
	for _, rule := range []func(*repo, *generated) []string{
		ruleImportCoverage,
		ruleRequires,
		ruleFactoryIDs,
		ruleLoaderPurity,
		ruleRoleCommands,
		ruleRawStorage,
		ruleRestrictedSymbols,
		ruleLeakedMutations,
		ruleEagerInit,
		rulePluginFiles,
	} {
		findings = append(findings, rule(scanned, graph)...)
	}
	if len(scanned.modelErrors) > 0 {
		return nil, fmt.Errorf("the type model is incomplete:\n%s",
			strings.Join(uniqueSorted(scanned.modelErrors), "\n"))
	}
	return uniqueSorted(findings), nil
}

// uniqueSorted drops duplicate messages and sorts what remains, so several files
// that share one cause report it once and two runs diff cleanly. It orders the
// caller's slice in place, as slices.Sort does.
func uniqueSorted(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}
