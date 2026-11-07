// Command go-cluster-backup is the distribution: the generated composition root
// (internal/plugins/all) over every plugin it carries, the CLI host included. It
// names no cluster and no deployment — what a deployment backs up lives in its
// configuration file.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// registerAll is the composition root's one registration call. It is a seam
// only for the test proving a refused registration reaches the loader; the
// binary never replaces it.
var registerAll = all.Register

// run is main's whole body: the one place a signal is turned into context
// cancellation, and the one call into the loader, which picks the process role
// and owns the exit code. It takes its arguments and its streams so the wiring
// is exercised without a process exiting.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return loader.Main(ctx, loader.Options{
		Register: registerAll,
		Args:     args,
		Stdout:   stdout,
		Stderr:   stderr,
	})
}
