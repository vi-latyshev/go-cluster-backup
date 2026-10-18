// Command go-cluster-backup is the distribution: the whole command surface
// (internal/cli) over every plugin the registry carries. It names no cluster and
// no deployment — what a deployment backs up lives in its configuration file.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/vi-latyshev/go-cluster-backup/internal/cli"
	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's whole body: the one place a signal is turned into context
// cancellation — every command returns as soon as the engine does — and the one
// call into the command layer, which owns the exit code. It takes its arguments
// and its streams so the wiring is exercised without a process exiting.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return cli.Main(ctx, cli.Options{
		Program:  "go-cluster-backup",
		Register: cli.Generic,
		Defaults: [][]byte{defaults.Defaults},
	}, args, stdout, stderr)
}
