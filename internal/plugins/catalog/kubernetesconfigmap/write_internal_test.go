package kubernetesconfigmap

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

const (
	internalHostEnv   = "GCB_INTERNAL_HOST"
	internalPortEnv   = "GCB_INTERNAL_PORT"
	internalNamespace = "infrastructure"
	internalName      = "cluster-backup-catalog"
	internalKey       = "catalog.json"
)

// newInternalCatalog builds the catalog the unexported write path needs, pointed
// at a fake API server.
func newInternalCatalog(t *testing.T, srv *kubeapitest.Server) *Catalog {
	t.Helper()
	opts := srv.Connection(t, internalHostEnv, internalPortEnv)
	return &Catalog{
		cfg: Config{
			APIHostEnv: internalHostEnv, APIPortEnv: internalPortEnv,
			TokenFile: opts.TokenFile, CAFile: opts.CAFile,
			RequestTimeout: model.Duration(10 * time.Second),
			Namespace:      internalNamespace, Name: internalName, Key: internalKey,
			LeaseKeySuffix: ".lease", MaxRuns: 10, MaxHistoryBytes: 512 << 10,
		},
		log: slog.New(slog.DiscardHandler),
	}
}

// TestWriteStoresAnEmptyHistoryForNoRuns: a nil history is stored as an empty
// JSON array, never as the null a nil slice would render — the stored key is a
// run history at every moment another process may read it.
func TestWriteStoresAnEmptyHistoryForNoRuns(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(internalNamespace, internalName, map[string]string{})
	c := newInternalCatalog(t, srv)

	if err := c.write(context.Background(), nil, "1"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := srv.ConfigMapData(internalNamespace, internalName)[internalKey]; got != "[]" {
		t.Fatalf("stored history = %q, want an empty JSON array", got)
	}
}

// TestWriteSurfacesAClientConstructionFailure: a client that cannot be built is
// reported, so the write is never mistaken for a stored history.
func TestWriteSurfacesAClientConstructionFailure(t *testing.T) {
	const unsetHostEnv = "GCB_INTERNAL_UNSET_HOST"
	t.Setenv(unsetHostEnv, "")
	c := &Catalog{
		cfg: Config{
			APIHostEnv: unsetHostEnv, APIPortEnv: "GCB_INTERNAL_PORT",
			TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
			RequestTimeout: model.Duration(time.Second),
			Namespace:      internalNamespace, Name: internalName, Key: internalKey,
			LeaseKeySuffix: ".lease", MaxRuns: 10, MaxHistoryBytes: 512 << 10,
		},
		log: slog.New(slog.DiscardHandler),
	}
	err := c.write(context.Background(), []model.RunReport{{RunID: "r1"}}, "1")
	if err == nil {
		t.Fatal("a client that cannot be built was not reported")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}
