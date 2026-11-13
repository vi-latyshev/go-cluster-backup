package assembly_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assemblytest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/files"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/k3sstate"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/postgres"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sqlite"
)

// fetchOnlyConfig declares the restore-apply capability only to answer that it
// has none: the by-call form of a fetch-only type.
type fetchOnlyConfig struct{}

func (fetchOnlyConfig) CheckApplyOptions(jsontext.Value, string) error {
	return fmt.Errorf("this type is fetch-only: %w", errors.ErrUnsupported)
}

// registerFetchOnly registers the capability's by-call refusal beside the real
// source types, so one apply key reaches it.
func registerFetchOnly(lr *loader.Registry, reg *instances.Registry) error {
	if err := instances.Register(reg, source.Kind, instances.FactorySpec[fetchOnlyConfig, source.Source]{
		Type: "fetchonly", Example: []byte(`{}`),
		New: func(context.Context, fetchOnlyConfig, instances.Deps) (source.Source, error) {
			return &assemblytest.Source{}, nil
		},
	}); err != nil {
		return err
	}
	return loader.Add(lr, loader.Spec{ID: source.Kind.ID("fetchonly")}, func(*loader.Registry) error { return nil })
}

// restoreApplyWorld is the fake world plus the real source types whose
// restore.apply blocks one test checks: the fakes have no restorer, and the
// capability lives on the config value of a real type.
func restoreApplyWorld(t *testing.T) *assemblytest.World {
	t.Helper()
	w, err := assemblytest.NewWorld(func(lr *loader.Registry, reg *instances.Registry) error {
		if err := registerFetchOnly(lr, reg); err != nil {
			return err
		}
		return errors.Join(
			loader.Add(lr, keeplast.Spec, func(lr *loader.Registry) error { return keeplast.Apply(lr, reg) }),
			loader.Add(lr, files.Spec, func(lr *loader.Registry) error { return files.Apply(lr, reg) }),
			loader.Add(lr, postgres.Spec, func(lr *loader.Registry) error { return postgres.Apply(lr, reg) }),
			loader.Add(lr, sqlite.Spec, func(lr *loader.Registry) error { return sqlite.Apply(lr, reg) }),
			loader.Add(lr, k3sstate.Spec, func(lr *loader.Registry) error { return k3sstate.Apply(lr, reg) }),
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// restoreApplyConfig is a complete run-shaped config carrying one storage and
// the restore section with the given apply blocks.
func restoreApplyConfig(t *testing.T, apply map[string]any) map[string]any {
	t.Helper()
	cfg := assembly.BaseConfig(t, map[string]any{"primary": assembly.StorageInstance("fake", "keep_last")})
	cfg["restore"] = map[string]any{
		"spool_dir": t.TempDir(), "spool_reserve_bytes": 0,
		"storage_order":      []any{"primary"},
		"unclassified_error": "transient",
		"retry":              map[string]any{"attempts": 1, "initial_delay": "1ms", "max_delay": "1ms", "jitter": 0},
		"apply":              apply,
	}
	return cfg
}

// newRestoreApplyAssembly builds one assembly and hands back its error.
func newRestoreApplyAssembly(t *testing.T, w *assemblytest.World, cfg map[string]any) error {
	t.Helper()
	w.SyncLoader()
	raw, err := assembly.JSONMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := assembly.New(context.Background(), raw, nil,
		assembly.Deps{Factories: w.Registry, Loader: w.Loader, Log: assembly.HarnessLogger()})
	if e != nil {
		t.Cleanup(func() { _ = e.Close(context.Background()) })
	}
	return err
}

// TestRestoreApplyIsCheckedOffline: every restore.apply block is decoded by its
// source type's own config at construction — a key naming no source type, a
// type with no restorer, and a block its decoder or Validate refuses are all
// reported under /restore/apply/<type>, before a restore has anything left to
// do about them.
func TestRestoreApplyIsCheckedOffline(t *testing.T) {
	validPostgres := map[string]any{
		"jobs": 1, "no_owner": false, "no_acl": false,
		"connect_attempts": 1, "connect_interval": "20ms",
	}
	cases := map[string]struct {
		apply map[string]any
		want  []string
	}{
		"every reference block passes": {
			apply: map[string]any{
				"files":    map[string]any{"preserve_owner": false, "side_by_side_dir": "{{ .Label }}"},
				"postgres": validPostgres,
				"sqlite":   map[string]any{"file_mode": "0640", "keep_previous": false},
			},
		},
		"an unknown source type": {
			apply: map[string]any{"minio": map[string]any{}},
			want:  []string{"/restore/apply/minio", `no source type "minio"`},
		},
		"a source type with no restore": {
			apply: map[string]any{"k3s_state": map[string]any{}},
			want:  []string{"/restore/apply/k3s_state", "has no restore; remove the block"},
		},
		"a type that answers no restore capability by call": {
			apply: map[string]any{"fetchonly": map[string]any{}},
			want:  []string{"/restore/apply/fetchonly", "has no restore; remove the block"},
		},
		"a mistyped postgres member": {
			apply: map[string]any{"postgres": map[string]any{
				"jbos": 1, "no_owner": false, "no_acl": false,
				"connect_attempts": 1, "connect_interval": "20ms",
			}},
			want: []string{"/restore/apply/postgres", "jbos"},
		},
		"a postgres member Validate refuses": {
			apply: map[string]any{"postgres": map[string]any{
				"jobs": 0, "no_owner": false, "no_acl": false,
				"connect_attempts": 1, "connect_interval": "20ms",
			}},
			want: []string{"/restore/apply/postgres", "jobs"},
		},
		"a files label the template misspells": {
			apply: map[string]any{"files": map[string]any{"preserve_owner": false, "side_by_side_dir": "{{ .Lable }}"}},
			want:  []string{"/restore/apply/files", "Lable"},
		},
		// The directory's confinement is the side-by-side mode's own rule:
		// validation has not chosen a mode, so a value an in-place restore
		// never reads is not refused here. Apply refuses it in side_by_side.
		"a files side_by_side_dir the side-by-side mode refuses stays out of the offline check": {
			apply: map[string]any{"files": map[string]any{"preserve_owner": false, "side_by_side_dir": "../x"}},
		},
		"a files side_by_side_dir that renders empty": {
			apply: map[string]any{"files": map[string]any{"preserve_owner": false, "side_by_side_dir": `{{ "" }}`}},
			want:  []string{"/restore/apply/files", "renders empty"},
		},
		"a sqlite file_mode that is not octal": {
			apply: map[string]any{"sqlite": map[string]any{"file_mode": "9", "keep_previous": false}},
			want:  []string{"/restore/apply/sqlite", "file_mode"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := restoreApplyWorld(t)
			err := newRestoreApplyAssembly(t, w, restoreApplyConfig(t, tc.apply))
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("a usable restore.apply block was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("the restore.apply block passed validation")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, err)
				}
			}
		})
	}
}

// TestRestoreApplyChecksADisabledSourceType: the block belongs to a type, not an
// instance, so turning the type off must not turn its block into an unknown one:
// a restoring type stays checked while disabled, and a broken block is still
// refused.
func TestRestoreApplyChecksADisabledSourceType(t *testing.T) {
	validPostgres := map[string]any{
		"jobs": 1, "no_owner": false, "no_acl": false,
		"connect_attempts": 1, "connect_interval": "20ms",
	}
	cfg := restoreApplyConfig(t, map[string]any{"postgres": validPostgres})
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{"source/postgres"}, "config": map[string]any{}}
	w := restoreApplyWorld(t)
	if err := newRestoreApplyAssembly(t, w, cfg); err != nil {
		t.Fatalf("a disabled type's usable block was refused: %v", err)
	}

	broken := restoreApplyConfig(t, map[string]any{"postgres": map[string]any{
		"jobs": 0, "no_owner": false, "no_acl": false,
		"connect_attempts": 1, "connect_interval": "20ms",
	}})
	broken["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{"source/postgres"}, "config": map[string]any{}}
	w = restoreApplyWorld(t)
	err := newRestoreApplyAssembly(t, w, broken)
	if err == nil || !strings.Contains(err.Error(), "/restore/apply/postgres") {
		t.Fatalf("New = %v, want the disabled type's broken block refused", err)
	}
}
