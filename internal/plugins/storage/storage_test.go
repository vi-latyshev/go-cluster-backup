package storage_test

// The storage factory's own contract: Build is the one construction path of a
// registered storage type and hands the factory its Deps and typed construction
// input; construction through the framework's generic path is refused, because
// it carries no input.

import (
	"context"
	"encoding/json/jsontext"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// factoryConfig is the plugin config of the sample storage type below.
type factoryConfig struct {
	PageSize int `json:"page_size"`
}

// sampleConstruction is what the sample storage's factory last received.
type sampleConstruction struct {
	in   storage.BuildInput
	deps instances.Deps
}

// registerSampleStorage registers one sample storage type in a factory registry
// and answers the Deps and input its factory last received.
func registerSampleStorage(t *testing.T, r *instances.Registry, seen *sampleConstruction) {
	t.Helper()
	err := storage.Register(r, storage.FactorySpec[factoryConfig]{
		Type:    "sample",
		Summary: "a sample storage",
		New: func(ctx context.Context, cfg factoryConfig, d instances.Deps, in storage.BuildInput) (storage.Storage, error) {
			*seen = sampleConstruction{in: in, deps: d}
			return storagetest.NewFake(), nil
		},
	})
	if err != nil {
		t.Fatalf("registering the sample storage: %v", err)
	}
}

func TestBuildHandsTheFactoryItsConstructionInput(t *testing.T) {
	var seen sampleConstruction
	r := instances.NewRegistry()
	registerSampleStorage(t, r, &seen)

	s, err := storage.Build(context.Background(), r, "sample", jsontext.Value(`{"page_size": 3}`),
		storage.BuildInput{CallParallelism: 2}, instances.Deps{Instance: "primary"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if s == nil {
		t.Fatal("Build returned no storage")
	}
	if seen.in.CallParallelism != 2 {
		t.Fatalf("the factory saw call_parallelism %d, want 2", seen.in.CallParallelism)
	}
	if seen.deps.Instance != "primary" {
		t.Fatalf("the factory saw instance %q, want primary", seen.deps.Instance)
	}
}

func TestBuildRefusesAZeroCallParallelism(t *testing.T) {
	var seen sampleConstruction
	r := instances.NewRegistry()
	registerSampleStorage(t, r, &seen)

	_, err := storage.Build(context.Background(), r, "sample", jsontext.Value(`{"page_size": 1}`),
		storage.BuildInput{}, instances.Deps{Instance: "primary"})
	if err == nil || !strings.Contains(err.Error(), "call_parallelism must be at least 1, got 0") {
		t.Fatalf("Build with no call_parallelism = %v, want the input refused", err)
	}
}

func TestBuildRefusesAFactoryWithNoConstructionInput(t *testing.T) {
	var seen sampleConstruction
	r := instances.NewRegistry()
	registerSampleStorage(t, r, &seen)

	_, err := instances.Build(context.Background(), r, storage.Kind, "sample", jsontext.Value(`{"page_size": 1}`),
		instances.Deps{Instance: "primary"})
	if err == nil || !strings.Contains(err.Error(), "construct it through storage.Build") {
		t.Fatalf("the framework's generic construction = %v, want it refused", err)
	}
}

func TestRegisterRefusesADuplicateTypeWithoutReplacingTheFactory(t *testing.T) {
	var seen sampleConstruction
	r := instances.NewRegistry()
	registerSampleStorage(t, r, &seen)

	err := storage.Register(r, storage.FactorySpec[factoryConfig]{
		Type: "sample",
		New: func(context.Context, factoryConfig, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return nil, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("second Register = %v, want the duplicate refused", err)
	}
	if _, err := storage.Build(context.Background(), r, "sample", jsontext.Value(`{"page_size": 1}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "primary"}); err != nil {
		t.Fatalf("Build after a refused duplicate: %v", err)
	}
}

func TestAddFactoryRefusesADuplicateType(t *testing.T) {
	var seen sampleConstruction
	lr := loader.New()
	r := instances.NewRegistry()
	add := func() error {
		return loader.Add(lr, loader.Spec{ID: "storage/sample", Summary: "a sample storage"},
			func(lr *loader.Registry) error {
				return storage.AddFactory(lr, r, storage.FactorySpec[factoryConfig]{
					Type: "sample",
					New: func(context.Context, factoryConfig, instances.Deps, storage.BuildInput) (storage.Storage, error) {
						return storagetest.NewFake(), nil
					},
				})
			})
	}
	registerSampleStorage(t, r, &seen)
	if err := add(); err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("duplicate AddFactory = %v, want the duplicate refused", err)
	}
}

func TestBuildReportsADecodeErrorBeforeTheFactory(t *testing.T) {
	var seen sampleConstruction
	r := instances.NewRegistry()
	registerSampleStorage(t, r, &seen)

	_, err := storage.Build(context.Background(), r, "sample", jsontext.Value(`{"page_size": "three"}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "primary"})
	if err == nil || !strings.Contains(err.Error(), `storage "primary"`) {
		t.Fatalf("Build with a malformed config = %v, want the decode error", err)
	}
}

func TestBuildReportsAnUnknownType(t *testing.T) {
	r := instances.NewRegistry()

	_, err := storage.Build(context.Background(), r, "absent", jsontext.Value(`{}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "primary"})
	if err == nil || !strings.Contains(err.Error(), `unknown storage type "absent"`) {
		t.Fatalf("Build of an unknown type = %v, want the framework's refusal", err)
	}
}

func TestBuildRefusesATypeRegisteredWithoutAStorageFactory(t *testing.T) {
	r := instances.NewRegistry()
	err := instances.Register(r, storage.Kind, instances.FactorySpec[factoryConfig, storage.Storage]{
		Type: "generic",
		New: func(context.Context, factoryConfig, instances.Deps) (storage.Storage, error) {
			return storagetest.NewFake(), nil
		},
	})
	if err != nil {
		t.Fatalf("instances.Register: %v", err)
	}

	_, err = storage.Build(context.Background(), r, "generic", jsontext.Value(`{"page_size": 1}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "primary"})
	if err == nil || !strings.Contains(err.Error(), "has no storage factory") {
		t.Fatalf("Build of a type with no storage factory = %v, want it refused", err)
	}
}

func TestAddFactoryContributesTheStorageDescriptor(t *testing.T) {
	var seen sampleConstruction
	lr := loader.New()
	r := instances.NewRegistry()
	err := loader.Add(lr, loader.Spec{ID: "storage/sample", Summary: "a sample storage"},
		func(lr *loader.Registry) error {
			return storage.AddFactory(lr, r, storage.FactorySpec[factoryConfig]{
				Type:    "sample",
				Summary: "a sample storage",
				New: func(ctx context.Context, cfg factoryConfig, d instances.Deps, in storage.BuildInput) (storage.Storage, error) {
					seen = sampleConstruction{in: in, deps: d}
					return storagetest.NewFake(), nil
				},
			})
		})
	if err != nil {
		t.Fatalf("loader.Add: %v", err)
	}
	descs := loader.All(lr, instances.Descriptors)
	if len(descs) != 1 || descs[0].ID != "storage/sample" || descs[0].Kind != "storage" {
		t.Fatalf("Descriptors = %+v, want the storage factory's one", descs)
	}
	if _, err := storage.Build(context.Background(), r, "sample", jsontext.Value(`{"page_size": 1}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "primary"}); err != nil {
		t.Fatalf("Build after AddFactory: %v", err)
	}
	if seen.in.CallParallelism != 1 {
		t.Fatalf("the factory saw call_parallelism %d, want 1", seen.in.CallParallelism)
	}
}
