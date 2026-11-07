package storagetest

// The kit's own construction contract: CheckSpec runs the general plugin checks
// on a storage registry and builds every example through storage.Build, since
// the framework's generic construction carries no BuildInput.

import (
	"context"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// checkSpecConfig is the plugin config of the type CheckSpec's test registers:
// every field documented and nothing required.
type checkSpecConfig struct {
	PageSize int `json:"page_size" doc:"objects per listing page"`
}

func TestCheckSpecBuildsEachStorageExampleThroughBuild(t *testing.T) {
	var seenDeps instances.Deps
	var seenInput storage.BuildInput
	r := instances.NewRegistry()
	err := storage.Register(r, storage.FactorySpec[checkSpecConfig]{
		Type:    "checked",
		Summary: "a checked storage",
		Example: []byte(`{"page_size": 2}`),
		New: func(ctx context.Context, cfg checkSpecConfig, d instances.Deps, in storage.BuildInput) (storage.Storage, error) {
			seenDeps, seenInput = d, in
			return NewFake(), nil
		},
	})
	if err != nil {
		t.Fatalf("registering the checked storage: %v", err)
	}

	CheckSpec(t, r)

	if seenInput.CallParallelism != 1 {
		t.Fatalf("CheckSpec built with call_parallelism %d, want 1", seenInput.CallParallelism)
	}
	if seenDeps.Instance != "checked" {
		t.Fatalf("CheckSpec built instance %q, want checked", seenDeps.Instance)
	}
}

// TestRegisterFakeBuildsTheInstanceThroughBuild: the fake type the shared world
// kits register is a real storage factory: storage.Build constructs it and hands
// the instance name to the callback.
func TestRegisterFakeBuildsTheInstanceThroughBuild(t *testing.T) {
	var seen string
	r := instances.NewRegistry()
	if err := RegisterFake(r, func(name string) storage.Storage {
		seen = name
		return NewFake()
	}); err != nil {
		t.Fatalf("registering the fake storage: %v", err)
	}
	s, err := storage.Build(context.Background(), r, "fake", jsontext.Value(`{}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "alpha"})
	if err != nil {
		t.Fatalf("building the fake storage: %v", err)
	}
	if seen != "alpha" {
		t.Fatalf("the factory saw instance %q, want alpha", seen)
	}
	if _, ok := s.(*Fake); !ok {
		t.Fatalf("built %T, want *Fake", s)
	}
}
