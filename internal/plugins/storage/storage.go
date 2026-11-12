// Package storage is the destination seam. Every method is ONE attempt: the
// plugin never retries on its own, execution owns retries, backoff and budgets.
// The interface obeys the seam rules documented in package instances.
//
// The package owns two shapes beyond the kind interface. Reader is the read-only
// port every restore path depends on: it has no method that could mutate a
// destination. Register, AddFactory and Build are the raw storage factory: the
// only construction path of a storage instance, carrying the storage-owned
// BuildInput, and closed by rule to this subtree (internal/plugins/storage/**).
// Every other consumer constructs through guard.Build and receives a Port or a
// guarded handle, never a mutable instance.
package storage

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"sync"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// StorageTraits are facts about a destination, not behaviour: what it exposes.
// Only declarations something actually reads live here — a field nobody reads
// is a field every plugin must nevertheless think about.
type StorageTraits struct {
	// Checksums lists what Stat reports in ObjectInfo: "sha256" and/or "md5".
	// The conformance suite holds a storage to exactly what it declares here.
	Checksums []string
}

// Purpose says what one object is for: an artifact's bytes or the run's own
// metadata. The distinction is explicit because a storage's class rules are per
// purpose — a payload class a deployment chose for artifacts must never make
// the manifest cold, and a manifest is what every survey, retention pass and
// restore reads first.
type Purpose string

const (
	// PurposePayload is an artifact's bytes. The zero value is the payload
	// purpose, so an upload that does not say what it is uploads data.
	PurposePayload Purpose = ""
	// PurposeMetadata is a small object the service itself reads back — the run
	// manifest. A storage may pin its own class for it, whatever the configured
	// payload class is.
	PurposeMetadata Purpose = "metadata"
)

// PutRequest identifies one object. Size is always known: transfer uploads
// from its spool file, never from a stream of unknown length.
type PutRequest struct {
	Key    string
	Size   int64
	SHA256 string
	// Purpose is what the object is; the zero value is PurposePayload. A
	// storage that distinguishes metadata from payloads reads it to choose the
	// object's class; every other storage ignores it.
	Purpose Purpose
}

// ListRequest pages a listing; Token "" starts at the first page.
type ListRequest struct {
	Prefix string
	Token  string
}

// ListPage is one page; NextToken "" means this was the last page.
type ListPage struct {
	Objects   []model.ObjectInfo
	NextToken string
}

// Reader is the read-only port of a destination: what a restore path and a
// survey may do. It has no method that could write — a caller that only reads
// depends on this, never on Storage.
type Reader interface {
	Describe(ctx context.Context) (StorageTraits, error)
	// Stat reports one object. It returns a pluginerrors.ClassNotFound error only
	// when the object is definitely absent; any doubt is a transient error.
	Stat(ctx context.Context, key string) (model.ObjectInfo, error)
	List(ctx context.Context, req ListRequest) (ListPage, error)
	// Get reads one object into dst; a definitely absent key is ClassNotFound.
	Get(ctx context.Context, key string, dst io.Writer) error
}

// Storage is one destination. Keys are relative to the plugin's own root
// (config), so layout builds identical keys for every storage.
type Storage interface {
	Reader
	// Put stores one object; body is a fresh reader for every attempt. The
	// returned ObjectInfo is the storage's knowledge, which transfer still
	// verifies with Stat before declaring the copy good.
	Put(ctx context.Context, req PutRequest, body io.Reader) (model.ObjectInfo, error)
	// Delete is idempotent: deleting a key that is definitely absent is success.
	Delete(ctx context.Context, key string) error
}

// BuildInput is the typed construction input of one storage instance: what the
// assembly knows about the instance and the plugin's own config does not carry.
// It is a plain value owned by this package, never a shared limiter and never a
// field of instances.Deps.
type BuildInput struct {
	// CallParallelism is the most calls the storage may hold in flight, always
	// at least 1: the assembly reads it from the instance envelope, and a plugin
	// that fans one call out into several holds at most this many, with no
	// second ceiling of its own.
	CallParallelism int
}

// Factory constructs one storage from the plugin's decoded config, its Deps
// (instance name and scoped logger, exactly as every other kind receives them)
// and its construction input.
type Factory[C any] func(ctx context.Context, cfg C, d instances.Deps, in BuildInput) (Storage, error)

// Spec describes one storage type for registration. It mirrors the framework's
// own registration record; New is the storage factory — the constructor every
// assembly calls through Build.
type FactorySpec[C any] struct {
	Type      string
	Summary   string
	Stability instances.Stability
	Binaries  []string
	Example   []byte
	New       Factory[C]
}

// factoryEntry is the storage-owned constructor of one registered type. The
// framework's registry keeps only the descriptor and config type; the typed
// factory lives in this package's table, because the framework's registration
// record has no place for a construction input.
type factoryEntry struct {
	build func(ctx context.Context, raw jsontext.Value, in BuildInput, d instances.Deps) (Storage, error)
}

// factoryTables maps each factory registry to the storage factories registered
// on it, so two independent registries in one process never share a constructor.
// ponytail: entries are never evicted; a registry lives for the process, and the
// only short-lived registries are test doubles inside one test binary.
var (
	factoryMu     sync.Mutex
	factoryTables = map[*instances.Registry]map[string]factoryEntry{}
)

func putFactory(r *instances.Registry, typ string, e factoryEntry) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if factoryTables[r] == nil {
		factoryTables[r] = map[string]factoryEntry{}
	}
	factoryTables[r][typ] = e
}

func storageFactory(r *instances.Registry, typ string) (factoryEntry, bool) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	e, ok := factoryTables[r][typ]
	return e, ok
}

// Register adds one storage type to the factory registry: its descriptor,
// config type and Example are contributed to the framework's registry, and its
// typed factory is kept here.
func Register[C any](r *instances.Registry, s FactorySpec[C]) error {
	if err := instances.Register(r, Kind, frameworkSpec(s)); err != nil {
		return err
	}
	putFactory(r, s.Type, entryFor(s))
	return nil
}

// AddFactory registers one storage type in a loader registry and contributes
// its descriptor to the compiled schema — the whole body of a storage backend's
// loader.Add apply.
func AddFactory[C any](lr *loader.Registry, r *instances.Registry, s FactorySpec[C]) error {
	if err := instances.AddFactory(lr, r, Kind, frameworkSpec(s)); err != nil {
		return err
	}
	putFactory(r, s.Type, entryFor(s))
	return nil
}

// frameworkSpec is the framework's registration record of one storage type. Its
// New refuses: the framework's fixed signature carries no construction input, so
// a storage constructed through the generic path is an error, never a plugin
// without its CallParallelism.
func frameworkSpec[C any](s FactorySpec[C]) instances.FactorySpec[C, Storage] {
	return instances.FactorySpec[C, Storage]{
		Type:      s.Type,
		Summary:   s.Summary,
		Stability: s.Stability,
		Binaries:  s.Binaries,
		Example:   s.Example,
		New: func(ctx context.Context, cfg C, d instances.Deps) (Storage, error) {
			return nil, fmt.Errorf("storage %q (%s) was built without a construction input: construct it through storage.Build",
				d.Instance, Kind.ID(s.Type))
		},
	}
}

// entryFor is the storage-owned constructor of one type: the same decode the
// framework runs (configuration.Decode, Validate included), then the typed
// factory with its input.
func entryFor[C any](s FactorySpec[C]) factoryEntry {
	return factoryEntry{
		build: func(ctx context.Context, raw jsontext.Value, in BuildInput, d instances.Deps) (Storage, error) {
			var cfg C
			if err := configuration.Decode(raw, &cfg); err != nil {
				return nil, fmt.Errorf("%s %q: %w", Kind.Name(), d.Instance, err)
			}
			return s.New(ctx, cfg, d, in)
		},
	}
}

// Build constructs one raw storage instance. It is the raw constructor, closed
// by rule to this subtree: a workflow, preflight or any other consumer
// constructs through guard.Build and receives a Port or a guarded handle, never
// a mutable instance it could Put or Delete through without the ledger.
func Build(ctx context.Context, r *instances.Registry, typ string, raw jsontext.Value, in BuildInput, d instances.Deps) (Storage, error) {
	if in.CallParallelism < 1 {
		return nil, fmt.Errorf("storage %q (%s): call_parallelism must be at least 1, got %d",
			d.Instance, Kind.ID(typ), in.CallParallelism)
	}
	e, ok := storageFactory(r, typ)
	if !ok {
		// The framework's verdict comes first: an unknown type or a config its
		// decoder refuses is the same refusal everywhere else.
		if err := r.Check(Kind.Name(), typ, raw); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("storage %q (%s) has no storage factory: register it through storage.Register or storage.AddFactory",
			d.Instance, Kind.ID(typ))
	}
	return e.build(ctx, raw, in, d)
}

var Kind = instances.NewKind[Storage]("storage")
