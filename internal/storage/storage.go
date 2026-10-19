// Package storage is the destination seam. Every method is ONE attempt: the
// plugin never retries on its own, the engine owns retries, backoff and budgets.
// The interface obeys the seam rules documented in package plugin.
package storage

import (
	"context"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// StorageTraits are facts about a destination, not behaviour: what it exposes.
// Only declarations something actually reads live here — a field nobody reads
// is a field every plugin must nevertheless think about.
type StorageTraits struct {
	// Checksums lists what Stat reports in ObjectInfo: "sha256" and/or "md5".
	// The conformance suite holds a storage to exactly what it declares here.
	Checksums []string
}

// PutRequest identifies one object. Size is always known: the engine uploads
// from its spool file, never from a stream of unknown length.
type PutRequest struct {
	Key    string
	Size   int64
	SHA256 string
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

// Storage is one destination. Keys are relative to the plugin's own root
// (config), so the engine builds identical keys for every storage.
type Storage interface {
	Describe(ctx context.Context) (StorageTraits, error)
	// Put stores one object; body is a fresh reader for every attempt. The
	// returned ObjectInfo is the storage's knowledge, which the engine still
	// verifies with Stat before declaring the copy good.
	Put(ctx context.Context, req PutRequest, body io.Reader) (model.ObjectInfo, error)
	// Stat reports one object. It returns a plugin.ClassNotFound error only when
	// the object is definitely absent; any doubt is a transient error.
	Stat(ctx context.Context, key string) (model.ObjectInfo, error)
	List(ctx context.Context, req ListRequest) (ListPage, error)
	// Delete is idempotent: deleting a key that is definitely absent is success.
	Delete(ctx context.Context, key string) error
	// Get reads one object into dst; a definitely absent key is ClassNotFound.
	Get(ctx context.Context, key string, dst io.Writer) error
}

var Kind = plugin.NewKind[Storage]("storage")
