package storage_test

// C7: the safety owner must learn about every potentially asynchronous writer
// before its first write. The multiplex-only classifier missed a provider that
// accepts a write asynchronously without implementing multipart; the explicit
// capability closes that gap.

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// asyncOnly declares the asynchronous-write capability without multipart: the
// shape the old multipart-only classifier missed.
type asyncOnly struct {
	storage.Storage
	async bool
}

func (a asyncOnly) WritesAsynchronously() bool { return a.async }

// multipartOnly is asynchronous only through the multipart capability.
type multipartOnly struct {
	storage.Storage
	storage.MultipartUploader
}

// asyncAndMultipart carries both capabilities, so the explicit answer decides.
type asyncAndMultipart struct {
	storage.Storage
	storage.MultipartUploader
	async bool
}

func (a asyncAndMultipart) WritesAsynchronously() bool { return a.async }

func TestC7_AsynchronousWriteCapabilityCoversEveryWriter(t *testing.T) {
	cases := []struct {
		name string
		s    storage.Storage
		want bool
	}{
		{"plain storage", storagetest.NewFake(), false},
		{"multipart capability alone", multipartOnly{}, true},
		{"explicit capability alone", asyncOnly{async: true}, true},
		{"explicit capability says no", asyncOnly{async: false}, false},
		{"explicit capability overrides multipart", asyncAndMultipart{async: false}, false},
		{"explicit capability agrees with multipart", asyncAndMultipart{async: true}, true},
		{"nil storage", nil, false},
	}
	for _, c := range cases {
		if got := storage.WritesAsynchronously(c.s); got != c.want {
			t.Errorf("%s: WritesAsynchronously = %v, want %v", c.name, got, c.want)
		}
	}
}
