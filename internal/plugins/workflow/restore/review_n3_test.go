package restore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// TestR3_N3_1_RestoreHookSeesWrittenBytes is the reviewer's N3-1 probe, copied
// as written: the checkpoint fires only after the bytes reached the writer.
func TestR3_N3_1_RestoreHookSeesWrittenBytes(t *testing.T) {
	f := storagetest.NewFake()
	f.PutObject("artifact", []byte("verified-copy"), time.Now())
	var dst bytes.Buffer
	s := &hookStorage{Fake: f, key: "artifact", onGet: func() {
		if dst.String() != "verified-copy" {
			t.Errorf("interleaving fired before copied bytes: got %q", dst.String())
		}
	}}
	if err := s.Get(context.Background(), "artifact", &dst); err != nil {
		t.Fatal(err)
	}
}

// TestR3_N3_1_NoCheckpointOnFailedGet: a Get that fails never signals a
// completed write.
func TestR3_N3_1_NoCheckpointOnFailedGet(t *testing.T) {
	f := storagetest.NewFake()
	fired := false
	s := &hookStorage{Fake: f, key: "artifact", onGet: func() { fired = true }}
	var dst bytes.Buffer
	if err := s.Get(context.Background(), "artifact", &dst); err == nil {
		t.Fatal("the absent object was read")
	}
	if fired {
		t.Fatal("a failed Get fired the completed-write checkpoint")
	}
}

// TestR3_N3_1_KeyNarrowing: an empty key hooks every object; a non-empty key
// hooks only its own, so one restore's checkpoint cannot stand for another's.
func TestR3_N3_1_KeyNarrowing(t *testing.T) {
	f := storagetest.NewFake()
	f.PutObject("a", []byte("one"), time.Now())
	f.PutObject("b", []byte("two"), time.Now())
	var hits []string
	s := &hookStorage{Fake: f, key: "a", onGet: func() { hits = append(hits, "a") }}
	if err := s.Get(context.Background(), "b", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("the key-narrowed hook fired for another object: %v", hits)
	}
	if err := s.Get(context.Background(), "a", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("the key-narrowed hook did not fire for its own object: %v", hits)
	}

	any := &hookStorage{Fake: f, onGet: func() { hits = append(hits, "any") }}
	if err := any.Get(context.Background(), "b", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[1] != "any" {
		t.Fatalf("the all-key hook did not fire: %v", hits)
	}
}
