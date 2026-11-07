package transfer

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// putBarrier releases every part once two uploads are in flight, so two
// destinations finish one artifact at once; concurrent records that the second
// arrival — not the timeout — did the releasing.
type putBarrier struct {
	mu         sync.Mutex
	waiting    int
	concurrent bool
	release    chan struct{}
}

func newPutBarrier() *putBarrier { return &putBarrier{release: make(chan struct{})} }

func (b *putBarrier) wait() {
	b.mu.Lock()
	b.waiting++
	n := b.waiting
	b.mu.Unlock()
	if n >= 2 {
		b.mu.Lock()
		b.concurrent = true
		b.mu.Unlock()
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		return
	}
	select {
	case <-b.release:
	case <-time.After(3 * time.Second): // a serial transfer must still finish; the test then reports the serialization
	}
}

// TestTwoTransfersFinishOneArtifactAtOnce races two destinations' uploads of one
// artifact: -race judges the transfer state, and the barrier makes the overlap
// real rather than incidental.
func TestTwoTransfersFinishOneArtifactAtOnce(t *testing.T) {
	barrier := newPutBarrier()
	parts := map[string]*fakeMultipart{}
	for _, name := range []string{"alpha", "beta"} {
		mp := newFakeMultipart(4)
		mp.partHook = func(int32) { barrier.wait() }
		parts[name] = mp
	}
	data := []byte("0123456789abcd")
	var wg sync.WaitGroup
	results := map[string]Outcome{}
	errs := map[string]error{}
	var mu sync.Mutex
	for _, name := range []string{"alpha", "beta"} {
		mp := parts[name]
		h := handle(t, destination(t, name, mp))
		tr := newTransfer(Options{Storage: name})
		sp := newSpool(t)
		file := spoolFile(t, sp, data)
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := tr.Upload(context.Background(), h, sp, Request{
				Key: "runs/r/artifact.bin", Label: "g/n", File: file, Size: int64(len(data)),
			})
			mu.Lock()
			results[name], errs[name] = out, err
			mu.Unlock()
		}()
	}
	wg.Wait()
	if !barrier.concurrent {
		t.Fatal("the two destinations never had a part in flight at once")
	}
	for _, name := range []string{"alpha", "beta"} {
		if errs[name] != nil {
			t.Fatalf("%s: %v", name, errs[name])
		}
		if !results[name].Handled {
			t.Fatalf("%s: outcome %+v, want the multipart upload", name, results[name])
		}
		got, ok := parts[name].Bytes("runs/r/artifact.bin")
		if !ok || !bytes.Equal(got, data) {
			t.Fatalf("%s: the committed object is %q", name, got)
		}
		if _, aborts, completes, _ := parts[name].stats(); aborts != 0 || completes != 1 {
			t.Fatalf("%s: aborts=%d completes=%d, want a clean commit", name, aborts, completes)
		}
	}
}

// TestConcurrentTransfersKeepTheirOwnUploads: two transfers on one destination
// abort only their own open uploads.
func TestConcurrentTransfersKeepTheirOwnUploads(t *testing.T) {
	mp := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", mp))
	u, _ := h.Uploader()
	first, second := newTransfer(Options{}), newTransfer(Options{})
	if _, _, err := first.begin(context.Background(), u, storage.PutRequest{Key: "k1", Size: 10}, "g/n"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := second.begin(context.Background(), u, storage.PutRequest{Key: "k2", Size: 10}, "g/n"); err != nil {
		t.Fatal(err)
	}
	if err := first.AbortOwn(context.Background()); err != nil {
		t.Fatal(err)
	}
	mp.mu.Lock()
	aborted := append([]string(nil), mp.abortedKeys...)
	mp.mu.Unlock()
	if len(aborted) != 1 || aborted[0] != "k1" {
		t.Fatalf("the first shutdown aborted %v, want only its own upload", aborted)
	}
	if err := second.AbortOwn(context.Background()); err != nil {
		t.Fatal(err)
	}
	mp.mu.Lock()
	aborted = append([]string(nil), mp.abortedKeys...)
	mp.mu.Unlock()
	if len(aborted) != 2 || aborted[1] != "k2" {
		t.Fatalf("the second shutdown aborted %v, want its own upload second", aborted)
	}
}
