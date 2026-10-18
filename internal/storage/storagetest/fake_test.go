package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

func TestFakeAccessorsAndEvents(t *testing.T) {
	f := NewFake()
	at := time.Date(2026, 10, 11, 4, 0, 0, 0, time.UTC)
	f.PutObject("runs/A/x.bin", []byte("hello"), at)

	if keys := f.Keys(); len(keys) != 1 || keys[0] != "runs/A/x.bin" {
		t.Fatalf("Keys = %v", keys)
	}
	if body, ok := f.Bytes("runs/A/x.bin"); !ok || string(body) != "hello" {
		t.Fatalf("Bytes = %q, %v", body, ok)
	}
	if _, ok := f.Bytes("missing"); ok {
		t.Fatal("Bytes of a missing key")
	}

	ctx := context.Background()
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2}, strings.NewReader("ok")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := f.Get(ctx, "k", io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := f.List(ctx, storage.ListRequest{Prefix: "k"}); err != nil {
		t.Fatal(err)
	}
	if err := f.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Describe(ctx); err != nil {
		t.Fatal(err)
	}
	ops := map[Op]bool{}
	for _, ev := range f.Events() {
		ops[ev.Op] = true
	}
	for _, op := range []Op{OpPut, OpStat, OpGet, OpList, OpDelete, OpDescribe} {
		if !ops[op] {
			t.Errorf("events lack %s: %v", op, f.Events())
		}
	}
}

func TestFakeCanceledContexts(t *testing.T) {
	f := NewFake()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.Describe(ctx); err == nil {
		t.Error("Describe with a cancelled context")
	}
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k"}, bytes.NewReader(nil)); err == nil {
		t.Error("Put with a cancelled context")
	}
	if _, err := f.Stat(ctx, "k"); err == nil {
		t.Error("Stat with a cancelled context")
	}
	if _, err := f.List(ctx, storage.ListRequest{}); err == nil {
		t.Error("List with a cancelled context")
	}
	if err := f.Delete(ctx, "k"); err == nil {
		t.Error("Delete with a cancelled context")
	}
	if err := f.Get(ctx, "k", io.Discard); err == nil {
		t.Error("Get with a cancelled context")
	}
}

func TestFakeFaultOnDescribe(t *testing.T) {
	f := NewFake()
	f.Fault = func(op Op, key string, call int) error {
		if op == OpDescribe {
			return plugin.Transient(errors.New("registry down"))
		}
		return nil
	}
	if _, err := f.Describe(context.Background()); err == nil {
		t.Fatal("Describe fault not injected")
	}
}

func TestFakePutRejectsInconsistentBodies(t *testing.T) {
	f := NewFake()
	ctx := context.Background()
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 100}, strings.NewReader("short")); err == nil {
		t.Fatal("a short body was accepted")
	}
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2, SHA256: "wrong"}, strings.NewReader("ok")); err == nil {
		t.Fatal("a wrong declared hash was accepted")
	}
}

func TestFakeGetMissingIsNotFound(t *testing.T) {
	f := NewFake()
	err := f.Get(context.Background(), "missing", io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("Get of a missing key: %v", err)
	}
}

// TestDescribeCountsLikeEveryOtherOperation: Describe's counter and its Fault
// hook are keyed on (op, "") the way every other operation's are, so a test can
// script "the first Describe fails" and read the count back with
// CallCount(OpDescribe, ""). It used to write a bare "describe" key, which
// CallCount never read.
func TestDescribeCountsLikeEveryOtherOperation(t *testing.T) {
	f := NewFake()
	if got := f.CallCount(OpDescribe, ""); got != 0 {
		t.Fatalf("CallCount before any call = %d, want 0", got)
	}
	if _, err := f.Describe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.CallCount(OpDescribe, ""); got != 1 {
		t.Fatalf("CallCount(OpDescribe) after one call = %d, want 1", got)
	}

	seen := 0
	f.Fault = func(op Op, key string, call int) error {
		if op != OpDescribe || key != "" {
			t.Fatalf("Fault got (%q, %q), want (describe, \"\")", op, key)
		}
		seen = call
		return errors.New("scripted")
	}
	if _, err := f.Describe(context.Background()); err == nil {
		t.Fatal("the scripted Describe failure did not surface")
	}
	if seen != 2 {
		t.Fatalf("Fault saw call %d, want 2", seen)
	}
	if got := f.CallCount(OpDescribe, ""); got != 2 {
		t.Fatalf("CallCount(OpDescribe) after two calls = %d, want 2", got)
	}
}

// TestFakeFaultOnEveryOperation: the Fault hook is consulted by every
// operation, so the engine's failure tests can script any of them.
func TestFakeFaultOnEveryOperation(t *testing.T) {
	ctx := context.Background()
	calls := map[Op]func(f *Fake) error{
		OpPut: func(f *Fake) error {
			_, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2}, strings.NewReader("ok"))
			return err
		},
		OpStat:   func(f *Fake) error { _, err := f.Stat(ctx, "k"); return err },
		OpList:   func(f *Fake) error { _, err := f.List(ctx, storage.ListRequest{}); return err },
		OpDelete: func(f *Fake) error { return f.Delete(ctx, "k") },
		OpGet:    func(f *Fake) error { return f.Get(ctx, "k", io.Discard) },
	}
	for op, call := range calls {
		f := NewFake()
		f.Fault = func(got Op, _ string, _ int) error {
			if got == op {
				return plugin.Transient(errors.New("scripted"))
			}
			return nil
		}
		if err := call(f); err == nil {
			t.Errorf("a scripted fault on %s did not surface", op)
		}
	}
}

// TestFakePutReportsABrokenBody: an upstream that fails mid-read is a transient
// failure, never a stored object.
func TestFakePutReportsABrokenBody(t *testing.T) {
	f := NewFake()
	if _, err := f.Put(context.Background(), storage.PutRequest{Key: "k", Size: 1}, plugintest.NewErrReader()); err == nil {
		t.Fatal("a body that fails to read was accepted")
	}
}

// TestFakeListFallsBackToItsDefaultPageSize: a zero PageSize means the default,
// not a page of no objects.
func TestFakeListFallsBackToItsDefaultPageSize(t *testing.T) {
	f := NewFake()
	f.PageSize = 0
	f.PutObject("a/1", []byte("x"), time.Now())
	page, err := f.List(context.Background(), storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("page %+v, want the one object", page)
	}
}
