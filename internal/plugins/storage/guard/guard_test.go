package guard

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	raw "github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/internal"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// destinationFor wraps one in-memory storage as a built destination: the guard
// tests exercise the boundary itself, not the factory hop (TestBuild does that).
func destinationFor(name string, s storage.Storage) Destination {
	return Destination{name: name, raw: raw.Raw{Storage: s}}
}

// TestPortRefusesWhenZero: a port that was never built refuses every read
// instead of dereferencing nothing, and the refusal is permanent.
func TestPortRefusesWhenZero(t *testing.T) {
	ctx := context.Background()
	var p Port
	if p.Name() != "" {
		t.Fatalf("a zero port named itself %q", p.Name())
	}
	calls := map[string]func() error{
		"Describe": func() error { _, err := p.Describe(ctx); return err },
		"Stat":     func() error { _, err := p.Stat(ctx, "k"); return err },
		"List":     func() error { _, err := p.List(ctx, storage.ListRequest{}); return err },
		"Get":      func() error { return p.Get(ctx, "k", io.Discard) },
	}
	for name, call := range calls {
		err := call()
		if !errors.Is(err, ErrNoReadPort) {
			t.Fatalf("%s on a zero port = %v, want ErrNoReadPort", name, err)
		}
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
			t.Fatalf("%s on a zero port is %q, want permanent", name, class)
		}
	}
}

// TestPortDelegatesEveryRead: a built port forwards the four reads and names its
// instance.
func TestPortDelegatesEveryRead(t *testing.T) {
	ctx := context.Background()
	f := storagetest.NewFake()
	f.PutObject("k", []byte("body"), time.Now())
	p := Read("alpha", f)
	if p.Name() != "alpha" {
		t.Fatalf("port name = %q, want alpha", p.Name())
	}
	if _, err := p.Describe(ctx); err != nil {
		t.Fatalf("Describe: %v", err)
	}
	info, err := p.Stat(ctx, "k")
	if err != nil || info.Size != 4 {
		t.Fatalf("Stat = %+v, %v", info, err)
	}
	page, err := p.List(ctx, storage.ListRequest{})
	if err != nil || len(page.Objects) != 1 {
		t.Fatalf("List = %+v, %v", page, err)
	}
	var buf bytes.Buffer
	if err := p.Get(ctx, "k", &buf); err != nil || buf.String() != "body" {
		t.Fatalf("Get = %q, %v", buf.String(), err)
	}
}

// TestAuthorityWindow: an authority admits only between its creation and its
// deadline; nil, expired and closed all refuse, and Close is safe on nil.
func TestAuthorityWindow(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	a := NewAuthority(deadline)
	if a.Expired() {
		t.Fatal("a fresh authority is already expired")
	}
	a.now = func() time.Time { return deadline }
	if !a.Expired() {
		t.Fatal("an authority at its deadline is not expired")
	}
	a.Close()
	if !a.Expired() {
		t.Fatal("a closed authority is not expired")
	}
	var nilAuthority *Authority
	if !nilAuthority.Expired() {
		t.Fatal("a nil authority is not expired")
	}
	nilAuthority.Close()

	past := NewAuthority(time.Now().Add(-time.Second))
	if !past.Expired() {
		t.Fatal("an authority born past its deadline is not expired")
	}
}

// TestHandleRefusesWhenZero: zero handles carry no authority, so no mutation is
// possible, while Name and Port stay inert.
func TestHandleRefusesWhenZero(t *testing.T) {
	var h Handle
	if h.Name() != "" {
		t.Fatalf("a zero handle named itself %q", h.Name())
	}
	if err := h.Port().Get(context.Background(), "k", io.Discard); !errors.Is(err, ErrNoReadPort) {
		t.Fatalf("zero handle port = %v, want ErrNoReadPort", err)
	}
	if _, err := h.Put(context.Background(), storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("zero handle Put = %v, want ErrNoAuthority", err)
	}
	if _, ok := h.Uploader(); ok {
		t.Fatal("a zero handle reported a multipart uploader")
	}
}

// TestDestinationRefusesWhenZero: a destination that was never built cannot
// issue a handle or a remover.
func TestDestinationRefusesWhenZero(t *testing.T) {
	var d Destination
	if d.Name() != "" {
		t.Fatalf("a zero destination named itself %q", d.Name())
	}
	if d.Log() != nil {
		t.Fatal("a zero destination carries a logger")
	}
	if err := d.Port().Get(context.Background(), "k", io.Discard); !errors.Is(err, ErrNoReadPort) {
		t.Fatalf("zero destination port = %v, want ErrNoReadPort", err)
	}
	if _, err := d.Port().Open(NewAuthority(time.Now().Add(time.Hour))); !errors.Is(err, ErrNoDestination) {
		t.Fatalf("zero destination Open = %v, want ErrNoDestination", err)
	}
	if err := d.Remover().Delete(context.Background(), "k"); !errors.Is(err, ErrNoDestination) {
		t.Fatalf("zero destination Delete = %v, want ErrNoDestination", err)
	}
}

// TestOpenRefusesWithoutALiveAuthority: the handle is issued only under an
// authority inside its window.
func TestOpenRefusesWithoutALiveAuthority(t *testing.T) {
	d := destinationFor("alpha", storagetest.NewFake())
	if _, err := d.Port().Open(nil); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Open(nil) = %v, want ErrNoAuthority", err)
	}
	closed := NewAuthority(time.Now().Add(time.Hour))
	closed.Close()
	if _, err := d.Port().Open(closed); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Open(closed) = %v, want ErrNoAuthority", err)
	}
}

// TestHandleWritesOnlyUnderALiveAuthority: Put works inside the window and
// refuses once the window has passed, while the read port stays available.
func TestHandleWritesOnlyUnderALiveAuthority(t *testing.T) {
	ctx := context.Background()
	f := storagetest.NewFake()
	d := destinationFor("alpha", f)
	deadline := time.Now().Add(time.Hour)
	a := NewAuthority(deadline)
	h, err := d.Port().Open(a)
	if err != nil {
		t.Fatal(err)
	}
	if h.Name() != "alpha" {
		t.Fatalf("handle name = %q, want alpha", h.Name())
	}
	body := "payload"
	info, err := h.Put(ctx, storage.PutRequest{Key: "k", Size: int64(len(body))}, strings.NewReader(body))
	if err != nil || info.Size != int64(len(body)) {
		t.Fatalf("Put = %+v, %v", info, err)
	}
	a.now = func() time.Time { return deadline }
	if _, err := h.Put(ctx, storage.PutRequest{Key: "k2", Size: 1}, strings.NewReader("x")); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired Put = %v, want ErrNoAuthority", err)
	}
	if _, err := h.Port().Stat(ctx, "k"); err != nil {
		t.Fatalf("reading after expiry: %v", err)
	}
}

// checkingFake is a fake storage whose preflight check a test scripts.
type checkingFake struct {
	*storagetest.Fake
	err error
}

func (c *checkingFake) Check(context.Context) error { return c.err }

// TestDestinationCheck: preflight reads the backend's optional check without a
// write authority; a backend without the capability reports healthy, and a
// destination that was never built refuses.
func TestDestinationCheck(t *testing.T) {
	ctx := context.Background()
	var zero Port
	if err := zero.Check(ctx); !errors.Is(err, ErrNoReadPort) {
		t.Fatalf("zero port Check = %v, want ErrNoReadPort", err)
	}
	if err := destinationFor("plain", storagetest.NewFake()).Port().Check(ctx); err != nil {
		t.Fatalf("a backend without a check = %v, want nil", err)
	}
	broken := &checkingFake{Fake: storagetest.NewFake(), err: errors.New("no credentials")}
	if err := destinationFor("broken", broken).Port().Check(ctx); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("the backend's check failure = %v", err)
	}
	// Destination satisfies instances.Checker, the preflight instance's shape.
	var _ instances.Checker = Port{}
}

// TestDestinationAsynchronousCapability: the metadata probe names the multipart
// capability without issuing anything; a destination that was never built
// answers false.
func TestDestinationAsynchronousCapability(t *testing.T) {
	var zero Port
	if zero.WritesAsynchronously() {
		t.Fatal("a zero port reported asynchronous writes")
	}
	if destinationFor("plain", storagetest.NewFake()).Port().WritesAsynchronously() {
		t.Fatal("a whole-object destination reported asynchronous writes")
	}
	if !destinationFor("multi", &multipartFake{Fake: storagetest.NewFake()}).Port().WritesAsynchronously() {
		t.Fatal("a multipart destination reported whole-object writes")
	}
}

// TestUploaderDiscovery: only a destination with the multipart capability hands
// out an uploader.
func TestUploaderDiscovery(t *testing.T) {
	plain := destinationFor("plain", storagetest.NewFake())
	a := NewAuthority(time.Now().Add(time.Hour))
	h, err := plain.Port().Open(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Uploader(); ok {
		t.Fatal("a whole-object storage reported a multipart uploader")
	}

	multi := destinationFor("multi", &multipartFake{Fake: storagetest.NewFake()})
	h, err = multi.Port().Open(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Uploader(); !ok {
		t.Fatal("a multipart storage reported no uploader")
	}
}

// TestUploaderRefusesWhenZero: every discovering call of a zero uploader refuses.
func TestUploaderRefusesWhenZero(t *testing.T) {
	ctx := context.Background()
	var u Uploader
	calls := map[string]func() error{
		"BeginMultipart": func() error { _, err := u.BeginMultipart(ctx, storage.PutRequest{}); return err },
		"PutPart":        func() error { _, err := u.PutPart(ctx, storage.MultipartSession{}, storage.Part{}); return err },
		"CompleteMultipart": func() error {
			return u.CompleteMultipart(ctx, storage.MultipartSession{}, nil)
		},
		"StaleMultipart": func() error { _, err := u.StaleMultipart(ctx, time.Now()); return err },
		"AbortMultipart": func() error { return u.AbortMultipart(ctx, storage.MultipartSession{}) },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrNoAuthority) {
			t.Fatalf("%s on a zero uploader = %v, want ErrNoAuthority", name, err)
		}
	}
}

// TestUploaderDelegatesUnderAuthority: the multipart calls forward while the
// authority is live, enumeration stops when it is not, and Abort still runs
// after Close because cleanup may destroy but never create.
func TestUploaderDelegatesUnderAuthority(t *testing.T) {
	ctx := context.Background()
	f := &multipartFake{Fake: storagetest.NewFake()}
	d := destinationFor("alpha", f)
	deadline := time.Now().Add(time.Hour)
	a := NewAuthority(deadline)
	h, err := d.Port().Open(a)
	if err != nil {
		t.Fatal(err)
	}
	u, ok := h.Uploader()
	if !ok {
		t.Fatal("no uploader")
	}
	if _, err := u.BeginMultipart(ctx, storage.PutRequest{}); err != nil {
		t.Fatalf("BeginMultipart: %v", err)
	}
	if _, err := u.PutPart(ctx, storage.MultipartSession{}, storage.Part{Number: 1}); err != nil {
		t.Fatalf("PutPart: %v", err)
	}
	if err := u.CompleteMultipart(ctx, storage.MultipartSession{}, []storage.PartResult{{Number: 1}}); err != nil {
		t.Fatalf("CompleteMultipart: %v", err)
	}
	if len(f.completed) != 1 {
		t.Fatalf("CompleteMultipart received %v", f.completed)
	}
	stale, err := u.StaleMultipart(ctx, time.Now())
	if err != nil || len(stale) != 1 {
		t.Fatalf("StaleMultipart = %v, %v", stale, err)
	}

	a.now = func() time.Time { return deadline }
	if _, err := u.BeginMultipart(ctx, storage.PutRequest{}); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired BeginMultipart = %v", err)
	}
	if _, err := u.PutPart(ctx, storage.MultipartSession{}, storage.Part{}); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired PutPart = %v", err)
	}
	if err := u.CompleteMultipart(ctx, storage.MultipartSession{}, nil); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired CompleteMultipart = %v", err)
	}
	if _, err := u.StaleMultipart(ctx, time.Now()); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired StaleMultipart = %v", err)
	}
	if err := u.AbortMultipart(ctx, storage.MultipartSession{}); err != nil {
		t.Fatalf("AbortMultipart after expiry = %v, want the cleanup call to run", err)
	}
	if f.aborted != 1 {
		t.Fatalf("AbortMultipart ran %d times, want once", f.aborted)
	}
}

// TestInventoryDelegatesWhenSupported: a destination with the inventory
// capability answers it under the authority.
func TestInventoryDelegatesWhenSupported(t *testing.T) {
	ctx := context.Background()
	f := &inventoryFake{multipartFake: &multipartFake{Fake: storagetest.NewFake()}}
	d := destinationFor("alpha", f)
	h, err := d.Port().Open(NewAuthority(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	inv, ok := h.Inventory()
	if !ok {
		t.Fatal("no inventory capability")
	}
	inventory, err := inv.List(ctx)
	if err != nil || len(inventory) != 1 || inventory[0].Key != "inventory" {
		t.Fatalf("Inventory = %v, %v", inventory, err)
	}
}

// TestRemoverDeletes: the ledger's primitive removes one object and refuses when
// it was never built.
func TestRemoverDeletes(t *testing.T) {
	ctx := context.Background()
	f := storagetest.NewFake()
	f.PutObject("k", []byte("body"), time.Now())
	r := destinationFor("alpha", f).Remover()
	if err := r.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Bytes("k"); ok {
		t.Fatal("Delete left the object behind")
	}
	if err := r.Delete(ctx, "absent"); err != nil {
		t.Fatalf("deleting an absent key = %v, want success", err)
	}
}

// TestBuildKeepsTheInstanceBehindTheBoundary: the assembly constructs through
// Build, gets a named destination, and a factory failure is returned as is.
func TestBuildKeepsTheInstanceBehindTheBoundary(t *testing.T) {
	ctx := context.Background()
	r := instances.NewRegistry()
	if _, err := Build(ctx, r, "absent", jsontext.Value(`{}`), storage.BuildInput{CallParallelism: 1},
		instances.Deps{Instance: "alpha"}); err == nil {
		t.Fatal("Build of an unregistered type succeeded")
	}
	type emptyConfig struct{}
	if err := storage.Register(r, storage.FactorySpec[emptyConfig]{
		Type: "fake",
		New: func(context.Context, emptyConfig, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return storagetest.NewFake(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	instanceLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	d, err := Build(ctx, r, "fake", jsontext.Value(`{}`), storage.BuildInput{CallParallelism: 1},
		instances.Deps{Instance: "alpha", Log: instanceLog})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if d.Name() != "alpha" {
		t.Fatalf("destination name = %q, want alpha", d.Name())
	}
	if d.Log() != instanceLog {
		t.Fatal("the destination does not carry its instance's scoped logger")
	}
	if d.Port().Log() != instanceLog {
		t.Fatal("the port does not carry its instance's scoped logger")
	}
	if _, err := d.Port().Open(NewAuthority(time.Now().Add(time.Hour))); err != nil {
		t.Fatalf("the built destination issued no handle: %v", err)
	}
}

// multipartFake is a whole-object fake with the multipart capability added.
type multipartFake struct {
	*storagetest.Fake
	completed []storage.PartResult
	aborted   int
}

func (m *multipartFake) BeginMultipart(context.Context, storage.PutRequest) (storage.MultipartSession, error) {
	return storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 1}, nil
}

func (m *multipartFake) PutPart(context.Context, storage.MultipartSession, storage.Part) (storage.PartResult, error) {
	return storage.PartResult{Number: 1, ETag: "etag"}, nil
}

func (m *multipartFake) CompleteMultipart(_ context.Context, _ storage.MultipartSession, parts []storage.PartResult) error {
	m.completed = parts
	return nil
}

func (m *multipartFake) AbortMultipart(context.Context, storage.MultipartSession) error {
	m.aborted++
	return nil
}

func (m *multipartFake) StaleMultipart(context.Context, time.Time) ([]storage.StaleUpload, error) {
	return []storage.StaleUpload{{Key: "stale", UploadID: "u"}}, nil
}

// inventoryFake adds the full inventory capability on top of multipart.
type inventoryFake struct {
	*multipartFake
}

func (m *inventoryFake) Inventory(context.Context) ([]storage.StaleUpload, error) {
	return []storage.StaleUpload{{Key: "inventory", UploadID: "u"}}, nil
}

// inventoryOnlyFake is a destination that reports foreign incomplete uploads
// but cannot upload parts itself: the capability combination that must still be
// discoverable through the handle.
type inventoryOnlyFake struct {
	*storagetest.Fake
}

func (f *inventoryOnlyFake) Inventory(context.Context) ([]storage.StaleUpload, error) {
	return []storage.StaleUpload{{Key: "foreign", UploadID: "u"}}, nil
}

// TestHandleInventoryDiscovery: the inventory capability is discovered on its
// own, independent of the multipart upload one.
func TestHandleInventoryDiscovery(t *testing.T) {
	a := NewAuthority(time.Now().Add(time.Hour))
	open := func(d Destination) Handle {
		t.Helper()
		h, err := d.Port().Open(a)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	if _, ok := open(destinationFor("plain", storagetest.NewFake())).Inventory(); ok {
		t.Fatal("a plain destination reported an inventory")
	}
	if _, ok := open(destinationFor("multi", &multipartFake{Fake: storagetest.NewFake()})).Inventory(); ok {
		t.Fatal("an upload-only destination reported an inventory")
	}
	if _, ok := open(destinationFor("both", &inventoryFake{multipartFake: &multipartFake{Fake: storagetest.NewFake()}})).Inventory(); !ok {
		t.Fatal("a destination with both capabilities reported no inventory")
	}
	if _, ok := open(destinationFor("inventory", &inventoryOnlyFake{Fake: storagetest.NewFake()})).Inventory(); !ok {
		t.Fatal("an inventory-only destination reported no inventory")
	}
	var zero Handle
	if _, ok := zero.Inventory(); ok {
		t.Fatal("a zero handle reported an inventory")
	}
}

// TestInventoryListRefusesWithoutAuthority: a zero inventory and one whose
// authority is expired or closed refuse; a live one delegates.
func TestInventoryListRefusesWithoutAuthority(t *testing.T) {
	ctx := context.Background()
	var zero Inventory
	if _, err := zero.List(ctx); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("zero inventory List = %v, want ErrNoAuthority", err)
	}
	deadline := time.Now().Add(time.Hour)
	a := NewAuthority(deadline)
	h, err := destinationFor("inventory", &inventoryOnlyFake{Fake: storagetest.NewFake()}).Port().Open(a)
	if err != nil {
		t.Fatal(err)
	}
	inv, ok := h.Inventory()
	if !ok {
		t.Fatal("no inventory capability")
	}
	uploads, err := inv.List(ctx)
	if err != nil || len(uploads) != 1 || uploads[0].Key != "foreign" {
		t.Fatalf("List = %v, %v", uploads, err)
	}
	a.now = func() time.Time { return deadline }
	if _, err := inv.List(ctx); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("expired List = %v, want ErrNoAuthority", err)
	}
	a.now = time.Now
	a.Close()
	if _, err := inv.List(ctx); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("closed List = %v, want ErrNoAuthority", err)
	}
}

// TestC2_WorkflowPortHasNoMutationSurface: the surface a workflow receives is
// Port — reads, metadata and Open. It has no delete or removal method and no
// method that hands back a Destination, Remover or raw storage value; only
// Destination exposes Remover, and Destination never leaves assembly/protection.
func TestC2_WorkflowPortHasNoMutationSurface(t *testing.T) {
	allowed := map[string]bool{
		"Describe": true, "Stat": true, "List": true, "Get": true,
		"Name": true, "Log": true, "Check": true, "WritesAsynchronously": true,
		"Open": true,
	}
	forbidden := []reflect.Type{
		reflect.TypeOf(Destination{}),
		reflect.TypeOf(Remover{}),
		reflect.TypeOf((*storage.Storage)(nil)).Elem(),
		reflect.TypeOf((*storage.Reader)(nil)).Elem(),
	}
	portType := reflect.TypeOf(Port{})
	for i := 0; i < portType.NumMethod(); i++ {
		m := portType.Method(i)
		if !allowed[m.Name] {
			t.Fatalf("Port exposes %q, outside the workflow surface", m.Name)
		}
		for _, ft := range forbidden {
			if m.Type.NumOut() > 0 && m.Type.Out(0) == ft {
				t.Fatalf("Port method %q returns %s, a mutation-capable value", m.Name, ft)
			}
		}
	}
	if _, ok := portType.MethodByName("Open"); !ok {
		t.Fatal("Port cannot open a handle under an authority")
	}
	destType := reflect.TypeOf(Destination{})
	destAllowed := map[string]bool{"Name": true, "Log": true, "Port": true, "Remover": true, "Close": true}
	for i := 0; i < destType.NumMethod(); i++ {
		m := destType.Method(i)
		if !destAllowed[m.Name] {
			t.Fatalf("Destination exposes %q, outside the assembly/protection surface", m.Name)
		}
		for _, ft := range []reflect.Type{
			reflect.TypeOf(Destination{}),
			reflect.TypeOf((*storage.Storage)(nil)).Elem(),
			reflect.TypeOf((*storage.Reader)(nil)).Elem(),
		} {
			if m.Type.NumOut() > 0 && m.Type.Out(0) == ft {
				t.Fatalf("Destination method %q returns %s, a raw storage value", m.Name, ft)
			}
		}
	}
	if _, ok := destType.MethodByName("Remover"); !ok {
		t.Fatal("Destination lost its remover")
	}
	if _, ok := destType.MethodByName("Open"); ok {
		t.Fatal("Destination still opens handles directly; Open belongs to Port")
	}
	if _, ok := reflect.TypeOf(Remover{}).MethodByName("Delete"); !ok {
		t.Fatal("Remover lost its delete")
	}
}

// TestM10_ZeroAuthorityRefuses: a non-nil zero Authority has no clock and must
// refuse through the shared check instead of panicking; nil and closed behave
// the same.
func TestM10_ZeroAuthorityRefuses(t *testing.T) {
	zero := &Authority{}
	if !zero.Expired() {
		t.Fatal("a zero authority reports itself live")
	}
	d := destinationFor("alpha", storagetest.NewFake())
	if _, err := d.Port().Open(zero); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Open with a zero authority = %v, want ErrNoAuthority", err)
	}
	zero.Close()
	var nilAuthority *Authority
	if _, err := d.Port().Open(nilAuthority); !errors.Is(err, ErrNoAuthority) {
		t.Fatalf("Open with a nil authority = %v, want ErrNoAuthority", err)
	}
}

// asyncOnlyFake declares the explicit asynchronous-writer capability without
// implementing multipart upload.
type asyncOnlyFake struct {
	*storagetest.Fake
}

func (*asyncOnlyFake) WritesAsynchronously() bool { return true }

// readerOnly exposes only the Reader methods plus the explicit async
// capability: a value that is not a full storage.Storage.
type readerOnly struct {
	inner storage.Reader
}

func (r *readerOnly) Describe(ctx context.Context) (storage.StorageTraits, error) {
	return r.inner.Describe(ctx)
}

func (r *readerOnly) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	return r.inner.Stat(ctx, key)
}

func (r *readerOnly) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	return r.inner.List(ctx, req)
}

func (r *readerOnly) Get(ctx context.Context, key string, dst io.Writer) error {
	return r.inner.Get(ctx, key, dst)
}

func (*readerOnly) WritesAsynchronously() bool { return true }

// TestC7_AsyncCapabilityIsNotMultipartOnly: the classifier honours the storage
// kind's explicit AsynchronousWriter capability, so a provider that accepts a
// write without multipart (an HTTP 202) still marks the record.
func TestC7_AsyncCapabilityIsNotMultipartOnly(t *testing.T) {
	if destinationFor("plain", storagetest.NewFake()).Port().WritesAsynchronously() {
		t.Fatal("a whole-object storage reported asynchronous writes")
	}
	if !destinationFor("async", &asyncOnlyFake{Fake: storagetest.NewFake()}).Port().WritesAsynchronously() {
		t.Fatal("an explicit asynchronous writer was classified as synchronous")
	}
	if !destinationFor("multi", &multipartFake{Fake: storagetest.NewFake()}).Port().WritesAsynchronously() {
		t.Fatal("a multipart storage was classified as synchronous")
	}
	// A bare Reader that declares the capability directly is honoured too.
	if !Read("ro", &readerOnly{inner: storagetest.NewFake()}).WritesAsynchronously() {
		t.Fatal("a Reader-only asynchronous writer was classified as synchronous")
	}
	// A read-only port built from a bare Reader keeps the multipart fallback.
	if !Read("multi", &multipartFake{Fake: storagetest.NewFake()}).WritesAsynchronously() {
		t.Fatal("the multipart fallback was lost for a Reader-only port")
	}
}

// closingFake is a constructed storage whose Close a test counts.
type closingFake struct {
	*storagetest.Fake
	closes int
}

func (c *closingFake) Close() error { c.closes++; return nil }

// TestR2_M4_DestinationOwnsTheRawCloser: the Destination is the ownership
// handle: it closes the raw instance when the plugin implements io.Closer, and
// a storage without Close (or a zero destination) is a no-op.
func TestR2_M4_DestinationOwnsTheRawCloser(t *testing.T) {
	closing := &closingFake{Fake: storagetest.NewFake()}
	if err := destinationFor("alpha", closing).Close(); err != nil {
		t.Fatal(err)
	}
	if closing.closes != 1 {
		t.Fatalf("raw storage closed %d times, want once", closing.closes)
	}
	if err := destinationFor("plain", storagetest.NewFake()).Close(); err != nil {
		t.Fatalf("a non-closer storage = %v, want nil", err)
	}
	var zero Destination
	if err := zero.Close(); err != nil {
		t.Fatalf("a zero destination = %v, want nil", err)
	}
}
