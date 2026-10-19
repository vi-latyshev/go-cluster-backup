package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// Op names a Fake operation for the Fault hook.
type Op string

const (
	OpDescribe Op = "describe"
	OpPut      Op = "put"
	OpStat     Op = "stat"
	OpList     Op = "list"
	OpDelete   Op = "delete"
	OpGet      Op = "get"
)

// Fake is an in-memory storage whose every operation can be scripted to fail —
// the storage the engine's failure-path tests run against. It is safe for
// concurrent use.
//
// Fault is consulted before the operation does its work, with the 1-based call
// number FOR THAT KEY, so "the very first call of the run fails" needs no other
// bookkeeping. DiscardPuts simulates the other asymmetry: Put accepts the bytes
// and returns successfully, but nothing becomes visible — an upload that was
// never committed.
type Fake struct {
	Traits      storage.StorageTraits
	PageSize    int
	Fault       func(op Op, key string, call int) error
	DiscardPuts bool

	mu      sync.Mutex
	objects map[string]fakeObject
	calls   map[string]int
	events  []Event
}

// Event is one operation as the Fake saw it, in order: the engine's sequencing
// (manifest last, eviction before upload) is asserted from this log.
type Event struct {
	Op  Op
	Key string
}

type fakeObject struct {
	body    []byte
	modTime time.Time
}

// NewFake returns a working in-memory storage: sha256 and md5 exposed, pages
// of 100.
func NewFake() *Fake {
	return &Fake{
		Traits:   storage.StorageTraits{Checksums: []string{"sha256", "md5"}},
		PageSize: 100,
		objects:  map[string]fakeObject{},
		calls:    map[string]int{},
	}
}

// CallCount reports how many times an operation ran for a key ("" for List).
func (f *Fake) CallCount(op Op, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[string(op)+"\x00"+key]
}

// Events returns the operation log, oldest first.
func (f *Fake) Events() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Event(nil), f.events...)
}

func (f *Fake) record(op Op, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, Event{Op: op, Key: key})
}

// Keys returns every stored key, sorted.
func (f *Fake) Keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Bytes returns a copy of one stored object.
func (f *Fake) Bytes(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), o.body...), true
}

// PutObject stores bytes directly, bypassing Fault — for arranging a state a run
// must find, such as runs uploaded by earlier nights.
func (f *Fake) PutObject(key string, body []byte, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{body: append([]byte(nil), body...), modTime: at}
}

func (f *Fake) fault(op Op, key string) error {
	f.mu.Lock()
	n := f.calls[string(op)+"\x00"+key] + 1
	f.calls[string(op)+"\x00"+key] = n
	g := f.Fault
	f.mu.Unlock()
	if g == nil {
		return nil
	}
	return g(op, key, n)
}

func (f *Fake) Describe(ctx context.Context) (storage.StorageTraits, error) {
	f.record(OpDescribe, "")
	if err := ctx.Err(); err != nil {
		return storage.StorageTraits{}, err
	}
	// Through fault, so the call counter is the one CallCount(OpDescribe, "")
	// reads: it keys on op+"\x00"+key like every other operation.
	if err := f.fault(OpDescribe, ""); err != nil {
		return storage.StorageTraits{}, err
	}
	return f.Traits, nil
}

func (f *Fake) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	f.record(OpPut, req.Key)
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	if err := f.fault(OpPut, req.Key); err != nil {
		return model.ObjectInfo{}, err
	}
	data, err := io.ReadAll(io.LimitReader(body, 1<<30))
	if err != nil {
		return model.ObjectInfo{}, plugin.Transient(err)
	}
	sum := model.SHA256Hex(data)
	if int64(len(data)) != req.Size {
		return model.ObjectInfo{}, plugin.Permanent(errors.New("fake: body length does not match the declared size"))
	}
	if req.SHA256 != "" && req.SHA256 != sum {
		return model.ObjectInfo{}, plugin.Permanent(errors.New("fake: body hash does not match the declared sha256"))
	}
	now := time.Now()
	f.mu.Lock()
	if !f.DiscardPuts {
		f.objects[req.Key] = fakeObject{body: data, modTime: now}
	}
	f.mu.Unlock()
	return model.ObjectInfo{Key: req.Key, Size: int64(len(data)), ModTime: now, SHA256: sum, MD5: md5Hex(data)}, nil
}

func (f *Fake) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	f.record(OpStat, key)
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	if err := f.fault(OpStat, key); err != nil {
		return model.ObjectInfo{}, err
	}
	f.mu.Lock()
	o, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		return model.ObjectInfo{}, plugin.NotFound(errors.New("fake: object is definitely absent"))
	}
	return model.ObjectInfo{Key: key, Size: int64(len(o.body)), ModTime: o.modTime,
		SHA256: model.SHA256Hex(o.body), MD5: md5Hex(o.body)}, nil
}

func (f *Fake) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	f.record(OpList, req.Prefix)
	if err := ctx.Err(); err != nil {
		return storage.ListPage{}, err
	}
	if err := f.fault(OpList, req.Prefix); err != nil {
		return storage.ListPage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, req.Prefix) && k > req.Token {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := storage.ListPage{}
	page := f.PageSize
	if page <= 0 {
		page = 100
	}
	for _, k := range keys[:min(page, len(keys))] {
		o := f.objects[k]
		res.Objects = append(res.Objects, model.ObjectInfo{Key: k, Size: int64(len(o.body)), ModTime: o.modTime,
			SHA256: model.SHA256Hex(o.body), MD5: md5Hex(o.body)})
	}
	if len(keys) > page {
		res.NextToken = keys[page-1]
	}
	return res, nil
}

func (f *Fake) Delete(ctx context.Context, key string) error {
	f.record(OpDelete, key)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.fault(OpDelete, key); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func (f *Fake) Get(ctx context.Context, key string, dst io.Writer) error {
	f.record(OpGet, key)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.fault(OpGet, key); err != nil {
		return err
	}
	f.mu.Lock()
	o, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		return plugin.NotFound(errors.New("fake: object is definitely absent"))
	}
	_, err := io.Copy(dst, bytes.NewReader(o.body))
	return err
}
