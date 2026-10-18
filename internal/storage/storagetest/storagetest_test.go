package storagetest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// broken wraps the Fake, breaking exactly one behaviour.
type broken struct {
	*Fake
	mode string
}

func (b broken) Describe(ctx context.Context) (storage.StorageTraits, error) {
	switch b.mode {
	case "badtraits":
		return storage.StorageTraits{Checksums: []string{"sha1"}}, nil
	case "describeerr":
		return storage.StorageTraits{}, plugin.Transient(errors.New("injected describe failure"))
	}
	return b.Fake.Describe(ctx)
}

func (b broken) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	switch b.mode {
	case "puterr":
		return model.ObjectInfo{}, plugin.Transient(errors.New("injected put failure"))
	case "wrongkey":
		info, err := b.Fake.Put(ctx, req, body)
		if err != nil {
			return info, err
		}
		info.Key = "somewhere/else"
		return info, nil
	}
	return b.Fake.Put(ctx, req, body)
}

func (b broken) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	switch b.mode {
	case "nilstat":
		return model.ObjectInfo{Key: key, Size: 3}, nil
	case "badhash":
		info, err := b.Fake.Stat(ctx, key)
		info.SHA256 = ""
		return info, err
	case "badmd5":
		info, err := b.Fake.Stat(ctx, key)
		info.MD5 = ""
		return info, err
	case "staterr":
		return model.ObjectInfo{}, plugin.Transient(errors.New("injected stat failure"))
	case "badsize":
		info, err := b.Fake.Stat(ctx, key)
		info.Size++
		return info, err
	case "zeromtime":
		info, err := b.Fake.Stat(ctx, key)
		info.ModTime = time.Time{}
		return info, err
	}
	return b.Fake.Stat(ctx, key)
}

func (b broken) Get(ctx context.Context, key string, dst io.Writer) error {
	switch b.mode {
	case "wrongget":
		_, err := dst.Write([]byte("the wrong bytes"))
		return err
	case "geterr":
		return plugin.Transient(errors.New("injected get failure"))
	}
	return b.Fake.Get(ctx, key, dst)
}

func (b broken) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	switch b.mode {
	case "truncate":
		page, err := b.Fake.List(ctx, storage.ListRequest{Prefix: req.Prefix})
		if err != nil {
			return page, err
		}
		if len(page.Objects) > 1 {
			page.Objects = page.Objects[:1]
		}
		page.NextToken = ""
		return page, nil
	case "listfail":
		return storage.ListPage{}, plugin.Transient(errors.New("injected list failure"))
	case "prefixfail":
		if strings.HasPrefix(req.Prefix, "conformance/list/object") {
			return storage.ListPage{}, plugin.Transient(errors.New("injected prefixed list failure"))
		}
	case "duptoken":
		// One object and a token that never advances: the paging loop cannot
		// terminate and the check must say so.
		page, err := b.Fake.List(ctx, storage.ListRequest{Prefix: req.Prefix})
		if err != nil {
			return page, err
		}
		if len(page.Objects) == 0 {
			return page, nil
		}
		return storage.ListPage{Objects: page.Objects[:1], NextToken: "stuck"}, nil
	case "dupobj":
		page, err := b.Fake.List(ctx, storage.ListRequest{Prefix: req.Prefix})
		if err != nil {
			return page, err
		}
		out := storage.ListPage{}
		for _, o := range page.Objects {
			out.Objects = append(out.Objects, o, o)
		}
		return out, nil
	case "growtoken":
		// A token that changes every page: the paging loop never sees the same
		// token twice and the check's own page cap must end it.
		page, err := b.Fake.List(ctx, storage.ListRequest{Prefix: req.Prefix})
		if err != nil {
			return page, err
		}
		if len(page.Objects) == 0 {
			return page, nil
		}
		return storage.ListPage{Objects: page.Objects[:1], NextToken: req.Token + "x"}, nil
	case "prefixwrong":
		if req.Prefix == "conformance/list/object-3" {
			return b.Fake.List(ctx, storage.ListRequest{Prefix: "conformance/list/"})
		}
	}
	return b.Fake.List(ctx, req)
}

func (b broken) Delete(ctx context.Context, key string) error {
	switch b.mode {
	case "deleteerr":
		if _, err := b.Fake.Stat(ctx, key); err != nil {
			return plugin.NotFound(errors.New("nothing to delete"))
		}
	case "deletefail":
		return plugin.Transient(errors.New("injected delete failure"))
	case "nodelete":
		return nil
	}
	return b.Fake.Delete(ctx, key)
}

// Fake-backed methods that ignore a cancelled context (via embedding) are the
// "ignorectx" mode: the base methods check ctx, so that mode is expressed by not
// delegating at all.

func TestSuitePassesFake(t *testing.T) {
	Run(t, func(*testing.T) storage.Storage { return NewFake() })
}

func TestFakePagesByPageSize(t *testing.T) {
	f := NewFake()
	f.PageSize = 2
	ctx := context.Background()
	for _, key := range []string{"a/1", "a/2", "a/3"} {
		if _, err := f.Put(ctx, storage.PutRequest{Key: key, Size: 1}, io.LimitReader(neverEnding{}, 1)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.List(ctx, storage.ListRequest{Prefix: "a/"})
	if err != nil || len(page.Objects) != 2 || page.NextToken == "" {
		t.Fatalf("first page: %+v, %v", page, err)
	}
	page2, err := f.List(ctx, storage.ListRequest{Prefix: "a/", Token: page.NextToken})
	if err != nil || len(page2.Objects) != 1 || page2.NextToken != "" {
		t.Fatalf("second page: %+v, %v", page2, err)
	}
}

func TestChecksCatchBrokenStorages(t *testing.T) {
	cases := []struct {
		check string
		mode  string
	}{
		{"describe", "badtraits"},
		{"describe", "describeerr"},
		{"put stat get", "wrongget"},
		{"put stat get", "puterr"},
		{"put stat get", "wrongkey"},
		{"put stat get", "staterr"},
		{"put stat get", "badsize"},
		{"put stat get", "zeromtime"},
		{"put stat get", "geterr"},
		{"stat of absent key is not_found", "nilstat"},
		{"stat of absent key is not_found", "staterr"},
		{"delete is idempotent", "deleteerr"},
		{"delete is idempotent", "puterr"},
		{"delete is idempotent", "deletefail"},
		{"delete is idempotent", "nodelete"},
		{"delete is idempotent", "staterr"},
		{"list pages every object", "truncate"},
		{"list pages every object", "puterr"},
		{"list pages every object", "listfail"},
		{"list pages every object", "duptoken"},
		{"list pages every object", "growtoken"},
		{"list pages every object", "dupobj"},
		{"list pages every object", "prefixfail"},
		{"list pages every object", "prefixwrong"},
		{"checksums per declared traits", "badhash"},
		{"checksums per declared traits", "badmd5"},
		{"checksums per declared traits", "describeerr"},
		{"checksums per declared traits", "puterr"},
		{"checksums per declared traits", "staterr"},
		// nilstat answers success whatever the context says, so the cancellation
		// check must catch it on the Stat leg alone.
		{"returns on cancellation", "nilstat"},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			s := broken{Fake: NewFake(), mode: tc.mode}
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, s); err == nil {
					t.Fatalf("check %q passed a broken storage", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

func TestGoodChecksFindNothingOnFake(t *testing.T) {
	for _, c := range Checks {
		if err := c.Run(t, NewFake()); err != nil {
			t.Errorf("check %q failed the fake: %v", c.Name, err)
		}
	}
}

func TestFakeDiscardPuts(t *testing.T) {
	f := NewFake()
	f.DiscardPuts = true
	ctx := context.Background()
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2}, io.LimitReader(neverEnding{}, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(ctx, "k"); err == nil {
		t.Fatal("a discarded put became visible")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("stat of a discarded put: %v", err)
	}
}

func TestFakeFaultHook(t *testing.T) {
	f := NewFake()
	f.Fault = func(op Op, key string, call int) error {
		if op == OpPut && call == 1 {
			return plugin.Transient(errors.New("injected 500"))
		}
		return nil
	}
	ctx := context.Background()
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2}, io.LimitReader(neverEnding{}, 2)); err == nil {
		t.Fatal("first put did not fail")
	}
	if _, err := f.Put(ctx, storage.PutRequest{Key: "k", Size: 2}, io.LimitReader(neverEnding{}, 2)); err != nil {
		t.Fatalf("second put failed: %v", err)
	}
	if n := f.CallCount(OpPut, "k"); n != 2 {
		t.Fatalf("CallCount = %d", n)
	}
}

type neverEnding struct{}

func (neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}
