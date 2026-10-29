package storagetest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// memMultipart is the kit's own multipart-capable storage: parts are held per
// upload and Complete assembles them into the in-memory Fake, so the capability
// checks have a working implementation to pass against and a broken mode to fail
// against. Its mode field breaks exactly one behaviour at a time.
type memMultipart struct {
	*Fake
	partSize int64
	mode     string

	mu      sync.Mutex
	uploads map[string]*memUpload
	next    int
	begins  int
	puts    int
	stales  int
}

type memUpload struct {
	key       string
	parts     map[int32][]byte
	etags     map[int32]string
	order     []int32
	initiated time.Time
}

func newMemMultipart(partSize int64) *memMultipart {
	return &memMultipart{Fake: NewFake(), partSize: partSize, uploads: map[string]*memUpload{}}
}

// acceptsOnePart reports whether the mode lets a one-part object through: the
// checks of checkMultipartSmallObject need a session back, not ErrUnsupported.
func (m *memMultipart) acceptsOnePart() bool {
	switch m.mode {
	case "smallbeginfail", "smallpartfail", "smallcompletefail", "smallwrongget":
		return true
	}
	return false
}

func (m *memMultipart) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return storage.MultipartSession{}, err
	}
	m.mu.Lock()
	m.begins++
	begins := m.begins
	m.mu.Unlock()
	switch m.mode {
	case "beginfail":
		return storage.MultipartSession{}, plugin.Transient(errors.New("injected begin failure"))
	case "beginfail2":
		if begins >= 2 {
			return storage.MultipartSession{}, plugin.Transient(errors.New("injected second begin failure"))
		}
	case "smallbeginfail":
		if req.Size <= m.partSize {
			return storage.MultipartSession{}, plugin.Transient(errors.New("injected small begin failure"))
		}
	}
	if req.Size <= m.partSize && !m.acceptsOnePart() {
		return storage.MultipartSession{}, fmt.Errorf("object %s fits one part: %w", req.Key, errors.ErrUnsupported)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	id := fmt.Sprintf("upload-%d", m.next)
	m.uploads[id] = &memUpload{key: req.Key, parts: map[int32][]byte{}, etags: map[int32]string{}, initiated: time.Now()}
	partSize := m.partSize
	if m.mode == "wrongpartsize" {
		partSize++
	}
	return storage.MultipartSession{Key: req.Key, UploadID: id, PartSize: partSize}, nil
}

func (m *memMultipart) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.PartResult{}, err
	}
	m.mu.Lock()
	m.puts++
	puts := m.puts
	m.mu.Unlock()
	switch m.mode {
	case "partfail", "smallpartfail":
		return storage.PartResult{}, plugin.Transient(errors.New("injected part failure"))
	case "partfailafter1":
		if puts >= 2 {
			return storage.PartResult{}, plugin.Transient(errors.New("injected later part failure"))
		}
	case "partfailafter2":
		if puts >= 3 {
			return storage.PartResult{}, plugin.Transient(errors.New("injected later part failure"))
		}
	}
	data, err := io.ReadAll(io.LimitReader(part.Body, part.Size+1))
	if err != nil {
		return storage.PartResult{}, err
	}
	if int64(len(data)) != part.Size {
		return storage.PartResult{}, fmt.Errorf("part %d carried %d bytes, want %d", part.Number, len(data), part.Size)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	upload, ok := m.uploads[session.UploadID]
	if !ok {
		return storage.PartResult{}, errors.New("no such upload")
	}
	upload.parts[part.Number] = data
	upload.etags[part.Number] = fmt.Sprintf("etag-%d-%d", part.Number, len(data))
	upload.order = append(upload.order, part.Number)
	switch m.mode {
	case "noetag":
		return storage.PartResult{Number: part.Number}, nil
	case "wrongpartnumber":
		return storage.PartResult{Number: part.Number + 1, ETag: upload.etags[part.Number]}, nil
	}
	return storage.PartResult{Number: part.Number, ETag: upload.etags[part.Number]}, nil
}

func (m *memMultipart) CompleteMultipart(ctx context.Context, session storage.MultipartSession, parts []storage.PartResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.mode == "completefail" || m.mode == "smallcompletefail" {
		return plugin.Transient(errors.New("injected complete failure"))
	}
	m.mu.Lock()
	upload, ok := m.uploads[session.UploadID]
	if !ok {
		m.mu.Unlock()
		return errors.New("no such upload")
	}
	order := parts
	var body []byte
	if m.mode == "append" {
		// Concatenate every part ever sent, in arrival order: a re-sent part
		// appears twice.
		numbers := append([]int32(nil), upload.order...)
		sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
		for _, n := range numbers {
			body = append(body, upload.parts[n]...)
		}
	} else {
		for i, p := range order {
			data, ok := upload.parts[p.Number]
			// The noetag mode models an endpoint that tolerates part answers
			// without an ETag at Complete time too, so nothing but the kit's own
			// check stands between such a plugin and a passing suite.
			if !ok || (m.mode != "noetag" && upload.etags[p.Number] != p.ETag) {
				m.mu.Unlock()
				return fmt.Errorf("part %d does not match", p.Number)
			}
			if m.mode == "shrink" && i == len(order)-1 {
				break
			}
			body = append(body, data...)
		}
	}
	delete(m.uploads, session.UploadID)
	m.mu.Unlock()
	m.PutObject(session.Key, body, time.Now())
	return nil
}

func (m *memMultipart) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	if m.mode == "abortfail" {
		return plugin.Transient(errors.New("injected abort failure"))
	}
	m.mu.Lock()
	upload, ok := m.uploads[session.UploadID]
	if !ok {
		m.mu.Unlock()
		return nil // an upload already gone is nothing left to clean
	}
	if m.mode == "keepaborted" || m.mode == "abortkeeps" {
		m.mu.Unlock()
		return nil
	}
	delete(m.uploads, session.UploadID)
	if m.mode != "abortcommit" {
		m.mu.Unlock()
		return nil
	}
	// abortcommit models the endpoint that committed the parts anyway: the
	// upload is gone but its object is there, which the check must refuse.
	numbers := append([]int32(nil), upload.order...)
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	var body []byte
	for _, n := range numbers {
		body = append(body, upload.parts[n]...)
	}
	m.mu.Unlock()
	m.PutObject(session.Key, body, time.Now())
	return nil
}

func (m *memMultipart) StaleMultipart(ctx context.Context, olderThan time.Time) ([]storage.StaleUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.stales++
	stales := m.stales
	m.mu.Unlock()
	switch m.mode {
	case "stalefail":
		return nil, plugin.Transient(errors.New("injected stale failure"))
	case "stalefail2":
		if stales >= 2 {
			return nil, plugin.Transient(errors.New("injected second stale failure"))
		}
	case "stalefail3":
		if stales >= 3 {
			return nil, plugin.Transient(errors.New("injected third stale failure"))
		}
	case "staletwo":
		if stales >= 2 {
			return []storage.StaleUpload{
				{Key: "multipart/stale.bin", UploadID: "one", Initiated: time.Now()},
				{Key: "multipart/stale.bin", UploadID: "two", Initiated: time.Now()},
			}, nil
		}
		return nil, nil
	case "stalewrong":
		if stales >= 2 {
			return []storage.StaleUpload{{Key: "multipart/other.bin", UploadID: "other", Initiated: time.Now()}}, nil
		}
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var stale []storage.StaleUpload
	for id, u := range m.uploads {
		// The keepstale mode lists every open upload whatever the cutoff: what a
		// listing that never filters would do, and what the check must refuse.
		if m.mode == "keepstale" || u.initiated.Before(olderThan) {
			stale = append(stale, storage.StaleUpload{Key: u.key, UploadID: id, Initiated: u.initiated})
		}
	}
	sort.Slice(stale, func(i, j int) bool { return stale[i].Key < stale[j].Key })
	return stale, nil
}

// Stat and Get are overridden so a mode can break the after-commit read-back
// without touching the parts the checks drive.
func (m *memMultipart) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	switch m.mode {
	case "statperm":
		return model.ObjectInfo{}, plugin.Transient(errors.New("injected stat failure"))
	}
	return m.Fake.Stat(ctx, key)
}

func (m *memMultipart) Get(ctx context.Context, key string, dst io.Writer) error {
	switch m.mode {
	case "getfail":
		return plugin.Transient(errors.New("injected get failure"))
	case "getwrong", "smallwrongget":
		_, err := dst.Write([]byte("the wrong bytes"))
		return err
	}
	return m.Fake.Get(ctx, key, dst)
}

// testPartSize is the part size these tests drive: the kit's checks only need
// two parts to be distinguishable, and a real service's 5 MiB minimum exists for
// its own SDK, not for the suite. Every place that builds a reference storage
// takes this one constant — two spellings of it desynchronised the size
// RunMultipart asserts and the size the storage reports.
const testPartSize = 64

func TestMultipartSuitePassesTheReference(t *testing.T) {
	RunMultipart(t, testPartSize, func(*testing.T) storage.Storage { return newMemMultipart(testPartSize) })
}

// TestMultipartChecksCatchBrokenStorages: every check must fail an
// implementation that breaks exactly what it is about — a check that cannot fail
// is not a check.
func TestMultipartChecksCatchBrokenStorages(t *testing.T) {
	cases := []struct {
		check string
		mode  string
	}{
		{"multipart: parts in order complete the object", "noetag"},
		{"multipart: parts in order complete the object", "shrink"},
		{"multipart: parts in order complete the object", "beginfail"},
		{"multipart: parts in order complete the object", "wrongpartsize"},
		{"multipart: parts in order complete the object", "partfail"},
		{"multipart: parts in order complete the object", "wrongpartnumber"},
		{"multipart: parts in order complete the object", "statperm"},
		{"multipart: parts in order complete the object", "getfail"},
		{"multipart: parts in order complete the object", "getwrong"},
		{"multipart: parts in order complete the object", "completefail"},
		{"multipart: a re-sent part replaces the earlier one", "append"},
		{"multipart: a re-sent part replaces the earlier one", "beginfail"},
		{"multipart: a re-sent part replaces the earlier one", "partfail"},
		{"multipart: a re-sent part replaces the earlier one", "partfailafter1"},
		{"multipart: a re-sent part replaces the earlier one", "completefail"},
		{"multipart: abort leaves nothing behind", "keepaborted"},
		{"multipart: abort leaves nothing behind", "abortcommit"},
		{"multipart: abort leaves nothing behind", "beginfail"},
		{"multipart: abort leaves nothing behind", "partfailafter1"},
		{"multipart: abort leaves nothing behind", "abortfail"},
		{"multipart: abort leaves nothing behind", "statperm"},
		{"multipart: abort leaves nothing behind", "beginfail2"},
		{"multipart: abort leaves nothing behind", "partfailafter2"},
		{"multipart: abort leaves nothing behind", "completefail"},
		{"multipart: an object inside one part is the plugin's own answer", "beginfail"},
		{"multipart: an object inside one part is the plugin's own answer", "smallbeginfail"},
		{"multipart: an object inside one part is the plugin's own answer", "smallpartfail"},
		{"multipart: an object inside one part is the plugin's own answer", "smallcompletefail"},
		{"multipart: an object inside one part is the plugin's own answer", "smallwrongget"},
		{"multipart: an upload is listed as stale only before the cutoff", "keepstale"},
		{"multipart: an upload is listed as stale only before the cutoff", "beginfail"},
		{"multipart: an upload is listed as stale only before the cutoff", "stalefail"},
		{"multipart: an upload is listed as stale only before the cutoff", "stalefail2"},
		{"multipart: an upload is listed as stale only before the cutoff", "stalefail3"},
		{"multipart: an upload is listed as stale only before the cutoff", "staletwo"},
		{"multipart: an upload is listed as stale only before the cutoff", "stalewrong"},
		{"multipart: an upload is listed as stale only before the cutoff", "abortfail"},
		{"multipart: an upload is listed as stale only before the cutoff", "abortkeeps"},
	}
	for _, tc := range cases {
		t.Run(tc.check+"/"+tc.mode, func(t *testing.T) {
			s := newMemMultipart(testPartSize)
			s.mode = tc.mode
			for _, c := range MultipartChecks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, s, s.partSize); err == nil {
					t.Fatalf("check %q passed a storage broken by %q", c.Name, tc.mode)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// TestMultipartRequiresTheCapability: a storage that does not implement
// storage.MultipartUploader must be refused by the kit's own lookup, so a plugin
// that silently loses the capability cannot pass the multipart suite.
func TestMultipartRequiresTheCapability(t *testing.T) {
	var only storage.Storage = NewFake()
	if _, err := multipartOf(only); err == nil {
		t.Fatal("a storage without the capability was accepted")
	}
	if _, err := multipartOf(newMemMultipart(testPartSize)); err != nil {
		t.Fatalf("the reference implementation was refused: %v", err)
	}
	// Every check must refuse such a storage rather than silently skip it.
	for _, c := range MultipartChecks {
		if err := c.Run(t, only, 5<<20); err == nil {
			t.Errorf("check %q passed a storage without the capability", c.Name)
		}
	}
}
