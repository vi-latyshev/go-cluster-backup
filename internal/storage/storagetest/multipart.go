package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// MultipartCheck is one check of the multipart capability. It differs from Check
// in one argument: the part size the plugin was configured with, which every
// payload of the check is built from. Like every check it returns an error rather
// than failing the test itself, so the suite can be tested against deliberately
// broken implementations.
type MultipartCheck struct {
	Name string
	Run  func(t *testing.T, s storage.Storage, partSize int64) error
}

// MultipartChecks are the conformance checks of the storage.MultipartUploader
// capability — the optional half of the storage kind. The checks drive the
// capability exactly the way the engine does, one call per attempt and parts in
// order, so what they prove is what the engine leans on.
var MultipartChecks = []MultipartCheck{
	{"multipart: parts in order complete the object", checkMultipartRoundTrip},
	{"multipart: a re-sent part replaces the earlier one", checkMultipartResend},
	{"multipart: abort leaves nothing behind", checkMultipartAbort},
	{"multipart: an object inside one part is the plugin's own answer", checkMultipartSmallObject},
	{"multipart: an upload is listed as stale only before the cutoff", checkMultipartStale},
}

// partSizeLimit bounds the part size the kit can drive in memory: a plugin whose
// parts are larger than this needs a smaller part_bytes to be conformance tested,
// and is refused here rather than trusted.
const partSizeLimit = 64 << 20

// RunMultipart executes the multipart capability checks against a freshly built
// storage instance whose configured part size is partSize: every session the
// checks open must report that size back, and the payloads are built from it.
func RunMultipart(t *testing.T, partSize int64, newFn func(t *testing.T) storage.Storage) {
	t.Helper()
	reportPartSize(t, partSize)
	for _, c := range MultipartChecks {
		t.Run(c.Name, func(t *testing.T) {
			reportMultipartCheck(t, c, c.Run(t, newFn(t), partSize))
		})
	}
}

// reportPartSize fails the test when the kit cannot drive the configured part
// size in memory.
func reportPartSize(t fatalReporter, partSize int64) {
	if partSize <= 0 || partSize > partSizeLimit {
		t.Fatalf("the configured part size %d is outside the kit's range of 1..%d bytes", partSize, int64(partSizeLimit))
	}
}

// reportMultipartCheck fails the test when a capability check reported a problem.
func reportMultipartCheck(t fatalReporter, c MultipartCheck, err error) {
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
}

// multipartOf is the capability itself: a plugin registered as multipart-capable
// must implement it, and a check running against a storage that does not fails —
// an instance that silently lost the capability is a broken plugin, not a skip.
func multipartOf(s storage.Storage) (storage.MultipartUploader, error) {
	mu, ok := s.(storage.MultipartUploader)
	if !ok {
		return nil, fmt.Errorf("%T does not implement storage.MultipartUploader", s)
	}
	return mu, nil
}

// beginOne opens an upload for size bytes and checks the facts every caller
// depends on: the size the caller chose is above the plugin's own part size (so
// the answer is a session, not errors.ErrUnsupported) and the part size reported
// back is the configured one.
func beginOne(mu storage.MultipartUploader, key string, size, partSize int64) (storage.MultipartSession, error) {
	session, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: key, Size: size})
	if err != nil {
		return storage.MultipartSession{}, fmt.Errorf("BeginMultipart(%d bytes): %w", size, err)
	}
	if session.PartSize != partSize {
		return storage.MultipartSession{}, fmt.Errorf("BeginMultipart reported a part size of %d, want the configured %d", session.PartSize, partSize)
	}
	return session, nil
}

// multipartBody is a deterministic payload: whichever part a check reads back can
// be compared with the same function.
func multipartBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

// partSlices slices body into the parts the engine would send: numbers from 1,
// ascending, each a section of the payload, the last one short when the size does
// not divide.
func partSlices(body []byte, partSize int64) []storage.Part {
	var parts []storage.Part
	for offset, number := int64(0), int32(1); offset < int64(len(body)); number++ {
		size := min(partSize, int64(len(body))-offset)
		parts = append(parts, storage.Part{
			Number: number,
			Size:   size,
			Body:   io.NewSectionReader(bytes.NewReader(body), offset, size),
		})
		offset += size
	}
	return parts
}

// sendParts sends parts in order and checks what the engine leans on: the result
// names the part it was sent for and carries the service's handle for it.
func sendParts(mu storage.MultipartUploader, session storage.MultipartSession, parts []storage.Part) ([]storage.PartResult, error) {
	results := make([]storage.PartResult, 0, len(parts))
	for _, p := range parts {
		res, err := mu.PutPart(context.Background(), session, p)
		if err != nil {
			return nil, fmt.Errorf("PutPart(%d): %w", p.Number, err)
		}
		if res.Number != p.Number {
			return nil, fmt.Errorf("PutPart(%d) answered for part %d", p.Number, res.Number)
		}
		if res.ETag == "" {
			return nil, fmt.Errorf("PutPart(%d) answered without an ETag, so the part cannot be committed", p.Number)
		}
		results = append(results, res)
	}
	return results, nil
}

// readBack proves what was committed: the storage reports the size and hands back
// exactly the bytes that were sent.
func readBack(s storage.Storage, key string, want []byte) error {
	ctx := context.Background()
	info, err := s.Stat(ctx, key)
	if err != nil {
		return fmt.Errorf("Stat: %w", err)
	}
	if info.Size != int64(len(want)) {
		return fmt.Errorf("Stat reports %d bytes, want %d", info.Size, len(want))
	}
	var got bytes.Buffer
	if err := s.Get(ctx, key, &got); err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		return fmt.Errorf("Get returned %d bytes, want the %d uploaded ones equal", got.Len(), len(want))
	}
	return nil
}

func checkMultipartRoundTrip(_ *testing.T, s storage.Storage, partSize int64) error {
	mu, err := multipartOf(s)
	if err != nil {
		return err
	}
	const key = "multipart/round-trip.bin"
	// Two full parts plus an odd tail: the shape every real upload ends on, and
	// the shape the slicing below is built from — the payload size and the part
	// size come from the same two numbers, so it is always three parts.
	body := multipartBody(int(2*partSize) + 7)
	session, err := beginOne(mu, key, int64(len(body)), partSize)
	if err != nil {
		return err
	}
	parts := partSlices(body, session.PartSize)
	results, err := sendParts(mu, session, parts)
	if err != nil {
		return err
	}
	if err := mu.CompleteMultipart(context.Background(), session, results); err != nil {
		return fmt.Errorf("CompleteMultipart: %w", err)
	}
	return readBack(s, key, body)
}

func checkMultipartResend(_ *testing.T, s storage.Storage, partSize int64) error {
	mu, err := multipartOf(s)
	if err != nil {
		return err
	}
	const key = "multipart/resend.bin"
	body := multipartBody(int(partSize) + 7)
	session, err := beginOne(mu, key, int64(len(body)), partSize)
	if err != nil {
		return err
	}
	parts := partSlices(body, session.PartSize)

	// The same part number twice with different bytes: the second one is what the
	// service must hold at Complete time. That is exactly what the engine's retry
	// of one part does.
	wrong := make([]byte, parts[0].Size)
	for i := range wrong {
		wrong[i] = 0xAA
	}
	if _, err := mu.PutPart(context.Background(), session, storage.Part{
		Number: parts[0].Number, Size: parts[0].Size, Body: bytes.NewReader(wrong),
	}); err != nil {
		return fmt.Errorf("first PutPart: %w", err)
	}
	results, err := sendParts(mu, session, parts)
	if err != nil {
		return err
	}
	if err := mu.CompleteMultipart(context.Background(), session, results); err != nil {
		return fmt.Errorf("CompleteMultipart: %w", err)
	}
	return readBack(s, key, body)
}

func checkMultipartAbort(_ *testing.T, s storage.Storage, partSize int64) error {
	ctx := context.Background()
	mu, err := multipartOf(s)
	if err != nil {
		return err
	}
	const key = "multipart/aborted.bin"
	body := multipartBody(int(partSize) + 7)

	session, err := beginOne(mu, key, int64(len(body)), partSize)
	if err != nil {
		return err
	}
	results, err := sendParts(mu, session, partSlices(body, session.PartSize))
	if err != nil {
		return err
	}
	if err := mu.AbortMultipart(ctx, session); err != nil {
		return fmt.Errorf("AbortMultipart: %w", err)
	}
	if err := mu.CompleteMultipart(ctx, session, results); err == nil {
		return errors.New("CompleteMultipart succeeded after the upload was aborted")
	}
	if _, err := s.Stat(ctx, key); err == nil {
		return errors.New("the aborted upload left an object behind")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		return fmt.Errorf("Stat after abort returned class %q (want not_found): %w", class, err)
	}
	// Nothing of the aborted upload may block the key: a fresh upload of the same
	// object must go through.
	fresh, err := beginOne(mu, key, int64(len(body)), partSize)
	if err != nil {
		return err
	}
	freshResults, err := sendParts(mu, fresh, partSlices(body, fresh.PartSize))
	if err != nil {
		return err
	}
	if err := mu.CompleteMultipart(ctx, fresh, freshResults); err != nil {
		return fmt.Errorf("CompleteMultipart after an abort: %w", err)
	}
	return readBack(s, key, body)
}

// checkMultipartSmallObject: an object that fits one request may be answered with
// errors.ErrUnsupported — the decision the engine reads as "use Put" — or with a
// session, which must then upload it correctly. Both answers are conforming; a
// session that cannot commit a one-part object is not.
func checkMultipartSmallObject(_ *testing.T, s storage.Storage, _ int64) error {
	ctx := context.Background()
	mu, err := multipartOf(s)
	if err != nil {
		return err
	}
	const key = "multipart/small.bin"
	body := []byte("x")
	session, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: key, Size: int64(len(body))})
	if errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("BeginMultipart of a one-byte object: %w", err)
	}
	results, err := sendParts(mu, session, partSlices(body, session.PartSize))
	if err != nil {
		return err
	}
	if err := mu.CompleteMultipart(ctx, session, results); err != nil {
		return fmt.Errorf("CompleteMultipart of a one-byte object: %w", err)
	}
	return readBack(s, key, body)
}

// checkMultipartStale: what the engine's sweep leans on. An upload started a
// moment ago must not be listed for a cutoff in the past — a run may be writing
// it right now — and once the cutoff is after it was started it must be listed,
// with exactly what AbortMultipart needs; aborting it must take it out of the
// listing for good.
func checkMultipartStale(_ *testing.T, s storage.Storage, partSize int64) error {
	ctx := context.Background()
	mu, err := multipartOf(s)
	if err != nil {
		return err
	}
	const key = "multipart/stale.bin"
	body := multipartBody(int(partSize) + 7)
	session, err := beginOne(mu, key, int64(len(body)), partSize)
	if err != nil {
		return err
	}
	if stale, err := mu.StaleMultipart(ctx, time.Now().Add(-time.Hour)); err != nil {
		return fmt.Errorf("StaleMultipart with a cutoff an hour back: %w", err)
	} else if len(stale) != 0 {
		return fmt.Errorf("a just-started upload was listed as stale under a past cutoff: %+v", stale)
	}
	stale, err := mu.StaleMultipart(ctx, time.Now().Add(time.Hour))
	if err != nil {
		return fmt.Errorf("StaleMultipart with a future cutoff: %w", err)
	}
	if len(stale) != 1 {
		return fmt.Errorf("the open upload appears %d times, want once: %+v", len(stale), stale)
	}
	if stale[0].Key != key || stale[0].UploadID != session.UploadID {
		return fmt.Errorf("the listed upload is %+v, want key %q and upload id %q", stale[0], key, session.UploadID)
	}
	if err := mu.AbortMultipart(ctx, storage.MultipartSession{Key: stale[0].Key, UploadID: stale[0].UploadID}); err != nil {
		return fmt.Errorf("aborting the listed upload: %w", err)
	}
	if stale, err = mu.StaleMultipart(ctx, time.Now().Add(time.Hour)); err != nil {
		return fmt.Errorf("StaleMultipart after the abort: %w", err)
	} else if len(stale) != 0 {
		return fmt.Errorf("the aborted upload is still listed: %+v", stale)
	}
	return nil
}
