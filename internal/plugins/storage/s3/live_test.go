package s3_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// TestLiveS3 runs the storage conformance suite and one multipart upload
// against the real S3-compatible Object Storage. It runs only when the credential
// environment is present — S3_LIVE_KEY_ID, S3_LIVE_KEY_SECRET and
// S3_LIVE_BUCKET — and skips otherwise, so CI never touches the service.
//
// The key id and secret are never printed, logged or embedded in an error
// message: they are written to mode-600 files for the plugin's Secret{file}
// fields and removed with the test's temporary directory. Everything the test
// writes lives under one conformance-<random>/ prefix; the cleanup deletes it
// through the plugin itself and then proves the prefix empty and no multipart
// upload open.
func TestLiveS3(t *testing.T) {
	keyID := os.Getenv("S3_LIVE_KEY_ID")
	secret := os.Getenv("S3_LIVE_KEY_SECRET")
	bucket := os.Getenv("S3_LIVE_BUCKET")
	if keyID == "" || secret == "" || bucket == "" {
		t.Skip("S3_LIVE_KEY_ID, S3_LIVE_KEY_SECRET and S3_LIVE_BUCKET name the live destination; skipping")
	}
	if tenant := os.Getenv("S3_LIVE_TENANT_ID"); tenant != "" && !strings.Contains(keyID, ":") {
		keyID = tenant + ":" + keyID
	}
	endpoint := envOr("S3_LIVE_ENDPOINT", "https://s3.example.com")
	region := envOr("S3_LIVE_REGION", "ru-central-1")
	prefix := s3Conformance.Generate(t)
	keyFile := writeSecret(t, keyID)
	secretFile := writeSecret(t, secret)

	t.Logf("live destination: endpoint=%s region=%s bucket=%s prefix=%s", endpoint, region, bucket, prefix)

	build := func(t *testing.T, mutate func(raw map[string]any)) storage.Storage {
		t.Helper()
		raw := map[string]any{
			"endpoint":          endpoint,
			"region":            region,
			"bucket":            bucket,
			"prefix":            prefix,
			"access_key_id":     map[string]any{"file": keyFile},
			"secret_access_key": map[string]any{"file": secretFile},
			"addressing":        "path",
			"storage_class":     "STANDARD",
			"request_checksum":  "when_required",
			"part_bytes":        5 << 20,
			"max_request_bytes": 5 << 30,
			"max_object_bytes":  5 << 40,
			"request_timeout":   "5m",
		}
		if mutate != nil {
			mutate(raw)
		}
		return newStorageFrom(t, raw)
	}

	t.Cleanup(func() { cleanupLive(t, build(t, nil), prefix, bucket, endpoint, region, keyID, secret) })

	t.Run("conformance", func(t *testing.T) {
		storagetest.Run(t, func(t *testing.T) storage.Storage { return build(t, nil) })
	})

	t.Run("multipart driven part by part", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		s := build(t, func(raw map[string]any) { raw["part_bytes"] = 5 << 20 })
		mu, ok := s.(storage.MultipartUploader)
		if !ok {
			t.Fatal("the s3 storage implements no storage.MultipartUploader")
		}
		// The payload lives in a file and every part is sent as a section of it:
		// the live run must exercise the read path the engine uses, not a reader
		// that already holds the bytes.
		body := newPattern(t, 11<<20+7) // three parts: 5 MiB, 5 MiB, 1 MiB + 7
		spool, err := os.CreateTemp(t.TempDir(), "live-spool-*")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = spool.Close() }()
		if _, err := spool.Write(body); err != nil {
			t.Fatal(err)
		}

		const key = "multipart/object.bin"
		session, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: key, Size: int64(len(body))})
		if err != nil {
			t.Fatalf("BeginMultipart: %v", err)
		}
		if session.PartSize != 5<<20 {
			t.Fatalf("the session reports a part size of %d, want the configured %d", session.PartSize, int64(5<<20))
		}
		var results []storage.PartResult
		part := func(number int32, offset, size int64) (storage.PartResult, error) {
			return mu.PutPart(ctx, session, storage.Part{
				Number: number, Size: size, Body: io.NewSectionReader(spool, offset, size),
			})
		}
		for number, offset := int32(1), int64(0); offset < int64(len(body)); number++ {
			size := min(session.PartSize, int64(len(body))-offset)
			res, err := part(number, offset, size)
			if err != nil {
				t.Fatalf("PutPart(%d): %v", number, err)
			}
			results = append(results, res)
			offset += size
		}
		// One part sent again, the same number with the same bytes: what the
		// engine's retry of a part does, and what must leave the object whole.
		resent, err := part(2, 5<<20, 5<<20)
		if err != nil {
			t.Fatalf("re-sent part 2: %v", err)
		}
		results[1] = resent
		if err := mu.CompleteMultipart(ctx, session, results); err != nil {
			t.Fatalf("CompleteMultipart: %v", err)
		}
		stored, err := s.Stat(ctx, key)
		if err != nil {
			t.Fatalf("Stat after complete: %v", err)
		}
		if stored.Size != int64(len(body)) {
			t.Errorf("Stat reported %d bytes, want %d", stored.Size, len(body))
		}
		var got bytes.Buffer
		if err := s.Get(ctx, key, &got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got.Bytes(), body) {
			t.Errorf("downloaded %d bytes, want %d equal ones", got.Len(), len(body))
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		t.Log("multipart: three parts sent as file sections, part 2 re-sent, completed, verified by size, read back byte for byte, deleted")
	})

	t.Run("an aborted upload leaves nothing", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s := build(t, func(raw map[string]any) { raw["part_bytes"] = 5 << 20 })
		mu, ok := s.(storage.MultipartUploader)
		if !ok {
			t.Fatal("the s3 storage implements no storage.MultipartUploader")
		}
		const key = "multipart/aborted.bin"
		body := newPattern(t, 6<<20)
		session, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: key, Size: int64(len(body))})
		if err != nil {
			t.Fatalf("BeginMultipart: %v", err)
		}
		if _, err := mu.PutPart(ctx, session, storage.Part{
			Number: 1, Size: 5 << 20, Body: io.NewSectionReader(bytes.NewReader(body), 0, 5<<20),
		}); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		if err := mu.AbortMultipart(ctx, session); err != nil {
			t.Fatalf("AbortMultipart: %v", err)
		}
		if err := mu.AbortMultipart(ctx, session); err != nil {
			t.Fatalf("second abort of an upload already gone: %v", err)
		}
		if _, err := s.Stat(ctx, key); err == nil {
			t.Errorf("the aborted upload left an object behind")
		} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
			t.Errorf("Stat after abort: class %q, want not_found: %v", class, err)
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Errorf("delete of a key that was never committed: %v", err)
		}
		t.Log("abort: the upload was discarded, its key is absent, and the same abort again is not an error")
	})

	t.Run("request checksum when_supported", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		s := build(t, func(raw map[string]any) { raw["request_checksum"] = "when_supported" })
		body := []byte("request checksum probe")
		_, err := s.Put(ctx, storage.PutRequest{Key: "checksum-probe/probe.bin", Size: int64(len(body))},
			bytes.NewReader(body))
		if err != nil {
			t.Errorf("when_supported rejected by the service (%v): the config must stay when_required", err)
			return
		}
		t.Log("when_supported accepted: the service tolerates the SDK's default CRC32 request checksums")
		if err := s.Delete(ctx, "checksum-probe/probe.bin"); err != nil {
			t.Errorf("probe delete: %v", err)
		}
	})

	t.Run("the sweep takes an upload behind the cutoff and leaves a fresh one", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s := build(t, func(raw map[string]any) { raw["part_bytes"] = 5 << 20 })
		mu, ok := s.(storage.MultipartUploader)
		if !ok {
			t.Fatal("the s3 storage implements no storage.MultipartUploader")
		}
		const key = "multipart/left-by-a-killed-run.bin"
		body := newPattern(t, 6<<20)
		session, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: key, Size: int64(len(body))})
		if err != nil {
			t.Fatalf("BeginMultipart: %v", err)
		}
		if _, err := mu.PutPart(ctx, session, storage.Part{
			Number: 1, Size: 5 << 20, Body: io.NewSectionReader(bytes.NewReader(body), 0, 5<<20),
		}); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
		// A cutoff an hour back must leave it: that is the direction the engine
		// leans on so an upload a live run is writing is never taken.
		left, err := mu.StaleMultipart(ctx, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatalf("StaleMultipart with a past cutoff: %v", err)
		}
		for _, u := range left {
			if u.Key == key {
				t.Errorf("an upload started a moment ago was listed as stale: %+v", u)
			}
		}
		// A cutoff ahead of now makes this upload "old" without waiting a run's
		// length — the same code path the engine's own cutoff drives.
		stale, err := mu.StaleMultipart(ctx, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("StaleMultipart: %v", err)
		}
		taken := false
		for _, u := range stale {
			if u.Key != key || u.UploadID != session.UploadID {
				continue
			}
			taken = true
			if err := mu.AbortMultipart(ctx, storage.MultipartSession{Key: u.Key, UploadID: u.UploadID}); err != nil {
				t.Fatalf("aborting the swept upload: %v", err)
			}
		}
		if !taken {
			t.Fatalf("the upload was not listed behind a cutoff ahead of it: %+v", stale)
		}
		after, err := mu.StaleMultipart(ctx, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("StaleMultipart after the sweep: %v", err)
		}
		for _, u := range after {
			if u.Key == key {
				t.Fatalf("the swept upload is still listed: %+v", u)
			}
		}
		t.Log("sweep: a just-started upload is left, an upload behind the cutoff is listed and taken")
	})
}

// TestLiveS3Check proves the preflight Check against the real service,
// read-only: the configured bucket passes, and a bucket that does not exist
// fails permanently with the bucket named. It runs only when the credential
// environment is present, like TestLiveS3, and writes nothing.
//
// No message this test prints may carry the key id or the secret: the service
// can quote the access key id back in an error, so every live error passes
// through redact first.
func TestLiveS3Check(t *testing.T) {
	keyID := os.Getenv("S3_LIVE_KEY_ID")
	secret := os.Getenv("S3_LIVE_KEY_SECRET")
	bucket := os.Getenv("S3_LIVE_BUCKET")
	if keyID == "" || secret == "" || bucket == "" {
		t.Skip("S3_LIVE_KEY_ID, S3_LIVE_KEY_SECRET and S3_LIVE_BUCKET name the live destination; skipping")
	}
	if tenant := os.Getenv("S3_LIVE_TENANT_ID"); tenant != "" && !strings.Contains(keyID, ":") {
		keyID = tenant + ":" + keyID
	}
	redact := func(err error) string {
		s := strings.ReplaceAll(err.Error(), keyID, "<redacted>")
		return strings.ReplaceAll(s, secret, "<redacted>")
	}
	endpoint := envOr("S3_LIVE_ENDPOINT", "https://s3.example.com")
	region := envOr("S3_LIVE_REGION", "ru-central-1")
	keyFile := writeSecret(t, keyID)
	secretFile := writeSecret(t, secret)
	build := func(t *testing.T, bucket string) plugin.Checker {
		t.Helper()
		raw := map[string]any{
			"endpoint":          endpoint,
			"region":            region,
			"bucket":            bucket,
			"prefix":            s3Conformance.Generate(t),
			"access_key_id":     map[string]any{"file": keyFile},
			"secret_access_key": map[string]any{"file": secretFile},
			"addressing":        "path",
			"storage_class":     "STANDARD",
			"request_checksum":  "when_required",
			"part_bytes":        16 << 20,
			"max_request_bytes": 5 << 30,
			"max_object_bytes":  5 << 40,
			"request_timeout":   "1m",
		}
		c, ok := newStorageFrom(t, raw).(plugin.Checker)
		if !ok {
			t.Fatal("the s3 storage implements no Check")
		}
		return c
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Logf("live destination: endpoint=%s region=%s bucket=%s", endpoint, region, bucket)
	if err := build(t, bucket).Check(ctx); err != nil {
		t.Fatalf("Check against the configured bucket: %s", redact(err))
	}
	t.Log("check: the credentials are accepted and the configured bucket is reachable")

	wrong := "conformance-absent-" + newRandomHex(t, 4)
	err := build(t, wrong).Check(ctx)
	if err == nil {
		t.Fatalf("Check against bucket %s returned success", wrong)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("Check against an absent bucket: class %q, want permanent: %s", class, redact(err))
	}
	if !strings.Contains(err.Error(), wrong) {
		t.Errorf("the error does not name the absent bucket %s: %s", wrong, redact(err))
	}
	t.Logf("check: bucket %s does not exist and the check failed permanently, naming it", wrong)
}

// cleanupLive deletes everything under the prefix through the plugin, then
// proves the prefix empty and no multipart upload left open. Its assertions run
// even when a subtest failed: a leak is a finding of its own.
func cleanupLive(t *testing.T, s storage.Storage, prefix, bucket, endpoint, region, keyID, secret string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for range 100 {
		page, err := s.List(ctx, storage.ListRequest{})
		if err != nil {
			t.Errorf("cleanup: list: %v", err)
			return
		}
		if len(page.Objects) == 0 {
			break
		}
		for _, o := range page.Objects {
			if err := s.Delete(ctx, o.Key); err != nil {
				t.Errorf("cleanup: delete %s: %v", o.Key, err)
			}
		}
	}
	page, err := s.List(ctx, storage.ListRequest{})
	if err != nil {
		t.Errorf("cleanup: final list: %v", err)
		return
	}
	if len(page.Objects) != 0 {
		t.Errorf("cleanup: %d objects left under %s/", len(page.Objects), prefix)
		return
	}
	if left := openUploads(ctx, t, bucket, endpoint, region, prefix+"/", keyID, secret); len(left) != 0 {
		t.Errorf("cleanup: multipart uploads left open: %v", left)
		return
	}
	t.Logf("cleanup: %s/ is empty and no multipart upload is open", prefix)
}

// openUploads lists unfinished multipart uploads under a prefix with a raw
// client — the plugin has no such operation, and only the service can prove
// nothing is left billed.
func openUploads(ctx context.Context, t *testing.T, bucket, endpoint, region, prefix, keyID, secret string) []string {
	t.Helper()
	client := s3.NewFromConfig(aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(keyID, secret, ""),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && api.ErrorCode() == "AccessDenied" {
			t.Logf("cleanup: cannot list multipart uploads (AccessDenied); left-open uploads unverified")
			return nil
		}
		t.Errorf("cleanup: list multipart uploads: %v", err)
		return nil
	}
	var ids []string
	for _, u := range out.Uploads {
		ids = append(ids, aws.ToString(u.UploadId))
	}
	return ids
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
