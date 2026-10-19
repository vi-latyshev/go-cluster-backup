package s3_test

// Maintenance entry point for the end-to-end verification: removes every
// object under S3_LIVE_CLEANUP_PREFIX through the plugin itself and proves
// the prefix empty and no multipart upload left open. Without the environment
// it skips; nothing outside the named prefix is touched.
//
// The named prefix must be a fresh conformance prefix of the shape the live
// tests generate: the cleanup deletes whatever it is pointed at, so anything
// else — a production prefix, a trailing slash, a path with ".." — is refused
// before a single request leaves the process.

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// s3Conformance is the only key prefix a live harness may write to or delete:
// a fresh prefix of one shape. storagetest.ConformanceLocation holds the one
// definition of the shape, shared with the Yandex Disk plugin's harness.
var s3Conformance = storagetest.NewConformanceLocation("conformance-", 12)

func TestLiveS3Cleanup(t *testing.T) {
	prefix, err := s3Conformance.Resolve(os.Getenv("S3_LIVE_CLEANUP_PREFIX"))
	switch {
	case errors.Is(err, storagetest.ErrLocationUnset):
		t.Skip("S3_LIVE_CLEANUP_PREFIX names the prefix to remove; skipping")
	case err != nil:
		t.Fatalf("%v", err)
	}
	keyID := os.Getenv("S3_LIVE_KEY_ID")
	secret := os.Getenv("S3_LIVE_KEY_SECRET")
	bucket := os.Getenv("S3_LIVE_BUCKET")
	if keyID == "" || secret == "" || bucket == "" {
		t.Skip("S3_LIVE_KEY_ID, S3_LIVE_KEY_SECRET and S3_LIVE_BUCKET name the destination; skipping")
	}
	if tenant := os.Getenv("S3_LIVE_TENANT_ID"); tenant != "" && !strings.Contains(keyID, ":") {
		keyID = tenant + ":" + keyID
	}
	endpoint := envOr("S3_LIVE_ENDPOINT", "https://s3.example.com")
	region := envOr("S3_LIVE_REGION", "ru-central-1")
	keyFile := writeSecret(t, keyID)
	secretFile := writeSecret(t, secret)
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
		"part_bytes":        16 << 20,
		"max_request_bytes": 5 << 30,
		"max_object_bytes":  5 << 40,
		"request_timeout":   "5m",
	}
	s := storage.Storage(newStorageFrom(t, raw))
	cleanupLive(t, s, prefix, bucket, endpoint, region, keyID, secret)
}
