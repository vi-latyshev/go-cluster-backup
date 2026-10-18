// Package storagetest is the conformance suite every storage implementation runs,
// plus the fault-injecting in-memory storage the engine's failure tests script,
// plus the live-location guard (ConformanceLocation) every storage plugin's
// live cleanup entry point resolves its target through. A storage that implements
// the optional storage.MultipartUploader capability proves that too, through
// RunMultipart — the capability's own checks.
//
// A step-2 storage package proves itself with:
//
//	storagetest.Run(t, func(t *testing.T) storage.Storage { return newTestStorage(t, server) })
//
// Each check is a function returning an error, so the suite itself is tested
// against deliberately broken implementations — a check that cannot fail is
// not a check.
package storagetest

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

type Check struct {
	Name string
	Run  func(t *testing.T, s storage.Storage) error
}

var Checks = []Check{
	{"describe", checkDescribe},
	{"put stat get", checkPutStatGet},
	{"stat of absent key is not_found", checkStatAbsent},
	{"delete is idempotent", checkDelete},
	{"list pages every object", checkList},
	{"checksums per declared traits", checkChecksums},
	{"returns on cancellation", checkCancel},
}

// fatalReporter is what the kit's one failure path needs from a test handle:
// *testing.T satisfies it, and the kit's own tests drive that path with a
// recorder — a live *testing.T cannot survive asserting its own failure, which
// is why the check-failure report is a function of its own rather than a
// t.Fatal inlined into the runner's callback.
type fatalReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// Run executes every check against a freshly built, empty storage instance.
func Run(t *testing.T, newFn func(t *testing.T) storage.Storage) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			reportCheck(t, c, c.Run(t, newFn(t)))
		})
	}
}

// reportCheck fails the test when a check reported a problem, naming the check
// the problem came from.
func reportCheck(t fatalReporter, c Check, err error) {
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
}

const cancelDeadline = 5 * time.Second

func checkDescribe(_ *testing.T, s storage.Storage) error {
	tr, err := s.Describe(context.Background())
	if err != nil {
		return fmt.Errorf("Describe: %w", err)
	}
	for _, c := range tr.Checksums {
		if c != "sha256" && c != "md5" {
			return fmt.Errorf("Describe: unknown checksum %q", c)
		}
	}
	return nil
}

func checkPutStatGet(_ *testing.T, s storage.Storage) error {
	ctx := context.Background()
	want := []byte("conformance: one object, written and read back")
	key := "conformance/put-stat-get.bin"
	info, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(want)), SHA256: model.SHA256Hex(want)},
		bytes.NewReader(want))
	if err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if info.Key != key {
		return fmt.Errorf("Put returned key %q, want %q", info.Key, key)
	}
	st, err := s.Stat(ctx, key)
	if err != nil {
		return fmt.Errorf("Stat after Put: %w", err)
	}
	if st.Size != int64(len(want)) {
		return fmt.Errorf("Stat size %d, want %d", st.Size, len(want))
	}
	if st.ModTime.IsZero() {
		return fmt.Errorf("Stat ModTime is zero")
	}
	var got bytes.Buffer
	if err := s.Get(ctx, key, &got); err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		return fmt.Errorf("Get returned %d bytes, want %d equal bytes", got.Len(), len(want))
	}
	return nil
}

func checkStatAbsent(_ *testing.T, s storage.Storage) error {
	_, err := s.Stat(context.Background(), "conformance/definitely-absent")
	if err == nil {
		return fmt.Errorf("Stat of an absent key returned no error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		return fmt.Errorf("Stat of an absent key returned class %q (want not_found): %w", class, err)
	}
	return nil
}

func checkDelete(_ *testing.T, s storage.Storage) error {
	ctx := context.Background()
	key := "conformance/delete-me.bin"
	want := []byte("delete me")
	if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(want)), SHA256: model.SHA256Hex(want)},
		bytes.NewReader(want)); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}
	if _, err := s.Stat(ctx, key); err == nil {
		return fmt.Errorf("Stat after Delete returned no error")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		return fmt.Errorf("Stat after Delete returned class %q (want not_found): %w", class, err)
	}
	if err := s.Delete(ctx, key); err != nil {
		return fmt.Errorf("second Delete of an absent key must be success, got: %w", err)
	}
	return nil
}

func checkList(_ *testing.T, s storage.Storage) error {
	ctx := context.Background()
	const n = 5
	want := map[string]bool{}
	for i := range n {
		key := fmt.Sprintf("conformance/list/object-%d", i)
		want[key] = true
		body := []byte(key)
		if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body)), SHA256: model.SHA256Hex(body)},
			bytes.NewReader(body)); err != nil {
			return fmt.Errorf("Put %s: %w", key, err)
		}
	}
	got := map[string]int{}
	token := ""
	for page := 0; ; page++ {
		if page > 50 {
			return fmt.Errorf("listing did not terminate after 50 pages")
		}
		res, err := s.List(ctx, storage.ListRequest{Prefix: "conformance/list/", Token: token})
		if err != nil {
			return fmt.Errorf("List page %d: %w", page, err)
		}
		for _, o := range res.Objects {
			got[o.Key]++
		}
		if res.NextToken == "" {
			break
		}
		if res.NextToken == token {
			return fmt.Errorf("page %d returned the same token %q", page, token)
		}
		token = res.NextToken
	}
	for key := range want {
		if got[key] == 0 {
			return fmt.Errorf("listing never returned %s", key)
		}
		if got[key] > 1 {
			return fmt.Errorf("listing returned %s %d times", key, got[key])
		}
	}
	res, err := s.List(ctx, storage.ListRequest{Prefix: "conformance/list/object-3"})
	if err != nil {
		return fmt.Errorf("prefixed List: %w", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].Key != "conformance/list/object-3" {
		return fmt.Errorf("prefixed List returned %+v", res.Objects)
	}
	return nil
}

func checkChecksums(_ *testing.T, s storage.Storage) error {
	ctx := context.Background()
	tr, err := s.Describe(ctx)
	if err != nil {
		return fmt.Errorf("Describe: %w", err)
	}
	want := []byte("conformance: checksum me")
	key := "conformance/checksum.bin"
	if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(want)), SHA256: model.SHA256Hex(want)},
		bytes.NewReader(want)); err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	st, err := s.Stat(ctx, key)
	if err != nil {
		return fmt.Errorf("Stat: %w", err)
	}
	for _, c := range tr.Checksums {
		switch c {
		case "sha256":
			if st.SHA256 != model.SHA256Hex(want) {
				return fmt.Errorf("declared sha256, Stat returned %q", st.SHA256)
			}
		case "md5":
			if st.MD5 != md5Hex(want) {
				return fmt.Errorf("declared md5, Stat returned %q", st.MD5)
			}
		}
	}
	return nil
}

func checkCancel(t *testing.T, s storage.Storage) error {
	body := []byte("cancel me")
	ops := []struct {
		name string
		fn   func(ctx context.Context) error
	}{
		{"Put", func(ctx context.Context) error {
			_, err := s.Put(ctx, storage.PutRequest{Key: "conformance/cancel.bin", Size: int64(len(body))},
				bytes.NewReader(body))
			return err
		}},
		{"Stat", func(ctx context.Context) error { _, err := s.Stat(ctx, "conformance/cancel.bin"); return err }},
		{"List", func(ctx context.Context) error {
			_, err := s.List(ctx, storage.ListRequest{Prefix: "conformance/"})
			return err
		}},
		{"Delete", func(ctx context.Context) error { return s.Delete(ctx, "conformance/cancel.bin") }},
		{"Get", func(ctx context.Context) error { return s.Get(ctx, "conformance/cancel.bin", io.Discard) }},
	}
	for _, op := range ops {
		if err := plugintest.MustReturnWithError(t, cancelDeadline, op.name, op.fn); err != nil {
			return err
		}
	}
	return nil
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
