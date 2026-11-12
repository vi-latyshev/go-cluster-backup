package collected_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/collected"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sourcetest"
)

const dataContent = "COLLECTED-VENDOR-SNAPSHOT-BYTES"

// stubEnv is one collector publication: a directory, the data file and its
// digest-only marker.
type stubEnv struct {
	dir    string
	data   string
	marker string
}

func newStub(t *testing.T) *stubEnv {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "collect")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	env := &stubEnv{dir: dir, data: filepath.Join(dir, "vendor.tar"), marker: filepath.Join(dir, "vendor.tar.sha256")}
	env.publish(t, []byte(dataContent), digestOf([]byte(dataContent)))
	return env
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// publish writes the pair as the collector does: data first, then the marker.
func (e *stubEnv) publish(t *testing.T, data []byte, marker string) {
	t.Helper()
	if err := os.WriteFile(e.data, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.marker, []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (e *stubEnv) config() map[string]any {
	return map[string]any{"path": e.data, "ext": ".tar", "max_age": "2h"}
}

func build(t *testing.T, cfg map[string]any) source.Source {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := instances.Build(context.Background(), testRegistry(t), source.Kind, "collected", raw,
		instances.Deps{Instance: "collected", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testRegistry(t *testing.T) *instances.Registry {
	t.Helper()
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, collected.Spec, func(lr *loader.Registry) error {
		return collected.Apply(lr, r)
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func check(t *testing.T, s source.Source) error {
	t.Helper()
	checker, ok := s.(instances.Checker)
	if !ok {
		t.Fatal("collected does not implement plugin.Checker, so this failure would surface mid-run")
	}
	return checker.Check(context.Background())
}

func dump(t *testing.T, s source.Source) ([]byte, error) {
	t.Helper()
	var dst bytes.Buffer
	err := s.Dump(context.Background(), &dst)
	return dst.Bytes(), err
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		return build(t, newStub(t).config())
	})
}

func TestDumpCopiesTheDataExactly(t *testing.T) {
	env := newStub(t)
	got, err := dump(t, build(t, env.config()))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != dataContent {
		t.Fatalf("Dump = %q, want the data file only", got)
	}
}

func TestMarkerShapes(t *testing.T) {
	canonical := digestOf([]byte(dataContent))
	for name, marker := range map[string]string{
		"bare": canonical,
		"LF":   canonical + "\n",
		"CRLF": canonical + "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			env.publish(t, []byte(dataContent), marker)
			got, err := dump(t, build(t, env.config()))
			if err != nil {
				t.Fatalf("the %s marker was refused: %v", name, err)
			}
			if string(got) != dataContent {
				t.Fatalf("Dump = %q", got)
			}
		})
	}
}

// TestMarkerLookAlikesAreRefused: every shape that is not the one canonical
// digest is refused, and nothing of the marker reaches the error.
func TestMarkerLookAlikesAreRefused(t *testing.T) {
	canonical := digestOf([]byte(dataContent))
	cases := map[string]string{
		"GNU escaped filename line": canonical + "  /etc/shadow\n",
		"filename suffix":           canonical + " vendor.tar\n",
		"uppercase":                 strings.ToUpper(canonical),
		"BOM":                       string(rune(0xFEFF)) + canonical,
		"short":                     canonical[:63],
		"long":                      canonical + "a",
		"malformed hex":             "g" + canonical[1:],
		"empty":                     "",
	}
	for name, marker := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			env.publish(t, []byte(dataContent), marker)
			s := build(t, env.config())
			err := check(t, s)
			if err == nil {
				t.Fatalf("%s passed the preflight", name)
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Errorf("%s is %q, want permanent", name, class)
			}
			if strings.Contains(err.Error(), canonical) || strings.Contains(err.Error(), "shadow") {
				t.Fatalf("%s echoed marker bytes: %v", name, err)
			}
		})
	}
}

func TestOversizedMarkerIsRefused(t *testing.T) {
	env := newStub(t)
	env.publish(t, []byte(dataContent), strings.Repeat("a", 4097))
	err := check(t, build(t, env.config()))
	if err == nil {
		t.Fatal("a marker larger than 4096 bytes passed the preflight")
	}
	if !strings.Contains(err.Error(), "larger than 4096") {
		t.Fatalf("the refusal does not name the bound: %v", err)
	}
}

func TestStaleness(t *testing.T) {
	t.Run("stale data", func(t *testing.T) {
		env := newStub(t)
		stale := time.Now().Add(-3 * time.Hour)
		if err := os.Chtimes(env.data, stale, stale); err != nil {
			t.Fatal(err)
		}
		err := check(t, build(t, env.config()))
		if err == nil || !strings.Contains(err.Error(), "max_age") {
			t.Fatalf("a stale data file = %v, want a max_age refusal", err)
		}
	})
	t.Run("stale marker", func(t *testing.T) {
		env := newStub(t)
		stale := time.Now().Add(-3 * time.Hour)
		if err := os.Chtimes(env.marker, stale, stale); err != nil {
			t.Fatal(err)
		}
		err := check(t, build(t, env.config()))
		if err == nil || !strings.Contains(err.Error(), "max_age") {
			t.Fatalf("a stale marker = %v, want a max_age refusal", err)
		}
	})
}

func TestMissingPairIsTransient(t *testing.T) {
	for name, remove := range map[string]string{"data": "data", "marker": "marker"} {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			path := env.data
			if remove == "marker" {
				path = env.marker
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			s := build(t, env.config())
			for _, err := range []error{check(t, s), dumpErr(t, s)} {
				if err == nil {
					t.Fatalf("a missing %s passed", name)
				}
				if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
					t.Errorf("a missing %s is %q, want transient", name, class)
				}
			}
		})
	}
}

func TestConfinedReads(t *testing.T) {
	t.Run("symlink data", func(t *testing.T) {
		env := newStub(t)
		secret := filepath.Join(env.dir, "outside")
		if err := os.WriteFile(secret, []byte("LINKED-SECRET"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(env.data); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secret, env.data); err != nil {
			t.Fatal(err)
		}
		err := check(t, build(t, env.config()))
		if err == nil {
			t.Fatal("a symlinked data file passed the preflight")
		}
		if strings.Contains(err.Error(), "LINKED-SECRET") || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the symlink was followed or reported as missing: %v", err)
		}
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
			t.Fatalf("class = %q, want permanent", class)
		}
	})
	t.Run("symlink marker", func(t *testing.T) {
		env := newStub(t)
		secret := filepath.Join(env.dir, "outside")
		if err := os.WriteFile(secret, []byte("LINKED-SECRET"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(env.marker); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secret, env.marker); err != nil {
			t.Fatal(err)
		}
		if err := check(t, build(t, env.config())); err == nil {
			t.Fatal("a symlinked marker passed the preflight")
		}
	})
	t.Run("FIFO data", func(t *testing.T) {
		env := newStub(t)
		if err := os.Remove(env.data); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(env.data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := check(t, build(t, env.config())); err == nil {
			t.Fatal("a FIFO passed the preflight")
		}
	})
	t.Run("FIFO marker", func(t *testing.T) {
		env := newStub(t)
		if err := os.Remove(env.marker); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(env.marker, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := check(t, build(t, env.config())); err == nil {
			t.Fatal("a FIFO marker passed the preflight")
		}
	})
	t.Run("device data", func(t *testing.T) {
		cfg := map[string]any{"path": os.DevNull, "ext": ".tar", "max_age": "2h"}
		if err := check(t, build(t, cfg)); err == nil {
			t.Fatal("a device passed the preflight")
		}
	})
	t.Run("missing directory", func(t *testing.T) {
		env := newStub(t)
		if err := os.RemoveAll(env.dir); err != nil {
			t.Fatal(err)
		}
		if err := check(t, build(t, env.config())); err == nil {
			t.Fatal("a missing collector directory passed the preflight")
		}
	})
}

func TestChangedDataIsTransientAndRetried(t *testing.T) {
	env := newStub(t)
	old := []byte(dataContent)
	oldMarker := digestOf(old)
	env.publish(t, old, oldMarker)
	newData := []byte(dataContent + "-v2")
	// The collector's publication in flight: the data is renamed into place,
	// the marker still describes the previous pair.
	if err := os.WriteFile(env.data, newData, 0o600); err != nil {
		t.Fatal(err)
	}
	s := build(t, env.config())
	_, err := dump(t, s)
	if err == nil {
		t.Fatal("a marker that does not describe the data was accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if strings.Contains(err.Error(), oldMarker) {
		t.Fatal("the digest reached the error")
	}

	// The marker lands: the same source now reads the finished pair.
	if err := os.WriteFile(env.marker, []byte(digestOf(newData)), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := dump(t, s)
	if err != nil {
		t.Fatalf("the retry after publication failed: %v", err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("Dump = %q, want the published data", got)
	}
}

func TestDumpDetectsAChangeWhileReading(t *testing.T) {
	env := newStub(t)
	change := &changeOnWrite{path: env.data, at: time.Now().Add(-time.Hour)}
	err := build(t, env.config()).Dump(context.Background(), change)
	if err == nil {
		t.Fatal("data that changed while it was read was accepted")
	}
	if !strings.Contains(err.Error(), "changed") {
		t.Fatalf("the refusal does not name the change: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
}

// changeOnWrite restamps the data file's mtime on the first write: the bytes
// stay correct, so this proves the identity check after the copy.
type changeOnWrite struct {
	path    string
	at      time.Time
	changed bool
}

func (w *changeOnWrite) Write(p []byte) (int, error) {
	if !w.changed {
		w.changed = true
		if err := os.Chtimes(w.path, w.at, w.at); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// TestCheckDoesNotHash: the preflight proves shape and freshness only; the
// digest is verified where the bytes stream, so a preflight of a huge collector
// file stays cheap.
func TestCheckDoesNotHash(t *testing.T) {
	env := newStub(t)
	env.publish(t, []byte(dataContent), digestOf([]byte("something else")))
	s := build(t, env.config())
	if err := check(t, s); err != nil {
		t.Fatalf("preflight hashed the data: %v", err)
	}
	if _, err := dump(t, s); err == nil {
		t.Fatal("Dump accepted data that does not match its marker")
	}
}

func TestDescribe(t *testing.T) {
	env := newStub(t)
	traits, err := build(t, env.config()).Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if traits.Ext != ".tar" {
		t.Errorf("Ext = %q, want .tar", traits.Ext)
	}
	if want := "file://" + env.data; traits.Origin != want {
		t.Errorf("Origin = %q, want %q", traits.Origin, want)
	}
	if traits.Credentials {
		t.Fatal("collected claimed credential content; the deployment owns sensitivity and sealing")
	}
	if _, ok := build(t, env.config()).(source.CredentialPolicy); ok {
		t.Fatal("collected published a credential warning exception")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"relative path":    `{"path": "vendor.tar", "ext": ".tar", "max_age": "2h"}`,
		"empty path":       `{"path": "", "ext": ".tar", "max_age": "2h"}`,
		"missing path":     `{"ext": ".tar", "max_age": "2h"}`,
		"empty ext":        `{"path": "/c/vendor.tar", "ext": "", "max_age": "2h"}`,
		"ext without dot":  `{"path": "/c/vendor.tar", "ext": "tar", "max_age": "2h"}`,
		"ext with slash":   `{"path": "/c/vendor.tar", "ext": "/tar", "max_age": "2h"}`,
		"ext with parent":  `{"path": "/c/vendor.tar", "ext": "..tar", "max_age": "2h"}`,
		"zero max age":     `{"path": "/c/vendor.tar", "ext": ".tar", "max_age": "0s"}`,
		"negative max age": `{"path": "/c/vendor.tar", "ext": ".tar", "max_age": "-1h"}`,
		"missing max age":  `{"path": "/c/vendor.tar", "ext": ".tar"}`,
		"unknown member":   `{"path": "/c/vendor.tar", "ext": ".tar", "max_age": "2h", "zzz": 1}`,
	}
	r := testRegistry(t)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("source", "collected", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
}

func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	env := newStub(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instances.Build(ctx, testRegistry(t), source.Kind, "collected",
		jsontext.Value(mustJSON(t, env.config())), instances.Deps{Instance: "collected"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s := build(t, env.config())
	if err := s.(instances.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Error("Dump accepted a cancelled context")
	}
}

func dumpErr(t *testing.T, s source.Source) error {
	t.Helper()
	var dst bytes.Buffer
	return s.Dump(context.Background(), &dst)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
