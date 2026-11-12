package k3setcd_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/k3setcd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sourcetest"
)

const tokenContent = "K10thisisasecrettoken::server:abc123"

// stubEnv is one fake node: a snapshot directory and the server token.
type stubEnv struct {
	dir   string
	token string
}

func newStub(t *testing.T) *stubEnv {
	t.Helper()
	root := t.TempDir()
	env := &stubEnv{dir: filepath.Join(root, "snapshots"), token: filepath.Join(root, "token")}
	if err := os.MkdirAll(env.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(env.token, []byte(tokenContent), 0o600); err != nil {
		t.Fatal(err)
	}
	return env
}

// snapshotBytes is the etcd snapshot shape: the data followed by the SHA256 of
// exactly that data.
func snapshotBytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	return append(append([]byte(nil), data...), sum[:]...)
}

// snapshot writes one finished snapshot and stamps its age.
func (e *stubEnv) snapshot(t *testing.T, name string, data []byte, age time.Duration) string {
	t.Helper()
	path := filepath.Join(e.dir, name)
	if err := os.WriteFile(path, snapshotBytes(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if age != 0 {
		mtime := time.Now().Add(-age)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func (e *stubEnv) config() map[string]any {
	return map[string]any{
		"snapshot_dir": e.dir,
		"token":        map[string]string{"file": e.token},
		"max_age":      "2h",
	}
}

func build(t *testing.T, cfg map[string]any) source.Source {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := instances.Build(context.Background(), testRegistry(t), source.Kind, "k3s_etcd", raw,
		instances.Deps{Instance: "k3s_etcd", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testRegistry(t *testing.T) *instances.Registry {
	t.Helper()
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, k3setcd.Spec, func(lr *loader.Registry) error {
		return k3setcd.Apply(lr, r)
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func check(t *testing.T, s source.Source) error {
	t.Helper()
	checker, ok := s.(instances.Checker)
	if !ok {
		t.Fatal("k3s_etcd does not implement plugin.Checker, so this failure would surface mid-run")
	}
	return checker.Check(context.Background())
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		env := newStub(t)
		env.snapshot(t, "etcd-snapshot-1", []byte("ETCD-SNAPSHOT-DATA"), 0)
		return build(t, env.config())
	})
}

func TestHappyPathArtifact(t *testing.T) {
	env := newStub(t)
	data := []byte("ETCD-SNAPSHOT-DATA-BYTES")
	env.snapshot(t, "etcd-snapshot-1", data, 0)
	s := build(t, env.config())
	var dst bytes.Buffer
	if err := s.Dump(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	entries := readTar(t, dst.Bytes())
	if len(entries) != 2 {
		t.Fatalf("the artifact holds %d entries, want exactly snapshot.db and token", len(entries))
	}
	snapshot := entries["snapshot.db"]
	if !bytes.Equal(snapshot.body, snapshotBytes(data)) {
		t.Fatalf("snapshot.db does not carry the exact file etcd saved")
	}
	if snapshot.hdr.Mode != 0o644 || snapshot.hdr.Typeflag != tar.TypeReg {
		t.Errorf("snapshot.db header = %+v, want a 0644 regular file", snapshot.hdr)
	}
	if !snapshot.hdr.ModTime.Equal(time.Unix(0, 0)) {
		t.Errorf("snapshot.db modtime = %v, want the fixed epoch", snapshot.hdr.ModTime)
	}
	token := entries["token"]
	if string(token.body) != tokenContent || token.hdr.Mode != 0o600 {
		t.Fatalf("token entry = %+v %q, want the 0600 token", token.hdr, token.body)
	}
}

func TestArtifactIsDeterministic(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-1", []byte("ETCD-SNAPSHOT-DATA"), 0)
	s := build(t, env.config())
	var first, second bytes.Buffer
	if err := s.Dump(context.Background(), &first); err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), &second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("two dumps of unchanged bytes produced different archives")
	}
}

func TestSelectsTheNewestSnapshot(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-old", []byte("OLD"), 2*time.Hour)
	env.snapshot(t, "etcd-snapshot-new", []byte("NEW"), time.Hour)
	entries := dumpEntries(t, env.config())
	if got := string(entries["snapshot.db"].body); got != string(snapshotBytes([]byte("NEW"))) {
		t.Fatalf("snapshot.db = %q, want the newest snapshot", got)
	}
}

func TestEqualTimesBreakTheTieByName(t *testing.T) {
	env := newStub(t)
	old := env.snapshot(t, "etcd-snapshot-a", []byte("A"), 0)
	env.snapshot(t, "etcd-snapshot-b", []byte("B"), 0)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, path := range []string{old, filepath.Join(env.dir, "etcd-snapshot-b")} {
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	entries := dumpEntries(t, env.config())
	if got := string(entries["snapshot.db"].body); got != string(snapshotBytes([]byte("B"))) {
		t.Fatalf("snapshot.db = %q, want the lexicographically greater name on a tie", got)
	}
}

func TestHiddenAndTemporaryNamesAreIgnored(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-real", []byte("REAL"), 90*time.Minute)
	for _, name := range []string{".etcd-snapshot-hidden", "etcd-snapshot-1.part", "etcd-snapshot-2.tmp"} {
		env.snapshot(t, name, []byte("NOT-A-SNAPSHOT"), 0)
	}
	entries := dumpEntries(t, env.config())
	if got := string(entries["snapshot.db"].body); got != string(snapshotBytes([]byte("REAL"))) {
		t.Fatalf("snapshot.db = %q, want the only finished snapshot", got)
	}
}

func TestCompressedNewestIsRefusedByName(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-old", []byte("OLD"), 2*time.Hour)
	compressed := filepath.Join(env.dir, "etcd-snapshot-new.zip")
	if err := os.WriteFile(compressed, []byte("PK\x03\x04compressed"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := build(t, env.config()).Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a compressed newest snapshot was accepted")
	}
	if !strings.Contains(err.Error(), "compressed") || !strings.Contains(err.Error(), ".zip") {
		t.Fatalf("the refusal does not name compression and the file: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

func TestStaleNewestIsRefused(t *testing.T) {
	env := newStub(t)
	path := env.snapshot(t, "etcd-snapshot-stale", []byte("STALE"), 3*time.Hour)
	err := build(t, env.config()).Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a stale snapshot was accepted")
	}
	if !strings.Contains(err.Error(), "max_age") || !strings.Contains(err.Error(), path) {
		t.Fatalf("the refusal does not name max_age and the file: %v", err)
	}
}

func TestShortSnapshotIsRefused(t *testing.T) {
	env := newStub(t)
	path := filepath.Join(env.dir, "etcd-snapshot-short")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 31), 0o600); err != nil {
		t.Fatal(err)
	}
	err := build(t, env.config()).Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a snapshot shorter than its trailer was accepted")
	}
	if !strings.Contains(err.Error(), "shorter") {
		t.Fatalf("the refusal does not name the short snapshot: %v", err)
	}
}

func TestChecksumMismatchFailsTheArtifact(t *testing.T) {
	env := newStub(t)
	path := env.snapshot(t, "etcd-snapshot-1", []byte("ETCD-SNAPSHOT-DATA"), 0)
	tampered := snapshotBytes([]byte("ETCD-SNAPSHOT-DATA"))
	tampered[0] ^= 0xff
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	err := build(t, env.config()).Dump(context.Background(), &dst)
	if err == nil {
		t.Fatal("a snapshot whose data does not match its trailer was accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
	if strings.Contains(err.Error(), tokenContent) {
		t.Fatal("the token reached the error")
	}
}

func TestDumpReportsAChangeWhileReading(t *testing.T) {
	env := newStub(t)
	path := env.snapshot(t, "etcd-snapshot-1", []byte("ETCD-SNAPSHOT-DATA"), 0)
	changed := &changeOnWrite{path: path, at: time.Now().Add(-time.Hour)}
	err := build(t, env.config()).Dump(context.Background(), changed)
	if err == nil {
		t.Fatal("a snapshot that changed while it was read was accepted")
	}
	if !strings.Contains(err.Error(), "changed") {
		t.Fatalf("the refusal does not name the change: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
}

// changeOnWrite restamps the snapshot's mtime as the first byte reaches the
// destination: the streamed bytes stay correct, so this proves the identity
// check after the copy, not the checksum.
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

func TestSymlinkAndFIFOAreNeverSnapshots(t *testing.T) {
	t.Run("a symlink is ignored and its target never read", func(t *testing.T) {
		env := newStub(t)
		secret := filepath.Join(filepath.Dir(env.dir), "outside")
		if err := os.WriteFile(secret, []byte("LINKED-SECRET"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(secret, filepath.Join(env.dir, "etcd-snapshot-link")); err != nil {
			t.Fatal(err)
		}
		env.snapshot(t, "etcd-snapshot-real", []byte("REAL"), time.Hour)
		entries := dumpEntries(t, env.config())
		if got := string(entries["snapshot.db"].body); got != string(snapshotBytes([]byte("REAL"))) {
			t.Fatalf("snapshot.db = %q, want the real snapshot", got)
		}
		if bytes.Contains(entries["snapshot.db"].body, []byte("LINKED-SECRET")) {
			t.Fatal("the symlink's target was read")
		}
	})
	t.Run("a FIFO is ignored and never opened", func(t *testing.T) {
		env := newStub(t)
		if err := syscall.Mkfifo(filepath.Join(env.dir, "etcd-snapshot-fifo"), 0o600); err != nil {
			t.Fatal(err)
		}
		env.snapshot(t, "etcd-snapshot-real", []byte("REAL"), time.Hour)
		entries := dumpEntries(t, env.config())
		if got := string(entries["snapshot.db"].body); got != string(snapshotBytes([]byte("REAL"))) {
			t.Fatalf("snapshot.db = %q, want the real snapshot", got)
		}
	})
	t.Run("only a symlink is no snapshot at all", func(t *testing.T) {
		env := newStub(t)
		if err := os.Symlink(env.token, filepath.Join(env.dir, "etcd-snapshot-link")); err != nil {
			t.Fatal(err)
		}
		if err := build(t, env.config()).Dump(context.Background(), io.Discard); err == nil {
			t.Fatal("a directory holding only a link was accepted")
		}
	})
}

func TestCheckIsThePreflight(t *testing.T) {
	cases := map[string]func(t *testing.T, e *stubEnv) map[string]any{
		"missing snapshot directory": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.Remove(e.dir); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"snapshot directory is a file": func(t *testing.T, e *stubEnv) map[string]any {
			cfg := e.config()
			cfg["snapshot_dir"] = e.token
			return cfg
		},
		"no snapshots": func(*testing.T, *stubEnv) map[string]any {
			return nil
		},
		"compressed snapshot": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.WriteFile(filepath.Join(e.dir, "etcd-snapshot.zip"), []byte("PK"), 0o600); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"stale snapshot": func(t *testing.T, e *stubEnv) map[string]any {
			e.snapshot(t, "etcd-snapshot-stale", []byte("STALE"), 3*time.Hour)
			return e.config()
		},
		"missing token": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.Remove(e.token); err != nil {
				t.Fatal(err)
			}
			e.snapshot(t, "etcd-snapshot-1", []byte("DATA"), 0)
			return e.config()
		},
	}
	for name, prep := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			cfg := prep(t, env)
			if cfg == nil {
				cfg = env.config()
			}
			if err := check(t, build(t, cfg)); err == nil {
				t.Fatalf("%s passed the preflight", name)
			}
		})
	}
}

func TestCheckPassesAFreshSnapshot(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-1", []byte("DATA"), 0)
	if err := check(t, build(t, env.config())); err != nil {
		t.Fatalf("preflight refused a fresh snapshot: %v", err)
	}
}

func TestDumpFailsWhenTheTokenIsMissing(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-1", []byte("DATA"), 0)
	if err := os.Remove(env.token); err != nil {
		t.Fatal(err)
	}
	err := build(t, env.config()).Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("Dump accepted a missing token")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
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
	if want := "k3s-etcd://" + env.dir; traits.Origin != want {
		t.Errorf("Origin = %q, want %q", traits.Origin, want)
	}
	if !traits.Credentials {
		t.Fatal("the bytes carry the server token but were not reported as credentials")
	}
}

// TestNoWarningException: k3s_etcd is a new type, so its credential bytes have
// no published warning exception — a plaintext pipeline is refused before the
// first byte, exactly like pg_dumpall globals.
func TestNoWarningException(t *testing.T) {
	env := newStub(t)
	if _, ok := build(t, env.config()).(source.CredentialPolicy); ok {
		t.Fatal("k3s_etcd published a credential warning exception")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"relative snapshot dir":  `{"snapshot_dir": "snapshots", "token": {"file": "/t"}, "max_age": "2h"}`,
		"empty snapshot dir":     `{"snapshot_dir": "", "token": {"file": "/t"}, "max_age": "2h"}`,
		"missing snapshot dir":   `{"token": {"file": "/t"}, "max_age": "2h"}`,
		"relative token":         `{"snapshot_dir": "/s", "token": {"file": "token"}, "max_age": "2h"}`,
		"empty token file":       `{"snapshot_dir": "/s", "token": {"file": ""}, "max_age": "2h"}`,
		"missing token":          `{"snapshot_dir": "/s", "max_age": "2h"}`,
		"zero max age":           `{"snapshot_dir": "/s", "token": {"file": "/t"}, "max_age": "0s"}`,
		"negative max age":       `{"snapshot_dir": "/s", "token": {"file": "/t"}, "max_age": "-1h"}`,
		"missing max age":        `{"snapshot_dir": "/s", "token": {"file": "/t"}}`,
		"unknown member":         `{"snapshot_dir": "/s", "token": {"file": "/t"}, "max_age": "2h", "zzz": 1}`,
		"token is not a secret":  `{"snapshot_dir": "/s", "token": "/t", "max_age": "2h"}`,
		"compressed max age set": `{"snapshot_dir": "/s", "token": {"file": "/t"}, "max_age": "soon"}`,
	}
	r := testRegistry(t)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("source", "k3s_etcd", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
}

func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-1", []byte("DATA"), 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instances.Build(ctx, testRegistry(t), source.Kind, "k3s_etcd",
		jsontext.Value(mustJSON(t, env.config())), instances.Deps{Instance: "k3s_etcd"}); err == nil {
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
	if _, err := s.(source.Prober).Probe(ctx, bytes.NewReader(nil)); err == nil {
		t.Error("Probe accepted a cancelled context")
	}
}

func TestProbe(t *testing.T) {
	data := []byte("ETCD-SNAPSHOT-DATA")
	validBody := snapshotBytes(data)
	valid := buildTar(t,
		tarFixture{name: "snapshot.db", mode: 0o644, body: validBody},
		tarFixture{name: "token", mode: 0o600, body: []byte(tokenContent)},
	)

	t.Run("verifies the artifact", func(t *testing.T) {
		env := newStub(t)
		env.snapshot(t, "etcd-snapshot-1", data, 0)
		var artifact bytes.Buffer
		if err := build(t, env.config()).Dump(context.Background(), &artifact); err != nil {
			t.Fatal(err)
		}
		report, err := probe(t, artifact.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if report.Entries != 2 {
			t.Fatalf("Entries = %d, want 2", report.Entries)
		}
		want := int64(len(validBody) + len(tokenContent))
		if report.Bytes != want {
			t.Fatalf("Bytes = %d, want %d", report.Bytes, want)
		}
		if report.Detail == "" {
			t.Fatal("Probe reported no detail")
		}
	})

	cases := map[string]struct {
		raw []byte
		ok  bool
	}{
		"tampered snapshot trailer": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: tampered(validBody)}, tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
		"garbage":   {raw: []byte("not a tar stream")},
		"truncated": {raw: valid[:len(valid)-100]},
		"missing token": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: validBody})},
		"missing snapshot": {raw: buildTar(t,
			tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
		"duplicate snapshot": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: validBody},
			tarFixture{name: "snapshot.db", mode: 0o644, body: validBody},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
		"duplicate token": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: validBody},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
		"unexpected entry": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: validBody},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")},
			tarFixture{name: "extra", mode: 0o600, body: []byte("x")})},
		"snapshot is not a regular file": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", typeflag: tar.TypeDir},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
		"snapshot shorter than its trailer": {raw: buildTar(t,
			tarFixture{name: "snapshot.db", mode: 0o644, body: []byte("short")},
			tarFixture{name: "token", mode: 0o600, body: []byte("t")})},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := probe(t, tc.raw); err == nil {
				t.Fatalf("%s passed the probe", name)
			}
		})
	}

	t.Run("propagates a read error", func(t *testing.T) {
		if _, err := probeReader(t, errReader{}); err == nil {
			t.Fatal("a failing stream passed the probe")
		}
	})
}

func TestProbeRepeatsTheChecksumVerification(t *testing.T) {
	data := []byte("ETCD-SNAPSHOT-DATA")
	env := newStub(t)
	path := env.snapshot(t, "etcd-snapshot-1", data, 0)
	var artifact bytes.Buffer
	if err := build(t, env.config()).Dump(context.Background(), &artifact); err != nil {
		t.Fatal(err)
	}
	// Rewrite the snapshot in the archive's byte stream: the tar header and
	// entry sizes stay valid, exactly like a tampered stored object.
	tamperedArtifact := bytes.Replace(artifact.Bytes(), snapshotBytes(data)[:4], []byte("EVIL"), 1)
	if _, err := probe(t, tamperedArtifact); err == nil {
		t.Fatal("Probe accepted an artifact whose snapshot no longer matches its trailer")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

// probe runs Probe over one artifact.
func probe(t *testing.T, raw []byte) (source.ProbeReport, error) {
	t.Helper()
	return probeReader(t, bytes.NewReader(raw))
}

func probeReader(t *testing.T, r io.Reader) (source.ProbeReport, error) {
	t.Helper()
	env := newStub(t)
	env.snapshot(t, "etcd-snapshot-1", []byte("DATA"), 0)
	prober, ok := build(t, env.config()).(source.Prober)
	if !ok {
		t.Fatal("k3s_etcd does not implement source.Prober")
	}
	return prober.Probe(context.Background(), r)
}

type tarFixture struct {
	name     string
	mode     int64
	body     []byte
	typeflag byte
}

func buildTar(t *testing.T, entries ...tarFixture) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.name, Mode: entry.mode, Typeflag: entry.typeflag}
		if header.Typeflag == 0 {
			header.Typeflag = tar.TypeReg
		}
		if header.Typeflag == tar.TypeReg {
			header.Size = int64(len(entry.body))
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			if _, err := tw.Write(entry.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// tampered flips the trailing checksum of a snapshot body.
func tampered(body []byte) []byte {
	out := append([]byte(nil), body...)
	out[len(out)-1] ^= 0xff
	return out
}

type tarEntry struct {
	hdr  *tar.Header
	body []byte
}

func readTar(t *testing.T, raw []byte) map[string]tarEntry {
	t.Helper()
	out := map[string]tarEntry{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("reading the tar stream: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("reading tar entry %s: %v", hdr.Name, err)
		}
		out[hdr.Name] = tarEntry{hdr: hdr, body: body}
	}
}

// dumpEntries dumps the artifact for cfg and returns its entries.
func dumpEntries(t *testing.T, cfg map[string]any) map[string]tarEntry {
	t.Helper()
	var dst bytes.Buffer
	if err := build(t, cfg).Dump(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	return readTar(t, dst.Bytes())
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk fell over") }
