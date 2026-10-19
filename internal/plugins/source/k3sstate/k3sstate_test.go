package k3sstate_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/k3sstate"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

const (
	dbContent    = "SQLITE-FORMAT-3-DATASTORE-BYTES"
	tokenContent = "K10thisisasecrettoken::server:abc123"

	// workPrefix is the prefix this package gives the working directory it
	// creates; the tests read it off the artefacts rather than assume it.
	workDirPrefix = "k3s-state-"
)

// stubEnv is one fake node: a datastore, a token, a scratch directory and a
// fake sqlite3 that records its argv and what it saw around the .backup
// destination.
type stubEnv struct {
	datastore string
	token     string
	scratch   string
	stubDir   string
	argvLog   string
	probeLog  string
}

// newStub installs a fake sqlite3 on PATH. The script records every argument
// it was called with, reports the integrity-check output and exit code the
// test asked for, and copies the "datastore" into the .backup destination —
// recording on the way the mode of that destination's directory, the
// SQLITE_TMPDIR in force, and what else sits beside it.
func newStub(t *testing.T) *stubEnv {
	t.Helper()
	root := t.TempDir()
	env := &stubEnv{
		datastore: filepath.Join(root, "db", "state.db"),
		token:     filepath.Join(root, "token"),
		scratch:   filepath.Join(root, "plain"),
		stubDir:   filepath.Join(root, "bin"),
		argvLog:   filepath.Join(root, "argv.log"),
		probeLog:  filepath.Join(root, "probe.log"),
	}
	for _, dir := range []string{filepath.Dir(env.datastore), env.scratch, env.stubDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(env.datastore, []byte(dbContent), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(env.token, []byte(tokenContent), 0o600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
printf '%s\n' "$@" >> "$K3S_TEST_ARGV_LOG"
case " $* " in
  *" PRAGMA "*)
    if [ -n "$K3S_TEST_UNLINK_ON_CHECK" ]; then rm -f "$3"; fi
    printf '%s\n' "$K3S_TEST_CHECK_OUTPUT"
    exit "${K3S_TEST_CHECK_EXIT:-0}"
    ;;
esac
for arg do
  case "$arg" in
    .backup\ *)
      dest=$(printf '%s' "$arg" | sed 's/^\.backup //; s/^"//; s/"$//')
      {
        printf 'mode %s\n' "$(ls -ld "$(dirname "$dest")" | cut -d' ' -f1)"
        printf 'tmpdir %s\n' "$SQLITE_TMPDIR"
        printf 'scratch %s\n' "$(ls -A "$(dirname "$(dirname "$dest")")" | tr '\n' ' ')"
      } >> "$K3S_TEST_PROBE_LOG"
      if [ -z "$K3S_TEST_NO_COPY" ]; then printf '%s' "$K3S_TEST_DB_CONTENT" > "$dest"; fi
      ;;
  esac
done
printf '%s\n' "$K3S_TEST_BACKUP_OUTPUT"
exit "${K3S_TEST_BACKUP_EXIT:-0}"
`
	if err := os.WriteFile(filepath.Join(env.stubDir, "sqlite3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", env.stubDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("K3S_TEST_ARGV_LOG", env.argvLog)
	t.Setenv("K3S_TEST_PROBE_LOG", env.probeLog)
	t.Setenv("K3S_TEST_DB_CONTENT", dbContent)
	t.Setenv("K3S_TEST_CHECK_OUTPUT", "ok")
	t.Setenv("K3S_TEST_CHECK_EXIT", "0")
	t.Setenv("K3S_TEST_BACKUP_OUTPUT", "")
	t.Setenv("K3S_TEST_BACKUP_EXIT", "0")
	return env
}

func (e *stubEnv) config() map[string]any {
	return map[string]any{
		"datastore": e.datastore, "token": map[string]string{"file": e.token}, "scratch_dir": e.scratch,
		"busy_timeout": "10s", "integrity_check": "quick", "timeout": "1m",
	}
}

func (e *stubEnv) argv(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(e.argvLog)
	if err != nil {
		t.Fatalf("the fake sqlite3 was never called: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// probeField returns what the fake sqlite3 saw for one key while the copy was
// being made, or "" when the run never reached that call.
func (e *stubEnv) probeField(t *testing.T, key string) string {
	t.Helper()
	raw, err := os.ReadFile(e.probeLog)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ""
		}
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if value, ok := strings.CutPrefix(line, key+" "); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func build(t *testing.T, cfg map[string]any) source.Source {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := plugin.NewRegistry()
	if err := k3sstate.Register(r); err != nil {
		t.Fatal(err)
	}
	s, err := plugin.Build(context.Background(), r, source.Kind, "k3s_state", raw,
		plugin.Deps{Instance: "k3s_state", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := k3sstate.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func check(t *testing.T, s source.Source) error {
	t.Helper()
	checker, ok := s.(plugin.Checker)
	if !ok {
		t.Fatal("k3s_state does not implement plugin.Checker, so this failure would surface mid-run")
	}
	return checker.Check(context.Background())
}

// kubeletScratch gives the scratch directory the mode a Kubernetes emptyDir
// really has: kubelet creates it 0777 and fsGroup adds setgid.
func kubeletScratch(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o777|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		env := newStub(t)
		kubeletScratch(t, env.scratch)
		return build(t, env.config())
	})
}

// TestKubeletEmptyDirPasses is the review finding's gate: a scratch directory
// with kubelet's own mode (0777 plus setgid) must pass preflight and produce
// the artifact. A mode test on that directory refused both, every night.
func TestKubeletEmptyDirPasses(t *testing.T) {
	env := newStub(t)
	kubeletScratch(t, env.scratch)
	s := build(t, env.config())
	if err := check(t, s); err != nil {
		t.Fatalf("preflight refused the emptyDir kubelet creates: %v", err)
	}
	var dst bytes.Buffer
	if err := s.Dump(context.Background(), &dst); err != nil {
		t.Fatalf("Dump against an emptyDir scratch directory: %v", err)
	}
	entries := readTar(t, dst.Bytes())
	if string(entries["state.db"]) != dbContent || string(entries["token"]) != tokenContent {
		t.Fatalf("the artifact holds %d entries and neither the datastore nor the token", len(entries))
	}
}

// TestPrivateWorkDirectoryIsTheOnlyPlaceHoldingPlaintext is the invariant in
// test form: whatever the scratch directory's mode, every plaintext byte lives
// in a 0700 directory this plugin created, nothing else is present beside it
// while the dump runs, and the directory is gone afterwards on every path.
func TestPrivateWorkDirectoryIsTheOnlyPlaceHoldingPlaintext(t *testing.T) {
	paths := map[string]func(t *testing.T, e *stubEnv){
		"success": func(*testing.T, *stubEnv) {},
		"integrity check fails": func(t *testing.T, e *stubEnv) {
			t.Setenv("K3S_TEST_CHECK_OUTPUT", "*** in database main ***")
		},
		"token missing": func(t *testing.T, e *stubEnv) {
			if err := os.Remove(e.token); err != nil {
				t.Fatal(err)
			}
		},
		"cancelled": func(*testing.T, *stubEnv) {},
	}
	for name, setup := range paths {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			kubeletScratch(t, env.scratch)
			setup(t, env)
			var dst io.Writer = &bytes.Buffer{}
			ctx := context.Background()
			if name == "cancelled" {
				cancelCtx, cancel := context.WithCancel(ctx)
				ctx = cancelCtx
				dst = &cancelAfterFirstWrite{cancel: cancel}
			}
			err := build(t, env.config()).Dump(ctx, dst)
			if name == "success" && err != nil {
				t.Fatalf("Dump: %v", err)
			}
			if name != "success" && err == nil {
				t.Fatalf("Dump reported success on the %s path", name)
			}

			assertScratchEmpty(t, env.scratch)
			// The cancel fires on the first byte of the tar stream, after both
			// sqlite3 calls: on that path too the working directory was alive
			// while the copy existed and had to go with the deferred cleanup.
			if mode := lsMode(env.probeField(t, "mode")); mode != "drwx------" {
				t.Errorf("the working directory had mode %q while it held the datastore copy, want drwx------", mode)
			}
			if name == "cancelled" {
				return
			}
			wantTmp := env.scratch + string(os.PathSeparator) + workDirPrefix
			if tmp := env.probeField(t, "tmpdir"); !strings.HasPrefix(tmp, wantTmp) {
				t.Errorf("SQLITE_TMPDIR was %q, want a directory inside the private working directory %q", tmp, wantTmp)
			}
			beside := strings.Fields(env.probeField(t, "scratch"))
			if len(beside) != 1 || !strings.HasPrefix(beside[0], workDirPrefix) {
				t.Errorf("the scratch directory held %v while the dump ran, want exactly the private working directory", beside)
			}
		})
	}
}

// TestForeignEntriesAreLeftAlone: the scratch directory is kubelet's and only
// entries this package created are its to remove.
func TestForeignEntriesAreLeftAlone(t *testing.T) {
	env := newStub(t)
	foreign := filepath.Join(env.scratch, "someone-elses-file")
	if err := os.WriteFile(foreign, []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("a file this package did not create was removed: %v", err)
	}
}

// TestLeftoverWorkDirectoryIsSwept: a run killed before its cleanup leaves
// plaintext behind, and the next run must remove it.
func TestLeftoverWorkDirectoryIsSwept(t *testing.T) {
	env := newStub(t)
	leftover := filepath.Join(env.scratch, workDirPrefix+"killed")
	if err := os.MkdirAll(leftover, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leftover, "state.db"), []byte(dbContent), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a leftover working directory survived the next run: %v", err)
	}
	assertScratchEmpty(t, env.scratch)
}

// TestHappyPathArtifact checks what actually leaves the container: one tar
// stream with the datastore copy and the token, and the sqlite3 invocation the
// snapshot contract asks for.
func TestHappyPathArtifact(t *testing.T) {
	env := newStub(t)
	var dst bytes.Buffer
	if err := build(t, env.config()).Dump(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	entries := readTar(t, dst.Bytes())
	if got := string(entries["state.db"]); got != dbContent {
		t.Errorf("state.db holds %q, want the fake datastore", got)
	}
	if got := string(entries["token"]); got != tokenContent {
		t.Errorf("token holds %q, want the fake token", got)
	}
	if len(entries) != 2 {
		t.Errorf("the artifact holds %d entries, want exactly state.db and token", len(entries))
	}

	argv := strings.Join(env.argv(t), " ")
	if !strings.Contains(argv, "-readonly") {
		t.Errorf("sqlite3 was not asked for a read-only source: %s", argv)
	}
	if !strings.Contains(argv, ".timeout 10000") {
		t.Errorf("the configured busy_timeout did not reach sqlite3: %s", argv)
	}
	if !strings.Contains(argv, env.datastore) {
		t.Errorf("sqlite3 was not pointed at the datastore: %s", argv)
	}
	if !strings.Contains(argv, "PRAGMA quick_check") {
		t.Errorf("the configured integrity check did not run: %s", argv)
	}
	dest := backupDestination(t, env)
	if !strings.HasPrefix(dest, env.scratch+string(os.PathSeparator)+workDirPrefix) {
		t.Errorf("the .backup destination %q is not inside a private working directory under the scratch dir", dest)
	}
	if filepath.Base(dest) != "state.db" {
		t.Errorf("the .backup destination is %q, want state.db inside the working directory", dest)
	}
}

// TestIntegrityCheckModes pins that the config chooses the check, including
// the explicit "none", and that a failed check names the copy as the problem.
func TestIntegrityCheckModes(t *testing.T) {
	t.Run("full runs integrity_check", func(t *testing.T) {
		env := newStub(t)
		cfg := env.config()
		cfg["integrity_check"] = "full"
		if err := build(t, cfg).Dump(context.Background(), &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		if argv := strings.Join(env.argv(t), " "); !strings.Contains(argv, "PRAGMA integrity_check") {
			t.Errorf("full did not run PRAGMA integrity_check: %s", argv)
		}
	})
	t.Run("none skips it", func(t *testing.T) {
		env := newStub(t)
		cfg := env.config()
		cfg["integrity_check"] = "none"
		if err := build(t, cfg).Dump(context.Background(), &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		if argv := strings.Join(env.argv(t), " "); strings.Contains(argv, "PRAGMA") {
			t.Errorf("none still ran a PRAGMA: %s", argv)
		}
	})
	t.Run("a failed check is permanent and names the verdict", func(t *testing.T) {
		env := newStub(t)
		t.Setenv("K3S_TEST_CHECK_OUTPUT", "row 12 missing from index sqlite_autoindex_x")
		err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{})
		if err == nil {
			t.Fatal("Dump accepted a datastore copy that failed its integrity check")
		}
		if !strings.Contains(err.Error(), "missing from index") {
			t.Errorf("the error does not carry sqlite's verdict: %v", err)
		}
		if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
			t.Errorf("a failed integrity check is %q, want permanent", class)
		}
	})
}

// TestCancellationStopsBeforeTheToken: a cancelled run must not read the
// server token, and must still leave the scratch directory empty.
func TestCancellationStopsBeforeTheToken(t *testing.T) {
	env := newStub(t)
	ctx, cancel := context.WithCancel(context.Background())
	dst := &cancelAfterFirstWrite{cancel: cancel}
	err := build(t, env.config()).Dump(ctx, dst)
	if err == nil {
		t.Fatal("Dump reported success after cancellation")
	}
	assertScratchEmpty(t, env.scratch)
	if strings.Contains(dst.written.String(), tokenContent) {
		t.Error("the token was streamed after cancellation")
	}
}

// TestCheckIsThePreflight covers the failures that must stop a run before it
// starts: a missing or unwritable scratch directory, an unopenable datastore
// or token, and a missing sqlite3. The scratch directory's own mode is not a
// failure — TestKubeletEmptyDirPasses is its counterexample.
func TestCheckIsThePreflight(t *testing.T) {
	cases := map[string]func(t *testing.T, e *stubEnv) map[string]any{
		"missing datastore": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.Remove(e.datastore); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"missing token": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.Remove(e.token); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"missing scratch dir": func(t *testing.T, e *stubEnv) map[string]any {
			if err := os.Remove(e.scratch); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"unwritable scratch dir": func(t *testing.T, e *stubEnv) map[string]any {
			if os.Geteuid() == 0 {
				t.Skip("root ignores directory modes")
			}
			if err := os.Chmod(e.scratch, 0o555); err != nil {
				t.Fatal(err)
			}
			return e.config()
		},
		"no sqlite3 on PATH": func(t *testing.T, e *stubEnv) map[string]any {
			t.Setenv("PATH", t.TempDir())
			return e.config()
		},
	}
	for name, prep := range cases {
		t.Run(name, func(t *testing.T) {
			env := newStub(t)
			s := build(t, prep(t, env))
			err := check(t, s)
			if err == nil {
				t.Fatalf("%s passed the preflight", name)
			}
			if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
				t.Errorf("%s is %q, want permanent", name, class)
			}
		})
	}
}

// TestCheckLeavesNothingBehind: the writability probe is a file this package
// creates in kubelet's directory, and it must not survive the check.
func TestCheckLeavesNothingBehind(t *testing.T) {
	env := newStub(t)
	kubeletScratch(t, env.scratch)
	if err := check(t, build(t, env.config())); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	assertScratchEmpty(t, env.scratch)
}

// TestConstructionIsFilesystemFree is what lets the conformance kit build this
// plugin from its example: no mount exists at construction time.
func TestConstructionIsFilesystemFree(t *testing.T) {
	raw := jsontext.Value(`{"datastore": "/k3s-db/state.db", "token": {"file": "/k3s-token"}, "scratch_dir": "/plain", ` +
		`"busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`)
	if _, err := plugin.Build(context.Background(), testRegistry(t), source.Kind, "k3s_state", raw,
		plugin.Deps{Instance: "k3s_state", Log: slog.New(slog.DiscardHandler)}); err != nil {
		t.Fatalf("construction touched the filesystem: %v", err)
	}
}

func TestReturnsOnCancellation(t *testing.T) {
	env := newStub(t)
	s := build(t, env.config())
	err := plugintest.MustReturnWithin(t, 15*time.Second, "Dump", func(ctx context.Context) error {
		return s.Dump(ctx, io.Discard)
	})
	if err == nil {
		t.Fatal("Dump with a cancelled context returned success")
	}
}

// TestConfigValidation pins the config contract: the paths a run needs absolute
// and the depths sqlite3 understands. A single quote in a path is NOT refused —
// the copy escapes it (see internal/sqlitecopy) — so it is absent here.
func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"relative datastore":   `{"datastore": "state.db", "token": {"file": "/t"}, "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
		"relative scratch":     `{"datastore": "/d", "token": {"file": "/t"}, "scratch_dir": "plain", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
		"unknown check":        `{"datastore": "/d", "token": {"file": "/t"}, "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "deep", "timeout": "5m"}`,
		"zero busy timeout":    `{"datastore": "/d", "token": {"file": "/t"}, "scratch_dir": "/p", "busy_timeout": "0s", "integrity_check": "quick", "timeout": "5m"}`,
		"zero command timeout": `{"datastore": "/d", "token": {"file": "/t"}, "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "0s"}`,
		"no token":             `{"datastore": "/d", "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
		"empty datastore":      `{"datastore": "", "token": {"file": "/t"}, "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
		"empty token file":     `{"datastore": "/d", "token": {"file": ""}, "scratch_dir": "/p", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
		"empty scratch dir":    `{"datastore": "/d", "token": {"file": "/t"}, "scratch_dir": "", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`,
	}
	r := testRegistry(t)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("source", "k3s_state", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
}

// TestRequiresNamesTheBinary keeps `doctor` able to check the tool this source
// shells out to.
func TestRequiresNamesTheBinary(t *testing.T) {
	for _, d := range testRegistry(t).Descriptors() {
		if d.Kind == "source" && d.Type == "k3s_state" {
			if len(d.Requires) != 1 || d.Requires[0] != "sqlite3" {
				t.Fatalf("Requires = %v, want [sqlite3]", d.Requires)
			}
			return
		}
	}
	t.Fatal("k3s_state is not registered")
}

// TestOriginNamesTheDatastore: the manifest records it, and a restore compares
// it against its target, so it must be the path and nothing else.
func TestOriginNamesTheDatastore(t *testing.T) {
	env := newStub(t)
	traits, err := build(t, env.config()).Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := "sqlite3://" + env.datastore
	if traits.Origin != want {
		t.Errorf("Origin = %q, want %q", traits.Origin, want)
	}
	if traits.Ext != ".tar" {
		t.Errorf("Ext = %q, want .tar", traits.Ext)
	}
}

// TestErrorsNeverCarryTheToken: the token is the one secret this source reads,
// and no failure path may put it into an error message or a log line.
func TestErrorsNeverCarryTheToken(t *testing.T) {
	env := newStub(t)
	t.Setenv("K3S_TEST_BACKUP_EXIT", "1")
	t.Setenv("K3S_TEST_BACKUP_OUTPUT", "Error: unable to open database file")
	var logged strings.Builder
	raw, err := json.Marshal(env.config())
	if err != nil {
		t.Fatal(err)
	}
	r := testRegistry(t)
	s, err := plugin.Build(context.Background(), r, source.Kind, "k3s_state", raw,
		plugin.Deps{Instance: "k3s_state", Log: slog.New(slog.NewTextHandler(&logged, nil))})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), &bytes.Buffer{})
	if err == nil {
		t.Fatal("a failing sqlite3 was reported as success")
	}
	if strings.Contains(err.Error(), tokenContent) || strings.Contains(logged.String(), tokenContent) {
		t.Fatal("the token reached an error message or the log")
	}
}

// lsMode normalises the mode field of `ls -ld`, which BSD and GNU both may
// suffix with markers for extended attributes ("@") or ACLs ("+").
func lsMode(field string) string {
	return strings.TrimRight(field, "@+")
}

// backupDestination extracts the path sqlite3 was told to write its copy to.
func backupDestination(t *testing.T, env *stubEnv) string {
	t.Helper()
	for _, arg := range env.argv(t) {
		if dest, ok := strings.CutPrefix(arg, ".backup "); ok {
			dest = strings.TrimSuffix(strings.TrimPrefix(dest, `"`), `"`)
			return strings.ReplaceAll(strings.ReplaceAll(dest, `\"`, `"`), `\\`, `\`)
		}
	}
	t.Fatal("sqlite3 was never given a .backup destination")
	return ""
}

func readTar(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
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
		out[hdr.Name] = body
	}
}

func assertScratchEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the scratch directory: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the scratch directory still holds %v; the plaintext datastore copy survived", names)
	}
}

// cancelAfterFirstWrite cancels the run as soon as the first byte is written.
type cancelAfterFirstWrite struct {
	cancel  context.CancelFunc
	written bytes.Buffer
}

func (w *cancelAfterFirstWrite) Write(p []byte) (int, error) {
	w.written.Write(p)
	w.cancel()
	return len(p), nil
}

// cancelOnWrite cancels the run on the nth non-empty write, so a test can place
// the cancellation after the copy of the first entry and before the next.
type cancelOnWrite struct {
	n      int
	cancel context.CancelFunc
	calls  int
}

func (w *cancelOnWrite) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.calls++
	if w.calls == w.n {
		w.cancel()
	}
	return len(p), nil
}

// failOnWrite fails the nth non-empty write: the shape of a destination that
// gives up partway through the stream.
type failOnWrite struct {
	n     int
	calls int
}

func (w *failOnWrite) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.calls++
	if w.calls == w.n {
		return 0, errors.New("injected write failure")
	}
	return len(p), nil
}

func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	env := newStub(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.Build(ctx, testRegistry(t), source.Kind, "k3s_state",
		jsontext.Value(mustJSON(t, env.config())), plugin.Deps{Instance: "k3s_state"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s := build(t, env.config())
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDumpFailsWhenTheScratchDirVanished(t *testing.T) {
	env := newStub(t)
	if err := os.Remove(env.scratch); err != nil {
		t.Fatal(err)
	}
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err == nil {
		t.Fatal("Dump accepted a scratch directory it could not read")
	}
}

func TestDumpFailsWhenTheScratchDirIsNotWritable(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	env := newStub(t)
	if err := os.Chmod(env.scratch, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(env.scratch, 0o700) })
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err == nil {
		t.Fatal("Dump accepted a scratch directory it could not write into")
	}
}

func TestDumpFailsWhenTheDatastoreIsMissing(t *testing.T) {
	env := newStub(t)
	if err := os.Remove(env.datastore); err != nil {
		t.Fatal(err)
	}
	err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{})
	if err == nil {
		t.Fatal("Dump accepted a missing datastore")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestDumpFailsWhenSqlite3IsMissing: the binary is resolved at the dump, and a
// deployment without it fails there rather than at 04:20.
func TestDumpFailsWhenSqlite3IsMissing(t *testing.T) {
	env := newStub(t)
	t.Setenv("PATH", t.TempDir())
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err == nil {
		t.Fatal("Dump reported success without sqlite3")
	}
}

func TestDumpFailsWhenTheCopyIsNotWritten(t *testing.T) {
	env := newStub(t)
	t.Setenv("K3S_TEST_NO_COPY", "1")
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err == nil {
		t.Fatal("a snapshot that wrote no copy was accepted")
	}
}

func TestDumpFailsWhenTheIntegrityCheckCommandFails(t *testing.T) {
	env := newStub(t)
	t.Setenv("K3S_TEST_CHECK_EXIT", "1")
	t.Setenv("K3S_TEST_CHECK_OUTPUT", "Error: unable to open database file")
	err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{})
	if err == nil {
		t.Fatal("a failing integrity-check command was accepted")
	}
	if !strings.Contains(err.Error(), "unable to open database file") {
		t.Fatalf("the error does not carry sqlite's own message: %v", err)
	}
}

func TestDumpFailsWhenTheCopyVanishesBeforeThePack(t *testing.T) {
	env := newStub(t)
	t.Setenv("K3S_TEST_UNLINK_ON_CHECK", "1")
	if err := build(t, env.config()).Dump(context.Background(), &bytes.Buffer{}); err == nil {
		t.Fatal("a copy that vanished before it was packed was accepted")
	}
	assertScratchEmpty(t, env.scratch)
}

// TestDumpReturnsOnCancellationDuringThePack: the pack checks the context at
// every entry, so a cancelled run stops at the next one.
func TestDumpReturnsOnCancellationDuringThePack(t *testing.T) {
	env := newStub(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dst := &cancelOnWrite{n: 2, cancel: cancel}
	err := build(t, env.config()).Dump(ctx, dst)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
	assertScratchEmpty(t, env.scratch)
}

// TestDumpReportsATokenWriteFailure: the token is the second entry, and a
// destination that fails while it is written fails the artifact.
func TestDumpReportsATokenWriteFailure(t *testing.T) {
	env := newStub(t)
	// An empty copy keeps the write count fixed: the state.db entry writes only
	// its header, so the third write is the token's body.
	t.Setenv("K3S_TEST_DB_CONTENT", "")
	if err := os.WriteFile(env.token, bytes.Repeat([]byte("t"), 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := build(t, env.config()).Dump(context.Background(), &failOnWrite{n: 3}); err == nil {
		t.Fatal("Dump reported success when the token could not be written")
	}
}

// TestDumpReportsATarTrailerFailure: the last write of the stream is the tar
// trailer, and a destination that fails there fails the artifact.
func TestDumpReportsATarTrailerFailure(t *testing.T) {
	env := newStub(t)
	// An empty copy and a short token leave the fourth write to the tar writer's
	// own Close, which writes the end-of-archive trailer.
	t.Setenv("K3S_TEST_DB_CONTENT", "")
	if err := build(t, env.config()).Dump(context.Background(), &failOnWrite{n: 4}); err == nil {
		t.Fatal("Dump reported success when the tar trailer could not be written")
	}
}
