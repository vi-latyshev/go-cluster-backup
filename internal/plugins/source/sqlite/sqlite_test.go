package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sqlite"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := sqlite.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return plugin.Build(context.Background(), testRegistry(t), source.Kind, "sqlite",
		jsontext.Value(cfg), plugin.Deps{Instance: "sqlite", Log: slog.New(slog.DiscardHandler)})
}

func config(t *testing.T, path string) string {
	t.Helper()
	return configWithScratch(t, path, t.TempDir())
}

func configWithScratch(t *testing.T, path, scratch string) string {
	t.Helper()
	return fmt.Sprintf(`{"path": %q, "scratch_dir": %q, "busy_timeout": "10s", "integrity_check": "quick"}`, path, scratch)
}

// databaseFile writes a file to stand in for the live database.
func databaseFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.db")
	if err := os.WriteFile(path, []byte("LIVE-DB"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// stubs writes a fake sqlite3 that records argv, writes the .backup target and
// answers pragmas from a file.
func stubs(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	script := `#!/bin/sh
printf '%s\n' "$@" > "$STUB_LOG/argv"
if [ -n "$4" ] && [ ! -e "$4" ]; then echo "Error: unable to open database file" >&2; exit 1; fi
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  .backup*)
    f=${last#".backup "}
    f=${f#\"}
    f=${f%\"}
    if [ -f "$STUB_LOG/copy" ]; then cat "$STUB_LOG/copy" > "$f"; else printf 'SQLITE-COPY' > "$f"; fi
    if [ -f "$STUB_LOG/unlink" ]; then rm -f "$f"; fi
    ;;
  *)
    cat "$STUB_LOG/pragma" 2>/dev/null
    ;;
esac
cat "$STUB_LOG/out" 2>/dev/null
if [ -f "$STUB_LOG/stderr" ]; then cat "$STUB_LOG/stderr" >&2; fi
code=0; if [ -f "$STUB_LOG/exit" ]; then code=$(cat "$STUB_LOG/exit"); fi
exit "$code"
`
	if err := os.WriteFile(filepath.Join(dir, "sqlite3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func stubSet(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(os.Getenv("STUB_LOG"), name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func stubLog(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(os.Getenv("STUB_LOG"), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		stubs(t)
		s, err := build(t, config(t, databaseFile(t)))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestDescribe(t *testing.T) {
	stubs(t)
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".db" || tr.Origin != "file://"+path || tr.Sealed {
		t.Fatalf("traits = %+v", tr)
	}
}

func TestDumpBacksUpOnline(t *testing.T) {
	stubs(t)
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "SQLITE-COPY" {
		t.Fatalf("dumped %q", buf.String())
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	want := []string{"-readonly", "-cmd", ".timeout 10000", path}
	if strings.Join(argv[:4], "|") != strings.Join(want, "|") {
		t.Fatalf("argv = %q, want %q first", argv, want)
	}
	if !strings.HasPrefix(argv[4], `.backup "`) {
		t.Fatalf("argv = %q, want the .backup command last", argv)
	}
}

func TestDumpDatabaseMissingIsPermanent(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, filepath.Join(t.TempDir(), "gone.db")))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a missing database was not an error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

func TestDumpLockedDatabaseIsTransient(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "Error: in prepare, database is locked (5)")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

func TestDumpMalformedDatabaseIsPermanent(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "Error: in prepare, file is not a database (26)")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

func TestDumpWriterFailureStopsTheCopy(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), &plugintest.ErrWriter{}); err == nil {
		t.Fatal("a failing destination was ignored")
	}
}

func TestCheckFindsTheFile(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err != nil {
		t.Fatal(err)
	}

	missing, err := build(t, config(t, filepath.Join(t.TempDir(), "gone.db")))
	if err != nil {
		t.Fatal(err)
	}
	if err := missing.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a missing database passed Check")
	}

	t.Setenv("PATH", t.TempDir())
	if err := s.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "sqlite3") {
		t.Fatalf("Check = %v, want a missing sqlite3 named", err)
	}
}

func TestCheckJudgesTheScratchDir(t *testing.T) {
	stubs(t)
	missing := filepath.Join(t.TempDir(), "gone")
	s, err := build(t, configWithScratch(t, databaseFile(t), missing))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("Check = %v, want the missing scratch_dir named", err)
	}

	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	notDir, err := build(t, configWithScratch(t, databaseFile(t), file))
	if err != nil {
		t.Fatal(err)
	}
	if err := notDir.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("Check = %v, want a scratch_dir that is a file refused", err)
	}

	if os.Getuid() != 0 {
		readOnly := t.TempDir()
		if err := os.Chmod(readOnly, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })
		denied, err := build(t, configWithScratch(t, databaseFile(t), readOnly))
		if err != nil {
			t.Fatal(err)
		}
		if err := denied.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
			t.Fatalf("Check = %v, want an unwritable scratch_dir refused", err)
		}
	}
}

func TestDescribeRespectsCancellation(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Fatal("Describe accepted a cancelled context")
	}
}

func TestCheckRefusesADirectory(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("Check = %v, want a directory named as the problem", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, cfg := range []string{
		`{"path": "", "scratch_dir": "/scratch", "busy_timeout": "10s", "integrity_check": "quick"}`,
		`{"path": "/x", "busy_timeout": "10s", "integrity_check": "quick"}`,
		`{"path": "/x", "scratch_dir": "", "busy_timeout": "10s", "integrity_check": "quick"}`,
		`{"path": "/x", "scratch_dir": "relative", "busy_timeout": "10s", "integrity_check": "quick"}`,
		`{"path": "/x", "scratch_dir": "/scratch", "busy_timeout": "0s", "integrity_check": "quick"}`,
		`{"path": "/x", "scratch_dir": "/scratch", "busy_timeout": "10s", "integrity_check": "none"}`,
	} {
		if _, err := build(t, cfg); err == nil {
			t.Errorf("config %s accepted", cfg)
		}
	}
}

func TestDumpSpoolsIntoTheScratchDir(t *testing.T) {
	stubs(t)
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, databaseFile(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	target := strings.TrimSuffix(strings.TrimPrefix(argv[4], `.backup "`), `"`)
	if !strings.HasPrefix(target, scratch+string(os.PathSeparator)) || !strings.Contains(target, "sqlite-backup-") {
		t.Fatalf("the snapshot target %q is not a temp file in the scratch dir %q", target, scratch)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temp files left in the scratch dir: %v", entries)
	}
}

const applyOptions = `{"file_mode": "0640", "keep_previous": true}`

func TestApplySideBySide(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != path+".20261012T042000Z" || rep.Previous != "" {
		t.Fatalf("report = %+v", rep)
	}
	got, err := os.ReadFile(rep.Target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "RESTORED" {
		t.Fatalf("target content = %q", got)
	}
	live, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(live) != "LIVE-DB" {
		t.Fatalf("the live database changed: %q", live)
	}
	info, err := os.Stat(rep.Target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("mode = %o, want 640", info.Mode().Perm())
	}
}

func TestApplySideBySideRefusesExistingTarget(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	existing := path + ".20261012T042000Z"
	if err := os.WriteFile(existing, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("an existing side-by-side target was overwritten")
	}
	if got, _ := os.ReadFile(existing); string(got) != "old" {
		t.Fatalf("the existing target changed: %q", got)
	}
}

func TestApplyInPlaceKeepsPrevious(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != path || rep.Previous != path+".previous_20261012T042000Z" {
		t.Fatalf("report = %+v", rep)
	}
	if got, _ := os.ReadFile(rep.Target); string(got) != "RESTORED" {
		t.Fatalf("in-place target = %q", got)
	}
	if got, _ := os.ReadFile(rep.Previous); string(got) != "LIVE-DB" {
		t.Fatalf("previous file = %q", got)
	}
}

func TestApplyInPlaceWithoutKeepingPrevious(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z",
		Options: jsontext.Value(`{"file_mode": "0640", "keep_previous": false}`),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Previous != "" {
		t.Fatalf("previous = %q, want none", rep.Previous)
	}
	if got, _ := os.ReadFile(path); string(got) != "RESTORED" {
		t.Fatalf("target = %q", got)
	}
}

func TestApplyInPlaceCreatesWhatIsNotThere(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := filepath.Join(t.TempDir(), "fresh.db")
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != path || rep.Previous != "" {
		t.Fatalf("report = %+v", rep)
	}
	if got, _ := os.ReadFile(path); string(got) != "RESTORED" {
		t.Fatalf("target = %q", got)
	}
}

func TestApplyInPlaceMovesWalAndShmWithThePreviousFile(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	if err := os.WriteFile(path+"-wal", []byte("OLD-WAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-shm", []byte("OLD-SHM"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	for suffix, want := range map[string]string{"": "LIVE-DB", "-wal": "OLD-WAL", "-shm": "OLD-SHM"} {
		got, err := os.ReadFile(rep.Previous + suffix)
		if err != nil {
			t.Fatalf("previous%s: %v", suffix, err)
		}
		if string(got) != want {
			t.Fatalf("previous%s = %q, want %q", suffix, got, want)
		}
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			t.Fatalf("a stale %s stayed beside the replaced database", suffix)
		}
	}
	if got, _ := os.ReadFile(rep.Target); string(got) != "RESTORED" {
		t.Fatalf("in-place target = %q", got)
	}
}

func TestApplyInPlaceDropsStaleWalAndShmWithoutAPreviousCopy(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	if err := os.WriteFile(path+"-wal", []byte("OLD-WAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-shm", []byte("OLD-SHM"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z",
		Options: jsontext.Value(`{"file_mode": "0640", "keep_previous": false}`),
	}, strings.NewReader("RESTORED"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Previous != "" {
		t.Fatalf("previous = %q, want none", rep.Previous)
	}
	if got, _ := os.ReadFile(path); string(got) != "RESTORED" {
		t.Fatalf("target = %q", got)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			t.Fatalf("a stale %s outlived the database it belonged to", suffix)
		}
	}
}

func TestApplyInPlaceRefusesAnExistingPrevious(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	if err := os.WriteFile(path+".previous_20261012T042000Z", []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("an existing previous file was overwritten")
	}
	if got, _ := os.ReadFile(path); string(got) != "LIVE-DB" {
		t.Fatalf("the live database changed: %q", got)
	}
}

func TestApplyFailingIntegrityCheckChangesNothing(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "*** in database main ***\nPage 3: btreeInitPage() returns error code 11\n")
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("TORN")); err == nil {
		t.Fatal("a copy that failed its integrity check was applied")
	}
	if got, _ := os.ReadFile(path); string(got) != "LIVE-DB" {
		t.Fatalf("the live database changed: %q", got)
	}
}

func TestApplyRefusesBadRequests(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	restorer := s.(source.Restorer)
	for _, req := range []source.ApplyRequest{
		{Mode: "", Label: "l", Options: jsontext.Value(applyOptions)},
		{Mode: "overwrite", Label: "l", Options: jsontext.Value(applyOptions)},
		{Mode: source.ApplySideBySide, Label: "../evil", Options: jsontext.Value(applyOptions)},
		{Mode: source.ApplySideBySide, Label: "l", Options: nil},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"file_mode": "0640"}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"file_mode": "", "keep_previous": true}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"file_mode": "999", "keep_previous": true}`)},
	} {
		if _, err := restorer.Apply(context.Background(), req, strings.NewReader("x")); err == nil {
			t.Errorf("request %+v accepted", req)
		}
	}
}

func TestProbe(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("SQLITE-COPY"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bytes != int64(len("SQLITE-COPY")) || !strings.Contains(rep.Detail, "quick_check") {
		t.Fatalf("probe report = %+v", rep)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	if argv[len(argv)-1] != "PRAGMA quick_check;" {
		t.Fatalf("argv = %q", argv)
	}

	stubSet(t, "pragma", "corrupt\n")
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("torn")); err == nil {
		t.Fatal("a corrupt file probed clean")
	}
}

func TestProbeUsesTheConfiguredCheck(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	s, err := build(t, `{"path": "/x", "scratch_dir": "`+t.TempDir()+`", "busy_timeout": "10s", "integrity_check": "full"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("SQLITE-COPY")); err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	if argv[len(argv)-1] != "PRAGMA integrity_check;" {
		t.Fatalf("argv = %q", argv)
	}
}

func TestProbeSpoolsIntoTheScratchDir(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, databaseFile(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("SQLITE-COPY")); err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	if !strings.HasPrefix(argv[1], scratch+string(os.PathSeparator)) || !strings.Contains(argv[1], "sqlite-probe-") {
		t.Fatalf("the probed file %q is not a temp file in the scratch dir %q", argv[1], scratch)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temp files left in the scratch dir: %v", entries)
	}
}

func TestDumpCancelled(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Fatal("a cancelled dump returned success")
	}
	if _, err := s.(source.Prober).Probe(ctx, strings.NewReader("x")); err == nil {
		t.Fatal("a cancelled probe returned success")
	}
}

// errReader fails on its first read: the source of a copy that cannot finish.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("injected read failure") }

func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	stubs(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.Build(ctx, testRegistry(t), source.Kind, "sqlite",
		jsontext.Value(config(t, databaseFile(t))), plugin.Deps{Instance: "sqlite"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Error("Dump accepted a cancelled context")
	}
	if _, err := s.(source.Restorer).Apply(ctx, source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("x")); err == nil {
		t.Error("Apply accepted a cancelled context")
	}
	if _, err := s.(source.Prober).Probe(ctx, strings.NewReader("x")); err == nil {
		t.Error("Probe accepted a cancelled context")
	}
}

func TestCheckRefusesAnUnreadableDatabaseFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root opens anything; the permission failure cannot be provoked")
	}
	stubs(t)
	path := databaseFile(t)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("an unreadable database file passed Check")
	}
}

func TestDumpFailsWhenTheScratchDirVanished(t *testing.T) {
	stubs(t)
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, databaseFile(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(scratch); err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("Dump accepted a scratch dir it could not spool into")
	}
}

func TestDumpRefusesAnEmptyCopy(t *testing.T) {
	stubs(t)
	stubSet(t, "copy", "")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "empty copy") {
		t.Fatalf("Dump = %v, want an empty copy refused", err)
	}
}

func TestDumpFailsWhenTheCopyVanishes(t *testing.T) {
	stubs(t)
	stubSet(t, "unlink", "1")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a copy that vanished before it was read was streamed")
	}
}

func TestApplyFailsWhenTheTargetDirectoryIsUnwritable(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	stubs(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "app.db")
	if err := os.WriteFile(path, []byte("LIVE-DB"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("Apply accepted a target directory it could not write into")
	}
}

func TestApplyFailsOnAFailingSourceStream(t *testing.T) {
	stubs(t)
	path := databaseFile(t)
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, errReader{}); err == nil {
		t.Fatal("Apply reported success over a failing source stream")
	}
	if got, _ := os.ReadFile(path); string(got) != "LIVE-DB" {
		t.Fatalf("the live database changed: %q", got)
	}
}

// TestApplyInPlaceRefusesPreviousSidecarsInTheWay: an in-place restore that
// keeps the previous file moves its write-ahead files aside, and a destination
// already standing there is refused rather than overwritten.
func TestApplyInPlaceRefusesPreviousSidecarsInTheWay(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	if err := os.WriteFile(path+"-wal", []byte("OLD-WAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	previous := path + ".previous_20261012T042000Z"
	if err := os.WriteFile(previous+"-wal", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("a write-ahead file already standing beside the previous copy was overwritten")
	}
	// The refusal names where the current database went, so nothing is lost.
	if got, _ := os.ReadFile(previous); string(got) != "LIVE-DB" {
		t.Fatalf("the current database is not at the previous path: %q", got)
	}
}

// TestApplyInPlaceFailsWhenAStaleSidecarIsADirectory: dropping the stale
// write-ahead files of a replaced database is a failure when one of them cannot
// be removed, and the live database is left untouched.
func TestApplyInPlaceFailsWhenAStaleSidecarIsADirectory(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := databaseFile(t)
	stale := path + "-wal"
	if err := os.Mkdir(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "inner"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l",
		Options: jsontext.Value(`{"file_mode": "0640", "keep_previous": false}`),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("a stale write-ahead directory that could not be removed was ignored")
	}
	if got, _ := os.ReadFile(path); string(got) != "LIVE-DB" {
		t.Fatalf("the live database changed: %q", got)
	}
}

// TestApplyInPlaceFailsWhenTheTargetIsADirectory: the publish is a rename, and
// renaming the restored copy over a directory fails — the report says so
// instead of leaving a torn database.
func TestApplyInPlaceFailsWhenTheTargetIsADirectory(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	path := filepath.Join(t.TempDir(), "app.db")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(t, path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l",
		Options: jsontext.Value(`{"file_mode": "0640", "keep_previous": false}`),
	}, strings.NewReader("RESTORED")); err == nil {
		t.Fatal("the restored copy was renamed over a directory")
	}
}

func TestProbeFailsWhenTheScratchDirVanished(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, databaseFile(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(scratch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("x")); err == nil {
		t.Fatal("Probe accepted a scratch dir it could not spool into")
	}
}

func TestProbeFailsOnAFailingSourceStream(t *testing.T) {
	stubs(t)
	stubSet(t, "pragma", "ok\n")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), errReader{}); err == nil {
		t.Fatal("Probe reported success over a failing source stream")
	}
}

// TestCheckCommandFailureIsReported: sqlite3 exiting non-zero is the check's
// own failure, classified from its message.
func TestCheckCommandFailureIsReported(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "Error: disk I/O error")
	s, err := build(t, config(t, databaseFile(t)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.Prober).Probe(context.Background(), strings.NewReader("x"))
	if err == nil {
		t.Fatal("a failing integrity-check command was reported as a clean copy")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}
