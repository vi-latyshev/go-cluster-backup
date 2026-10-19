package staged_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/staged"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := staged.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return plugin.Build(context.Background(), testRegistry(t), source.Kind, "staged",
		jsontext.Value(cfg), plugin.Deps{Instance: "staged", Log: slog.New(slog.DiscardHandler)})
}

func config(dir, name string) string {
	return fmt.Sprintf(`{"dir": %q, "name": %q}`, dir, name)
}

// writeRecord writes the chain record the engine's Stage leaves beside the
// artifact: the exact extension tail and the chain.
func writeRecord(t *testing.T, dir, name, ext, chain string) {
	t.Helper()
	rec := `{"ext": "` + ext + `", "chain": ` + chain + `}`
	if err := os.WriteFile(source.StagedChainFile(dir, name), []byte(rec), 0o644); err != nil {
		t.Fatal(err)
	}
}

// stagedDir writes the pair the engine's Stage would have left behind: the
// artifact and the record that names it.
func stagedDir(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "example-state.tar.age")
	if err := os.WriteFile(path, []byte("SEALED-BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeRecord(t, dir, "example-state", ".tar.age",
		`[{"instance":"seal","type":"age","ext":".age","seals":true}]`)
	return dir, path
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		dir, _ := stagedDir(t)
		s, err := build(t, config(dir, "example-state"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestDescribeReadsTheStagedFile(t *testing.T) {
	dir, path := stagedDir(t)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".tar.age" {
		t.Fatalf("Ext = %q, want .tar.age", tr.Ext)
	}
	// The source claims nothing about sealing: the recorded chain's steps are
	// what say whether the bytes are sealed, and claiming it here would call
	// plaintext sealed whenever the stage ran without a sealing processor.
	if tr.Sealed {
		t.Fatal("a staged artifact claims to be sealed on its own")
	}
	if tr.Origin != "file://"+path {
		t.Fatalf("Origin = %q", tr.Origin)
	}
}

func TestDumpStreamsTheFile(t *testing.T) {
	dir, _ := stagedDir(t)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "SEALED-BYTES" {
		t.Fatalf("dumped %q", buf.String())
	}
	if err := s.Dump(context.Background(), &plugintest.ErrWriter{}); err == nil {
		t.Fatal("a failing destination was ignored")
	}
}

func TestMissingStagedArtifactRefuses(t *testing.T) {
	dir := t.TempDir()
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a missing staged artifact passed Check")
	}
	if _, err := s.Describe(context.Background()); err == nil {
		t.Fatal("Describe accepted a missing staged artifact")
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("Dump accepted a missing staged artifact")
	}
	missingDir, err := build(t, config(filepath.Join(t.TempDir(), "gone"), "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := missingDir.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a missing staged directory passed Check")
	}
}

// TestABesideFileIsNotThisArtifact: the record names the file exactly, so an
// entry merely sharing the name prefix is never read — not even when the named
// file is missing, where reading the neighbour would upload its bytes under
// this artifact's name.
func TestABesideFileIsNotThisArtifact(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "example-state.tar.age"), []byte("NEIGHBOUR"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a longer-named neighbour satisfied this source's check")
	}
	// With the record and its own file present, only the named file is read.
	own := filepath.Join(dir, "example-state.db")
	if err := os.WriteFile(own, []byte("MINE"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeRecord(t, dir, "example-state", ".db", `[]`)
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".db" || tr.Origin != "file://"+own {
		t.Fatalf("Describe = %+v, want the record's own file %s", tr, own)
	}
	// The named file gone, a same-prefix neighbour beside it must not step in.
	if err := os.Remove(own); err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil {
		t.Fatal("a neighbour stood in for the record's missing file")
	}
	if !strings.Contains(err.Error(), "no staged artifact named example-state.db") {
		t.Fatalf("the refusal does not name the exact file: %v", err)
	}
}

func TestRecordedArtifactMissingRefuses(t *testing.T) {
	dir := t.TempDir()
	writeRecord(t, dir, "example-state", ".tar.age", `[]`)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil {
		t.Fatal("a record whose artifact is absent passed Check")
	}
	if !strings.Contains(err.Error(), "no staged artifact named example-state.tar.age") {
		t.Fatalf("the refusal does not name the exact file: %v", err)
	}
}

func TestEmptyStagedArtifactRefuses(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "example-state.tar.age"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	writeRecord(t, dir, "example-state", ".tar.age", `[]`)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("an empty staged artifact passed Check")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, cfg := range []string{
		`{"dir": "", "name": "x"}`,
		`{"dir": "relative", "name": "x"}`,
		`{"dir": "/d", "name": ""}`,
		`{"dir": "/d", "name": "a/b"}`,
	} {
		if _, err := build(t, cfg); err == nil {
			t.Errorf("config %s accepted", cfg)
		}
	}
}

// TestRecordedChainReadsTheStagesRecord: the chain the stage published beside
// the artifact is what the source reports, step for step, and the record never
// shadows the artifact it belongs to.
func TestRecordedChainReadsTheStagesRecord(t *testing.T) {
	dir, _ := stagedDir(t)
	chain := []model.ProcessorStep{
		{Instance: "squeeze", Type: "zstd", Ext: ".zst"},
		{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
	}
	writeRecord(t, dir, "example-state", ".tar.age",
		`[{"instance":"squeeze","type":"zstd","ext":".zst","seals":false},`+
			`{"instance":"seal","type":"age","ext":".age","seals":true}]`)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.(source.RecordedChain).RecordedChain(context.Background())
	if err != nil {
		t.Fatalf("RecordedChain: %v", err)
	}
	if len(got) != len(chain) || got[0] != chain[0] || got[1] != chain[1] {
		t.Fatalf("RecordedChain = %+v, want %+v", got, chain)
	}
	// The record does not count as an artifact: Describe still finds the file.
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatalf("Describe with a chain record present: %v", err)
	}
	if tr.Ext != ".tar.age" {
		t.Fatalf("Ext = %q, want .tar.age", tr.Ext)
	}
}

// TestCancelledContextIsRefusedEverywhere: every entry point of the source
// reports a cancelled context instead of touching the directory.
func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	dir, _ := stagedDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := plugin.Build(ctx, testRegistry(t), source.Kind, "staged",
		jsontext.Value(config(dir, "example-state")), plugin.Deps{Instance: "staged"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Error("Dump accepted a cancelled context")
	}
	if _, err := s.(source.RecordedChain).RecordedChain(ctx); err == nil {
		t.Error("RecordedChain accepted a cancelled context")
	}
}

// TestAnUnreadableChainRecordRefuses: a record that exists but cannot be read
// as a file is a permanent fault, never "there is no record".
func TestAnUnreadableChainRecordRefuses(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(source.StagedChainFile(dir, "example-state"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.RecordedChain).RecordedChain(context.Background())
	if err == nil {
		t.Fatal("a record that is not a file was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent: %v", class, err)
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an unreadable record was reported as a missing one: %v", err)
	}
}

// TestAStatFailureThatIsNotAbsenceRefuses: a path the record names but the
// filesystem cannot resolve for a reason other than absence is a permanent
// fault of its own, not "the stage left no artifact".
func TestAStatFailureThatIsNotAbsenceRefuses(t *testing.T) {
	dir := t.TempDir()
	writeRecord(t, dir, "example-state", ".x", `[]`)
	// A symlink loop: os.Stat follows it and fails with ELOOP, which is neither
	// a missing file nor a readable one.
	if err := os.Symlink("loop-b", filepath.Join(dir, "example-state.x")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("example-state.x", filepath.Join(dir, "loop-b")); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil {
		t.Fatal("an unresolvable artifact path was accepted")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a stat failure was reported as a missing artifact: %v", err)
	}
}

// TestAnUnopenableStagedFileRefuses: the file is there and non-empty, but this
// process cannot open it — a permanent fault, not a retry.
func TestAnUnopenableStagedFileRefuses(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root opens anything; the permission failure cannot be provoked")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "example-state.tar.age")
	if err := os.WriteFile(path, []byte("SEALED-BYTES"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	writeRecord(t, dir, "example-state", ".tar.age", `[]`)
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("an unopenable staged artifact was streamed")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent: %v", class, err)
	}
}

// TestRecordedChainWithoutARecordRefuses: bytes whose encoding nobody recorded
// are bytes nobody can prove are sealed — a permanent error, never an empty
// chain that a restore would take for "stored as produced".
func TestRecordedChainWithoutARecordRefuses(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "example-state.tar.age"), []byte("SEALED-BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(dir, "example-state"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.RecordedChain).RecordedChain(context.Background())
	if err == nil {
		t.Fatal("a missing chain record was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class %q, want permanent: %v", class, err)
	}
	if !strings.Contains(err.Error(), source.StagedChainFile(dir, "example-state")) {
		t.Fatalf("the error does not name the missing record: %v", err)
	}
}
