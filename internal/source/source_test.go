package source_test

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// TestStagedChainFileNamesTheRecord pins the one derivation the writer and the
// reader share: the record's name is the artifact's own name behind a leading
// dot, so it can never be mistaken for an artifact.
func TestStagedChainFileNamesTheRecord(t *testing.T) {
	if got, want := source.StagedChainFile("/backup/staged", "example-state"), "/backup/staged/.chain-example-state.json"; got != want {
		t.Fatalf("StagedChainFile = %q, want %q", got, want)
	}
}

// TestStagedArtifactNameIsTheRecordsOwnTail: the artifact is exactly
// <name><Ext>, so nothing else in the directory can be mistaken for it.
func TestStagedArtifactNameIsTheRecordsOwnTail(t *testing.T) {
	rec := source.StagedRecord{Ext: ".tar.age"}
	if got := source.StagedArtifactName("example-state", rec); got != "example-state.tar.age" {
		t.Fatalf("StagedArtifactName = %q, want example-state.tar.age", got)
	}
	// An empty tail is valid for a format without a suffix.
	if got := source.StagedArtifactName("example-state", source.StagedRecord{}); got != "example-state" {
		t.Fatalf("StagedArtifactName = %q, want the bare name", got)
	}
}

func TestReadStagedRecordRoundTripsTheStage(t *testing.T) {
	dir := t.TempDir()
	rec := source.StagedRecord{
		Ext: ".tar.age",
		Chain: []model.ProcessorStep{
			{Instance: "squeeze", Type: "zstd", Ext: ".zst"},
			{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
		},
	}
	raw := `{"ext": ".tar.age", "chain": [{"instance":"squeeze","type":"zstd","ext":".zst","seals":false},` +
		`{"instance":"seal","type":"age","ext":".age","seals":true}]}`
	if err := os.WriteFile(source.StagedChainFile(dir, "example-state"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := source.ReadStagedRecord(dir, "example-state")
	if err != nil {
		t.Fatalf("ReadStagedRecord: %v", err)
	}
	if got.Ext != rec.Ext || len(got.Chain) != len(rec.Chain) || got.Chain[1] != rec.Chain[1] {
		t.Fatalf("ReadStagedRecord = %+v, want %+v", got, rec)
	}
}

// TestReadStagedRecordReportsWhatTheStageLeftOut: a missing record is the
// caller's fs.ErrNotExist to report, and a record that is not the JSON it must
// be names the path it read.
func TestReadStagedRecordReportsWhatTheStageLeftOut(t *testing.T) {
	dir := t.TempDir()
	if _, err := source.ReadStagedRecord(dir, "example-state"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing record = %v, want fs.ErrNotExist", err)
	}
	path := source.StagedChainFile(dir, "example-state")
	if err := os.WriteFile(path, []byte(`{"ext":`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := source.ReadStagedRecord(dir, "example-state")
	if err == nil {
		t.Fatal("a malformed record was accepted")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("the error does not name the record it read: %v", err)
	}
}
