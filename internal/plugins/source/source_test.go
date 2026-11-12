package source_test

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
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
	if got, err := source.StagedArtifactName("example-state", rec); err != nil || got != "example-state.tar.age" {
		t.Fatalf("StagedArtifactName = %q, %v, want example-state.tar.age", got, err)
	}
	// An empty tail is valid for a format without a suffix.
	if got, err := source.StagedArtifactName("example-state", source.StagedRecord{}); err != nil || got != "example-state" {
		t.Fatalf("StagedArtifactName = %q, %v, want the bare name", got, err)
	}
	// A tail that could steer a write or a read out of the stage dir is refused
	// by the one derivation the writer and the reader share.
	for _, ext := range []string{"/../../x", "..", ".a/b"} {
		if name, err := source.StagedArtifactName("example-state", source.StagedRecord{Ext: ext}); err == nil {
			t.Fatalf("StagedArtifactName accepted ext %q as %q", ext, name)
		}
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

// TestStagedRecordCarriesWarningsAndReadsAnOlderOne: the stage's findings ride
// the record into the run, and a record written before the field existed still
// decodes — the reader accepts absent optional members, it never requires them.
func TestStagedRecordCarriesWarningsAndReadsAnOlderOne(t *testing.T) {
	dir := t.TempDir()
	want := model.Warning{Code: source.CredentialWarningCode, Message: "plaintext credentials"}
	body := []byte(`{"ext":".tar.age","warnings":[{"code":"` + want.Code + `","message":"` + want.Message + `"}]}`)
	if err := os.WriteFile(source.StagedChainFile(dir, "with-warnings"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := source.ReadStagedRecord(dir, "with-warnings")
	if err != nil {
		t.Fatalf("ReadStagedRecord: %v", err)
	}
	if len(got.Warnings) != 1 || got.Warnings[0] != want {
		t.Fatalf("warnings = %+v, want %+v", got.Warnings, want)
	}

	legacy := `{"ext": ".tar.age", "chain": []}`
	if err := os.WriteFile(source.StagedChainFile(dir, "legacy"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = source.ReadStagedRecord(dir, "legacy")
	if err != nil {
		t.Fatalf("an older record was refused: %v", err)
	}
	if got.Warnings != nil {
		t.Fatalf("an older record invented warnings: %+v", got.Warnings)
	}
}

// TestDecideCredentials: credential bytes are refused unless something seals
// them; a source type's published warning exception is the only way past that,
// and it never applies to bytes the source did not name as credentials.
func TestDecideCredentials(t *testing.T) {
	artifact := model.Artifact{Name: "cluster/state"}
	credentials := source.SourceTraits{Credentials: true}

	if got := source.DecideCredentials(artifact, source.SourceTraits{}, false, source.CredentialRefuse); got != (source.CredentialDecision{}) {
		t.Fatalf("a source without credential bytes decided %+v", got)
	}
	if got := source.DecideCredentials(artifact, credentials, true, source.CredentialRefuse); got != (source.CredentialDecision{}) {
		t.Fatalf("sealed credential bytes decided %+v", got)
	}

	refused := source.DecideCredentials(artifact, credentials, false, source.CredentialRefuse)
	if refused.Refused == "" || refused.Warning.Code != "" {
		t.Fatalf("unsealed credentials = %+v, want a refusal", refused)
	}
	if !strings.Contains(refused.Refused, artifact.Name) || !strings.Contains(refused.Refused, "sealing") {
		t.Fatalf("refusal does not name the artifact and the remedy: %q", refused.Refused)
	}

	warned := source.DecideCredentials(artifact, credentials, false, source.CredentialWarn)
	if warned.Refused != "" || warned.Warning.Code != source.CredentialWarningCode {
		t.Fatalf("a warning verdict = %+v", warned)
	}
	if !strings.Contains(warned.Warning.Message, artifact.Name) {
		t.Fatalf("warning does not name the artifact: %q", warned.Warning.Message)
	}
	if got := source.DecideCredentials(artifact, source.SourceTraits{}, false, source.CredentialWarn); got != (source.CredentialDecision{}) {
		t.Fatalf("a warning verdict warned about bytes without credentials: %+v", got)
	}
}
