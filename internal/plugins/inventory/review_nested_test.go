package inventory

import (
	"context"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// TestR2_C3_MissingRecordedDecoderIsAProblemRun is the reviewer's probe, kept as
// written: a manifest whose recorded chain loses its decoder type is a problem
// run — the chain cannot be replayed, so the copy is not protection.
func TestR2_C3_MissingRecordedDecoderIsAProblemRun(t *testing.T) {
	rep := model.RunReport{Schema: model.ManifestSchema, RunID: c3RunID,
		Kind: model.RunKindScheduled, StartedAt: time.Now().UTC(), Outcome: model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Group: "db", Name: "prod", Ext: ".dump.age", Importance: model.ImportanceRequired, Sensitivity: model.SensitivitySecret, Sealed: true},
			Outcome: model.ArtifactOK, Key: "runs/" + c3RunID + "/prod", Bytes: 7, SHA256: model.SHA256Hex([]byte("sealed!")), Source: model.SourceRef{Instance: "prod", Type: "postgres", Origin: "postgres://example.com/prod"},
			Chain: []model.ProcessorStep{{Instance: "seal", Type: "age", Ext: ".age", Seals: true}}}},
		Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
	}
	var doc map[string]any
	if err := json.Unmarshal(mustMarshal(t, rep), &doc); err != nil {
		t.Fatal(err)
	}
	artifact := doc["artifacts"].([]any)[0].(map[string]any)
	delete(artifact["chain"].([]any)[0].(map[string]any), "type")
	fake := storagetest.NewFake()
	fake.PutObject("runs/"+c3RunID+"/manifest.json", mustMarshal(t, doc), time.Now().UTC())
	fake.PutObject("runs/"+c3RunID+"/prod", []byte("sealed!"), time.Now().UTC())
	survey := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", fake, 1))
	run := survey.Runs[c3RunID]
	if run.ManifestProblem == "" || run.Stored.Verified || run.Stored.Manifest {
		t.Fatalf("missing recorded decoder accepted: problem=%q verified=%v manifest=%v", run.ManifestProblem, run.Stored.Verified, run.Stored.Manifest)
	}
}
