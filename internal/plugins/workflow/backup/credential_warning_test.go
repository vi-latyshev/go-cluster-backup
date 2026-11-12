package backup_test

// The source envelope's facts travel with the artifact: a credential warning
// the producer decided before the first byte is recorded on the artifact result
// and in the run's manifest, where a message template can read it.

import (
	"context"
	"slices"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// credentialWarnSource is a plain fake that publishes the one credential
// exception the policy allows: a warning instead of a refusal.
type credentialWarnSource struct{ *fakeSource }

func (credentialWarnSource) CredentialPolicy(context.Context) (source.CredentialVerdict, error) {
	return source.CredentialWarn, nil
}

func TestCredentialWarningReachesTheArtifactAndItsManifest(t *testing.T) {
	w := newWorld(t)
	w.ExtraSources["vpn"] = credentialWarnSource{fakeSource: &fakeSource{
		Data:   []byte("credential bytes"),
		Traits: source.SourceTraits{Credentials: true},
	}}
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if !slices.ContainsFunc(rep.Artifacts[0].Warnings, func(w model.Warning) bool {
		return w.Code == source.CredentialWarningCode
	}) {
		t.Fatalf("the artifact lost the source's credential warning: %+v", rep.Artifacts[0].Warnings)
	}
	body, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/manifest.json")
	if !ok {
		t.Fatal("the manifest was not written")
	}
	var stored model.RunReport
	if err := jsonUnmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(stored.Artifacts[0].Warnings, func(w model.Warning) bool {
		return w.Code == source.CredentialWarningCode
	}) {
		t.Fatalf("the manifest lost the credential warning: %+v", stored.Artifacts[0].Warnings)
	}
}
