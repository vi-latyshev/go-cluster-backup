package backup_test

import (
	"context"
	"errors"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestListedManifestThatReadsBackAbsentIsNeverDeleted(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	withRetention(cfg, "primary", keepLastN(1))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "primary")
	seedID := "20261001T040000Z"
	putCopiedRun(t, f, "primary", seedID, time.Now().Add(-24*time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	key := "runs/" + seedID + "/manifest.json"
	f.Fault = func(op storagetest.Op, k string, _ int) error {
		if op == storagetest.OpGet && k == key {
			return pluginerrors.NotFound(errors.New("gone"))
		}
		return nil
	}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if !runIDsOn(f)[seedID] {
		t.Fatalf("a run whose listed manifest read back absent was deleted: %v", runIDsOn(f))
	}
}

// TestBudgetExpiryLeavesEveryManifestReadAccounted (F4, S2): the budget can run
// out between the manifest reads, and every run whose manifest is in the listing
// still gets an outcome — its manifest or a problem — so retention never takes a
// run the survey could not read as one with no manifest.
func TestBudgetExpiryLeavesEveryManifestReadAccounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		withRetention(cfg, "primary", keepLastN(1))
		cfg["storages"].(map[string]any)["primary"].(map[string]any)["time_budget"] = "1s"
		e := w.newAssembly(t, cfg)
		f := w.storage(t, "primary")
		now := time.Now().UTC()
		ids := []string{"20261001T040000Z", "20261002T040000Z", "20261003T040000Z", "20261004T040000Z"}
		for i, id := range ids {
			putCopiedRun(t, f, "primary", id, now.Add(-time.Duration(len(ids)-i)*time.Hour),
				model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
		}
		// The run's own budget expires inside its one survey interval: the listing
		// alone outlasts it, so every manifest read starts on an expired context.
		f.Fault = func(op storagetest.Op, _ string, _ int) error {
			if op == storagetest.OpList {
				time.Sleep(2 * time.Second)
			}
			return nil
		}
		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		// Not one manifest was read, so no run is a verified copy: the policy may
		// delete none of them, whatever the budget exhaustion did to the sweep.
		kept := runIDsOn(f)
		for _, id := range ids {
			if !kept[id] {
				t.Fatalf("retention deleted run %s, whose manifest it could not read", id)
			}
		}
		if rep.Outcome == model.OutcomeDelivered {
			t.Fatalf("a run whose storage budget expired reported delivered: %+v", rep.Storages)
		}
	})
}
