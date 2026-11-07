package backup_test

import (
	"context"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"testing/synctest"
)

func TestR4_M14_CapRefusalWithoutWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		addMultipart(w, "alpha", 4)
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1)}))
		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
		if err != nil {
			t.Fatal(err)
		}
		if !storageResult(rep, "alpha").CapExceeded {
			t.Fatalf("no cap refusal: %+v", rep.Storages)
		}
		if keys := w.storage(t, "alpha").Keys(); len(keys) != 0 {
			t.Fatalf("writes occurred: %v", keys)
		}
		if u := w.Catalog.Uncertainty(); u != nil {
			t.Fatalf("no-write cap refusal retained own pending id: %v", u.RunIDs)
		}
	})
}
