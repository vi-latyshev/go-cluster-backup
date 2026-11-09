package backup_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

type review3ExcludedStorage struct{ *fakeMultipart }

func (*review3ExcludedStorage) Check(context.Context) error {
	return pluginerrors.Permanent(errors.New("permanently excluded before writes"))
}

func TestR3_M14_ExcludedPeerCannotGrowPendingSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		addMultipart(w, "alpha", 4)
		w.ExtraStorages["beta"] = &review3ExcludedStorage{addMultipart(w, "beta", 4)}
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		config := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40), "beta": manualStorage(1 << 40)})
		for i := 0; i < 3; i++ {
			e := w.newAssembly(t, config)
			if i == 0 {
				w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
			}
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			if err != nil || storageResult(rep, "alpha").Outcome != model.StorageVerified {
				t.Fatalf("healthy own writes did not settle: %v %+v", err, rep.Storages)
			}
			if len(w.storage(t, "beta").Keys()) != 0 {
				t.Fatal("excluded peer received writes")
			}
			time.Sleep(time.Second)
		}
		if u := w.Catalog.Uncertainty(); u != nil {
			t.Fatalf("settled own writes plus an excluded peer retained growing pending set: %v", u.RunIDs)
		}
	})
}
