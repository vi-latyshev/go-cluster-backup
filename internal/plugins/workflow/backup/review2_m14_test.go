package backup_test

import (
	"context"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"testing/synctest"
	"time"
)

func TestR2_M14_SuccessfulSuccessorsEventuallySettle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		addMultipart(w, "alpha", 4)
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		config := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
		for i := 0; i < 3; i++ {
			e := w.newAssembly(t, config)
			if i == 0 {
				w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
			}
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			if err != nil || rep.Outcome != model.OutcomeDelivered {
				t.Fatalf("successor failed: %v %s", err, rep.Outcome)
			}
			time.Sleep(time.Second)
		}
		if uncertainty := w.Catalog.Uncertainty(); uncertainty != nil {
			t.Fatalf("three fully settled successors retained growing pending set: %v", uncertainty.RunIDs)
		}
	})
}
