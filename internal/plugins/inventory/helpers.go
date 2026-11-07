package inventory

import (
	"context"
	"log/slog"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// event writes one survey event through the storage's instance-prefixed
// logger.
func event(s Storage, ev human.Event) {
	s.Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

func storageOutcome(rep *model.RunReport, storageName string) string {
	for _, s := range rep.Storages {
		if s.Storage == storageName {
			return s.Outcome
		}
	}
	return ""
}
