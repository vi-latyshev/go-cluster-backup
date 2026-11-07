package yandexdisk

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(storage.Kind.Name(), "yandex_disk"),
	Summary: "the Yandex Disk REST API, scoped to one folder of the application folder",
}

// Apply contributes the Yandex Disk storage factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return storage.AddFactory(lr, factories, storage.FactorySpec[Config]{
		Type:      "yandex_disk",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example: []byte(`{"api_url": "https://cloud-api.yandex.net/v1/disk",` +
			` "token": {"file": "/run/secrets/yandex/token"}, "root": "app:/cluster-backup",` +
			` "permanent_delete": true, "prune_keep_depth": 1, "request_timeout": "30s",` +
			` "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`),
		New: newStorage,
	})
}
