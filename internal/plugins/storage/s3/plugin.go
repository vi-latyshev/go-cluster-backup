package s3

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(storage.Kind.Name(), "s3"),
	Summary: "an S3-compatible endpoint",
}

// Apply contributes the S3 storage factory and its descriptor to the compiled
// schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return storage.AddFactory(lr, factories, storage.FactorySpec[Config]{
		Type:      "s3",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example: []byte(`{"endpoint": "https://s3.example.com", "region": "ru-central-1", "bucket": "bucket-name",` +
			` "prefix": "cluster-backup", "access_key_id": {"file": "/run/secrets/s3/access-key-id"},` +
			` "secret_access_key": {"file": "/run/secrets/s3/secret-access-key"}, "addressing": "path",` +
			` "storage_class": "STANDARD", "request_checksum": "when_required", "part_bytes": 16777216,` +
			` "max_request_bytes": 5368709120, "max_object_bytes": 5497558138880,` +
			` "request_timeout": "5m"}`),
		New: newStorage,
	})
}
