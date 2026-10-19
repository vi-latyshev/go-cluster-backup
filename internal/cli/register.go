package cli

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/kubernetesconfigmap"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier/webhook"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor/age"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor/zstd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/gfs"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/files"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/k3sstate"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/postgres"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/redis"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sqlite"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/staged"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/local"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/s3"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/yandexdisk"
)

// Generic registers every plugin of the distribution — one registration line per
// plugin, the whole cost of adding a new one. Nothing here names a cluster or a
// deployment: a deployment's own names live in its configuration file.
func Generic(r *plugin.Registry) error {
	for _, register := range []func(*plugin.Registry) error{
		kubernetesconfigmap.Register,
		kuberneteslabels.Register,
		webhook.Register,
		age.Register,
		zstd.Register,
		gfs.Register,
		keeplast.Register,
		files.Register,
		k3sstate.Register,
		postgres.Register,
		redis.Register,
		sqlite.Register,
		staged.Register,
		local.Register,
		s3.Register,
		yandexdisk.Register,
	} {
		if err := register(r); err != nil {
			return err
		}
	}
	return nil
}
