// Package reporting owns the previous-run record a report and every notification
// carry: what the storages hold of earlier scheduled runs, and why the previous
// Job of this run's own CronJob failed before this tool could report. The
// cluster keeps no run history — the catalog holds only the writer lease — so
// the storages' manifests are the record of what ran, and the cluster is read
// for the Job status alone.
package reporting

import (
	"context"
	"errors"
	"fmt"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Config is the reporting plugin's own settings: how to reach the API server and
// which environment variables carry this process's own pod identity. The
// defaults document fills every field for the Kubernetes deployment; a
// deployment outside a pod leaves the identity variables unset at run time, and
// then there is no previous Job to read.
type Config struct {
	APIHostEnv     string         `json:"api_host_env" doc:"name of the environment variable holding the API server host"`
	APIPortEnv     string         `json:"api_port_env" doc:"name of the environment variable holding the API server port"`
	TokenFile      string         `json:"token_file" doc:"path of the ServiceAccount token file, re-read on every request"`
	CAFile         string         `json:"ca_file" doc:"path of the CA bundle the API server certificate is verified against"`
	RequestTimeout model.Duration `json:"request_timeout" doc:"timeout of one API request"`
	NamespaceEnv   string         `json:"namespace_env" doc:"name of the environment variable holding this process's pod namespace"`
	PodEnv         string         `json:"pod_env" doc:"name of the environment variable holding this process's pod name"`
}

// Validate reports every setting that cannot work. It performs no I/O: a
// missing file is the client's business when it first reads one.
func (c *Config) Validate() error {
	var errs []error
	if c.APIHostEnv == "" {
		errs = append(errs, errors.New("/api_host_env is empty"))
	}
	if c.APIPortEnv == "" {
		errs = append(errs, errors.New("/api_port_env is empty"))
	}
	errs = append(errs, configuration.AbsolutePath(c.TokenFile, "/token_file")...)
	errs = append(errs, configuration.AbsolutePath(c.CAFile, "/ca_file")...)
	if c.RequestTimeout.Std() <= 0 {
		errs = append(errs, fmt.Errorf("/request_timeout must be positive, got %s", c.RequestTimeout))
	}
	if c.NamespaceEnv == "" {
		errs = append(errs, errors.New("/namespace_env is empty"))
	}
	if c.PodEnv == "" {
		errs = append(errs, errors.New("/pod_env is empty"))
	}
	return errors.Join(errs...)
}

// Reporter answers what the previous run left behind. It holds no run history:
// both answers come from their own source — the storages' manifests and the
// cluster's Job status.
type Reporter struct {
	cfg    Config
	client *kubeapi.Client
}

// New validates the config and builds the reporter. It performs no I/O: the API
// client is built on the first inspection, so a run that never needs it never
// touches the cluster.
func New(ctx context.Context, cfg Config) (*Reporter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Reporter{cfg: cfg}, nil
}
