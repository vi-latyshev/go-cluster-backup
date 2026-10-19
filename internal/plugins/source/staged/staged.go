// Package staged reads an artifact another process already encoded and left in
// a directory — the run-side half of the stage command, whose init container
// produces it. The engine's Stage writes <stage.dir>/<name><ext> and, beside
// it, the chain record of what it ran (source.StagedChainFile); this source
// reads the single file that is this artifact — the entry named <name> or
// <name>.<ext>, never a neighbour whose name merely starts with it — reports
// that extension as the artifact's, and reports the recorded chain, so the
// manifest carries how the bytes were encoded and a restore decodes with
// exactly that. Whether the bytes are sealed is the chain's to say — this
// source claims nothing itself — and the stage's pipeline has to agree with the
// artifact's `sensitivity: secret`: the run refuses a secret artifact whose
// chain has no sealing step.
package staged

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// Config is the plugin config. Every field is required.
type Config struct {
	Dir  string `json:"dir" required:"true" doc:"directory the stage command leaves its sealed artifact in"`
	Name string `json:"name" required:"true" doc:"artifact name the staged file starts with"`
}

// Register adds the staged source to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, source.Kind, plugin.Spec[Config, source.Source]{
		Type:      "staged",
		Summary:   "a sealed artifact an earlier stage left in a directory",
		Stability: plugin.Stable,
		Example:   []byte(`{"dir": "/backup/staged", "name": "example-state"}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Dir == "" {
				return nil, errors.New("dir is empty")
			}
			if !filepath.IsAbs(cfg.Dir) {
				return nil, fmt.Errorf("dir %q must be absolute", cfg.Dir)
			}
			if cfg.Name == "" {
				return nil, errors.New("name is empty")
			}
			if strings.ContainsAny(cfg.Name, "/\x00") {
				return nil, fmt.Errorf("name %q must be a plain file name prefix", cfg.Name)
			}
			return &Source{cfg: cfg}, nil
		},
	})
}

type Source struct {
	cfg Config
}

func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	path, rec, err := s.artifact()
	if err != nil {
		return source.SourceTraits{}, err
	}
	// Sealed stays false: what this source's bytes are is the recorded chain's
	// to say, step by step, and claiming it here would call plaintext sealed
	// whenever a stage pipeline ran without a sealing processor.
	return source.SourceTraits{Ext: rec.Ext, Origin: "file://" + path}, nil
}

// Check runs at preflight: the record and the artifact it names must be there. A
// missing pair is the stage init container having failed or not run — the run
// must refuse, not upload an artifact that is not the one it claims to be.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, _, err := s.artifact()
	return err
}

// Dump streams the staged file into dst.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, _, err := s.artifact()
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("staged artifact: %w", err))
	}
	defer func() { _ = f.Close() }()
	_, err = plugin.CopyContext(ctx, dst, f)
	return err
}

// RecordedChain reports the processor chain the stage ran, from the record it
// left beside the artifact: this source's bytes are that chain's output, so the
// run records it as the artifact's chain and a restore reverses exactly it. A
// missing record is a permanent error, never an empty chain — bytes whose
// encoding nobody recorded are bytes nobody can prove are sealed.
func (s *Source) RecordedChain(ctx context.Context) ([]model.ProcessorStep, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rec, err := s.record()
	if err != nil {
		return nil, err
	}
	return rec.Chain, nil
}

// record reads the record the stage left for this source's artifact.
func (s *Source) record() (source.StagedRecord, error) {
	rec, err := source.ReadStagedRecord(s.cfg.Dir, s.cfg.Name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return source.StagedRecord{}, plugin.Permanent(fmt.Errorf(
				"staged artifact %s has no chain record at %s: the stage did not leave one, and nothing tells a restore how these bytes are encoded",
				s.cfg.Name, source.StagedChainFile(s.cfg.Dir, s.cfg.Name)))
		}
		return source.StagedRecord{}, plugin.Permanent(fmt.Errorf("staged chain record: %w", err))
	}
	return rec, nil
}

// artifact locates exactly the file the record names — no directory scan, so no
// other entry in the directory can be read as this artifact, whatever its name
// starts with — and reports its path and the record.
func (s *Source) artifact() (string, source.StagedRecord, error) {
	rec, err := s.record()
	if err != nil {
		return "", source.StagedRecord{}, err
	}
	name := source.StagedArtifactName(s.cfg.Name, rec)
	path := filepath.Join(s.cfg.Dir, name)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", source.StagedRecord{}, plugin.Permanent(fmt.Errorf(
				"no staged artifact named %s in %s; the stage step did not leave one", name, s.cfg.Dir))
		}
		return "", source.StagedRecord{}, plugin.Permanent(err)
	}
	if info.Size() == 0 {
		return "", source.StagedRecord{}, plugin.Permanent(fmt.Errorf("staged artifact %s is empty", path))
	}
	return path, rec, nil
}
