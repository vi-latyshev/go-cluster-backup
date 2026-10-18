package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// readConfig reads the YAML configuration file and converts it into the JSON
// bytes the engine decodes.
func readConfig(path string) (jsontext.Value, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := yamlJSON(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return raw, nil
}

// defaultsDocuments converts a distribution's defaults documents — YAML, like
// the configuration file — into the JSON bytes the engine reads, naming the
// document that failed.
func defaultsDocuments(docs [][]byte) ([]jsontext.Value, error) {
	out := make([]jsontext.Value, 0, len(docs))
	for i, doc := range docs {
		raw, err := yamlJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("defaults document %d: %w", i+1, err)
		}
		out = append(out, raw)
	}
	return out, nil
}

// yamlJSON is the whole of the YAML layer: a member this build does not know
// survives the conversion and is rejected by the strict decoder with its
// JSON-pointer path, never here.
func yamlJSON(data []byte) (jsontext.Value, error) {
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("converting YAML to JSON: %w", err)
	}
	return raw, nil
}

// openEngine merges the distribution's defaults under the configuration file
// and decodes the result through the engine: every plugin instance is built and
// every reference between instances is checked, which is the whole of
// `validate` and the first half of every other command. engine.New performs no
// network I/O, so secret files that do not exist on this machine are not a
// failure here.
func openEngine(ctx context.Context, reg *plugin.Registry, defs *engine.Defaults, path string, log *slog.Logger) (*engine.Engine, error) {
	raw, err := readConfig(path)
	if err != nil {
		return nil, err
	}
	e, err := engine.New(ctx, raw, defs, reg, log)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return e, nil
}
