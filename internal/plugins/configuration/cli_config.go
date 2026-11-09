package configuration

import (
	"fmt"
	"os"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v4"
)

// ReadConfig reads the YAML configuration file and converts it into the JSON
// bytes the strict decoder reads.
func ReadConfig(path string) (jsontext.Value, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := YAMLJSON(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return raw, nil
}

// DefaultsDocuments converts a distribution's defaults documents — YAML, like
// the configuration file — into the JSON bytes the configuration layer reads,
// naming the document that failed.
func DefaultsDocuments(docs [][]byte) ([]jsontext.Value, error) {
	out := make([]jsontext.Value, 0, len(docs))
	for i, doc := range docs {
		raw, err := YAMLJSON(doc)
		if err != nil {
			return nil, fmt.Errorf("defaults document %d: %w", i+1, err)
		}
		out = append(out, raw)
	}
	return out, nil
}

// YAMLJSON is the whole of the YAML layer: a member this build does not know
// survives the conversion and is rejected by the strict decoder with its
// JSON-pointer path, never here.
func YAMLJSON(data []byte) (jsontext.Value, error) {
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
