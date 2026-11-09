package instances

import (
	"fmt"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
)

// CheckShape validates the structure of one instance config without decoding a
// single value: every missing member, every unknown member and every value
// whose JSON kind the field cannot decode is reported with its JSON-pointer
// path under path, and no string is parsed as a template. It is the offline
// check a nested config gets, where the strict decoder sees only opaque bytes —
// a discovery rule's source config, whose strings the discovery renders per
// matched object.
//
// A key is never templated, so a key-level answer is always safe. A leaf's kind
// is judged because a template renders to a JSON string and to nothing else: a
// template in a field that needs a number, a boolean, an object or an array
// cannot decode at run time, and a literal of the wrong kind is wrong already.
// What cannot be classified with certainty stays unconstrained — a false
// refusal blocks a correct production config, which is worse than a miss.
func (r *Registry) CheckShape(kind, typ string, raw jsontext.Value, path string) error {
	e, ok := r.kinds[kind][typ]
	if !ok {
		return fmt.Errorf("%s: %w", path, r.unknown(kind, typ))
	}
	return configuration.ShapeErrors(e.cfg, raw, path)
}
