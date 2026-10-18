package plugin

import (
	"fmt"
	"strings"
	"text/template"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
)

// Secret is a reference to a mounted secret file. A literal value never decodes
// into it: the type is the enforcement, not a validator.
type Secret struct {
	File string `json:"file" required:"true" doc:"path of a mounted secret file"`
}

// Template is a typed template config field. Decode parses every Template field of
// a config at decode time — syntax errors surface in `validate` — with missingkey=error
// and the core function set plus, for a plugin's own config, the functions its
// TemplateFuncs method adds. Only fields of this type are templated; there is no
// templating of the file as a whole.
type Template struct {
	text string
	tmpl *template.Template
}

func (t *Template) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("template must be a string: %w", err)
	}
	t.text = s
	t.tmpl = nil
	return nil
}

func (t Template) MarshalJSON() ([]byte, error) { return json.Marshal(t.text) }

// Text returns the unparsed template source.
func (t Template) Text() string { return t.text }

// Render executes the template against data into a string. Data is a flat DTO
// documented beside its consumer; a missing map key is an error (missingkey=error).
func (t Template) Render(data any) (string, error) {
	if t.tmpl == nil {
		if t.text == "" {
			return "", nil
		}
		return "", fmt.Errorf("template was not parsed at decode time")
	}
	var b strings.Builder
	if err := t.tmpl.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

func (t *Template) finalize(name string, funcs template.FuncMap) error {
	if name == "" {
		name = "template"
	}
	tmpl, err := template.New(name).Option("missingkey=error").Funcs(funcs).Parse(t.text)
	if err != nil {
		return err
	}
	t.tmpl = tmpl
	return nil
}

// CoreFuncs is the small documented function set available to every template field:
//
//	json      value -> its JSON encoding, for values embedded in a JSON body
//	date      time.Time -> RFC3339 UTC; the zero time renders as "never"
//	bytes     int64 byte count -> binary units: "3.4 GiB", "512.0 MiB"
//	sibytes   int64 byte count -> SI units: "14.0 GB", "13.8 GB"
//	duration  time.Duration -> "4m39s"
//	pick      value, then key/text pairs -> the text of the pair whose key is the value
//
// A plugin config adds its own functions by implementing TemplateFuncs; a name that
// already exists in this set is an error, never a silent override.
func CoreFuncs() template.FuncMap {
	return template.FuncMap{
		"json":     funcJSON,
		"date":     funcDate,
		"bytes":    funcBytes,
		"sibytes":  funcSIBytes,
		"duration": funcDuration,
		"pick":     funcPick,
	}
}

// funcPick is `pick VALUE KEY TEXT KEY TEXT …`: the text of the first pair whose
// key is the value, matched as its string form — an enum like "failed" matches
// the string, so a template writes {{ pick .Run.Outcome "delivered" "✅" "failed" "❌" }}.
// Nothing matching and an odd number of pairs are both errors: a message line
// that cannot be written is never silently empty.
func funcPick(v any, pairs ...any) (string, error) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return "", fmt.Errorf("pick: %d arguments after the value; it takes key/text pairs", len(pairs))
	}
	key := fmt.Sprint(v)
	keys := make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		k := fmt.Sprint(pairs[i])
		keys = append(keys, k)
		if k == key {
			return fmt.Sprint(pairs[i+1]), nil
		}
	}
	return "", fmt.Errorf("pick: %q matches none of %s", key, strings.Join(keys, ", "))
}

func funcJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func funcDate(v time.Time) string {
	if v.IsZero() {
		return "never"
	}
	return v.UTC().Format(time.RFC3339)
}

// funcBytes renders a byte count in binary units (KiB, MiB, GiB) — the base an
// artifact's own stored size is read in.
func funcBytes(v int64) string {
	return formatBytes(v, 1024, []string{"KiB", "MiB", "GiB"}, []int{1, 1, 2})
}

// funcSIBytes renders a byte count in SI units (kB, MB, GB, TB, one decimal).
// A deployment states its caps in SI — `max_total_bytes: 14GB` is 14·10⁹ — so a cap
// refusal must name the same number the config does ("14.0 GB", "13.8 GB"),
// never the binary reading of it.
func funcSIBytes(v int64) string {
	return formatBytes(v, 1000, []string{"kB", "MB", "GB", "TB"}, []int{1, 1, 1, 1})
}

// formatBytes is the one renderer behind both helpers: below base it is whole
// bytes; otherwise the largest unit the raw value reaches, rendered with that
// unit's own number of decimals. The two helpers differ only in base, units and
// precision, so a size never comes to mean two different things in one message.
func formatBytes(v, base int64, units []string, decimals []int) string {
	if v < base {
		return fmt.Sprintf("%d B", v)
	}
	div := float64(base)
	last := len(units) - 1
	for i, unit := range units[:last] {
		if float64(v) < div*float64(base) {
			return fmt.Sprintf("%.*f %s", decimals[i], float64(v)/div, unit)
		}
		div *= float64(base)
	}
	return fmt.Sprintf("%.*f %s", decimals[last], float64(v)/div, units[last])
}

// funcDuration renders a duration the way this set's doc says: rounded to the
// tenth of a second below ten seconds and to the second above, never the
// nanosecond tail of the raw value ("4m39s", not "4m39.620943507s"). It takes
// anything carrying a time.Duration — time.Duration itself and model.Duration,
// whose Std is it.
func funcDuration(v any) string {
	switch d := v.(type) {
	case time.Duration:
		return human.Dur(d)
	case interface{ Std() time.Duration }:
		return human.Dur(d.Std())
	case fmt.Stringer:
		return d.String()
	default:
		return fmt.Sprint(v)
	}
}
