package webhook

import (
	"fmt"
	"text/template"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// blankMarker is what {{ blank }} renders: one byte no message line may carry
// otherwise, which the folder below turns into a deliberate empty line. A
// message template cannot ask for a separator with an empty line, because an
// empty line is also what a template line holding only an action renders to —
// the two are different intentions and only the template's own text tells them
// apart. The byte never arrives from data: cleanEvent replaces every control
// rune a string carries.
const blankMarker = "\x03"

// TemplateFuncs adds this plugin's own functions to the framework's core set.
// The core set (json, date, bytes, duration, pick) is what every template field
// has; these are the ones a message that walks a run report needs.
//
//	blank     -> the marker of a deliberate empty line: {{ blank }} on its own line
//	byGroup   -> []model.ArtifactResult grouped by Artifact.Group, groups in first
//	             appearance order, failed artifacts first inside each group
//	failed    -> the failed artifacts of a list, for a section that lists them first
//	sumBytes  -> the total Bytes of a list of artifacts, for the run's own total
//	pickdef   -> pick VALUE DEFAULT KEY TEXT ...: the core pick, with a default for
//	             a value no pair names — a group added to a run must not lose the
//	             whole message to an unknown name
//	clip      -> clip N STRING: at most N characters, the rest replaced by "…";
//	             a notification shows the first sentence, the log keeps the rest
func (Config) TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"blank":    funcBlank,
		"byGroup":  funcByGroup,
		"failed":   funcFailed,
		"sumBytes": funcSumBytes,
		"pickdef":  funcPickDef,
		"clip":     funcClip,
	}
}

func funcBlank() string { return blankMarker }

// Group is one artifact group of a report: its name, how many artifacts it
// holds, and the artifacts themselves, failures first.
type Group struct {
	Name      string
	Count     int
	Artifacts []model.ArtifactResult
}

func funcByGroup(arts []model.ArtifactResult) []Group {
	var groups []Group
	index := map[string]int{}
	for _, a := range arts {
		i, ok := index[a.Artifact.Group]
		if !ok {
			i = len(groups)
			index[a.Artifact.Group] = i
			groups = append(groups, Group{Name: a.Artifact.Group})
		}
		groups[i].Artifacts = append(groups[i].Artifacts, a)
	}
	for i := range groups {
		// A failure is what the reader is looking for: it goes first, and the
		// order inside each half stays the run's own.
		g := &groups[i]
		failed := make([]model.ArtifactResult, 0, len(g.Artifacts))
		ok := make([]model.ArtifactResult, 0, len(g.Artifacts))
		for _, a := range g.Artifacts {
			if a.Outcome == model.ArtifactOK {
				ok = append(ok, a)
				continue
			}
			failed = append(failed, a)
		}
		g.Artifacts = append(failed, ok...)
		g.Count = len(g.Artifacts)
	}
	return groups
}

func funcFailed(arts []model.ArtifactResult) []model.ArtifactResult {
	out := make([]model.ArtifactResult, 0, len(arts))
	for _, a := range arts {
		if a.Outcome != model.ArtifactOK {
			out = append(out, a)
		}
	}
	return out
}

// funcPickDef is `pickdef VALUE DEFAULT KEY TEXT …`: the text of the first pair
// whose key is the value's string form, or DEFAULT when nothing matches. It is
// the core pick with the one difference a message walking a report needs — a
// group, a storage type or an outcome this template has never seen renders as
// itself instead of failing the whole message.
func funcPickDef(v any, def string, pairs ...any) (string, error) {
	if len(pairs)%2 != 0 {
		return "", fmt.Errorf("pickdef: %d key/text arguments; it takes pairs", len(pairs))
	}
	key := fmt.Sprint(v)
	for i := 0; i < len(pairs); i += 2 {
		if fmt.Sprint(pairs[i]) == key {
			return fmt.Sprint(pairs[i+1]), nil
		}
	}
	return def, nil
}

// funcClip is `clip N STRING`: at most N characters of the string, with "…" when
// it had to cut. A failure's full reason belongs in the log; a notification
// shows what a person reads on a phone.
func funcClip(n int, s string) string {
	if n < 1 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

func funcSumBytes(arts []model.ArtifactResult) int64 {
	var total int64
	for _, a := range arts {
		total += a.Bytes
	}
	return total
}
