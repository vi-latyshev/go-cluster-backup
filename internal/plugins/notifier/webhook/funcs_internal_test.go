package webhook

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

func artifactResult(group, name, outcome string, size int64) model.ArtifactResult {
	return model.ArtifactResult{
		Artifact: model.Artifact{Group: group, Name: name},
		Outcome:  outcome,
		Bytes:    size,
	}
}

// TestFuncBlankIsTheDeliberateEmptyLineMarker: the marker is one byte no message
// line may carry otherwise, so a template line holding only {{ blank }} is told
// apart from a line that happened to render empty.
func TestFuncBlankIsTheDeliberateEmptyLineMarker(t *testing.T) {
	if got := funcBlank(); got != blankMarker {
		t.Fatalf("funcBlank() = %q, want %q", got, blankMarker)
	}
	if len(blankMarker) != 1 {
		t.Fatalf("the marker is %d bytes; one byte is what tells a deliberate empty line from an empty render", len(blankMarker))
	}
}

// TestFuncByGroupKeepsTheRunsOrderAndPutsFailuresFirst: groups come out in the
// order they first appear in the report, and inside a group a failure is what
// the reader is looking for.
func TestFuncByGroupKeepsTheRunsOrderAndPutsFailuresFirst(t *testing.T) {
	groups := funcByGroup([]model.ArtifactResult{
		artifactResult("databases", "a", model.ArtifactOK, 1),
		artifactResult("volumes", "b", model.ArtifactOK, 2),
		artifactResult("databases", "c", model.ArtifactFailed, 4),
		artifactResult("volumes", "d", model.ArtifactFailed, 8),
		artifactResult("databases", "e", model.ArtifactOK, 16),
	})
	if len(groups) != 2 {
		t.Fatalf("funcByGroup returned %d groups, want 2: %+v", len(groups), groups)
	}
	if groups[0].Name != "databases" || groups[1].Name != "volumes" {
		t.Fatalf("groups are %q, %q; want them in first-appearance order", groups[0].Name, groups[1].Name)
	}
	want := map[string]struct {
		names []string
		count int
	}{
		"databases": {[]string{"c", "a", "e"}, 3},
		"volumes":   {[]string{"d", "b"}, 2},
	}
	for _, g := range groups {
		w := want[g.Name]
		if g.Count != w.count {
			t.Errorf("group %q counts %d artifacts, want %d", g.Name, g.Count, w.count)
		}
		if len(g.Artifacts) != len(w.names) {
			t.Fatalf("group %q holds %d artifacts, want %d", g.Name, len(g.Artifacts), len(w.names))
		}
		for i, name := range w.names {
			if g.Artifacts[i].Artifact.Name != name {
				t.Errorf("group %q artifact %d is %q, want %q", g.Name, i, g.Artifacts[i].Artifact.Name, name)
			}
		}
	}
	if got := funcByGroup(nil); len(got) != 0 {
		t.Errorf("funcByGroup of no artifact returned %+v, want none", got)
	}
}

func TestFuncFailedKeepsOnlyTheFailuresInOrder(t *testing.T) {
	got := funcFailed([]model.ArtifactResult{
		artifactResult("g", "ok", model.ArtifactOK, 1),
		artifactResult("g", "first", model.ArtifactFailed, 2),
		artifactResult("g", "second", model.ArtifactFailed, 4),
	})
	if len(got) != 2 || got[0].Artifact.Name != "first" || got[1].Artifact.Name != "second" {
		t.Fatalf("funcFailed returned %+v, want the two failures in the run's order", got)
	}
	if got := funcFailed(nil); len(got) != 0 {
		t.Errorf("funcFailed of no artifact returned %+v, want none", got)
	}
}

func TestFuncSumBytesAddsEveryArtifact(t *testing.T) {
	got := funcSumBytes([]model.ArtifactResult{
		artifactResult("g", "a", model.ArtifactOK, 1<<20),
		artifactResult("g", "b", model.ArtifactFailed, 0),
		artifactResult("g", "c", model.ArtifactOK, 3<<20),
	})
	if want := int64(4 << 20); got != want {
		t.Errorf("funcSumBytes = %d, want %d", got, want)
	}
	if got := funcSumBytes(nil); got != 0 {
		t.Errorf("funcSumBytes of no artifact = %d, want 0", got)
	}
}

func TestFuncClipCountsCharactersNotBytes(t *testing.T) {
	cases := []struct {
		n    int
		in   string
		want string
	}{
		{0, "abc", ""},
		{-1, "abc", ""},
		{3, "abc", "abc"},
		{9, "abc", "abc"},
		{2, "abc", "ab…"},
		{1, "héllo", "h…"},
		{4, "héllo", "héll…"},
	}
	for _, tc := range cases {
		if got := funcClip(tc.n, tc.in); got != tc.want {
			t.Errorf("funcClip(%d, %q) = %q, want %q", tc.n, tc.in, got, tc.want)
		}
	}
}

func TestFuncPickDefFallsBackToTheDefault(t *testing.T) {
	got, err := funcPickDef("delivered", "unknown", "delivered", "OK", "failed", "NO")
	if err != nil {
		t.Fatalf("funcPickDef: %v", err)
	}
	if got != "OK" {
		t.Errorf("funcPickDef of a named value = %q, want %q", got, "OK")
	}
	got, err = funcPickDef("never-seen", "unknown", "delivered", "OK")
	if err != nil {
		t.Fatalf("funcPickDef with no matching pair: %v", err)
	}
	if got != "unknown" {
		t.Errorf("funcPickDef of an unnamed value = %q, want the default %q", got, "unknown")
	}
	if _, err := funcPickDef("x", "d", "a"); err == nil {
		t.Fatal("funcPickDef accepted a lone key with no text")
	}
}
