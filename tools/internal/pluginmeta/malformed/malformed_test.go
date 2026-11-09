package malformed

import "testing"

// TestCasesAreComplete pins the corpus itself: every case names one rule, ships
// its source and its diagnostic, and appears once, so a consumer loop cannot
// silently skip or shadow a case.
func TestCasesAreComplete(t *testing.T) {
	cases := Cases()
	if len(cases) < 29 {
		t.Fatalf("corpus has %d cases, want the whole malformed-metadata matrix", len(cases))
	}
	seen := make(map[string]bool, len(cases))
	for _, test := range cases {
		if test.Name == "" || test.Want == "" || len(test.Files) == 0 {
			t.Errorf("incomplete case %+v", test)
		}
		if seen[test.Name] {
			t.Errorf("case %q appears twice", test.Name)
		}
		seen[test.Name] = true
	}
}
