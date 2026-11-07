package main

import "testing"

func TestR5_C2_IssuerParameter(t *testing.T) {
	r := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative
import("time"; "example.test/app/internal/plugins/storage/guard")
func mint(i interface{ Authorize(time.Time) *guard.Authority }) { _ = i.Authorize }
`,
	})
	if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
		t.Fatal("issuer parameter passed both mutation rules")
	}
}
