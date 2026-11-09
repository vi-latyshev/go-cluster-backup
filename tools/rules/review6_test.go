package main

import "testing"

func TestR6_C2_CompilerSweep(t *testing.T) {
	for name, body := range map[string]string{
		"aggregate parameter": `func hold(o assembly.Envelope) {}`,
		"aggregate result":    `func hold() assembly.Envelope { return assembly.Envelope{} }`,
		"aggregate local":     `func hold() { var o assembly.Envelope; _ = o }`,
		"promoted issuer":     `func mint(o assembly.Embedded) { _ = o.Authorize }`,
		"promoted remover":    `func drop(o assembly.Deleter) { _ = o.Delete }`,
	} {
		t.Run(name, func(t *testing.T) {
			r := mustScan(t, map[string]string{
				"internal/plugins/assembly/holder.go": `package assembly
import ("example.test/app/internal/plugins/protection"; "example.test/app/internal/plugins/storage/guard")
type Envelope struct{ Ledger *protection.Ledger }
type Embedded struct{ *protection.Ledger }
type Deleter struct{ guard.Remover }
`,
				"internal/plugins/workflow/alternative/write.go": `package alternative
import "example.test/app/internal/plugins/assembly"
` + body,
			})
			if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
				t.Fatal("statically resolved mutation holder passed both rules")
			}
		})
	}
}

func TestR6_C2_ConstructorIdent(t *testing.T) {
	r := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative
import . "example.test/app/internal/plugins/protection"
var maker = NewLedger
`,
	})
	if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
		t.Fatal("NewLedger function value passed both rules")
	}
}
