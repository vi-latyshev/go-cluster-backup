package main

import (
	"fmt"
	"go/types"
	"testing"
)

func TestR7_C2_PromotedFields(t *testing.T) {
	for _, mutation := range []string{"*protection.Ledger", "guard.Remover", "guard.Destination"} {
		for _, embedded := range []string{"hidden", "*hidden"} {
			for name, body := range map[string]string{
				"parameter": `func hold(v assembly.Public) {}`,
				"result":    `func hold() assembly.Public { return assembly.Public{} }`,
				"local":     `func hold() { var v assembly.Public; _ = v }`,
			} {
				t.Run(mutation+"/"+embedded+"/"+name, func(t *testing.T) {
					r := mustScan(t, map[string]string{
						"internal/plugins/assembly/holder.go": fmt.Sprintf(`package assembly
import ("example.test/app/internal/plugins/protection"; "example.test/app/internal/plugins/storage/guard")
type hidden struct{ Mutation %s }
type Public struct{ %s }
`, mutation, embedded),
						"internal/plugins/workflow/alternative/hold.go": `package alternative
import "example.test/app/internal/plugins/assembly"
` + body,
					})
					checker := newTypeChecker(r.Tree)
					owner := checker.typesFor(r.ByDir[assemblyDir]).pkg
					consumer := checker.typesFor(r.ByDir["internal/plugins/workflow/alternative"]).pkg
					field, _, _ := types.LookupFieldOrMethod(owner.Scope().Lookup("Public").Type(), true, consumer, "Mutation")
					if field == nil || !field.Exported() {
						t.Fatal("fixture field is not publicly reachable")
					}
					if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
						t.Fatal("publicly reachable mutation holder passed both rules")
					}
				})
			}
		}
	}
}
