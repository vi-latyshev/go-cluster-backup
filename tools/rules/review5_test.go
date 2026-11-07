package main

import "testing"

func TestR5_C2_TypeSweep(t *testing.T) {
	for name, body := range map[string]string{
		"outer parameter in nested block": `func mint(o backup.Options) { if true { _ = o.Ledger.Authorize } }`,
		"embedded issuer interface":       `type Issuer interface{ Authorize(time.Time) *guard.Authority }; type Wrapped interface{ Issuer }; func mint(a any){ _ = a.(Wrapped).Authorize }`,
		"range value":                     `func mint(list []backup.Options){ for _,o := range list { _ = o.Ledger.Authorize } }`,
		"function literal parameter":      `func mint(){ _ = func(o backup.Options){ _ = o.Ledger.Authorize } }`,
	} {
		t.Run(name, func(t *testing.T) {
			r := mustScan(t, map[string]string{
				"internal/plugins/workflow/backup/options.go": `package backup
import "example.test/app/internal/plugins/protection"
type Options struct{ Ledger *protection.Ledger }`,
				"internal/plugins/workflow/alternative/write.go": `package alternative
import("time";"example.test/app/internal/plugins/workflow/backup";"example.test/app/internal/plugins/storage/guard")
` + body,
			})
			if f := append(ruleRestrictedSymbols(r, nil), ruleLeakedMutations(r, nil)...); len(f) == 0 {
				t.Fatal("issuer form passed both mutation rules")
			}
		})
	}
}
