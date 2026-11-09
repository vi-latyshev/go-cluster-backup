package main

import (
	"strings"
	"testing"
)

func TestR3_C2_IssuerLaundering(t *testing.T) {
	cases := map[string]string{
		"assertion":          `func mint(a protection.Accounting) { a.(interface{ Authorize(time.Time) *guard.Authority }).Authorize(time.Now()) }`,
		"concrete assertion": `func mint(a protection.Accounting) { l := a.(*protection.Ledger); l.Authorize(time.Now()) }`,
		"exported aggregate": `func mint(o backup.Options) { o.Ledger.Authorize(time.Now()) }`,
		"function value":     `func mint() { maker := protection.NewLedger; maker(protection.LedgerOptions{}).Authorize(time.Now()) }`,
		"channel":            `func mint(a protection.Accounting) { ch := make(chan *protection.Ledger, 1); ch <- a.(*protection.Ledger); (<-ch).Authorize(time.Now()) }`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			scanned := mustScan(t, map[string]string{
				"internal/plugins/workflow/backup/options.go": `package backup
import "example.test/app/internal/plugins/protection"
type Options struct { Ledger *protection.Ledger }
`,
				"internal/plugins/workflow/alternative/write.go": `package alternative
import (
 "time"
 "example.test/app/internal/plugins/protection"
 "example.test/app/internal/plugins/storage/guard"
 "example.test/app/internal/plugins/workflow/backup"
)
` + body,
			})
			findings := append(ruleRestrictedSymbols(scanned, nil), ruleLeakedMutations(scanned, nil)...)
			if len(findings) == 0 {
				t.Fatal("consumer can launder an issuer; both mutation rules report no findings")
			}
		})
	}
}

// TestR3_C2_TypeSwitchAssertion proves a type switch to an authority-issuer
// interface is refused, while non-issuer interfaces and ordinary types stay
// clean.
func TestR3_C2_TypeSwitchAssertion(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"time"

	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/storage/guard"
)

func switchMint(a protection.Accounting) {
	switch a.(type) {
	case interface{ Authorize(time.Time) *guard.Authority }:
	case interface{ Delete() }:
	case string:
	}
}
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	if len(findings) != 1 || !strings.Contains(findings[0], "an authority issuer assertion is restricted") {
		t.Fatalf("findings = %v", findings)
	}
}

// TestR3_C2_ProjectionForms proves the receiver typing: an exported aggregate
// field projection is refused through parameters, pointers, parentheses, local
// declarations, short assignments and method values, while a same-named field
// whose declared type is the sealed accounting view stays clean.
func TestR3_C2_ProjectionForms(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/backup/options.go": `package backup

import "example.test/app/internal/plugins/protection"

type Options struct{ Ledger *protection.Ledger }
`,
		"internal/plugins/workflow/alternative/forms.go": `package alternative

import (
	"example.test/app/internal/plugins/protection"
	"example.test/app/internal/plugins/workflow/backup"
)

type Holder struct{ Ledger protection.Accounting }

type Ptr *struct{ Ledger protection.Accounting }

func paren(o (backup.Options)) { _ = (o).Ledger }

func pointer(o *backup.Options) { _ = (*o).Ledger }

func localVar() {
	var o backup.Options
	_ = o.Ledger
}

func shortComposite() {
	v := backup.Options{}
	_ = v.Ledger
}

func shortAlias() {
	v := backup.Options{}
	w := v
	_ = w.Ledger
}

func methodValue(o backup.Options) {
	f := o.Ledger.Authorize
	_ = f
}

func cleanHolder(h Holder) { _ = h.Ledger }

func localForms() {
	const c = 1
	type localType int
	var untyped = 1
	_ = c
	_ = localType(0)
	_ = untyped
	var list []backup.Options
	list[0] = backup.Options{}
	_ = c, c
}

func missingVar() { _ = unknown.Ledger }

func unknownType(o Missing) { _ = o.Ledger }

func nonNamed(o struct{ Ledger protection.Accounting }) { _ = o.Ledger }

var global backup.Options

func packageVar() { _ = global.Ledger }

func noBody()
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	if len(findings) != 16 {
		t.Fatalf("findings = %v", findings)
	}
	for _, finding := range findings {
		if !restrictedMutationMessage(finding) {
			t.Fatalf("unexpected finding: %q", finding)
		}
	}
}

// restrictedMutationMessage accepts either restricted-authority diagnostic the
// go/types pass emits for a Ledger expression.
func restrictedMutationMessage(finding string) bool {
	return strings.Contains(finding, "only protection, assembly and the Floor may hold protection.Ledger") ||
		strings.Contains(finding, "protection.Ledger.Authorize is restricted")
}
