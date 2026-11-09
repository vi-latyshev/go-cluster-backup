package main

import (
	"strings"
	"testing"
)

// TestC2_RestrictedSymbols is the negative consumer proof for the mutation
// boundary: each public bypass is refused outside the packages that own it, and
// the designated consumers and a Port-only workflow stay clean.
func TestC2_RestrictedSymbols(t *testing.T) {
	t.Run("raw storage construction outside the storage subtree", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow.go": `package backup

import "example.test/app/internal/plugins/storage"

var _ = storage.Build
var _ = storage.Register
var _ = storage.AddFactory
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		for _, want := range []string{
			"storage.Build is restricted to internal/plugins/storage/**",
			"storage.Register is restricted to internal/plugins/storage/**",
			"storage.AddFactory is restricted to internal/plugins/storage/**",
		} {
			wantFinding(t, findings, want)
		}
		if len(findings) != 3 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("guard construction outside assembly", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow.go": `package backup

import "example.test/app/internal/plugins/storage/guard"

var _ = guard.Build
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 1 || !strings.Contains(findings[0], "guard.Build is restricted to internal/plugins/assembly and internal/plugins/cli") {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("authority minting outside protection", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow.go": `package backup

import (
	"time"

	"example.test/app/internal/plugins/storage/guard"
)

var _ = guard.NewAuthority(time.Time{})
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 1 || !strings.Contains(findings[0], "guard.NewAuthority is restricted to internal/plugins/protection") {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("destination removal outside assembly and protection", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow.go": `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func remove(d guard.Destination) error {
	return d.Remover().Delete(context.Background(), "key")
}
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		for _, want := range []string{
			"guard.Destination is restricted to internal/plugins/assembly and internal/plugins/protection",
			"Destination.Remover is restricted to internal/plugins/assembly and internal/plugins/protection",
			"Remover.Delete is restricted to internal/plugins/assembly and internal/plugins/protection",
			"only protection, assembly and the Floor may hold guard.Remover",
		} {
			wantFinding(t, findings, want)
		}
		if len(findings) != 5 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("guard.Admit must not exist", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/storage/guard/guard.go": `package guard

import "time"

// Admit must not exist in any form.
func Admit() {}

func check() {}

type Admit struct{}

var Admit = time.Time{}
`,
			"internal/plugins/workflow/backup/workflow.go": `package backup

import "example.test/app/internal/plugins/storage/guard"

var _ = guard.Admit
`,
		})
		findings := ruleRestrictedSymbols(scanned, nil)
		if len(findings) != 4 {
			t.Fatalf("findings = %v", findings)
		}
		for _, finding := range findings {
			if !strings.Contains(finding, "guard.Admit must not exist") {
				t.Fatalf("finding = %q", finding)
			}
		}
	})
	t.Run("designated consumers", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/storage/local/local.go": `package local

import "example.test/app/internal/plugins/storage"

var _ = storage.Build
var _ = storage.Register
var _ = storage.AddFactory
`,
			"internal/plugins/assembly/assembly.go": `package assembly

import (
	"example.test/app/internal/plugins/storage/guard"
)

var _ = guard.Build
var _ guard.Destination

func remove(d guard.Destination) { _ = d.Remover().Delete }
`,
			"internal/plugins/protection/ledger.go": `package protection

import (
	"time"

	"example.test/app/internal/plugins/storage/guard"
)

var _ = guard.NewAuthority(time.Time{})
var _ guard.Destination
`,
			"internal/plugins/cli/doctor.go": `package cli

import "example.test/app/internal/plugins/storage/guard"

var _ = guard.Build
`,
		})
		if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("a Port-only workflow", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow.go": `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func read(ctx context.Context, port guard.Port) error {
	_ = port.Describe(ctx)
	_, _ = port.List(ctx)
	_, _ = port.Get(ctx, "key")
	_, _ = port.Open(nil)
	return port.Check(ctx)
}
`,
		})
		if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("test files are exempt", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/workflow/backup/workflow_test.go": `package backup

import "example.test/app/internal/plugins/storage"

var _ = storage.Build
`,
		})
		if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
}

// TestC2_UnrelatedDeletesStayClean proves the Remover chain is specific: a
// method named Delete on another receiver in a guard-importing package is not a
// mutation-boundary bypass.
func TestC2_UnrelatedDeletesStayClean(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/backup/workflow.go": `package backup

import (
	"example.test/app/internal/plugins/storage/guard"
	other "example.test/app/internal/plugins/library"
)

type upload struct{}

func (upload) Delete() {}

func (upload) Remover(u upload) {
	u.Delete()
	factory().Delete()
	other.Value.Delete()
	_ = guard.Port{}
}

func factory() upload { return upload{} }
`,
	})
	findings := ruleRestrictedSymbols(scanned, nil)
	if len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestC2_RestrictedSymbolsBypass is the review's counterexample through the
// production entry: one consumer reaches all three bypasses, and check refuses
// every one. It runs through check(), so it is RED on the pre-fix tree, where no
// restricted-symbol rule exists.
func TestC2_RestrictedSymbolsBypass(t *testing.T) {
	findings := checkTree(t, checkedTree(map[string]string{
		"internal/plugins/library/bypass.go": `package library

import (
	"context"
	"time"

	"example.test/app/internal/plugins/storage"
	"example.test/app/internal/plugins/storage/guard"
)

func bypass(d guard.Destination) {
	_, _ = storage.Build(context.Background(), nil, "s3", nil, storage.BuildInput{}, struct{}{})
	_, _ = guard.Build(context.Background(), nil, "s3", nil, storage.BuildInput{}, struct{}{})
	_ = guard.NewAuthority(time.Time{})
	_ = d.Remover().Delete(context.Background(), "key")
}
`,
	}))
	for _, want := range []string{
		"storage.Build is restricted to internal/plugins/storage/**",
		"guard.Build is restricted to internal/plugins/assembly and internal/plugins/cli",
		"guard.NewAuthority is restricted to internal/plugins/protection",
		"Destination.Remover is restricted to internal/plugins/assembly and internal/plugins/protection",
		"Remover.Delete is restricted to internal/plugins/assembly and internal/plugins/protection",
	} {
		wantFinding(t, findings, want)
	}
}

// TestC2_HeldRemoverDelete proves a Remover held in a variable, a struct field,
// a parameter, an alias or a slice element cannot delete outside the designated
// packages: the coordinator's leaked-value counterexample.
func TestC2_HeldRemoverDelete(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		want  []string
		count int
	}{
		{
			name: "variable",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func drop(d guard.Destination) {
	r := d.Remover()
	r.Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Destination", "Destination.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 6,
		},
		{
			name: "alias of an alias",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func drop(d guard.Destination) {
	r := d.Remover()
	alias := r
	alias.Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Destination", "Destination.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 7,
		},
		{
			name: "struct field",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

type bin struct{ remover guard.Remover }

func discard(b bin) {
	b.remover.Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 5,
		},
		{
			name: "parameter",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func drop(remover guard.Remover) {
	remover.Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 4,
		},
		{
			name: "producer result",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func makeRemover(d guard.Destination) guard.Remover { return d.Remover() }

func drop(d guard.Destination) {
	makeRemover(d).Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Destination", "Destination.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 8,
		},
		{
			name: "slice element",
			src: `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func drop(removers []guard.Remover) {
	removers[0].Delete(context.Background(), "key")
}
`,
			want:  []string{"guard.Remover", "Remover.Delete", "only protection, assembly and the Floor may hold guard.Remover"},
			count: 4,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			scanned := mustScan(t, map[string]string{
				"internal/plugins/workflow/backup/workflow.go": test.src,
			})
			findings := ruleRestrictedSymbols(scanned, nil)
			for _, want := range test.want {
				wantFinding(t, findings, want)
			}
			if len(findings) != test.count {
				t.Fatalf("findings = %v", findings)
			}
			if test.name == "variable" {
				wantFinding(t, findings, "workflow.go:11: Remover.Delete")
			}
		})
	}
}

// TestC2_HeldRemoverDeleteAllowed proves the designated packages may hand a
// Remover around and delete: the boundary is about who holds it, not the syntax.
func TestC2_HeldRemoverDeleteAllowed(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/assembly/assembly.go": `package assembly

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

func sweep(d guard.Destination) {
	r := d.Remover()
	r.Delete(context.Background(), "key")
}
`,
		"internal/plugins/protection/ledger.go": `package protection

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

type ledger struct{ removers []guard.Remover }

func (l ledger) deleteAt(i int) {
	l.removers[i].Delete(context.Background(), "key")
}
`,
	})
	if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestC2_RemoverValueForms proves every way a Remover value may be held is
// tracked to a later Delete: written types, aliases, map and channel elements,
// parameters and values assigned from a Remover() call.
func TestC2_RemoverValueForms(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/backup/workflow.go": `package backup

import (
	"context"

	"example.test/app/internal/plugins/storage/guard"
)

type alias = guard.Remover

type holder struct {
	removers map[string]guard.Remover
	queue    chan guard.Remover
}

func drop(d guard.Destination, parenthesised (guard.Remover), _ guard.Remover, held guard.Remover) {
	r := d.Remover()
	aliasOfAlias := r
	var typed guard.Remover
	var assigned = d.Remover()
	_ = typed
	_ = assigned
	_ = held
	_ = parenthesised
	_ = holder{}
	(aliasOfAlias).Delete(context.Background(), "key")
	(*r).Delete(context.Background(), "key")
	_, _ = r, r
	_ = r, r
}
`,
	})
	findings := ruleRestrictedSymbols(scanned, nil)
	for _, want := range []string{
		"guard.Remover is restricted",
		"guard.Destination is restricted",
		"Destination.Remover is restricted",
		"Remover.Delete is restricted",
		"only protection, assembly and the Floor may hold guard.Remover",
	} {
		wantFinding(t, findings, want)
	}
	if len(findings) != 29 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestMutationAuthorityIsOneTypesPass proves both mutation rules share the one
// go/types pass: the second call returns the cached findings unchanged.
func TestMutationAuthorityIsOneTypesPass(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/workflow/alternative/write.go": `package alternative

import (
	"time"

	"example.test/app/internal/plugins/protection"
)

func mint() { new(protection.Ledger).Authorize(time.Time{}) }
`,
	})
	first := scanned.authorityFindings()
	second := scanned.authorityFindings()
	if len(first) == 0 || len(second) != len(first) {
		t.Fatalf("authority findings = %v, %v", first, second)
	}
	if got := ruleLeakedMutations(scanned, nil); len(got) != len(first) {
		t.Fatalf("ruleLeakedMutations = %v", got)
	}
}
