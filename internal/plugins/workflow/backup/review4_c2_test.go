package backup_test

import (
	"reflect"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// TestR4_C2_OptionsCarriesNoIssuer: the public aggregate never projects the
// concrete ledger, so an alias, inferred value or indexed element of it cannot
// mint an authority outside the Floor. The ledger enters through New's argument
// in the allowlisted package only.
func TestR4_C2_OptionsCarriesNoIssuer(t *testing.T) {
	ledgerType := reflect.TypeOf((*protection.Ledger)(nil))
	typ := reflect.TypeOf(backup.Options{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Type == ledgerType {
			t.Fatalf("Options.%s projects *protection.Ledger", field.Name)
		}
	}
	shell := reflect.TypeOf((*backup.Workflow)(nil))
	for i := 0; i < shell.NumMethod(); i++ {
		method := shell.Method(i)
		if method.Type.NumOut() > 0 && method.Type.Out(0) == ledgerType {
			t.Fatalf("Workflow.%s returns *protection.Ledger", method.Name)
		}
	}
}
