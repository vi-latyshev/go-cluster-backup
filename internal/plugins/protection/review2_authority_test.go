package protection_test

import (
	"context"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"strings"
	"testing"
	"time"
)

func TestR2_C2_ZeroLedgerCannotMintUsableWriteAuthority(t *testing.T) {
	fake := storagetest.NewFake()
	port := guard.Read("primary", fake)
	authority := new(protection.Ledger).Authorize(time.Now().Add(time.Hour))
	handle, err := port.Open(authority)
	if err != nil {
		return
	}
	_, err = handle.Put(context.Background(), storage.PutRequest{Key: "unadmitted", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("write succeeded without lease, survey, collision check or admission")
	}
}
