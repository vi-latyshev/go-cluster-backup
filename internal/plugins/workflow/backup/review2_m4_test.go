package backup_test

import (
	"context"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"testing"
)

type reviewClosingStorage struct {
	*storagetest.Fake
	closes int
}

func (s *reviewClosingStorage) Close() error { s.closes++; return nil }
func TestR2_M4_StorageConstructionStaysOwned(t *testing.T) {
	w := newWorld(t)
	s := &reviewClosingStorage{Fake: storagetest.NewFake()}
	w.ExtraStorages["alpha"] = s
	e := w.newAssembly(t, baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")}))
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.closes != 1 {
		t.Fatalf("constructed storage closed %d times, want 1", s.closes)
	}
}
