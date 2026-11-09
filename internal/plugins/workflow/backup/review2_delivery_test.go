package backup_test

import (
	"context"
	"errors"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"io"
	"strings"
	"testing"
	"testing/synctest"
)

type reviewHeldSource struct {
	fakeSource
	entered, release chan struct{}
}

func (s *reviewHeldSource) Dump(_ context.Context, dst io.Writer) error {
	_, err := io.WriteString(dst, "0123456789")
	close(s.entered)
	<-s.release
	return err
}

type reviewRejectedMultipart struct{ *fakeMultipart }

func (*reviewRejectedMultipart) Check(context.Context) error {
	return pluginerrors.Permanent(errors.New("storage permanently unavailable"))
}
func TestR2_M16_PermanentCheckForbidsOverlappingCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		mp := addMultipart(w, "alpha", 4)
		w.ExtraStorages["alpha"] = &reviewRejectedMultipart{mp}
		src := &reviewHeldSource{entered: make(chan struct{}), release: make(chan struct{})}
		w.ExtraSources["vpn"] = src
		cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40), "beta": manualStorage(1 << 40)})
		cfg["run"].(map[string]any)["storage_parallelism"] = 2
		e := w.newAssembly(t, cfg)
		done := make(chan error, 1)
		go func() {
			_, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			done <- err
		}()
		<-src.entered
		synctest.Wait()
		begins, _, _, _ := mp.stats()
		if begins != 0 {
			t.Errorf("permanently rejected storage received %d multipart calls", begins)
		}
		close(src.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		_, _, completes, _ := mp.stats()
		if completes != 0 {
			t.Errorf("permanently rejected storage completed %d multipart writes", completes)
		}
	})
}
func TestR2_M17_StalledMultipartCannotStarveHealthyStorage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		mp := addMultipart(w, "alpha", 4)
		mp.beginGate = make(chan struct{})
		src := &reviewHeldSource{entered: make(chan struct{}), release: make(chan struct{})}
		w.ExtraSources["vpn"] = src
		cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40), "beta": manualStorage(1 << 40)})
		cfg["run"].(map[string]any)["storage_parallelism"] = 2
		cfg["storages"].(map[string]any)["alpha"].(map[string]any)["time_budget"] = "1m"
		cfg["run"].(map[string]any)["deadline"] = "5s"
		e := w.newAssembly(t, cfg)
		done := make(chan error, 1)
		go func() {
			_, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			done <- err
		}()
		<-src.entered
		synctest.Wait()
		begins, _, _, _ := mp.stats()
		if begins == 0 {
			close(src.release)
			t.Fatal("fixture did not hold an overlapping call")
		}
		close(src.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		artifactPuts := 0
		for _, key := range w.storage(t, "beta").Keys() {
			if !strings.HasSuffix(key, "manifest.json") {
				artifactPuts++
			}
		}
		if artifactPuts == 0 {
			t.Fatal("healthy storage got no artifact before stalled multipart spent the run deadline")
		}
	})
}
