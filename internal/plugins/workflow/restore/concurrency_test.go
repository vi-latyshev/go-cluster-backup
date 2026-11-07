package restore

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// hookStorage delegates to the in-memory fake and runs one hook after a
// successful Get: the checkpoint means the bytes have reached the writer, which
// is how a test interleaves two restores at the exact point where one copy's
// bytes sit in a spool file and the other restore could truncate them. key
// narrows the hook to one artifact; empty means any.
type hookStorage struct {
	*storagetest.Fake
	key   string
	onGet func()
}

func (s *hookStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	if err := s.Fake.Get(ctx, key, dst); err != nil {
		return err
	}
	if s.onGet != nil && (s.key == "" || s.key == key) {
		s.onGet()
	}
	return nil
}

// TestConcurrentRestoresShareOneSpoolDirectory: two restore fetches on one
// restore.spool_dir must each hand back exactly the bytes their manifest
// verified. The hooks interleave them inside Get, after one restore's bytes sit
// in the spool file and before the hash check and the re-read by name — a
// shared spool name lets the second restore truncate the first's file there.
func TestConcurrentRestoresShareOneSpoolDirectory(t *testing.T) {
	w := newWorld(t)
	f := w.addStorage("primary")
	small := []byte("small artifact")
	large := []byte("a much larger artifact body")

	smallKey := "runs/r/vpn/small.dat.zst"
	largeKey := "runs/r/vpn/large.dat.zst"
	rep := covManifest("r", "small", smallKey, small)
	rep.Artifacts = append(rep.Artifacts, model.ArtifactResult{
		Artifact: model.Artifact{Name: "large", Group: "vpn", Sensitivity: model.SensitivityPlain},
		Outcome:  model.ArtifactOK,
		Key:      largeKey,
		Bytes:    int64(len(large)),
		RawBytes: int64(len(large)),
		SHA256:   model.SHA256Hex(large),
		Chain:    []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}},
		Source:   model.SourceRef{Instance: "vpn", Type: "fake", Origin: "test://fake/source"},
	})
	seed(t, f, rep, smallKey, small)
	f.PutObject(largeKey, large, time.Unix(1700000000, 0).UTC())
	wantA, okA := f.Bytes(smallKey)
	wantB, okB := f.Bytes(largeKey)
	if !okA || !okB {
		t.Fatalf("the fixture objects are missing: %v", f.Keys())
	}

	aWritten, bWritten := make(chan struct{}), make(chan struct{})
	aDone, bDone := make(chan struct{}), make(chan struct{})

	// Two workflows share the one spool directory: exactly what two restores of
	// one deployment do.
	w.replace("primary", &hookStorage{Fake: f, key: smallKey,
		onGet: func() { close(aWritten); <-bWritten }})
	eA := w.flow()
	w.replace("primary", &hookStorage{Fake: f, key: largeKey,
		onGet: func() { close(bWritten); <-aDone }})
	eB := w.flow()

	var outA, outB bytes.Buffer
	var repA FetchReport
	var errA error
	go func() {
		repA, errA = eA.Fetch(context.Background(), FetchRequest{RunID: "r", Artifact: "small"}, &outA)
		close(aDone)
	}()
	<-aWritten
	var errB error
	go func() {
		_, errB = eB.Fetch(context.Background(), FetchRequest{RunID: "r", Artifact: "large"}, &outB)
		close(bDone)
	}()
	<-bWritten
	<-aDone
	<-bDone

	if errA != nil {
		t.Fatalf("the first restore failed: %v", errA)
	}
	if !repA.Verified || !bytes.Equal(outA.Bytes(), wantA) {
		t.Fatalf("the first restore returned %d bytes with verified=%v, want the %d bytes of %q: got %q",
			outA.Len(), repA.Verified, len(wantA), smallKey, outA.Bytes())
	}
	if errB != nil {
		t.Fatalf("the second restore failed: %v", errB)
	}
	if !bytes.Equal(outB.Bytes(), wantB) {
		t.Fatalf("the second restore returned %q, want the bytes of %q", outB.Bytes(), largeKey)
	}
}
