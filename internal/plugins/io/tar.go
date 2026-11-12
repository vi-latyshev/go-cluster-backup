package pluginio

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"time"
)

// TarEntry is one regular-file entry of a deterministic artifact tar.
type TarEntry struct {
	// Name is the entry's name inside the archive.
	Name string
	// Mode is the permission bits the header records.
	Mode int64
	// Body holds the entry's bytes; when Read is nil it is the whole body.
	Body []byte
	// Read streams the entry's body instead of Body — a file too large to hold
	// in memory. Exactly Size bytes are copied and the reader is not closed.
	Read io.Reader
	// Size is the number of bytes Read carries.
	Size int64
}

// WriteTar writes entries, in order, as one tar stream of regular files. The
// headers carry a fixed epoch, so two runs over unchanged bytes produce the
// same archive and an artifact is compared by content instead of by a clock.
// Every entry is written whole or the call fails: a short Read is a failed
// artifact, never a damaged entry that still looks valid.
func WriteTar(ctx context.Context, dst io.Writer, entries []TarEntry) error {
	tw := tar.NewWriter(dst)
	// Header times are fixed: two snapshots of unchanged data are identical.
	epoch := time.Unix(0, 0)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		size := int64(len(entry.Body))
		if entry.Read != nil {
			size = entry.Size
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: entry.Name, Mode: entry.Mode, Size: size,
			Typeflag: tar.TypeReg, ModTime: epoch,
		}); err != nil {
			return fmt.Errorf("tar header %s: %w", entry.Name, err)
		}
		if entry.Read == nil {
			if _, err := tw.Write(entry.Body); err != nil {
				return fmt.Errorf("tar body %s: %w", entry.Name, err)
			}
			continue
		}
		n, err := CopyContext(ctx, tw, io.LimitReader(entry.Read, entry.Size))
		if err != nil {
			return fmt.Errorf("tar body %s: %w", entry.Name, err)
		}
		if n != entry.Size {
			return fmt.Errorf("tar body %s: %w", entry.Name, io.ErrUnexpectedEOF)
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar trailer: %w", err)
	}
	return nil
}
