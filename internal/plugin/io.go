package plugin

import (
	"context"
	"io"
)

// copyBufSize is the chunk every plugin-side copy streams in: 256 KiB is one
// read per syscall burst on a dump, small enough that cancellation is noticed
// within one chunk.
const copyBufSize = 256 << 10

// CopyContext copies src into dst until EOF, giving up as soon as ctx is done
// — between two reads, never in the middle of one. It is the copy every plugin
// streams with instead of io.Copy: a plugin that keeps reading after its
// context is done breaks the engine's cancellation and budget accounting, and
// io.Copy has no ctx. It returns the bytes copied so far and the first error —
// cancellation included, in which case the partial count is unusable on
// purpose: a plugin reports the error, never the short stream.
func CopyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, copyBufSize)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if rerr == io.EOF {
			return total, nil
		}
		if rerr != nil {
			return total, rerr
		}
	}
}
