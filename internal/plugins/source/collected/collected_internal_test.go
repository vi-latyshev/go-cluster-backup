package collected

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// internalStub writes one valid pair and returns the source over it.
func internalStub(t *testing.T) (data, marker string, s *Source) {
	t.Helper()
	root := t.TempDir()
	data = filepath.Join(root, "vendor.tar")
	marker = data + markerSuffix
	if err := os.WriteFile(data, []byte("COLLECTED-DATA"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte(digestOf("COLLECTED-DATA")), 0o600); err != nil {
		t.Fatal(err)
	}
	return data, marker, &Source{cfg: Config{
		Path:   data,
		Ext:    ".tar",
		MaxAge: model.Duration(2 * time.Hour),
	}}
}

func digestOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func withSeam[T any](t *testing.T, slot *T, fake T) {
	t.Helper()
	orig := *slot
	*slot = fake
	t.Cleanup(func() { *slot = orig })
}

func TestOpenReportsAFileItCannotRead(t *testing.T) {
	for name, seam := range map[string]func(t *testing.T){
		"stat": func(t *testing.T) {
			withSeam(t, &statFile, func(*os.File) (fs.FileInfo, error) {
				return nil, errors.New("injected stat failure")
			})
		},
		"marker read": func(t *testing.T) {
			withSeam(t, &readMarker, func(io.Reader) ([]byte, error) {
				return nil, errors.New("injected read failure")
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, s := internalStub(t)
			seam(t)
			if err := s.Check(context.Background()); err == nil {
				t.Fatalf("a pair whose %s failed passed the preflight", name)
			}
		})
	}
}

func TestDumpReportsAPostStatFailure(t *testing.T) {
	_, _, s := internalStub(t)
	calls := 0
	withSeam(t, &statFile, func(f *os.File) (fs.FileInfo, error) {
		calls++
		if calls > 2 {
			return nil, errors.New("injected stat failure")
		}
		return f.Stat()
	})
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a pair whose data could not be re-read passed the dump")
	}
}

func TestDumpReportsACopyFailure(t *testing.T) {
	_, _, s := internalStub(t)
	withSeam(t, &copyCollected, func(context.Context, io.Writer, io.Reader) (int64, error) {
		return 0, errors.New("injected copy failure")
	})
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a pair whose copy failed passed the dump")
	}
}

func TestParseDigest(t *testing.T) {
	sum := sha256.Sum256([]byte("data"))
	canonical := hex.EncodeToString(sum[:])
	cases := map[string]struct {
		raw string
		ok  bool
	}{
		"bare":             {raw: canonical, ok: true},
		"LF":               {raw: canonical + "\n", ok: true},
		"CRLF":             {raw: canonical + "\r\n", ok: true},
		"short":            {raw: canonical[:63]},
		"long":             {raw: canonical + "a"},
		"uppercase":        {raw: "ABCDEF" + canonical[6:]},
		"not hex":          {raw: "g" + canonical[1:]},
		"uppercase later":  {raw: canonical[:1] + "A" + canonical[2:]},
		"GNU escaped line": {raw: canonical + "  /etc/shadow\n"},
		"bare CR":          {raw: canonical + "\r"},
		"BOM":              {raw: string(rune(0xFEFF)) + canonical},
		"two LFs":          {raw: canonical + "\n\n"},
		"empty":            {raw: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseDigest([]byte(tc.raw))
			if tc.ok {
				if err != nil {
					t.Fatalf("parseDigest = %v", err)
				}
				if got != sum {
					t.Fatalf("parseDigest = %x, want %x", got, sum)
				}
				return
			}
			if err == nil {
				t.Fatal("parseDigest accepted a look-alike")
			}
		})
	}
}
