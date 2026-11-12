package postgres

import (
	"testing"
)

// TestCompressionValue: PostgreSQL 16 introduced METHOD[:LEVEL]; the 15
// client takes a numeric level only, so the configured value is mapped there
// and a method it cannot express is refused by name, never handed to pg_dump.
func TestCompressionValue(t *testing.T) {
	for _, tc := range []struct {
		name        string
		major       int
		compression string
		want        string
		wantErr     bool
	}{
		{"18 keeps the configured value", 18, "zstd:3", "zstd:3", false},
		{"18 keeps gzip without a level", 18, "gzip", "gzip", false},
		{"17 keeps the configured value", 17, "none", "none", false},
		{"15 maps none to zero", 15, "none", "0", false},
		{"15 maps a bare level", 15, "9", "9", false},
		{"15 maps gzip:LEVEL to the level", 15, "gzip:3", "3", false},
		{"15 maps gzip:0 to no compression", 15, "gzip:0", "0", false},
		{"15 refuses an unspecified gzip level", 15, "gzip", "", true},
		{"15 refuses a long gzip level", 15, "gzip:abc", "", true},
		{"15 refuses a non-numeric gzip level", 15, "gzip:a1", "", true},
		{"15 refuses a level above nine", 15, "gzip:10", "", true},
		{"15 refuses zstd", 15, "zstd:3", "", true},
		{"15 refuses lz4", 15, "lz4", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := compressionValue(tc.major, tc.compression)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("compressionValue(%d, %q) = %q, want a refusal", tc.major, tc.compression, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("compressionValue(%d, %q): %v", tc.major, tc.compression, err)
			}
			if got != tc.want {
				t.Fatalf("compressionValue(%d, %q) = %q, want %q", tc.major, tc.compression, got, tc.want)
			}
		})
	}
}
