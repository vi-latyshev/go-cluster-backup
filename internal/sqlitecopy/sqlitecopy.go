// Package sqlitecopy is the shared plumbing of the sources that snapshot a
// SQLite database through the sqlite3 shell's online .backup dot-command. Both
// the generic sqlite source and the k3s_state source copy a live database the
// same way — read-only, with a busy timeout, escaping the destination path — so
// the argument vector lives here once and neither plugin spells it again.
//
// The package is internal: it is the module's own library, not part of the
// plugin API a third-party plugin is written against.
package sqlitecopy

import (
	"strconv"
	"strings"
	"time"
)

// BusyMillis renders a busy timeout the way the shell's `.timeout` command
// expects it: whole milliseconds.
func BusyMillis(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

// BackupArgs is the sqlite3 argument vector that copies src into dst with the
// online .backup dot-command: read-only, the busy timeout, the source database,
// and the dot-command itself.
//
// The destination is a dot-command argument, not a SQL string, so it is quoted
// for the shell's own parser (QuoteArg), never escaped as SQL. A path is never
// refused: the shell's parser handles every byte a POSIX path may carry.
func BackupArgs(src, dst string, busy time.Duration) []string {
	return []string{
		"-readonly",
		"-cmd", ".timeout " + BusyMillis(busy),
		src,
		".backup " + QuoteArg(dst),
	}
}

// QuoteArg quotes one path for the sqlite3 shell's dot-command parser: the
// argument is wrapped in double quotes and a backslash or a double quote inside
// it is escaped with a backslash. The shell reads the rest of the line as the
// destination and unescapes exactly those two characters.
//
// SQL-style escaping is WRONG here and this is why the function exists: a
// single-quoted argument whose inner quote is doubled the way SQL spells it is
// not one argument to the shell — the shell ends it at the doubled quote, splits
// on the space, and answers "Usage: .backup ?DB? ?OPTIONS? FILENAME". Verified
// against sqlite3 3.43 (see the round-trip test), on a path carrying a quote,
// a double quote, a backslash and a space.
func QuoteArg(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// Pragma names the integrity pragma a check depth runs over the COPY, after the
// copy is complete. "quick" and "full" are the two depths; any other name —
// including the "none" a source offers to skip the check — is not a pragma, and
// ok is false so the caller decides what skipping means.
func Pragma(check string) (pragma string, ok bool) {
	switch check {
	case "quick":
		return "PRAGMA quick_check", true
	case "full":
		return "PRAGMA integrity_check", true
	}
	return "", false
}
