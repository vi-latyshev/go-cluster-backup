package cli

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
)

// loggerFor builds the diagnostic logger of one command. Diagnostics go to
// stderr; the command's own output goes to stdout, so a pipeline consuming a
// report never sees a log line. The handler is human.Handler — the one renderer
// — so every line, the engine's and the plugins' alike, reads the same way.
func loggerFor(w io.Writer, level string) (*slog.Logger, error) {
	l, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	return slog.New(human.NewHandler(w, l)), nil
}

// parseLevel maps the --log-level flag onto a slog level.
func parseLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("log level %q must be debug, info, warn or error", level)
	}
}
