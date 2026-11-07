package human

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every plugin that renders a log line imports it directly.
var Spec = loader.Spec{
	ID:      "human",
	Summary: "the log's rendering of sizes, durations, compression ratios and the run clock",
}
