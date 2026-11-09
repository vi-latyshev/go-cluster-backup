package transfer

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled identity of the transfer library: it contributes no
// factory and no role, so the backup workflow imports it directly. It owns the
// multipart upload of one artifact to one destination — bounded parts, producer
// overlap, whole-object fallback and abort of this run's own uploads.
var Spec = loader.Spec{
	ID:      "transfer",
	Summary: "multipart uploads through a guarded handle: bounded parts, producer overlap and abort",
}
