// Package raw is the raw storage boundary: the one place a constructed
// destination is still a mutable instance. It lives under the storage package's
// Go internal directory, so only packages under internal/plugins/storage may
// import it — a workflow, an uploader or a restore path never holds one, and the
// guard hands them a Port or a Handle instead.
package raw

import "github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"

// Raw is one constructed destination exactly as its factory returned it.
type Raw struct {
	Storage storage.Storage
}
