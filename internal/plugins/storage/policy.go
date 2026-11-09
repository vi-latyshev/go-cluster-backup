package storage

// AsynchronousWriter is the optional capability of a Storage whose accepted
// write may settle after the call returns: a multipart commit, an HTTP 202, any
// provider-side acceptance that makes bytes visible later. The safety owner asks
// before the first write, so the durable uncertainty record names this holder
// before any such write leaves the process.
type AsynchronousWriter interface {
	WritesAsynchronously() bool
}

// WritesAsynchronously reports whether one storage's accepted writes may settle
// after the call returns: the explicit capability, or the multipart capability.
// A storage that declares the capability answers for itself — including "no",
// which overrides the multipart default.
func WritesAsynchronously(s Storage) bool {
	if w, ok := s.(AsynchronousWriter); ok {
		return w.WritesAsynchronously()
	}
	_, ok := s.(MultipartUploader)
	return ok
}
