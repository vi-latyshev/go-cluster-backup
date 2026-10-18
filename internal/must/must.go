// Package must turns an error that cannot happen for the given inputs into a
// panic. It exists so an impossible error is neither swallowed nor spelled as a
// branch no test can reach: the call site states out loud that the call cannot
// fail, and the panic is what a bug in that claim looks like.
//
// The engine's own re-encode of a defaults document panics the same way; this
// package is that decision, named once and shared.
package must

// Do returns v and panics when err is non-nil.
//
// It is for an error that is impossible BY CONSTRUCTION: a value this process
// just encoded, a type it just looked up in its own registry. It is never for an
// error a caller, a file or a network could produce — a reachable error is
// handled, reported or returned, never wrapped here.
func Do[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
