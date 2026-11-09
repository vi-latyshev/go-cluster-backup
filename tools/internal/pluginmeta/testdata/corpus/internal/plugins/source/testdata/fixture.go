package fixture

import "example.test/app/internal/loader"

// Spec inside testdata is never part of the distribution.
var Spec = loader.Spec{ID: "testdata-fixture", Summary: "fixture"}
