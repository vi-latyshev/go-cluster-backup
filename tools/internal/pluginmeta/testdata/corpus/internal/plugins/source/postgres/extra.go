package postgres

import "example.test/app/internal/plugins/model"

// helper proves a second file importing the same plugin does not duplicate the
// generated edge.
func helper() string { return model.Spec.ID }
