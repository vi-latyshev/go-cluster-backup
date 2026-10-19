package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// checkDiscoveryRules validates what a discovery's rules nest, offline: a
// nested source config is opaque bytes to the strict decoder — its strings are
// templates the discovery renders per matched object — so a typo in a field
// name, an unregistered source type or an unknown pipeline would otherwise
// pass validation and fail at night, in the cluster. Values are not evaluated
// and templates are not rendered; shapes, names and routing are.
func (e *Engine) checkDiscoveryRules(ctx context.Context) []error {
	var errs []error
	for _, db := range e.discoveries {
		reporter, ok := db.inst.(discovery.RuleSourceReporter)
		if !ok {
			continue
		}
		rules, err := safeValue(func() ([]discovery.RuleSource, error) { return reporter.RuleSources(ctx) })
		if err != nil {
			errs = append(errs, fmt.Errorf("/discoveries/%s: reading the rules: %w", db.name, err))
			continue
		}
		prefix := "/discoveries/" + db.name
		for _, rs := range rules {
			errs = append(errs, e.checkRuleSource(ctx, prefix, rs)...)
		}
	}
	return errs
}

// checkRuleSource is one rule's offline check: the source type must be
// registered, the nested config's shape must be the type's own, the pipeline
// must exist, and a secret artifact may only route through a pipeline that
// seals — the run would refuse it before the first byte otherwise.
func (e *Engine) checkRuleSource(ctx context.Context, prefix string, rs discovery.RuleSource) []error {
	var errs []error
	rulePath := prefix + rs.Path
	known := e.reg.Types(source.Kind.Name())
	switch {
	case rs.Type == "":
		// A rule that renders no source — the one accounting for the objects
		// no other rule claimed has an envelope and a routing, no source.
	case !slices.Contains(known, rs.Type):
		errs = append(errs, fmt.Errorf("%s: unknown source type %q (known: %v)", rulePath, rs.Type, known))
	default:
		if err := e.reg.CheckShape(source.Kind.Name(), rs.Type, rs.Config, prefix+rs.ConfigPath); err != nil {
			errs = append(errs, err)
		}
		// The shape check judges kinds, and a null has a kind the decoder reads
		// as "absent": the rule's source config gets the same null walk the
		// strict decoder gives an instance's own config.
		errs = append(errs, plugin.CheckNulls(rs.Config, prefix+rs.ConfigPath)...)
	}
	if rs.Importance != model.ImportanceRequired && rs.Importance != model.ImportanceBestEffort {
		errs = append(errs, fmt.Errorf("%s: importance must be \"required\" or \"best_effort\", got %q", rulePath, rs.Importance))
	}
	if rs.Sensitivity != model.SensitivityPlain && rs.Sensitivity != model.SensitivitySecret {
		errs = append(errs, fmt.Errorf("%s: sensitivity must be \"plain\" or \"secret\", got %q", rulePath, rs.Sensitivity))
	}
	procs, _, err := e.resolvePipeline(ctx, rulePath+"/pipeline", rs.Pipeline)
	switch {
	case err != nil:
		errs = append(errs, err)
	case rs.Sensitivity == model.SensitivitySecret:
		seals, err := e.pipelineSeals(ctx, procs)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: pipeline %q: %w", rulePath, rs.Pipeline, err))
		} else if !seals {
			errs = append(errs, fmt.Errorf("%s: a secret artifact's pipeline %v has no sealing processor; the run would refuse it before the first byte", rulePath, rs.Pipeline))
		}
	}
	return errs
}

// pipelineSeals reports whether any processor of a pipeline seals a stream,
// which is what the run-time secret invariant asks of it too.
func (e *Engine) pipelineSeals(ctx context.Context, procs []processor.Processor) (bool, error) {
	for _, p := range procs {
		tr, err := attempt(ctx, e.checkPolicy(), "describe processor", func(ctx context.Context, _ int) (processor.ProcessorTraits, error) {
			return safeValue(func() (processor.ProcessorTraits, error) { return p.Describe(ctx) })
		})
		if err != nil {
			return false, err
		}
		if tr.Seals {
			return true, nil
		}
	}
	return false, nil
}
