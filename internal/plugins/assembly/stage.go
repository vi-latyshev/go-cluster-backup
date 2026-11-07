package assembly

import (
	"context"
	"errors"
	"fmt"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/stage"
)

// Stage runs the sources of the stage section through the stage workflow and
// returns its report. This method is the assembly the workflow receives: the
// stage section's own exit codes, its sources and their check set, and the
// pipeline the provider produces with. Stage has no run id: the file name is the
// identity, and the stage section has no run section to read one from.
func (e *Assembly) Stage(ctx context.Context, req StageRequest) (model.RunReport, error) {
	cfg := e.cfg
	if cfg.Stage == nil || cfg.Stage.Dir == "" {
		return model.RunReport{}, errors.New("stage: config needs the stage section with a dir")
	}
	if len(e.stageSources) == 0 {
		return model.RunReport{}, errors.New("stage: config has no stage.sources")
	}
	// Missing required binaries are the deployment, not a blip: refuse before
	// any plugin Check runs.
	if errs := e.binaryErrors(scopeStage); len(errs) > 0 {
		return model.RunReport{}, fmt.Errorf("preflight: %w", errors.Join(errs...))
	}
	retry := e.checkPolicy()
	opts := stage.Options{
		Dir:              cfg.Stage.Dir,
		ConfigPath:       req.ConfigPath,
		DegradedExitCode: cfg.Stage.DegradedExitCode,
		FailedExitCode:   cfg.Stage.FailedExitCode,
		Sources:          e.projectedStageSources(),
		Checks:           func() map[stage.Check]error { return projectedChecks(e.scopedChecks(ctx, retry, scopeStage)) },
		Retry:            retry,
		Pipeline:         pipeline.New(pipeline.Options{Policy: e.retry, Log: e.log}),
		Log:              e.log,
	}
	return stage.Run(ctx, opts)
}

// projectedStageSources is the stage section's sources as the workflow receives
// them: the built instance, its envelope and its pipeline steps, never the
// assembly or the whole configuration.
func (e *Assembly) projectedStageSources() []stage.Source {
	out := make([]stage.Source, 0, len(e.stageSources))
	for _, sb := range e.stageSources {
		out = append(out, stage.Source{
			Name:      sb.name,
			CheckKind: sb.checkKind,
			Type:      sb.typ,
			Instance:  sb.inst,
			Artifact:  sb.artifact,
			Pipeline:  sb.pipeline,
			Steps:     e.pipelineSteps(sb.refs, sb.procs),
		})
	}
	return out
}

// pipelineSteps pairs each built processor with the config instance name and
// type its pipeline declared, in config order.
func (e *Assembly) pipelineSteps(refs []string, procs []processor.Processor) []pipeline.Step {
	steps := make([]pipeline.Step, 0, len(procs))
	for i, p := range procs {
		step := pipeline.Step{Processor: p}
		if i < len(refs) {
			step.Instance = refs[i]
			step.Type = e.cfg.Processors[refs[i]].Type
		}
		steps = append(steps, step)
	}
	return steps
}

// projectedChecks turns the scoped preflight failures into the workflow's own
// check table.
func projectedChecks(checks failedChecks) map[stage.Check]error {
	out := make(map[stage.Check]error, len(checks))
	for k, err := range checks {
		out[stage.Check{Kind: k.kind, Name: k.name}] = err
	}
	return out
}
