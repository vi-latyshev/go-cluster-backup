package webhook

import (
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// The samples every template of this plugin is rendered against while a
// configuration is validated, offline, before the first night: a message whose
// title is empty, whose fields do not exist or whose body is not the document
// its format promises is a defect of the config file, not a receiver's outage,
// and with notifier_failure_is_fatal false a body that never renders would cost
// the night silently.
//
// One sample is not enough: a template is written for the night it describes,
// and a `pick` that covers the degraded wording but not the delivered one — or
// a line that only exists when an artifact failed — renders perfectly against
// the wrong run and then fails on the night it was written for. Every template
// is therefore rendered against the whole matrix below, and a template that
// fails on any of them is refused with the sample it failed on named.

// sample is one run a template is rendered against, and what to call it in a
// refusal.
type sample struct {
	name string
	ev   model.Event
}

// samples is the matrix: both events, every run outcome, an artifact that
// succeeded and one that failed, an artifact that reported what its source left
// out and one that reported nothing, every storage outcome, an ignored and a
// covered exclusion, a previous run that never reported and one that did, and
// the ends of the range — a run with no artifact and no storage at all.
func samples() []sample {
	at := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	lastGood := at.Add(-24 * time.Hour)
	okArtifact := model.ArtifactResult{
		Artifact: model.Artifact{
			Group: "databases", Name: "example/postgres",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
		},
		Outcome: model.ArtifactOK, Key: "runs/20260102T030405Z/databases/example/postgres.dump.zst",
		Pipeline: "zstd", RawBytes: 4 << 20, Bytes: 1 << 20,
		Stats:    model.DumpStats{SkippedEntries: 2, PaddedFiles: 1},
		SHA256:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Duration: model.Duration(30 * time.Second),
	}
	failedArtifact := model.ArtifactResult{
		Artifact: model.Artifact{
			Group: "volumes", Name: "example/data",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
		},
		Outcome:  model.ArtifactFailed,
		Duration: model.Duration(time.Second),
		// Two sentences, the way errors.Join writes them: a template that puts
		// this inside a code span must still render one line.
		Error: "the tree changed under the read\nskipped 2, padded 1",
	}
	verified := model.StorageResult{
		Storage: "yandex", Type: "yandex_disk", Outcome: model.StorageVerified,
		Objects: 2, Bytes: 1 << 20, Attempts: 1, Kept: 7,
		Deleted: []string{"runs/20251226T030405Z"}, Freed: 1 << 20,
		Duration: model.Duration(20 * time.Second),
	}
	report := func(outcome model.Outcome, artifacts []model.ArtifactResult, storages []model.StorageResult, excluded []model.Exclusion, previous model.PreviousRun) model.RunReport {
		return model.RunReport{
			Schema: model.ManifestSchema, RunID: "20260102T030405Z", Kind: model.RunKindScheduled, StartedAt: at,
			Duration: model.Duration(5*time.Minute + 12*time.Second), Outcome: outcome,
			Artifacts: artifacts, Storages: storages, Excluded: excluded,
			Previous: previous, ExitCode: 0,
		}
	}
	// manual builds the same report as a manual run: its id carries the suffix,
	// its kind is manual, and a storage may carry a cap refusal.
	manual := func(outcome model.Outcome, artifacts []model.ArtifactResult, storages []model.StorageResult) model.RunReport {
		rep := report(outcome, artifacts, storages, nil, model.PreviousRun{RunID: "20260101T030405Z", LastGoodAt: lastGood})
		rep.RunID = "20260102T030405Z-manual"
		rep.Kind = model.RunKindManual
		return rep
	}
	finished := func(name string, rep model.RunReport) sample {
		return sample{name: name, ev: model.Event{Name: model.EventFinished, At: at, Run: rep}}
	}
	started := func(name string, previous model.PreviousRun) sample {
		return sample{name: name, ev: model.Event{Name: model.EventStarted, At: at, Run: report(model.OutcomeRunning, nil, nil, nil, previous)}}
	}
	// startedManual is the start of a manual run: a template that branches on
	// the kind renders here, not only on a night that happens to be manual.
	startedManual := func(name string, previous model.PreviousRun) sample {
		rep := report(model.OutcomeRunning, nil, nil, nil, previous)
		rep.RunID = "20260102T030405Z-manual"
		rep.Kind = model.RunKindManual
		return sample{name: name, ev: model.Event{Name: model.EventStarted, At: at, Run: rep}}
	}

	return []sample{
		started("a run that just started", model.PreviousRun{RunID: "20260101T030405Z", LastGoodAt: lastGood}),
		started("a run that started after one which never reported", model.PreviousRun{RunID: "20260101T030405Z", Unfinished: true, LastGoodAt: lastGood}),
		startedManual("a manual run that just started", model.PreviousRun{RunID: "20260101T030405Z", LastGoodAt: lastGood}),
		finished("a delivered run", report(model.OutcomeDelivered,
			[]model.ArtifactResult{okArtifact}, []model.StorageResult{verified},
			[]model.Exclusion{{Name: "volumes/example/cache", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker"}},
			model.PreviousRun{RunID: "20260101T030405Z", LastGoodAt: lastGood})),
		finished("a degraded run", report(model.OutcomeDegraded,
			[]model.ArtifactResult{okArtifact, failedArtifact},
			[]model.StorageResult{
				verified,
				{Storage: "offsite", Type: "s3", Outcome: model.StorageFailed, Attempts: 4, Kept: 7, Duration: model.Duration(time.Minute), Error: "HTTP 503 from the endpoint"},
				{Storage: "exchange", Type: "local", Outcome: model.StorageSkipped, Duration: model.Duration(0)},
			},
			[]model.Exclusion{
				{Name: "volumes/example/cache", Group: "volumes", Kind: model.ExclusionIgnored, Reason: "carries the exclusion marker"},
				{Name: "volumes/example/data", Group: "volumes", Kind: model.ExclusionCovered, Reason: `covered by the dump of pod "postgres-0"`},
			},
			model.PreviousRun{RunID: "20260101T030405Z", Unfinished: true, LastGoodAt: lastGood})),
		finished("a failed run with nothing to report", report(model.OutcomeFailed, nil, nil, nil,
			model.PreviousRun{RunID: "20260101T030405Z", Unfinished: true, LastGoodAt: lastGood})),
		// A manual run delivered: its kind and its suffixed id reach every
		// template, so a message can tell a manual copy from a night's.
		finished("a manual run that was delivered", manual(model.OutcomeDelivered,
			[]model.ArtifactResult{okArtifact}, []model.StorageResult{verified})),
		// A manual run whose cap refused one storage while the other took it all:
		// the cap refusal carries its limit and the bytes that tripped it.
		finished("a manual run refused by the cap on one storage", manual(model.OutcomeDegraded,
			[]model.ArtifactResult{okArtifact},
			[]model.StorageResult{
				verified,
				{Storage: "offsite", Type: "s3", Outcome: model.StorageFailed, CapExceeded: true,
					CapLimitBytes: 14_000_000_000, CapUsedBytes: 13_800_000_000,
					Duration: model.Duration(time.Minute), Error: "cap_exceeded: 1.0 GiB would put the storage past its configured max_total_bytes (13.8 GB of 14 GB used)"},
			})),
		// A manual run refused the writer lease: failed, no storage touched, and
		// the claim that was in the way is in the report.
		finished("a manual run refused by the lease", leaseRefused()),
	}
}

// leaseRefused is the report of a manual run that never started writing because
// a live claim was held: no artifact, no storage, and the machine-readable
// LeaseState naming the holder.
func leaseRefused() model.RunReport {
	return model.RunReport{
		Schema: model.ManifestSchema, RunID: "20260102T030405Z-manual", Kind: model.RunKindManual,
		StartedAt: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
		Duration:  model.Duration(time.Second), Outcome: model.OutcomeFailed, ExitCode: 1,
		Previous: model.PreviousRun{RunID: "20260101T030405Z", LastGoodAt: time.Date(2026, time.January, 1, 3, 4, 5, 0, time.UTC)},
		Lease:    &model.LeaseState{Refused: true, Holder: "20260102T030000Z-manual", HolderKind: model.RunKindManual},
	}
}

// sampleMessage is the message the payload template is rendered against: a
// title and two lines, one of them with the code part a payload may hand its
// receiver as monospace.
func sampleMessage() Message {
	return Message{
		Title: "⚠️ Backup DEGRADED",
		Lines: []Line{
			{Text: "📦 20260102T030405Z"},
			{Text: "⏱ Duration: ", Code: "5m12s"},
		},
	}
}
