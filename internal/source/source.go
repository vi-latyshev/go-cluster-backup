// Package source is the seam that produces the bytes of one artifact — a database
// dump, a cache snapshot, a file tree as one tar stream. The interface obeys the
// seam rules documented in package plugin.
package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// StagedChainFile names the record the `stage` command leaves beside a staged
// artifact. The leading dot can never be mistaken for an artifact, and the
// record itself carries the artifact's exact file name tail (StagedRecord.Ext),
// so the run side reads the chain from the file and records it in the manifest,
// and a restore decodes with exactly what the stage applied — no second copy of
// the chain in another config that could drift.
func StagedChainFile(dir, name string) string {
	return filepath.Join(dir, ".chain-"+name+".json")
}

// StagedRecord is what the `stage` command leaves beside a staged artifact, and
// it is the pair's identity: the exact tail of the artifact's file name — the
// extension, the source's own and then every processor's, in encode order — and
// the processor chain that produced the bytes, which the run records as the
// artifact's chain and a restore decodes with. The artifact is exactly
// <name><Ext>, so nothing else in the directory can be mistaken for it: no
// prefix can reach a neighbour's file, and a pipeline that changed since last
// night is visible as a differently named file rather than a collision.
type StagedRecord struct {
	Ext   string                `json:"ext"`
	Chain []model.ProcessorStep `json:"chain"`
}

// StagedArtifactName is the exact file name of the artifact one record
// describes: the artifact's name plus the record's extension tail. The writer
// names the file this way, the reader opens exactly this file, and the clear
// removes exactly this file — one derivation, both sides.
func StagedArtifactName(name string, rec StagedRecord) string {
	return name + rec.Ext
}

// ReadStagedRecord reads the record a stage left for the artifact named name. A
// missing file is the caller's fs.ErrNotExist to report: the stage published no
// record, and nothing tells a restore how those bytes are encoded.
func ReadStagedRecord(dir, name string) (StagedRecord, error) {
	path := StagedChainFile(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return StagedRecord{}, err
	}
	var rec StagedRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return StagedRecord{}, fmt.Errorf("%s: %w", path, err)
	}
	return rec, nil
}

// SourceTraits are facts about the bytes a source produces, known before Dump runs.
type SourceTraits struct {
	// Ext is the artifact extension the source appends, e.g. ".dump", ".rdb", ".tar";
	// "" is valid for a format without a suffix.
	Ext string
	// Sealed is true when the bytes are already encrypted (a staged artifact).
	Sealed bool
	// Origin is a non-secret identity of where the data was read from, e.g.
	// "postgres://host:5432/dbname" or "file:///volumes/ns/name". It is recorded
	// in the run manifest and compared against a restore target; it must carry
	// no credentials.
	Origin string
}

// Source produces one artifact. Name, group, importance and sensitivity of the
// artifact are the config envelope's, not the plugin's.
type Source interface {
	// Describe returns static facts only: it must not touch the data source.
	Describe(ctx context.Context) (SourceTraits, error)
	// Dump writes the whole artifact to dst and returns only after the producer
	// has exited: a non-zero exit of pg_dump is this call's error, never a short
	// stream returned as success. One attempt per call — retries are the engine's.
	// A cancelled context is an error.
	Dump(ctx context.Context, dst io.Writer) error
}

var Kind = plugin.NewKind[Source]("source")

// Unit is one artifact an instance's domain holds several of: a database of a
// server, a mailbox of an account.
type Unit struct {
	// Name names the unit within its instance, unique within one result. The
	// engine builds both names from it — the artifact "<envelope>/<name>", the
	// instance "<instance>/<name>" — so a unit never names itself, and the
	// engine owns the shape of a usable name: one it cannot build a name from
	// (empty, a separator, a control character) is that unit's own failed
	// artifact and leaves the instance's other units alone, which is what a
	// name a server hands back needs.
	Name string
	// Config is the complete config of a source of the SAME type that dumps
	// exactly this unit and answers errors.ErrUnsupported to Units itself: the
	// instance's own config with the unit substituted in.
	Config jsontext.Value
}

// Enumerator is an optional capability of a Source instance whose domain holds
// several units (see Unit): each becomes its own artifact, built by the engine
// from the instance's own plugin type. An instance without the capability — or
// answering errors.ErrUnsupported — is one artifact. An
// empty result is an error of the plugin's own ("this instance holds no
// units"), never a run that silently backs up nothing. One attempt per call:
// retries are the engine's.
type Enumerator interface {
	Units(ctx context.Context) ([]Unit, error)
}

// DumpStatsReporter is an optional capability of a Source instance: it reports
// what the Dump calls on this instance left out of the stream (see
// model.DumpStats). The engine records the counts beside the artifact, so a
// tree a source could not copy in full is visible in the manifest and in the
// nightly message, not only in a log line. A source without the capability — or
// answering errors.ErrUnsupported — leaves nothing out that anyone must know
// about. One attempt per call; the counts describe the most recent Dump on this
// instance, and a call that fails fails that artifact: an artifact whose report
// cannot be produced is not a copy anyone can lean on.
type DumpStatsReporter interface {
	DumpStats(ctx context.Context) (model.DumpStats, error)
}

// RecordedChain is an optional capability of a Source instance whose bytes are
// already the output of a processor chain another process ran — the run-side
// `staged` source reading what `stage` left. The chain it reports, in encode
// order, is recorded as the artifact's chain: a restore reverses exactly it.
// The instance's own pipeline must be empty — nothing is applied to bytes that
// are already a pipeline's output, and a config stating one is refused rather
// than silently ignored. errors.ErrUnsupported means the instance carries no
// chain of its own, like every source without the capability. One attempt per
// call: retries are the engine's.
type RecordedChain interface {
	RecordedChain(ctx context.Context) ([]model.ProcessorStep, error)
}

// ApplyMode says what an Apply may replace. The empty value is an error: nothing
// destructive is ever a default.
type ApplyMode string

const (
	// ApplySideBySide writes to a new target labelled with the run id and leaves
	// the current data untouched.
	ApplySideBySide ApplyMode = "side_by_side"
	// ApplyInPlace replaces the target's current data.
	ApplyInPlace ApplyMode = "in_place"
)

type ApplyRequest struct {
	Mode ApplyMode
	// Label distinguishes a side-by-side target; the engine passes the run id.
	Label string
	// Options arrives as JSON bytes from restore.apply.<type>; the plugin
	// decodes it strictly, itself.
	Options jsontext.Value
}

type ApplyReport struct {
	// Target is what was written: a database, a directory, a file.
	Target string
	// Previous is where the replaced data went, "" when nothing was replaced.
	Previous string
	Bytes    int64
}

// Restorer writes a decoded artifact where the instance reads its data. It is an
// optional capability of a Source instance: one attempt per call, and a target
// without it — or answering errors.ErrUnsupported — is fetch-only.
type Restorer interface {
	Apply(ctx context.Context, req ApplyRequest, src io.Reader) (ApplyReport, error)
}

type ProbeReport struct {
	Entries int64
	Bytes   int64
	Detail  string
}

// Prober proves an artifact is readable without a target and without writing:
// a pg_restore listing pass, a walk over a tar stream, an integrity check.
type Prober interface {
	Probe(ctx context.Context, src io.Reader) (ProbeReport, error)
}
