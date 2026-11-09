package inventory

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// Storage is one storage the inventory reads.
type Storage struct {
	// Name is the storage's configured name: every problem the survey reports
	// names it.
	Name string
	// Reader is the storage's read-only port.
	Reader storage.Reader
	// CallParallelism bounds the manifest reads in flight.
	CallParallelism int
	// Log receives the survey's events, already prefixed with the instance.
	Log *slog.Logger
}

// Options are the survey's typed collaborators, projected by assembly.
type Options struct {
	// Layout renders the manifest key and reads the run kind back from an id.
	Layout layout.Layout
	// Policy is the execution owner's retry policy for this path's calls.
	Policy execution.Options
}

// Inventory is the survey bound to one execution policy. It is immutable:
// every call reads the projected collaborators and returns a fresh value.
type Inventory struct {
	layout layout.Layout
	policy execution.Options
}

// New binds the survey to its collaborators.
func New(o Options) Inventory {
	return Inventory{layout: o.Layout, policy: o.Policy}
}

// Survey is one storage's inventory: every run present under the run prefix,
// its objects and its manifest when the manifest is there. OK false means "do
// not know": the listing did not finish and Runs holds nothing.
type Survey struct {
	Storage string
	OK      bool
	Err     error
	Runs    map[string]Run
}

// Run is one run the survey found under the prefix.
type Run struct {
	Stored  model.StoredRun
	Objects []model.ObjectInfo
	// Manifest is the parsed report, also when it is a problem run: the restore
	// side refuses it in its own words, the deletion decision refuses it here.
	Manifest *model.RunReport
	// ManifestKey is where the manifest sits among Objects, "" when the key
	// could not be rendered; deletion removes it first.
	ManifestKey string
	// ManifestProblem is set when the manifest is listed but could not be read,
	// parsed or accepted — never treated as "this run has no manifest": a run
	// whose manifest is unreadable is refused deletion for this sweep, because
	// the survey cannot see which artifacts it holds.
	ManifestProblem string
}

// SurveyStorage lists one storage under the run prefix and reads the manifests.
// The caller owns the storage's time budget and passes the context it bounds.
func (inv Inventory) SurveyStorage(ctx context.Context, prefix string, s Storage) Survey {
	started := time.Now()
	res := Survey{Storage: s.Name, Runs: map[string]Run{}}
	var objs []model.ObjectInfo
	token := ""
	pages := 0
	listingStart := time.Now()
	for page := 0; ; page++ {
		if page > 500 {
			res.Err = fmt.Errorf("listing did not terminate after 500 pages")
			return res
		}
		pages++
		got, err := execution.Attempt(ctx, inv.policy, "list "+s.Name, func(ctx context.Context, _ int) (storage.ListPage, error) {
			return execution.SafeValue(func() (storage.ListPage, error) {
				return s.Reader.List(ctx, storage.ListRequest{Prefix: prefix, Token: token})
			})
		})
		if err != nil {
			res.Err = err
			event(s, human.Failure("storage.survey.failed", human.Text("reason", err.Error())))
			return res
		}
		objs = append(objs, got.Objects...)
		if got.NextToken == "" {
			break
		}
		token = got.NextToken
	}
	listing := time.Since(listingStart)
	for _, o := range objs {
		rel := strings.TrimPrefix(o.Key, prefix)
		id, _, found := strings.Cut(rel, "/")
		if !found || id == "" {
			continue // an object outside any run directory is not ours to reason about
		}
		r := res.Runs[id]
		r.Objects = append(r.Objects, o)
		res.Runs[id] = r
	}
	// Pass 1: every run's manifest key, in run order. Nothing here touches the
	// storage — the reads are pass 2.
	type manifestJob struct {
		id     string
		run    Run
		key    string
		keyErr error
		raw    []byte
		err    error
	}
	jobs := make([]manifestJob, 0, len(res.Runs))
	manifests := 0
	for _, id := range slices.Sorted(maps.Keys(res.Runs)) {
		r := res.Runs[id]
		mkey, err := inv.layout.ManifestKey(id)
		job := manifestJob{id: id, keyErr: err}
		if err == nil {
			for _, obj := range r.Objects {
				if obj.Key == mkey {
					r.ManifestKey = mkey
					job.key = mkey
					manifests++
					break
				}
			}
		}
		// The snapshot is taken after the key is recorded: pass 3 stores it
		// back, and deletion orders the manifest first by this exact key.
		job.run = r
		jobs = append(jobs, job)
	}
	// Pass 2: the manifest reads, at most call_parallelism in flight. A job
	// writes only its own outcome, so no event and no shared field is touched
	// here; every listed manifest is read even when the budget has already
	// expired — a read that cannot run is a problem, never the verdict that the
	// run has no manifest.
	readingStart := time.Now()
	slots := make(chan struct{}, s.CallParallelism)
	var wg sync.WaitGroup
	for i := range jobs {
		if jobs[i].key == "" {
			continue
		}
		// The slot is taken before the goroutine exists, so a survey of a
		// thousand runs still holds at most call_parallelism readers, not a
		// thousand waiting goroutines.
		slots <- struct{}{}
		wg.Add(1)
		go func(j *manifestJob) {
			defer wg.Done()
			defer func() { <-slots }()
			j.raw, j.err = inv.manifest(ctx, s, j.key)
		}(&jobs[i])
	}
	wg.Wait()
	reading := time.Since(readingStart)
	// Pass 3: the outcomes, their events and the deletion inputs, in run order.
	for i := range jobs {
		job := &jobs[i]
		r, id := job.run, job.id
		kind := inv.layout.Kind(id)
		switch {
		case job.keyErr != nil:
			// The key cannot be rendered, so whether a manifest exists is
			// unknown: the run is never dropped while that is true.
			r.ManifestProblem = "the manifest key does not render: " + job.keyErr.Error()
			event(s, human.Failure("storage.manifest-key.failed",
				human.Text("run", id), human.Text("reason", job.keyErr.Error())))
		case job.key != "":
			rep, err := (*model.RunReport)(nil), job.err
			if err == nil {
				rep, err = decodeManifest(job.raw, id, kind)
			}
			if err != nil {
				r.ManifestProblem = err.Error()
				event(s, human.Failure("storage.manifest-unreadable",
					human.Text("run", id), human.Text("reason", err.Error()), human.ObjectKey("key", job.key)))
			}
			r.Manifest = rep
		}
		var bytes int64
		at := time.Time{}
		for _, obj := range r.Objects {
			bytes += obj.Size
			if obj.ModTime.After(at) {
				at = obj.ModTime
			}
		}
		verified := false
		if r.Manifest != nil && r.ManifestProblem == "" {
			at = r.Manifest.StartedAt
			// A run that came out failed is missing something required on every
			// storage that took it: it must never stand in for "the last
			// verified run" the deletion guard protects.
			verified = r.Manifest.Outcome != model.OutcomeFailed &&
				storageOutcome(r.Manifest, s.Name) == model.StorageVerified
		}
		r.Stored = model.StoredRun{RunID: id, Kind: kind, At: at, Bytes: bytes, Verified: verified, Manifest: r.Manifest != nil && r.ManifestProblem == ""}
		res.Runs[id] = r
	}
	event(s, human.New("storage.survey",
		human.Count("runs", int64(len(res.Runs))),
		human.Count("objects", int64(len(objs))),
		human.Count("pages", int64(pages)),
		human.Count("manifests", int64(manifests)),
		human.Duration("listing", listing),
		human.Duration("reading", reading),
		human.Duration("took", time.Since(started))))
	res.OK = true
	return res
}

// manifest reads one manifest's bytes. A listed manifest that reads back absent —
// the listing and the object disagree — is a problem like an unreadable body,
// never "this run has no manifest": a deletion decision would take the absent
// answer for permission to remove a run whose artifacts the survey cannot see.
func (inv Inventory) manifest(ctx context.Context, s Storage, key string) ([]byte, error) {
	body, err := execution.Attempt(ctx, inv.policy, "get manifest "+s.Name, func(ctx context.Context, _ int) ([]byte, error) {
		var buf bytes.Buffer
		if err := execution.SafeCall(func() error { return s.Reader.Get(ctx, key, &buf) }); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	})
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			return nil, fmt.Errorf("the manifest is listed but reads back absent: %w", err)
		}
		return nil, fmt.Errorf("reading the manifest: %w", err)
	}
	return body, nil
}
