package inventory

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// chainStorage serves a scripted continuation chain instead of the fake's own
// contiguous pages: the fake cannot produce a checkpoint page that carries a
// token without objects, nor a failure in the middle of a chain.
type chainStorage struct {
	*storagetest.Fake
	script []storage.ListPage
	// fail runs before the next scripted page; a non-nil error is returned as
	// the listing's outcome for that call.
	fail func(call int) error

	mu     sync.Mutex
	index  int
	calls  int
	tokens []string
}

func (s *chainStorage) List(_ context.Context, req storage.ListRequest) (storage.ListPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.tokens = append(s.tokens, req.Token)
	if s.fail != nil {
		if err := s.fail(s.calls); err != nil {
			return storage.ListPage{}, err
		}
	}
	if s.index >= len(s.script) {
		return storage.ListPage{}, nil
	}
	page := s.script[s.index]
	s.index++
	return page, nil
}

// reset re-arms the chain and forgets the recorded calls: one storage then
// serves the same script twice.
func (s *chainStorage) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index, s.calls, s.tokens = 0, 0, nil
}

func (s *chainStorage) recorded() (calls int, tokens []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, slices.Clone(s.tokens)
}

// TestListChainStoredRunsFollowsTheContinuationChain (A17): every NextToken
// page is listed, a zero-object checkpoint page included, so the inventory is
// the whole chain rather than the first page.
func TestListChainStoredRunsFollowsTheContinuationChain(t *testing.T) {
	now := time.Now().UTC()
	older, newer := "20261001T040000Z", "20261002T040000Z"
	f := &chainStorage{Fake: storagetest.NewFake(), script: []storage.ListPage{
		{Objects: []model.ObjectInfo{{Key: "runs/" + older + "/a.bin", Size: 1, ModTime: now.Add(-48 * time.Hour)}}, NextToken: "page-2"},
		{NextToken: "checkpoint"},
		{Objects: []model.ObjectInfo{{Key: "runs/" + newer + "/a.bin", Size: 1, ModTime: now.Add(-24 * time.Hour)}}, NextToken: "page-4"},
	}}
	opts := testOptions(t)

	runs, err := opts.StoredRuns(context.Background(), "runs/", []Storage{testStorage("primary", f, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].RunID != newer || runs[1].RunID != older {
		t.Fatalf("stored runs = %+v, want both chained runs newest first", runs)
	}
	for _, r := range runs {
		if r.Kind != model.RunKindScheduled || r.HasManifest {
			t.Fatalf("run %+v, want a scheduled run without a manifest", r)
		}
	}
	calls, tokens := f.recorded()
	if calls != 4 || !slices.Equal(tokens, []string{"", "page-2", "checkpoint", "page-4"}) {
		t.Fatalf("listed %d pages with tokens %v, want the whole chain", calls, tokens)
	}
}

// TestListChainFinalFailureLeavesNoPartialView (A17): a List that fails after
// the retries are exhausted is the survey's answer for the whole storage — the
// pages it already listed are never reported as a partial run list.
func TestListChainFinalFailureLeavesNoPartialView(t *testing.T) {
	now := time.Now().UTC()
	f := &chainStorage{
		Fake: storagetest.NewFake(),
		script: []storage.ListPage{
			{Objects: []model.ObjectInfo{{Key: "runs/20261001T040000Z/a.bin", Size: 1, ModTime: now}}, NextToken: "page-2"},
		},
		fail: func(call int) error {
			if call > 1 {
				return errors.New("listing refused")
			}
			return nil
		},
	}
	opts := testOptions(t)
	s := testStorage("primary", f, 1)

	sur := opts.SurveyStorage(context.Background(), "runs/", s)
	if sur.OK || sur.Err == nil || len(sur.Runs) != 0 {
		t.Fatalf("survey = ok=%v err=%v runs=%d, want a failure and no runs", sur.OK, sur.Err, len(sur.Runs))
	}
	if calls, tokens := f.recorded(); calls != 4 || !slices.Equal(tokens, []string{"", "page-2", "page-2", "page-2"}) {
		t.Fatalf("survey listed %d times with tokens %v, want one page and three exhausted retries", calls, tokens)
	}

	f.reset()
	runs, err := opts.StoredRuns(context.Background(), "runs/", []Storage{s})
	if err == nil || runs != nil {
		t.Fatalf("StoredRuns = (%+v, %v), want a failure and no partial run list", runs, err)
	}
	if !strings.Contains(err.Error(), "stored runs") || !strings.Contains(err.Error(), "listing refused") {
		t.Fatalf("StoredRuns error = %q, want the storage's listing failure", err)
	}
}

// TestStoredRunsTieBreak: two runs whose manifests carry the same instant come
// back newest id first, and each view carries the manifest it was read from.
func TestStoredRunsTieBreak(t *testing.T) {
	f := storagetest.NewFake()
	at := time.Unix(1700000000, 0).UTC()
	for _, id := range []string{"20990101T000000Z", "20990101T000001Z"} {
		rep := model.RunReport{
			Schema: model.ManifestSchema, RunID: id, Kind: model.RunKindScheduled, StartedAt: at, Outcome: model.OutcomeDelivered,
			Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
		}
		f.PutObject("runs/"+id+"/a.bin", []byte("x"), at)
		f.PutObject("runs/"+id+"/manifest.json", mustMarshal(t, rep), at)
	}
	opts := testOptions(t)

	runs, err := opts.StoredRuns(context.Background(), "runs/", []Storage{testStorage("primary", f, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].RunID != "20990101T000001Z" || runs[1].RunID != "20990101T000000Z" {
		t.Fatalf("stored runs = %+v, want the higher id first on an equal instant", runs)
	}
	for _, r := range runs {
		if !r.HasManifest || r.Report.RunID != r.RunID {
			t.Fatalf("run %+v, want its own manifest", r)
		}
	}
}
