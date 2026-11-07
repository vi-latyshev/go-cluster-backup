package yandexdisk_test

// BenchmarkListing measures one complete survey listing of the same nested
// 25-run corpus at call_parallelism 1 and 2, through the same fake service.
// Every folder's answer carries a seeded, skewed latency, so the only
// difference between the two runs is the walk's own parallelism. The
// tail-faults profile adds one transient answer and one rate limit near the
// end of the walk and reports how many service requests a full replay costs.
// These are offline numbers: they promise no production multiplier.

import (
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

const benchmarkRoot = "app:/cluster-backup"

// latency classes of the corpus: a flat one, a mild skew and a heavy tail.
func balancedLatency(string) time.Duration { return 2 * time.Millisecond }

func skewedLatency(path string) time.Duration {
	return time.Duration(1+stableHash(path)%5) * time.Millisecond
}

func heavyTailLatency(path string) time.Duration {
	switch h := stableHash(path) % 100; {
	case h < 1:
		return 60 * time.Millisecond
	case h < 10:
		return 12 * time.Millisecond
	default:
		return time.Millisecond
	}
}

func stableHash(path string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return h.Sum32()
}

// seedListingCorpus fills the fake with the production-shaped tree: 25 runs,
// each with two databases (one with a nested pgdata chain), a manifest and a
// files folder. It returns the run folder names in seed order.
func seedListingCorpus(b *testing.B, f *fakeDiskServer) []string {
	b.Helper()
	runs := make([]string, 0, 25)
	for run := range 25 {
		id := fmt.Sprintf("runs/2026-10-%02dT03-00-00Z", run+1)
		runs = append(runs, id)
		for _, key := range []string{
			id + "/manifest.json",
			id + "/databases/example/db-0/postgres.dump.zst",
			id + "/databases/example/db-0/pgdata/full/postgres.dump.zst",
			id + "/databases/example/db-1/postgres.dump.zst",
			id + "/files/archive.tar.zst",
		} {
			f.putObject(benchmarkRoot+"/"+key, []byte(key))
		}
	}
	return runs
}

// BenchmarkListing runs the whole survey listing for one corpus profile at
// call_parallelism 1 and 2. It reports the service requests the listing spent
// and the replays a failure cost; wall time and allocations come from the
// benchmark itself.
func BenchmarkListing(b *testing.B) {
	profiles := []struct {
		name    string
		latency func(string) time.Duration
		faults  bool
	}{
		{"balanced", balancedLatency, false},
		{"skew", skewedLatency, false},
		{"heavy", heavyTailLatency, false},
		{"tail-faults", heavyTailLatency, true},
	}
	for _, profile := range profiles {
		for _, parallelism := range []int{1, 2} {
			b.Run(fmt.Sprintf("%s/parallelism=%d", profile.name, parallelism), func(b *testing.B) {
				f := newFakeDisk(b)
				runs := seedListingCorpus(b, f)
				f.listLatency = profile.latency
				s := newStorageAt(b, baseConfig(b, f), parallelism)
				b.ReportAllocs()
				b.ResetTimer()
				requests, replays, objects := 0, 0, 0
				for range b.N {
					if profile.faults {
						f.failListOnce(benchmarkRoot+"/"+runs[1]+"/databases/example/db-1", 500, 0)
						f.failListOnce(benchmarkRoot+"/"+runs[0]+"/files", 429, 1)
					}
					before := f.Count("list")
					for {
						page, err := s.List(b.Context(), storage.ListRequest{})
						if err == nil {
							objects += len(page.Objects)
							break
						}
						replays++
						if replays > 4*b.N {
							b.Fatalf("listing did not recover: %v", err)
						}
					}
					requests += f.Count("list") - before
				}
				b.StopTimer()
				b.ReportMetric(float64(requests)/float64(b.N), "requests/op")
				b.ReportMetric(float64(replays)/float64(b.N), "replays/op")
				b.ReportMetric(float64(objects)/float64(b.N), "objects/op")
			})
		}
	}
}
