package reporting_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
)

const (
	testHostEnv      = "GCB_TEST_HOST"
	testPortEnv      = "GCB_TEST_PORT"
	testNamespaceEnv = "GCB_TEST_NAMESPACE"
	testPodEnv       = "GCB_TEST_POD"

	testNamespace = "infrastructure"
)

// newReporterIn points a reporter at the fake API server, with this process's
// own identity in the two configured environment variables.
func newReporterIn(t *testing.T, srv *kubeapitest.Server) *reporting.Reporter {
	t.Helper()
	return newReporterWith(t, srv, nil)
}

// newReporterWith is newReporterIn with one config override, for the tests that
// budget the walk.
func newReporterWith(t *testing.T, srv *kubeapitest.Server, mutate func(*reporting.Config)) *reporting.Reporter {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	t.Setenv(testNamespaceEnv, testNamespace)
	t.Setenv(testPodEnv, "backup-now")
	cfg := reporting.Config{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: model.Duration(10 * time.Second),
		WalkTimeout:    model.Duration(10 * time.Second),
		NamespaceEnv:   testNamespaceEnv, PodEnv: testPodEnv,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	rep, err := reporting.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// ownJobFixture arranges the caller's own pod and Job; the Job's status carries
// the failed-pod count an earlier attempt left.
func ownJobFixture(t *testing.T, srv *kubeapitest.Server, jobStatus string) {
	t.Helper()
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Running"}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}` + jobStatus + `}`)
}

// previousJobFixtures arranges the ownership chain of one run in the fake API:
// the caller's own pod and Job, a newer failed Job of the same CronJob, an older
// successful one, and the failed Job's pod.
func previousJobFixtures(t *testing.T, srv *kubeapitest.Server) {
	t.Helper()
	ownJobFixture(t, srv, "")
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","uid":"job-uid-1","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-old-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-13T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + testNamespace + `",
		"labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},"status":{"phase":"Failed",
		"initContainerStatuses":[{"name":"init","state":{"terminated":{"exitCode":1,"reason":"Error","message":"invalid config"}}}],
		"containerStatuses":[{"name":"backup","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}`)
}

// TestPreviousJobReportsTheFailedJob: the walk finds the newest other Job of the
// caller's CronJob, reads its Failed condition, and reports every container that
// explains it — a terminated init container and a waiting main container.
func TestPreviousJobReportsTheFailedJob(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rep := newReporterIn(t, srv)
	job := rep.PreviousJob(context.Background())
	if job.Unavailable != "" {
		t.Fatalf("a readable chain reported unavailable: %q", job.Unavailable)
	}
	failure := job.Failure
	if failure == nil {
		t.Fatal("the failed previous Job was not reported")
	}
	if again := rep.PreviousJob(context.Background()); again.Failure == nil || again.Failure.Job != failure.Job {
		t.Fatalf("the second read of the same reporter = %+v, want the same Job", again)
	}
	if failure.Job != "backup-prev-job" || failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was not read: %+v", failure)
	}
	if failure.Consecutive != 1 {
		t.Fatalf("Consecutive = %d, want 1: only the newest Job failed", failure.Consecutive)
	}
	if len(failure.Containers) != 2 {
		t.Fatalf("containers = %+v, want the init and the main container", failure.Containers)
	}
	init := failure.Containers[0]
	if !init.Init {
		t.Fatalf("the init container is not marked as init: %+v", init)
	}
	if init.Terminated == nil || init.Terminated.ExitCode != 1 || init.Terminated.Message != "invalid config" {
		t.Fatalf("the init container's termination was not read: %+v", init)
	}
	main := failure.Containers[1]
	if main.Init || main.Waiting != "ImagePullBackOff" {
		t.Fatalf("the main container's waiting reason was not read: %+v", main)
	}
}

// TestPreviousJobIsSilentWhenThePreviousJobSucceeded: a Complete previous Job is
// not a failure, and neither is a chain that does not lead to a CronJob.
func TestPreviousJobIsSilentWhenThePreviousJobSucceeded(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)
	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if job.Failure != nil {
		t.Fatalf("a successful previous Job was reported as a failure: %+v", job.Failure)
	}
}

// TestPreviousJobWithoutAnIdentityIsANoOp: outside a pod the configured
// environment variables are empty, and the cluster is never read.
func TestPreviousJobWithoutAnIdentityIsANoOp(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rep := newReporterIn(t, srv)

	t.Setenv(testPodEnv, "")
	if job := rep.PreviousJob(context.Background()); job != (model.PreviousRun{}) {
		t.Fatalf("a missing pod identity was not a no-op: %+v", job)
	}
	t.Setenv(testPodEnv, "backup-now")
	t.Setenv(testNamespaceEnv, "")
	if job := rep.PreviousJob(context.Background()); job != (model.PreviousRun{}) {
		t.Fatalf("a missing namespace identity was not a no-op: %+v", job)
	}
	if calls := srv.Calls("pods"); calls != 0 {
		t.Fatalf("the cluster was read without an identity: %d pod calls", calls)
	}
}

// TestPreviousJobSurfacesAMissingGrant: a denied listing is reported as
// Unavailable, never as a failure of the run.
func TestPreviousJobSurfacesAMissingGrant(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("jobs")
	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("a denied Job listing was not reported: %+v", job)
	}
}

// TestPreviousJobSurfacesAnUnusableClient: an API client that cannot be built —
// here its host environment variable is unset — is reported, never a silent
// no-op.
func TestPreviousJobSurfacesAnUnusableClient(t *testing.T) {
	t.Setenv(testNamespaceEnv, testNamespace)
	t.Setenv(testPodEnv, "backup-now")
	rep, err := reporting.New(context.Background(), reporting.Config{
		APIHostEnv: "GCB_TEST_UNSET_HOST", APIPortEnv: "GCB_TEST_UNSET_PORT",
		TokenFile: "/token", CAFile: "/ca",
		RequestTimeout: model.Duration(time.Second), WalkTimeout: model.Duration(time.Second),
		NamespaceEnv: testNamespaceEnv, PodEnv: testPodEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	if job := rep.PreviousJob(context.Background()); job.Unavailable == "" {
		t.Fatal("an unbuildable API client was not reported")
	}
}

// TestPreviousJobReportsAdmissionEvents: when the pod never gets created — a Pod
// Security admission refusal, so there is no Pod and no container status at
// all — the Job's own Warning events are the only record. Only Warning events
// are kept, newest first, and a Normal event alongside them is ignored.
func TestPreviousJobReportsAdmissionEvents(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	// The failed previous Job has NO pods: admission refused every attempt.
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","uid":"job-uid-1",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.1","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:20Z","uid":"ev-1"},
		"type":"Warning","reason":"FailedCreate","count":7,
		"message":"Error creating: pods \"backup-prev-job-x\" is forbidden: violates PodSecurity \"restricted:latest\": allowPrivilegeEscalation != false, unrestricted capabilities, runAsNonRoot != true, seccompProfile",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:20Z"}`)
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.2","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:10Z","uid":"ev-2"},
		"type":"Warning","reason":"FailedCreate","count":3,
		"message":"Error creating: pods \"backup-prev-job-y\" is forbidden: violates PodSecurity \"restricted:latest\": runAsNonRoot != true",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:10Z"}`)
	// A Normal event on the same Job is not a failure reason and is left out.
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.3","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:30Z","uid":"ev-3"},
		"type":"Normal","reason":"SuccessfulCreate","message":"Created pod: backup-prev-job-z",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:30Z"}`)
	// An event on a DIFFERENT Job must not leak in.
	srv.AddEvent(`{"metadata":{"name":"other.1","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:40Z","uid":"ev-4"},
		"type":"Warning","reason":"FailedCreate","message":"unrelated",
		"involvedObject":{"kind":"Job","name":"other-job","uid":"job-uid-2"},
		"lastTimestamp":"2026-10-14T03:00:40Z"}`)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	failure := job.Failure
	if failure == nil {
		t.Fatalf("the failed previous Job was not reported: %+v", job)
	}
	if len(failure.Containers) != 0 {
		t.Fatalf("containers = %+v, want none: no pod was created", failure.Containers)
	}
	if len(failure.Events) != 2 {
		t.Fatalf("events = %+v, want the two Warning events, newest first", failure.Events)
	}
	if failure.Events[0].Reason != "FailedCreate" || !strings.Contains(failure.Events[0].Message, "allowPrivilegeEscalation") {
		t.Fatalf("the newest Warning event is not first: %+v", failure.Events)
	}
	if !strings.Contains(failure.Events[1].Message, "runAsNonRoot") {
		t.Fatalf("the older Warning event is not second: %+v", failure.Events)
	}
}

// TestPreviousJobKeepsTheFailureWhenTheEventListFails: the events explain a
// failure the container statuses already describe, so a denied listing keeps the
// condition and the containers — and is still reported as unavailable, so the
// finish re-reads instead of leaving the events silently as "none".
func TestPreviousJobKeepsTheFailureWhenTheEventListFails(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("events")
	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("a denied event listing was not reported: %q", job.Unavailable)
	}
	if job.Failure == nil || job.Failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was lost with the events: %+v", job.Failure)
	}
	if len(job.Failure.Events) != 0 {
		t.Fatalf("events = %+v, want none", job.Failure.Events)
	}
	if len(job.Failure.Containers) != 2 {
		t.Fatalf("the container statuses were lost with the events: %+v", job.Failure.Containers)
	}
}

// TestPreviousJobStopsOnCancellation: a cancelled context is refused before any
// read and reported as the unavailable status.
func TestPreviousJobStopsOnCancellation(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rc := newReporterIn(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if job := rc.PreviousJob(ctx); !strings.Contains(job.Unavailable, "context canceled") {
		t.Fatalf("a cancelled context = %+v, want it reported", job)
	}
}

// TestPreviousJobStopsAtABrokenChain: a pod owned by something other than a Job,
// and a Job owned by something other than a CronJob, both mean there is no
// previous run of this CronJob to report.
func TestPreviousJobStopsAtABrokenChain(t *testing.T) {
	ctx := context.Background()

	noJob := kubeapitest.New(t)
	noJob.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"ReplicaSet","name":"backup-now-rs"}]}}`)
	if job := newReporterIn(t, noJob).PreviousJob(ctx); job.Failure != nil || job.Attempts != nil || job.Unavailable != "" {
		t.Fatalf("a pod without a Job owner = %+v, want nothing", job)
	}

	// A Job created by hand from a CronJob template: kubectl sets no CronJob
	// owner, so there is no previous Job of the CronJob — but the own Job's
	// attempts still count.
	noCron := kubeapitest.New(t)
	noCron.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Running"}}`)
	noCron.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `"},"status":{"failed":1}}`)
	noCron.AddPod(`{"metadata":{"name":"backup-now-failed","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":137,"reason":"OOMKilled"}}}]}}`)
	job := newReporterIn(t, noCron).PreviousJob(ctx)
	if job.Failure != nil || job.Unavailable != "" {
		t.Fatalf("a Job without a CronJob owner = %+v, want no previous Job", job)
	}
	if job.Attempts == nil || job.Attempts.Failed != 1 || len(job.Attempts.Pods) != 1 {
		t.Fatalf("the own Job's failed attempt was lost with the CronJob chain: %+v", job.Attempts)
	}
}

// TestPreviousJobSurfacesEachFailedRead: the own pod, the own Job's pod
// listing, the Job listing and the previous Job's pod listing are each reported
// when the API server refuses them, because a missing RBAC grant must be
// visible.
func TestPreviousJobSurfacesEachFailedRead(t *testing.T) {
	ctx := context.Background()

	noPod := kubeapitest.New(t)
	previousJobFixtures(t, noPod)
	noPod.Deny("pods/" + testNamespace + "/backup-now")
	if job := newReporterIn(t, noPod).PreviousJob(ctx); job.Unavailable == "" {
		t.Fatal("a denied own-pod read was not reported")
	}

	noOwnPods := kubeapitest.New(t)
	previousJobFixtures(t, noOwnPods)
	// The own-pod read is the first request of the pods key; the own Job's pod
	// listing is the second, and it is the one refused.
	noOwnPods.FailNth("pods", 500, 2)
	if job := newReporterIn(t, noOwnPods).PreviousJob(ctx); job.Unavailable == "" {
		t.Fatal("a failed own-Job pod listing was not reported")
	}

	noJobs := kubeapitest.New(t)
	previousJobFixtures(t, noJobs)
	// The own-Job read is the first request of the jobs key; the listing is the
	// second, and it is the one refused.
	noJobs.FailNth("jobs", 500, 2)
	if job := newReporterIn(t, noJobs).PreviousJob(ctx); job.Unavailable == "" {
		t.Fatal("a failed Job listing was not reported")
	}

	noPrevPods := kubeapitest.New(t)
	previousJobFixtures(t, noPrevPods)
	// The own-pod read, the own Job's listing and then the previous Job's
	// listing are the three requests of the pods key.
	noPrevPods.FailNth("pods", 500, 3)
	if job := newReporterIn(t, noPrevPods).PreviousJob(ctx); job.Unavailable == "" {
		t.Fatal("a failed pod listing was not reported")
	}
}

// TestPreviousJobKeepsTheNewestWarningsOnly: a controller retrying a doomed Job
// can emit hundreds of Warning events, so the report keeps the newest few — and
// an event that carries no readable timestamp at all is still counted and
// ordered last.
func TestPreviousJobKeepsTheNewestWarningsOnly(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","uid":"job-uid-9",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"FailedCreate","message":"refused"}]}}`)
	for i := 0; i < 7; i++ {
		srv.AddEvent(fmt.Sprintf(`{"metadata":{"name":"ev-%d","namespace":"%s","creationTimestamp":"2026-10-14T03:00:%02dZ","uid":"ev-%d"},
			"type":"Warning","reason":"FailedCreate","message":"attempt %d",
			"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-9"}}`, i, testNamespace, i, i, i))
	}
	// No eventTime, no lastTimestamp, no creationTimestamp: nothing to order it by.
	srv.AddEvent(`{"metadata":{"name":"ev-undated","namespace":"` + testNamespace + `","uid":"ev-undated"},
		"type":"Warning","reason":"FailedCreate","message":"undated",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-9"}}`)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if job.Failure == nil {
		t.Fatalf("PreviousJob = %+v, want the failed Job", job)
	}
	if len(job.Failure.Events) != 5 {
		t.Fatalf("events = %+v, want the newest five", job.Failure.Events)
	}
	for i, ev := range job.Failure.Events {
		want := fmt.Sprintf("attempt %d", 6-i)
		if ev.Message != want {
			t.Fatalf("events = %+v, want %q first", job.Failure.Events, want)
		}
	}
}

// TestPreviousJobKeepsTheFailureWhenTheEventListFailsWithAJobUID: the same rule
// with a UID set, so the event listing is attempted at all.
func TestPreviousJobKeepsTheFailureWhenTheEventListFailsWithAJobUID(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	// The previous Job carries a UID, so the event listing is attempted at all.
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","uid":"job-uid-1",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + testNamespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},
		"status":{"phase":"Failed","containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)
	srv.Deny("events")

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") || job.Failure == nil {
		t.Fatalf("a denied event listing = %+v, want the failure kept beside unavailable", job)
	}
	if len(job.Failure.Events) != 0 {
		t.Fatalf("events = %+v, want none", job.Failure.Events)
	}
	if job.Failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was lost with the events: %+v", job.Failure)
	}
	if len(job.Failure.Containers) != 1 {
		t.Fatalf("the container statuses were lost with the events: %+v", job.Failure.Containers)
	}
}

// TestPreviousJobSkipsWhatItCannotRead: a Job of the CronJob whose creation time
// is unreadable, a pod that only looks like the previous Job's, and a container
// that completed cleanly are each left out of the report.
func TestPreviousJobSkipsWhatItCannotRead(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	// A newer Job of the same CronJob with no readable creation time: it must not
	// be taken for the previous run.
	srv.AddJob(`{"metadata":{"name":"backup-broken-job","namespace":"` + testNamespace + `","creationTimestamp":"not-a-time",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"FailedCreate","message":"unreadable"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-13T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	// Labeled as the previous Job's pod, but owned by another Job.
	srv.AddPod(`{"metadata":{"name":"foreign-pod","namespace":"` + testNamespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"other-job"}]},
		"status":{"phase":"Failed","containerStatuses":[{"name":"foreign","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}`)
	// The previous Job's own pod: one container that completed cleanly, one that
	// did not.
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + testNamespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},
		"status":{"phase":"Failed","containerStatuses":[
			{"name":"clean","state":{"terminated":{"exitCode":0,"reason":"Completed"}}},
			{"name":"backup","state":{"terminated":{"exitCode":2,"reason":"Error","message":"boom"}}}]}}`)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	failure := job.Failure
	if failure == nil {
		t.Fatalf("PreviousJob = %+v, want the failed Job", job)
	}
	if failure.Job != "backup-prev-job" || failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the readable previous Job was not reported: %+v", failure)
	}
	if len(failure.Containers) != 1 {
		t.Fatalf("containers = %+v, want the one that failed", failure.Containers)
	}
	if got := failure.Containers[0]; got.Pod != "backup-prev-pod" || got.Container != "backup" || got.Terminated == nil || got.Terminated.ExitCode != 2 {
		t.Fatalf("container = %+v, want the failed container of the previous Job's pod", got)
	}
}

// TestPreviousJobReportsTheRetriedAttempt: the first attempt of the own Job died
// OOMKilled and the retry is running now. The previous Job succeeded, so the
// only cluster fact is the own Job's failed attempt — which no report could see
// before.
func TestPreviousJobReportsTheRetriedAttempt(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, `,"status":{"failed":1}`)
	srv.AddPod(`{"metadata":{"name":"backup-now-failed","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":137,"reason":"OOMKilled","message":"killed"}}}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if job.Failure != nil || job.Unavailable != "" {
		t.Fatalf("a failed attempt beside a successful previous Job = %+v", job)
	}
	got := job.Attempts
	if got == nil || got.Job != "backup-now-job" || got.Failed != 1 || got.Deleted != 0 {
		t.Fatalf("attempts = %+v, want one failed attempt", got)
	}
	if len(got.Pods) != 1 || got.Pods[0].Name != "backup-now-failed" || len(got.Pods[0].Containers) != 1 {
		t.Fatalf("pods = %+v, want the OOMKilled pod", got.Pods)
	}
	terminated := got.Pods[0].Containers[0].Terminated
	if terminated == nil || terminated.Reason != "OOMKilled" || terminated.ExitCode != 137 {
		t.Fatalf("the container's termination = %+v", terminated)
	}

	// A pod of the own Job that is still running, and a failed pod that only
	// carries the Job's label, are not failed attempts of this Job.
	srv.AddPod(`{"metadata":{"name":"backup-now-running","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Running"}}`)
	srv.AddPod(`{"metadata":{"name":"backup-foreign","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"other-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)
	if got := newReporterIn(t, srv).PreviousJob(context.Background()).Attempts; got == nil || len(got.Pods) != 1 {
		t.Fatalf("attempts = %+v, want only the own Job's failed pod", got)
	}
}

// TestPreviousJobCountsADrainedAttempt: the Job counted two failed attempts but
// a drain deleted one pod; the deleted count is what keeps the attempt visible.
func TestPreviousJobCountsADrainedAttempt(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, `,"status":{"failed":2}`)
	srv.AddPod(`{"metadata":{"name":"backup-now-failed","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)

	got := newReporterIn(t, srv).PreviousJob(context.Background()).Attempts
	if got == nil || got.Failed != 2 || len(got.Pods) != 1 || got.Deleted != 1 {
		t.Fatalf("attempts = %+v, want two failed with one pod and one deleted", got)
	}
}

// TestPreviousJobKeepsAnIgnoredPodWithoutANegativeDeleted: a podFailurePolicy
// Ignore rule fails a pod without counting it in status.failed, so the deleted
// count is clamped at zero and the pod's own reason is the record.
func TestPreviousJobKeepsAnIgnoredPodWithoutANegativeDeleted(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, "")
	srv.AddPod(`{"metadata":{"name":"backup-now-ignored","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)

	got := newReporterIn(t, srv).PreviousJob(context.Background()).Attempts
	if got == nil || got.Failed != 0 || got.Deleted != 0 || len(got.Pods) != 1 {
		t.Fatalf("attempts = %+v, want one listed pod and no deleted count", got)
	}
}

// TestPreviousJobReportsAnEvictedPodWithoutContainers: a kubelet eviction leaves
// the pod status's reason and message and no container status; the pod-level
// explanation is what a template renders.
func TestPreviousJobReportsAnEvictedPodWithoutContainers(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, `,"status":{"failed":1}`)
	srv.AddPod(`{"metadata":{"name":"backup-now-evicted","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},
		"status":{"phase":"Failed","reason":"Evicted","message":"The node was low on resource: memory."}}`)

	got := newReporterIn(t, srv).PreviousJob(context.Background()).Attempts
	if got == nil || len(got.Pods) != 1 {
		t.Fatalf("attempts = %+v, want the Evicted pod", got)
	}
	pod := got.Pods[0]
	if pod.Name != "backup-now-evicted" || pod.Reason != "Evicted" || pod.Message != "The node was low on resource: memory." || pod.Containers != nil {
		t.Fatalf("pod = %+v, want the Evicted pod's own reason and no containers", pod)
	}
}

// TestPreviousJobKeepsTheFailedCountWhenTheOwnPodListFails: status.failed was
// read with the Job, so a denied pod listing cannot erase the count — and the
// unavailable status sends the finish to re-read. The failed listing proves
// nothing about which attempts left a pod, so Deleted stays zero even though
// this run's two failed pods still exist.
func TestPreviousJobKeepsTheFailedCountWhenTheOwnPodListFails(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, `,"status":{"failed":2}`)
	for _, name := range []string{"backup-now-failed-a", "backup-now-failed-b"} {
		srv.AddPod(`{"metadata":{"name":"` + name + `","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
			"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
			"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":137,"reason":"OOMKilled"}}}]}}`)
	}
	// The own-pod read is the first request of the pods key; the own Job's pod
	// listing is the second, and it is the one refused.
	srv.FailNth("pods", 500, 2)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("unavailable = %q, want the read failure", job.Unavailable)
	}
	got := job.Attempts
	if got == nil || got.Failed != 2 || len(got.Pods) != 0 {
		t.Fatalf("attempts = %+v, want the count kept without its pods", got)
	}
	if got.Deleted != 0 {
		t.Fatalf("Deleted = %d, want 0: a failed listing cannot prove a pod was deleted", got.Deleted)
	}

	// With the same two pods listed, the complete listing proves both exist.
	if got := newReporterIn(t, srv).PreviousJob(context.Background()).Attempts; got == nil || got.Failed != 2 || got.Deleted != 0 || len(got.Pods) != 2 {
		t.Fatalf("attempts after a successful listing = %+v, want both pods and no deleted count", got)
	}
}

// TestPreviousJobKeepsThePreviousFailureWhenItsPodListFails: the Failed
// condition and the consecutive count are facts of the Job listing, so a denied
// pod listing keeps both and only the containers are lost.
func TestPreviousJobKeepsThePreviousFailureWhenItsPodListFails(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	// The own-pod read, the own Job's listing and then the previous Job's
	// listing are the three requests of the pods key.
	srv.FailNth("pods", 500, 3)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("unavailable = %q, want the read failure", job.Unavailable)
	}
	failure := job.Failure
	if failure == nil || failure.Job != "backup-prev-job" || failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Failed condition was lost with the pods: %+v", failure)
	}
	if failure.Consecutive != 1 {
		t.Fatalf("Consecutive = %d, want the listing's 1", failure.Consecutive)
	}
	if len(failure.Containers) != 0 {
		t.Fatalf("containers = %+v, want none without the pod listing", failure.Containers)
	}
}

// TestPreviousJobKeepsTheAttemptsWhenTheJobListingFails: what was read before
// the error is kept — an attempts list beside the unavailable status.
func TestPreviousJobKeepsTheAttemptsWhenTheJobListingFails(t *testing.T) {
	srv := kubeapitest.New(t)
	ownJobFixture(t, srv, `,"status":{"failed":1}`)
	srv.AddPod(`{"metadata":{"name":"backup-now-failed","namespace":"` + testNamespace + `","labels":{"job-name":"backup-now-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Failed",
		"containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":137,"reason":"OOMKilled"}}}]}}`)
	// The own-Job read is the first request of the jobs key; the listing is the
	// second, and it is the one refused.
	srv.FailNth("jobs", 500, 2)

	job := newReporterIn(t, srv).PreviousJob(context.Background())
	if !strings.HasPrefix(job.Unavailable, "previous run status unavailable: ") {
		t.Fatalf("unavailable = %q, want the read failure", job.Unavailable)
	}
	if job.Attempts == nil || job.Attempts.Failed != 1 || len(job.Attempts.Pods) != 1 {
		t.Fatalf("the attempts read before the error were lost: %+v", job.Attempts)
	}
}

// TestPreviousJobCountsConsecutiveFailures: Consecutive counts the leading
// failed Jobs of the own CronJob, newest first, as far as the listing keeps
// them — so a night that died before its start still says how many nights in a
// row were lost.
func TestPreviousJobCountsConsecutiveFailures(t *testing.T) {
	jobAt := func(name, at, status string) string {
		return `{"metadata":{"name":"` + name + `","namespace":"` + testNamespace + `","creationTimestamp":"` + at + `",
			"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":` + status + `}`
	}
	failed := `{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"limit"}]}`
	complete := `{"conditions":[{"type":"Complete","status":"True"}]}`

	cases := []struct {
		name string
		jobs []string
		want int
		job  string
	}{
		{
			name: "the newest two failed",
			jobs: []string{
				jobAt("backup-c", "2026-10-14T03:00:00Z", failed),
				jobAt("backup-b", "2026-10-13T03:00:00Z", failed),
				jobAt("backup-a", "2026-10-12T03:00:00Z", complete),
				jobAt("backup-z", "2026-10-11T03:00:00Z", failed),
			},
			want: 2, job: "backup-c",
		},
		{
			name: "every listed Job failed",
			jobs: []string{
				jobAt("backup-c", "2026-10-14T03:00:00Z", failed),
				jobAt("backup-b", "2026-10-13T03:00:00Z", failed),
				jobAt("backup-a", "2026-10-12T03:00:00Z", failed),
			},
			want: 3, job: "backup-c",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			ownJobFixture(t, srv, "")
			for _, raw := range tc.jobs {
				srv.AddJob(raw)
			}
			job := newReporterIn(t, srv).PreviousJob(context.Background())
			if job.Failure == nil {
				t.Fatalf("PreviousJob = %+v, want the failed previous Job", job)
			}
			if job.Failure.Job != tc.job || job.Failure.Consecutive != tc.want {
				t.Fatalf("failure = %+v, want %s with Consecutive %d", job.Failure, tc.job, tc.want)
			}
		})
	}
}

// TestPreviousJobStopsAtTheWalkBudget: a held Job listing cannot spend more than
// walk_timeout, however large request_timeout is — run_started waits for the
// walk and nothing else. The fake releases the held request when the client's
// context ends, so the test needs no real hold.
func TestPreviousJobStopsAtTheWalkBudget(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Hold = map[string]time.Duration{"jobs": 10 * time.Second}
	rep := newReporterWith(t, srv, func(c *reporting.Config) {
		c.WalkTimeout = model.Duration(200 * time.Millisecond)
	})

	start := time.Now()
	job := rep.PreviousJob(context.Background())
	took := time.Since(start)
	if !strings.Contains(job.Unavailable, "context deadline exceeded") {
		t.Fatalf("unavailable = %q, want the walk's deadline", job.Unavailable)
	}
	if took >= 2*time.Second {
		t.Fatalf("the walk took %s, want it bounded by walk_timeout", took)
	}
	// The attempt listing was read before the held Job listing: it is kept.
	if job.Attempts != nil {
		t.Fatalf("attempts = %+v, want none: the own Job has no failed pod", job.Attempts)
	}
}
