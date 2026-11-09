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
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	t.Setenv(testNamespaceEnv, testNamespace)
	t.Setenv(testPodEnv, "backup-now")
	rep, err := reporting.New(context.Background(), reporting.Config{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: model.Duration(10 * time.Second),
		NamespaceEnv:   testNamespaceEnv, PodEnv: testPodEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// previousJobFixtures arranges the ownership chain of one run in the fake API:
// the caller's own pod and Job, a newer failed Job of the same CronJob, an older
// successful one, and the failed Job's pod.
func previousJobFixtures(t *testing.T, srv *kubeapitest.Server) {
	t.Helper()
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Running"}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
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

// TestPreviousJobFailureReportsTheFailedJob: the walk finds the newest other Job
// of the caller's CronJob, reads its Failed condition, and reports every
// container that explains it — a terminated init container and a waiting main
// container.
func TestPreviousJobFailureReportsTheFailedJob(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rep := newReporterIn(t, srv)
	failure, err := rep.PreviousJobFailure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if failure == nil {
		t.Fatal("the failed previous Job was not reported")
	}
	if again, err := rep.PreviousJobFailure(context.Background()); err != nil || again == nil || again.Job != failure.Job {
		t.Fatalf("the second read of the same reporter = %+v, %v", again, err)
	}
	if failure.Job != "backup-prev-job" || failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was not read: %+v", failure)
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

// TestPreviousJobFailureIsSilentWhenThePreviousJobSucceeded: a Complete previous
// Job is not a failure, and neither is a chain that does not lead to a CronJob.
func TestPreviousJobFailureIsSilentWhenThePreviousJobSucceeded(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)
	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		t.Fatalf("a successful previous Job was reported as a failure: %+v", failure)
	}
}

// TestPreviousJobFailureWithoutAnIdentityIsANoOp: outside a pod the configured
// environment variables are empty, and the cluster is never read.
func TestPreviousJobFailureWithoutAnIdentityIsANoOp(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rep := newReporterIn(t, srv)

	t.Setenv(testPodEnv, "")
	if failure, err := rep.PreviousJobFailure(context.Background()); err != nil || failure != nil {
		t.Fatalf("a missing pod identity was not a no-op: %+v, %v", failure, err)
	}
	t.Setenv(testPodEnv, "backup-now")
	t.Setenv(testNamespaceEnv, "")
	if failure, err := rep.PreviousJobFailure(context.Background()); err != nil || failure != nil {
		t.Fatalf("a missing namespace identity was not a no-op: %+v, %v", failure, err)
	}
	if calls := srv.Calls("pods"); calls != 0 {
		t.Fatalf("the cluster was read without an identity: %d pod calls", calls)
	}
}

// TestPreviousJobFailureSurfacesAMissingGrant: a denied listing is returned as
// an error the caller surfaces, never as a failure of the run.
func TestPreviousJobFailureSurfacesAMissingGrant(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("jobs")
	if _, err := newReporterIn(t, srv).PreviousJobFailure(context.Background()); err == nil {
		t.Fatal("a denied Job listing was not reported")
	}
}

// TestPreviousJobFailureSurfacesAnUnusableClient: an API client that cannot be
// built — here its host environment variable is unset — is reported, never a
// silent no-op.
func TestPreviousJobFailureSurfacesAnUnusableClient(t *testing.T) {
	t.Setenv(testNamespaceEnv, testNamespace)
	t.Setenv(testPodEnv, "backup-now")
	rep, err := reporting.New(context.Background(), reporting.Config{
		APIHostEnv: "GCB_TEST_UNSET_HOST", APIPortEnv: "GCB_TEST_UNSET_PORT",
		TokenFile: "/token", CAFile: "/ca",
		RequestTimeout: model.Duration(time.Second),
		NamespaceEnv:   testNamespaceEnv, PodEnv: testPodEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rep.PreviousJobFailure(context.Background()); err == nil {
		t.Fatal("an unbuildable API client was not reported")
	}
}

// TestPreviousJobFailureReportsAdmissionEvents: when the pod never gets created —
// a Pod Security admission refusal, so there is no Pod and no container status at
// all — the Job's own Warning events are the only record. Only Warning events are
// kept, newest first, and a Normal event alongside them is ignored.
func TestPreviousJobFailureReportsAdmissionEvents(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
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

	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if failure == nil {
		t.Fatal("the failed previous Job was not reported")
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

// TestPreviousJobFailureSurvivesADeniedEventList: the events are the least
// certain part of the report, so a denied listing leaves the Job condition and
// the container statuses intact instead of losing the whole failure.
func TestPreviousJobFailureSurvivesADeniedEventList(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("events")
	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil {
		t.Fatalf("a denied event listing failed the read: %v", err)
	}
	if failure == nil || failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was lost with the events: %+v", failure)
	}
	if len(failure.Events) != 0 {
		t.Fatalf("events = %+v, want none", failure.Events)
	}
	if len(failure.Containers) != 2 {
		t.Fatalf("the container statuses were lost with the events: %+v", failure.Containers)
	}
}

// TestPreviousJobFailureStopsOnCancellation: a cancelled context is refused
// before any read.
func TestPreviousJobFailureStopsOnCancellation(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	rep := newReporterIn(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rep.PreviousJobFailure(ctx); err == nil {
		t.Fatal("a cancelled context was not refused")
	}
}

// TestPreviousJobFailureStopsAtABrokenChain: a pod owned by something other than
// a Job, and a Job owned by something other than a CronJob, both mean there is
// no previous run of this CronJob to report.
func TestPreviousJobFailureStopsAtABrokenChain(t *testing.T) {
	ctx := context.Background()

	noJob := kubeapitest.New(t)
	noJob.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"ReplicaSet","name":"backup-now-rs"}]}}`)
	if failure, err := newReporterIn(t, noJob).PreviousJobFailure(ctx); err != nil || failure != nil {
		t.Fatalf("a pod without a Job owner = %+v, %v", failure, err)
	}

	noCron := kubeapitest.New(t)
	noCron.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	noCron.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `"}}`)
	if failure, err := newReporterIn(t, noCron).PreviousJobFailure(ctx); err != nil || failure != nil {
		t.Fatalf("a Job without a CronJob owner = %+v, %v", failure, err)
	}
}

// TestPreviousJobFailureSurfacesEachFailedRead: the own pod, the Job listing and
// the pod listing of the previous Job are each reported when the API server
// refuses them, because a missing RBAC grant must be visible.
func TestPreviousJobFailureSurfacesEachFailedRead(t *testing.T) {
	ctx := context.Background()

	noPod := kubeapitest.New(t)
	previousJobFixtures(t, noPod)
	noPod.Deny("pods/" + testNamespace + "/backup-now")
	if _, err := newReporterIn(t, noPod).PreviousJobFailure(ctx); err == nil {
		t.Fatal("a denied own-pod read was not reported")
	}

	noJobs := kubeapitest.New(t)
	previousJobFixtures(t, noJobs)
	// The own-Job read is the first request of the jobs key; the listing is the
	// second, and it is the one refused.
	noJobs.FailNth("jobs", 500, 2)
	if _, err := newReporterIn(t, noJobs).PreviousJobFailure(ctx); err == nil {
		t.Fatal("a failed Job listing was not reported")
	}

	noPrevPods := kubeapitest.New(t)
	previousJobFixtures(t, noPrevPods)
	// The own-pod read is the first request of the pods key; the previous Job's
	// pod listing is the second.
	noPrevPods.FailNth("pods", 500, 2)
	if _, err := newReporterIn(t, noPrevPods).PreviousJobFailure(ctx); err == nil {
		t.Fatal("a failed pod listing was not reported")
	}
}

// TestPreviousJobFailureKeepsTheNewestWarningsOnly: a controller retrying a
// doomed Job can emit hundreds of Warning events, so the report keeps the newest
// few — and an event that carries no readable timestamp at all is still counted
// and ordered last.
func TestPreviousJobFailureKeepsTheNewestWarningsOnly(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
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

	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil || failure == nil {
		t.Fatalf("PreviousJobFailure = %+v, %v", failure, err)
	}
	if len(failure.Events) != 5 {
		t.Fatalf("events = %+v, want the newest five", failure.Events)
	}
	for i, ev := range failure.Events {
		want := fmt.Sprintf("attempt %d", 6-i)
		if ev.Message != want {
			t.Fatalf("events = %+v, want %q first", failure.Events, want)
		}
	}
}

// TestPreviousJobFailureSurvivesADeniedEventListWithAJobUID: the events are the
// least certain part of the report, so refusing that one listing keeps the Job
// condition and the container statuses intact.
func TestPreviousJobFailureSurvivesADeniedEventListWithAJobUID(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	// The previous Job carries a UID, so the event listing is attempted at all.
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + testNamespace + `","uid":"job-uid-1",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + testNamespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},
		"status":{"phase":"Failed","containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)
	srv.Deny("events")

	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil || failure == nil {
		t.Fatalf("a denied event listing failed the read: %+v, %v", failure, err)
	}
	if len(failure.Events) != 0 {
		t.Fatalf("events = %+v, want none", failure.Events)
	}
	if failure.Reason != "BackoffLimitExceeded" {
		t.Fatalf("the Job condition was lost with the events: %+v", failure)
	}
	if len(failure.Containers) != 1 {
		t.Fatalf("the container statuses were lost with the events: %+v", failure.Containers)
	}
}

// TestPreviousJobFailureSkipsWhatItCannotRead: a Job of the CronJob whose
// creation time is unreadable, a pod that only looks like the previous Job's,
// and a container that completed cleanly are each left out of the report.
func TestPreviousJobFailureSkipsWhatItCannotRead(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + testNamespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
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

	failure, err := newReporterIn(t, srv).PreviousJobFailure(context.Background())
	if err != nil || failure == nil {
		t.Fatalf("PreviousJobFailure = %+v, %v", failure, err)
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
