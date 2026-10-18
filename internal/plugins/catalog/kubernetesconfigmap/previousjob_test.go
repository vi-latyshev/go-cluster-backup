package kubernetesconfigmap_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
)

// previousJobFixtures arranges the ownership chain of one run in the fake API:
// the caller's own pod and Job, a newer failed Job of the same CronJob, an older
// successful one, and the failed Job's pod.
func previousJobFixtures(t *testing.T, srv *kubeapitest.Server) {
	t.Helper()
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]},"status":{"phase":"Running"}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-old-job","namespace":"` + namespace + `","creationTimestamp":"2026-10-13T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + namespace + `",
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
	c := newCatalogIn(t, srv, 60)
	inspector, ok := c.(catalog.PreviousRunInspector)
	if !ok {
		t.Fatal("the catalog does not implement PreviousRunInspector")
	}
	failure, err := inspector.PreviousJobFailure(context.Background(), namespace, "backup-now")
	if err != nil {
		t.Fatal(err)
	}
	if failure == nil {
		t.Fatal("the failed previous Job was not reported")
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
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Complete","status":"True"}]}}`)
	c := newCatalogIn(t, srv, 60)
	inspector := c.(catalog.PreviousRunInspector)
	failure, err := inspector.PreviousJobFailure(context.Background(), namespace, "backup-now")
	if err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		t.Fatalf("a successful previous Job was reported as a failure: %+v", failure)
	}
	// No pod identity is not a failure either: outside a CronJob there is nothing
	// to inspect.
	if failure, err := inspector.PreviousJobFailure(context.Background(), namespace, ""); err != nil || failure != nil {
		t.Fatalf("an empty pod identity was not a no-op: %+v, %v", failure, err)
	}
}

// TestPreviousJobFailureSurfacesAMissingGrant: a denied listing is returned as
// an error the engine surfaces, never as a failure of the run.
func TestPreviousJobFailureSurfacesAMissingGrant(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	srv.Deny("jobs")
	c := newCatalogIn(t, srv, 60)
	inspector := c.(catalog.PreviousRunInspector)
	if _, err := inspector.PreviousJobFailure(context.Background(), namespace, "backup-now"); err == nil {
		t.Fatal("a denied Job listing was not reported")
	}
}

// TestPreviousJobFailureReportsAdmissionEvents: when the pod never gets created —
// a Pod Security admission refusal, so there is no Pod and no container status at
// all — the Job's own Warning events are the only record. Only Warning events are
// kept, newest first, and a Normal event alongside them is ignored.
func TestPreviousJobFailureReportsAdmissionEvents(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	// The failed previous Job has NO pods: admission refused every attempt.
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","uid":"job-uid-1",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.1","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:20Z","uid":"ev-1"},
		"type":"Warning","reason":"FailedCreate","count":7,
		"message":"Error creating: pods \"backup-prev-job-x\" is forbidden: violates PodSecurity \"restricted:latest\": allowPrivilegeEscalation != false, unrestricted capabilities, runAsNonRoot != true, seccompProfile",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:20Z"}`)
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.2","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:10Z","uid":"ev-2"},
		"type":"Warning","reason":"FailedCreate","count":3,
		"message":"Error creating: pods \"backup-prev-job-y\" is forbidden: violates PodSecurity \"restricted:latest\": runAsNonRoot != true",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:10Z"}`)
	// A Normal event on the same Job is not a failure reason and is left out.
	srv.AddEvent(`{"metadata":{"name":"backup-prev-job.3","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:30Z","uid":"ev-3"},
		"type":"Normal","reason":"SuccessfulCreate","message":"Created pod: backup-prev-job-z",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-1"},
		"lastTimestamp":"2026-10-14T03:00:30Z"}`)
	// An event on a DIFFERENT Job must not leak in.
	srv.AddEvent(`{"metadata":{"name":"other.1","namespace":"` + namespace + `","creationTimestamp":"2026-10-14T03:00:40Z","uid":"ev-4"},
		"type":"Warning","reason":"FailedCreate","message":"unrelated",
		"involvedObject":{"kind":"Job","name":"other-job","uid":"job-uid-2"},
		"lastTimestamp":"2026-10-14T03:00:40Z"}`)

	c := newCatalogIn(t, srv, 60)
	inspector := c.(catalog.PreviousRunInspector)
	failure, err := inspector.PreviousJobFailure(context.Background(), namespace, "backup-now")
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
	c := newCatalogIn(t, srv, 60)
	inspector := c.(catalog.PreviousRunInspector)
	failure, err := inspector.PreviousJobFailure(context.Background(), namespace, "backup-now")
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

// TestPreviousJobFailureReadsItsOwnNamespaceAndStopsOnCancellation: an empty
// namespace means the caller's configured one, and a cancelled context is
// refused before any read.
func TestPreviousJobFailureReadsItsOwnNamespaceAndStopsOnCancellation(t *testing.T) {
	srv := kubeapitest.New(t)
	previousJobFixtures(t, srv)
	inspector := newCatalogIn(t, srv, 60).(catalog.PreviousRunInspector)

	failure, err := inspector.PreviousJobFailure(context.Background(), "", "backup-now")
	if err != nil || failure == nil {
		t.Fatalf("PreviousJobFailure in the configured namespace = %+v, %v", failure, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := inspector.PreviousJobFailure(ctx, namespace, "backup-now"); err == nil {
		t.Fatal("a cancelled context was not refused")
	}
}

// TestPreviousJobFailureStopsAtABrokenChain: a pod owned by something other than
// a Job, and a Job owned by something other than a CronJob, both mean there is
// no previous run of this CronJob to report.
func TestPreviousJobFailureStopsAtABrokenChain(t *testing.T) {
	ctx := context.Background()

	noJob := kubeapitest.New(t)
	noJob.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"ReplicaSet","name":"backup-now-rs"}]}}`)
	if failure, err := newCatalogIn(t, noJob, 60).(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-now"); err != nil || failure != nil {
		t.Fatalf("a pod without a Job owner = %+v, %v", failure, err)
	}

	noCron := kubeapitest.New(t)
	noCron.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	noCron.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `"}}`)
	if failure, err := newCatalogIn(t, noCron, 60).(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-now"); err != nil || failure != nil {
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
	noPod.Deny("pods/" + namespace + "/backup-now")
	if _, err := newCatalogIn(t, noPod, 60).(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-now"); err == nil {
		t.Fatal("a denied own-pod read was not reported")
	}

	noJobs := kubeapitest.New(t)
	previousJobFixtures(t, noJobs)
	// The own-Job read is the first request of the jobs key; the listing is the
	// second, and it is the one refused.
	noJobs.FailNth("jobs", 500, 2)
	if _, err := newCatalogIn(t, noJobs, 60).(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-now"); err == nil {
		t.Fatal("a failed Job listing was not reported")
	}

	noPrevPods := kubeapitest.New(t)
	previousJobFixtures(t, noPrevPods)
	// The own-pod read is the first request of the pods key; the previous Job's
	// pod listing is the second.
	noPrevPods.FailNth("pods", 500, 2)
	if _, err := newCatalogIn(t, noPrevPods, 60).(catalog.PreviousRunInspector).PreviousJobFailure(ctx, namespace, "backup-now"); err == nil {
		t.Fatal("a failed pod listing was not reported")
	}
}

// TestPreviousJobFailureKeepsTheNewestWarningsOnly: a controller retrying a
// doomed Job can emit hundreds of Warning events, so the report keeps the newest
// few — and an event that carries no readable timestamp at all is still counted
// and ordered last.
func TestPreviousJobFailureKeepsTheNewestWarningsOnly(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","uid":"job-uid-9",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"FailedCreate","message":"refused"}]}}`)
	for i := 0; i < 7; i++ {
		srv.AddEvent(fmt.Sprintf(`{"metadata":{"name":"ev-%d","namespace":"%s","creationTimestamp":"2026-10-14T03:00:%02dZ","uid":"ev-%d"},
			"type":"Warning","reason":"FailedCreate","message":"attempt %d",
			"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-9"}}`, i, namespace, i, i, i))
	}
	// No eventTime, no lastTimestamp, no creationTimestamp: nothing to order it by.
	srv.AddEvent(`{"metadata":{"name":"ev-undated","namespace":"` + namespace + `","uid":"ev-undated"},
		"type":"Warning","reason":"FailedCreate","message":"undated",
		"involvedObject":{"kind":"Job","name":"backup-prev-job","uid":"job-uid-9"}}`)

	failure, err := newCatalogIn(t, srv, 60).(catalog.PreviousRunInspector).PreviousJobFailure(context.Background(), namespace, "backup-now")
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
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	// The previous Job carries a UID, so the event listing is attempted at all.
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","uid":"job-uid-1",
		"creationTimestamp":"2026-10-14T03:00:00Z","ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + namespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},
		"status":{"phase":"Failed","containerStatuses":[{"name":"backup","state":{"terminated":{"exitCode":1,"reason":"Error"}}}]}}`)
	srv.Deny("events")

	failure, err := newCatalogIn(t, srv, 60).(catalog.PreviousRunInspector).PreviousJobFailure(context.Background(), namespace, "backup-now")
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
	srv.AddPod(`{"metadata":{"name":"backup-now","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"Job","name":"backup-now-job"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-now-job","namespace":"` + namespace + `",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]}}`)
	// A newer Job of the same CronJob with no readable creation time: it must not
	// be taken for the previous run.
	srv.AddJob(`{"metadata":{"name":"backup-broken-job","namespace":"` + namespace + `","creationTimestamp":"not-a-time",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"FailedCreate","message":"unreadable"}]}}`)
	srv.AddJob(`{"metadata":{"name":"backup-prev-job","namespace":"` + namespace + `","creationTimestamp":"2026-10-13T03:00:00Z",
		"ownerReferences":[{"kind":"CronJob","name":"backup"}]},
		"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"Job has reached the specified backoff limit"}]}}`)
	// Labeled as the previous Job's pod, but owned by another Job.
	srv.AddPod(`{"metadata":{"name":"foreign-pod","namespace":"` + namespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"other-job"}]},
		"status":{"phase":"Failed","containerStatuses":[{"name":"foreign","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}`)
	// The previous Job's own pod: one container that completed cleanly, one that
	// did not.
	srv.AddPod(`{"metadata":{"name":"backup-prev-pod","namespace":"` + namespace + `","labels":{"job-name":"backup-prev-job"},
		"ownerReferences":[{"kind":"Job","name":"backup-prev-job"}]},
		"status":{"phase":"Failed","containerStatuses":[
			{"name":"clean","state":{"terminated":{"exitCode":0,"reason":"Completed"}}},
			{"name":"backup","state":{"terminated":{"exitCode":2,"reason":"Error","message":"boom"}}}]}}`)

	failure, err := newCatalogIn(t, srv, 60).(catalog.PreviousRunInspector).PreviousJobFailure(context.Background(), namespace, "backup-now")
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
