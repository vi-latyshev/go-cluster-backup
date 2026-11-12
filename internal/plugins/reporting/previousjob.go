package reporting

import (
	"context"
	"os"
	"sort"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// walk reads the cluster's half of the previous-run record. It follows the
// ownership chain the cluster records — the caller's pod to its Job, that Job
// to its CronJob — and reads two independent facts:
//
//   - the failed earlier attempts of the caller's OWN Job: its status.failed
//     count and its still-listed failed pods, each with the pod status's own
//     reason and its containers (see earlierAttempts);
//   - the newest OTHER Job of the same CronJob: its Failed condition, its
//     pods' container statuses, its own Warning events and how many Jobs of the
//     CronJob failed in a row before it (see previousFailure and jobsOf).
//
// An absent chain returns nil facts; a read it cannot make is returned as an
// error the caller surfaces and never acts on. The first error stops the walk,
// and every fact the reads before it established is returned beside the error:
// a failed pod listing still leaves the Job's failed count, a failed previous
// pod listing still leaves the Failed condition and its Consecutive count, and
// a failed event listing still leaves the failure it explains. The caller marks
// the record Unavailable and repeats the walk at finish, so a transient error
// never silently degrades the record to "nothing happened".
//
// The caller's identity comes from the configured environment variables: an
// incomplete identity is not a pod, so there is nothing to inspect — the
// ordinary case outside a Kubernetes CronJob.
func (r *Reporter) walk(ctx context.Context) (attempts *model.AttemptFailure, failure *model.PreviousFailure, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	namespace, pod := r.identity()
	if namespace == "" || pod == "" {
		return nil, nil, nil
	}
	client, err := r.clientFor()
	if err != nil {
		return nil, nil, err
	}
	own, err := client.GetPod(ctx, namespace, pod)
	if err != nil {
		return nil, nil, err
	}
	jobName := ownerName(own.Metadata.OwnerReferences, "Job")
	if jobName == "" {
		return nil, nil, nil
	}
	job, err := client.GetJob(ctx, namespace, jobName)
	if err != nil {
		return nil, nil, err
	}
	// The own Job's attempts are read before the CronJob is even looked for: a
	// Job created by hand from a CronJob template still holds them, and a
	// retried night must not lose why its first attempt died. A failed pod
	// listing keeps the count the Job itself carries, but proves nothing about
	// which attempts left a pod: Deleted needs the complete listing.
	pods, err := client.ListPodsIn(ctx, namespace, kubeapi.ListRequest{
		Selector: "job-name=" + jobName, PageSize: previousJobPageSize,
	})
	if err != nil {
		return knownAttempts(job), nil, err
	}
	attempts = earlierAttempts(job, pods, pod)
	cronName := ownerName(job.Metadata.OwnerReferences, "CronJob")
	if cronName == "" {
		return attempts, nil, nil
	}
	jobs, err := client.ListJobs(ctx, namespace, kubeapi.ListRequest{PageSize: previousJobPageSize})
	if err != nil {
		return attempts, nil, err
	}
	previous := jobsOf(jobs, cronName, jobName)
	if len(previous) == 0 || !jobFailed(previous[0]) {
		return attempts, nil, nil
	}
	// The Failed condition and the consecutive count are facts of the listing
	// already read: they stand even when the pods cannot be listed.
	prev := previous[0]
	failure = previousFailure(prev, nil)
	// Consecutive counts the leading failed Jobs of the same CronJob, newest
	// first, from the one listing already read: as far as failedJobsHistoryLimit
	// keeps them, which is the only history the cluster holds.
	failure.Consecutive = consecutiveFailures(previous)
	// The previous Job's own pods explain it per container; the Job's Warning
	// events are the only record when no pod was ever created (a Pod Security
	// admission refusal leaves nothing else), so both are read.
	prevPods, err := client.ListPodsIn(ctx, namespace, kubeapi.ListRequest{
		Selector: "job-name=" + prev.Metadata.Name, PageSize: previousJobPageSize,
	})
	if err != nil {
		return attempts, failure, err
	}
	failure.Containers = previousFailure(prev, prevPods).Containers
	failure.Events, err = jobWarningEvents(ctx, client, namespace, prev.Metadata.UID)
	if err != nil {
		return attempts, failure, err
	}
	return attempts, failure, nil
}

// consecutiveFailures counts the leading failed Jobs of one CronJob's listing,
// newest first.
func consecutiveFailures(jobs []kubeapi.Job) int {
	count := 0
	for _, j := range jobs {
		if !jobFailed(j) {
			break
		}
		count++
	}
	return count
}

// identity reads this process's pod identity from the configured environment
// variables. Both are set by the downward API together; an absent or partial
// pair means the process is not a pod.
func (r *Reporter) identity() (namespace, pod string) {
	return os.Getenv(r.cfg.NamespaceEnv), os.Getenv(r.cfg.PodEnv)
}

// clientFor builds the API client on first use.
func (r *Reporter) clientFor() (*kubeapi.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	client, err := kubeapi.New(kubeapi.Options{
		HostEnv:   r.cfg.APIHostEnv,
		PortEnv:   r.cfg.APIPortEnv,
		TokenFile: r.cfg.TokenFile,
		CAFile:    r.cfg.CAFile,
		Timeout:   r.cfg.RequestTimeout.Std(),
	})
	if err != nil {
		return nil, err
	}
	r.client = client
	return client, nil
}

// previousJobPageSize bounds one listing page of the Job/pod walk. It is a fixed
// internal page size, not a deployment knob: the walk reads a handful of objects.
const previousJobPageSize = 100

// previousJobEventLimit caps how many Warning events are kept, newest first: an
// operator reads the first few, and a controller retrying a doomed Job can emit
// hundreds.
const previousJobEventLimit = 5

// jobWarningEvents reads the Warning events recorded against one Job, newest
// first and capped. The API server filters by involvedObject, so the listing
// carries the Job's own events alone. A failed read is returned as an error
// beside whatever the walk already established: the failure the events explain
// still stands, and the caller repeats the walk at finish rather than letting
// the events silently read as "none".
func jobWarningEvents(ctx context.Context, client *kubeapi.Client, namespace, jobUID string) ([]model.EventFailure, error) {
	if jobUID == "" {
		return nil, nil
	}
	events, err := client.ListEvents(ctx, namespace, kubeapi.ListRequest{
		FieldSelector: "involvedObject.uid=" + jobUID + ",involvedObject.kind=Job",
		PageSize:      previousJobPageSize,
	})
	if err != nil {
		return nil, err
	}
	warnings := make([]kubeapi.Event, 0, len(events))
	for _, e := range events {
		if e.Type == "Warning" {
			warnings = append(warnings, e)
		}
	}
	sort.SliceStable(warnings, func(i, j int) bool {
		return eventTime(warnings[i]).After(eventTime(warnings[j]))
	})
	if len(warnings) > previousJobEventLimit {
		warnings = warnings[:previousJobEventLimit]
	}
	out := make([]model.EventFailure, 0, len(warnings))
	for _, e := range warnings {
		out = append(out, model.EventFailure{Reason: e.Reason, Message: e.Message})
	}
	return out, nil
}

// eventTime is when an Event was last seen: its own eventTime, then its
// deprecated lastTimestamp, then the object's creation time. A series writer sets
// one of the first two; an event read back from an old watch may carry neither.
func eventTime(e kubeapi.Event) time.Time {
	for _, s := range []string{e.EventTime, e.LastTimestamp, e.Metadata.CreationTimestamp} {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// ownerName returns the name of the first owner reference of the given kind.
func ownerName(refs []kubeapi.OwnerReference, kind string) string {
	for _, ref := range refs {
		if ref.Kind == kind {
			return ref.Name
		}
	}
	return ""
}

// jobsOf returns the Jobs of cronName that are not exclude, newest first by
// creation timestamp. A Job whose creation time cannot be read cannot be
// ordered and is skipped, as the newest-only walk did.
func jobsOf(jobs []kubeapi.Job, cronName, exclude string) []kubeapi.Job {
	type datedJob struct {
		job kubeapi.Job
		at  time.Time
	}
	var dated []datedJob
	for _, j := range jobs {
		if j.Metadata.Name == exclude || ownerName(j.Metadata.OwnerReferences, "CronJob") != cronName {
			continue
		}
		at, err := time.Parse(time.RFC3339, j.Metadata.CreationTimestamp)
		if err != nil {
			continue
		}
		dated = append(dated, datedJob{job: j, at: at})
	}
	sort.SliceStable(dated, func(i, j int) bool { return dated[i].at.After(dated[j].at) })
	out := make([]kubeapi.Job, 0, len(dated))
	for _, d := range dated {
		out = append(out, d.job)
	}
	return out
}

// knownAttempts is the attempt record a failed pod listing still supports: the
// Job's own failed count, without the deleted count only a complete listing can
// prove.
func knownAttempts(job kubeapi.Job) *model.AttemptFailure {
	if job.Status.Failed == 0 {
		return nil
	}
	return &model.AttemptFailure{Job: job.Metadata.Name, Failed: job.Status.Failed}
}

// earlierAttempts renders the failed earlier attempts of the run's own Job: one
// FailedPod per listed pod with phase Failed other than this run's own pod, each
// carrying the pod status's reason and message (a kubelet Evicted pod has no
// container status left) and its containers. Deleted is the part of the Job's
// own status.failed that no listed pod explains — an attempt whose pod a drain
// or a deletion removed — clamped at zero, because a podFailurePolicy Ignore
// rule may fail a pod without counting it. Nil when the Job counted no failure
// and listed none.
func earlierAttempts(job kubeapi.Job, pods []kubeapi.Pod, self string) *model.AttemptFailure {
	out := &model.AttemptFailure{Job: job.Metadata.Name, Failed: job.Status.Failed}
	for _, pod := range pods {
		if pod.Metadata.Name == self || pod.Status.Phase != "Failed" {
			continue
		}
		if ownerName(pod.Metadata.OwnerReferences, "Job") != job.Metadata.Name {
			continue
		}
		containers := containerFailures(pod, pod.Status.InitContainerStatuses, true)
		containers = append(containers, containerFailures(pod, pod.Status.ContainerStatuses, false)...)
		out.Pods = append(out.Pods, model.FailedPod{
			Name: pod.Metadata.Name, Reason: pod.Status.Reason, Message: pod.Status.Message,
			Containers: containers,
		})
	}
	out.Deleted = max(0, out.Failed-len(out.Pods))
	if out.Failed == 0 && len(out.Pods) == 0 {
		return nil
	}
	return out
}

// jobFailed reports whether the Job carries a Failed condition that is true.
func jobFailed(job kubeapi.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == "Failed" && cond.Status == "True" {
			return true
		}
	}
	return false
}

// previousFailure renders the readable failure of one failed Job: its condition,
// and every pod container's terminal or waiting reason.
func previousFailure(job kubeapi.Job, pods []kubeapi.Pod) *model.PreviousFailure {
	out := &model.PreviousFailure{Job: job.Metadata.Name}
	for _, cond := range job.Status.Conditions {
		if cond.Type == "Failed" && cond.Status == "True" {
			out.Reason, out.Message = cond.Reason, cond.Message
		}
	}
	for _, pod := range pods {
		if ownerName(pod.Metadata.OwnerReferences, "Job") != job.Metadata.Name {
			continue
		}
		out.Containers = append(out.Containers, containerFailures(pod, pod.Status.InitContainerStatuses, true)...)
		out.Containers = append(out.Containers, containerFailures(pod, pod.Status.ContainerStatuses, false)...)
	}
	return out
}

// containerFailures renders the containers of one pod that explain its failure:
// one that ran and stopped with a failure, or one that never started. A
// container that completed cleanly is not an explanation and is left out.
func containerFailures(pod kubeapi.Pod, statuses []kubeapi.ContainerStatus, init bool) []model.ContainerFailure {
	var out []model.ContainerFailure
	for _, cs := range statuses {
		switch {
		case cs.State.Terminated != nil:
			t := cs.State.Terminated
			if t.ExitCode == 0 && t.Reason == "Completed" {
				continue
			}
			out = append(out, model.ContainerFailure{Pod: pod.Metadata.Name, Container: cs.Name, Init: init,
				Terminated: &model.ContainerTermination{Reason: t.Reason, ExitCode: t.ExitCode, Message: t.Message}})
		case cs.State.Waiting != nil:
			out = append(out, model.ContainerFailure{Pod: pod.Metadata.Name, Container: cs.Name, Init: init,
				Waiting: cs.State.Waiting.Reason})
		}
	}
	return out
}
