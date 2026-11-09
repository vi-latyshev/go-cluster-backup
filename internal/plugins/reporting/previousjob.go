package reporting

import (
	"context"
	"os"
	"sort"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// PreviousJobFailure reads why the previous Job of this run's own CronJob failed
// before this tool could report. It walks the ownership chain the cluster
// records — the caller's pod to its Job, that Job to its CronJob, then the newest
// OTHER Job of the same CronJob — and reads that Job's Failed condition, its
// pods' container statuses and its own Warning events (the only record when no
// pod was ever created). An absent chain returns nil; a read it cannot make is
// returned as an error the caller surfaces and never acts on.
//
// The caller's identity comes from the configured environment variables: an
// incomplete identity is not a pod, so there is nothing to inspect — the
// ordinary case outside a Kubernetes CronJob.
func (r *Reporter) PreviousJobFailure(ctx context.Context) (*model.PreviousFailure, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespace, pod := r.identity()
	if namespace == "" || pod == "" {
		return nil, nil
	}
	client, err := r.clientFor()
	if err != nil {
		return nil, err
	}
	own, err := client.GetPod(ctx, namespace, pod)
	if err != nil {
		return nil, err
	}
	jobName := ownerName(own.Metadata.OwnerReferences, "Job")
	if jobName == "" {
		return nil, nil
	}
	job, err := client.GetJob(ctx, namespace, jobName)
	if err != nil {
		return nil, err
	}
	cronName := ownerName(job.Metadata.OwnerReferences, "CronJob")
	if cronName == "" {
		return nil, nil
	}
	jobs, err := client.ListJobs(ctx, namespace, kubeapi.ListRequest{PageSize: previousJobPageSize})
	if err != nil {
		return nil, err
	}
	previous := newestJobOf(jobs, cronName, jobName)
	if previous == nil || !jobFailed(*previous) {
		return nil, nil
	}
	pods, err := client.ListPodsIn(ctx, namespace, kubeapi.ListRequest{
		Selector: "job-name=" + previous.Metadata.Name, PageSize: previousJobPageSize,
	})
	if err != nil {
		return nil, err
	}
	failure := previousFailure(*previous, pods)
	// The Job's own Warning events, read whether or not any pod exists: a Pod
	// Security admission refusal (FailedCreate) creates no pod at all, so the
	// event is the only record of it. A read that fails is not fatal — the
	// container statuses already read still stand.
	failure.Events = jobWarningEvents(ctx, client, namespace, previous.Metadata.UID)
	return failure, nil
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
// carries the Job's own events alone. A read that fails returns nil: the events
// are the least certain part of this report and never worth losing the rest.
func jobWarningEvents(ctx context.Context, client *kubeapi.Client, namespace, jobUID string) []model.EventFailure {
	if jobUID == "" {
		return nil
	}
	events, err := client.ListEvents(ctx, namespace, kubeapi.ListRequest{
		FieldSelector: "involvedObject.uid=" + jobUID + ",involvedObject.kind=Job",
		PageSize:      previousJobPageSize,
	})
	if err != nil {
		return nil
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
	return out
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

// newestJobOf returns the newest Job of cronName that is not exclude, by creation
// timestamp, or nil when there is none.
func newestJobOf(jobs []kubeapi.Job, cronName, exclude string) *kubeapi.Job {
	var newest *kubeapi.Job
	var newestAt time.Time
	for i := range jobs {
		j := &jobs[i]
		if j.Metadata.Name == exclude || ownerName(j.Metadata.OwnerReferences, "CronJob") != cronName {
			continue
		}
		at, err := time.Parse(time.RFC3339, j.Metadata.CreationTimestamp)
		if err != nil {
			continue
		}
		if newest == nil || at.After(newestAt) {
			newest, newestAt = j, at
		}
	}
	return newest
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
