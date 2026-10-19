package kubeapi_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// TestListJobsAndGetJob: the Jobs of one namespace list with the label
// selector, one Job reads back by name with its condition, and a phase filter
// is refused because the API server registers no status.phase for Jobs.
func TestListJobsAndGetJob(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddJob(`{"metadata":{"name":"run-1","namespace":"ns-a","labels":{"type":"backup"},"ownerReferences":[{"kind":"CronJob","name":"backup"}]},"status":{"conditions":[{"type":"Failed","status":"True","reason":"DeadlineExceeded","message":"too slow"}]}}`)
	srv.AddJob(`{"metadata":{"name":"other","namespace":"ns-b"}}`)
	c := client(t, srv)
	ctx := context.Background()

	jobs, err := c.ListJobs(ctx, "ns-a", kubeapi.ListRequest{Selector: "type=backup", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Metadata.Name != "run-1" {
		t.Fatalf("jobs = %+v", jobs)
	}
	if len(jobs[0].Metadata.OwnerReferences) != 1 || jobs[0].Metadata.OwnerReferences[0].Kind != "CronJob" {
		t.Fatalf("ownerReferences = %+v", jobs[0].Metadata.OwnerReferences)
	}
	if len(jobs[0].Status.Conditions) != 1 || jobs[0].Status.Conditions[0].Reason != "DeadlineExceeded" {
		t.Fatalf("conditions = %+v", jobs[0].Status.Conditions)
	}

	j, err := c.GetJob(ctx, "ns-a", "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Metadata.Name != "run-1" || j.Metadata.Namespace != "ns-a" {
		t.Fatalf("job = %+v", j.Metadata)
	}

	if _, err := c.GetJob(ctx, "ns-a", "missing"); err == nil {
		t.Fatal("a missing job was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}

	if _, err := c.ListJobs(ctx, "ns-a", kubeapi.ListRequest{Phase: "Running", PageSize: 500}); err == nil {
		t.Fatal("a phase-filtered job listing was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestListPodsInNamespace: a namespaced pod listing honours the namespace, the
// label selector and the status.phase field selector together.
func TestListPodsInNamespace(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(podJSON("ns-a", "db-0", map[string]string{"type": "database"}, "Running"))
	srv.AddPod(podJSON("ns-a", "db-1", map[string]string{"type": "database"}, "Succeeded"))
	srv.AddPod(podJSON("ns-b", "db-2", map[string]string{"type": "database"}, "Running"))

	pods, err := client(t, srv).ListPodsIn(context.Background(), "ns-a", kubeapi.ListRequest{Selector: "type=database", Phase: "Running", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 1 || pods[0].Metadata.Namespace != "ns-a" || pods[0].Metadata.Name != "db-0" {
		t.Fatalf("pods = %+v", pods)
	}
}

// TestListEventsByInvolvedUID: an Event listing is filtered by the caller's own
// field selector — involvedObject.uid is how one Job's events are picked out —
// and a phase filter is refused because Events carry no status.phase.
func TestListEventsByInvolvedUID(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddEvent(`{"metadata":{"name":"e1","namespace":"ns-a","labels":{"type":"backup"}},"type":"Warning","reason":"FailedCreate","message":"denied","count":3,"involvedObject":{"kind":"Job","name":"run-1","uid":"uid-1"}}`)
	srv.AddEvent(`{"metadata":{"name":"e2","namespace":"ns-a","labels":{"type":"backup"}},"type":"Warning","reason":"FailedCreate","involvedObject":{"kind":"Job","name":"run-2","uid":"uid-2"}}`)
	c := client(t, srv)
	ctx := context.Background()

	events, err := c.ListEvents(ctx, "ns-a", kubeapi.ListRequest{Selector: "type=backup", FieldSelector: "involvedObject.uid=uid-1", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Metadata.Name != "e1" {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Reason != "FailedCreate" || events[0].InvolvedObject.UID != "uid-1" || events[0].Count != 3 {
		t.Fatalf("event = %+v", events[0])
	}

	if _, err := c.ListEvents(ctx, "ns-a", kubeapi.ListRequest{Phase: "Running", PageSize: 500}); err == nil {
		t.Fatal("a phase-filtered event listing was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestGetPodAndConfigMapErrors: a pod reads back by name, and both a missing pod
// and a missing ConfigMap are classified not_found.
func TestGetPodAndConfigMapErrors(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(podJSON("ns-a", "db-0", nil, "Running"))
	c := client(t, srv)
	ctx := context.Background()

	p, err := c.GetPod(ctx, "ns-a", "db-0")
	if err != nil {
		t.Fatal(err)
	}
	if p.Metadata.Name != "db-0" || p.Status.Phase != "Running" {
		t.Fatalf("pod = %+v", p)
	}
	if _, err := c.GetPod(ctx, "ns-a", "missing"); err == nil {
		t.Fatal("a missing pod was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
	if _, err := c.GetConfigMap(ctx, "ns-a", "missing"); err == nil {
		t.Fatal("a missing configmap was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
}

// TestListPVsRefusesPhase: a volume listing carries every phase; a request that
// asks for one is refused rather than answered.
func TestListPVsRefusesPhase(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPV(`{"metadata":{"name":"pv-1"},"spec":{"local":{"path":"/data"}}}`)
	_, err := client(t, srv).ListPVs(context.Background(), kubeapi.ListRequest{Phase: "Bound", PageSize: 500})
	if err == nil {
		t.Fatal("a phase-filtered volume listing was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestSecretValueRejectsInvalidBase64: a stored value that is not base64 is an
// error naming the secret and the key, never a silently dropped value.
func TestSecretValueRejectsInvalidBase64(t *testing.T) {
	s := kubeapi.Secret{
		Metadata: kubeapi.ObjectMeta{Namespace: "ns-a", Name: "pw"},
		Data:     map[string]string{"PASSWORD": "not base64!!"},
	}
	value, found, err := s.Value("PASSWORD")
	if err == nil {
		t.Fatal("an invalid base64 value was accepted")
	}
	if !found || value != "" {
		t.Fatalf("Value = %q, %v", value, found)
	}
	if !strings.Contains(err.Error(), "is not valid base64") || !strings.Contains(err.Error(), "ns-a/pw") {
		t.Fatalf("error = %v", err)
	}
}
