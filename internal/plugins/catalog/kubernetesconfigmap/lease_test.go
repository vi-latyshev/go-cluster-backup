package kubernetesconfigmap_test

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// emptyVersionCatalog points the plugin at an API server that answers a
// ConfigMap read with an object carrying no resourceVersion — the one shape the
// real API server never produces, and the reason an empty version reads as "not
// there".
func emptyVersionCatalog(t *testing.T) catalog.Catalog {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"metadata":{"name":"`+name+`","namespace":"`+namespace+`"},"data":{}}`); err != nil {
			t.Errorf("stub: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, ca, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	const hostEnv, portEnv = "GCB_STUB_HOST", "GCB_STUB_PORT"
	t.Setenv(hostEnv, u.Hostname())
	t.Setenv(portEnv, u.Port())
	return buildCatalog(t, leaseConfig(hostEnv, portEnv, tokenPath, caPath))
}

// TestAConfigMapWithoutAVersionIsNotOneToWriteTo: a lease is neither taken nor
// released against an object the API server answered without a version, because
// an unconditional patch is not a compare-and-swap.
func TestAConfigMapWithoutAVersionIsNotOneToWriteTo(t *testing.T) {
	c := emptyVersionCatalog(t)
	l := c.(catalog.Leaser)
	ctx := context.Background()

	_, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run", Kind: model.RunKindManual})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Acquire = %v, want the missing-ConfigMap refusal", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
	if err := l.Release(ctx, catalog.Lease{Token: "t"}); err != nil {
		t.Fatalf("Release = %v, want a no-op", err)
	}
}

// TestAcquireRefusesWhatItCannotClaim: an absent ConfigMap, a corrupt lease key,
// a failed patch and an unencodable quarantine are each reported, never turned
// into a claim.
func TestAcquireRefusesWhatItCannotClaim(t *testing.T) {
	ctx := context.Background()
	lease := catalog.LeaseRequest{Holder: "run", Kind: model.RunKindManual}

	absent := kubeapitest.New(t)
	l := newCatalogIn(t, absent, 60).(catalog.Leaser)
	_, err := l.Acquire(ctx, lease)
	if err == nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("Acquire without a ConfigMap = %v, want the object named", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}

	corrupt := kubeapitest.New(t)
	corrupt.AddConfigMap(namespace, name, map[string]string{key + ".lease": "not a lease"})
	if _, err := newCatalogIn(t, corrupt, 60).(catalog.Leaser).Acquire(ctx, lease); err == nil ||
		!strings.Contains(err.Error(), "not a lease document") {
		t.Fatalf("Acquire on a corrupt lease key = %v", err)
	}

	// The read succeeds and the patch loses: the claim was not taken.
	unpatched := kubeapitest.New(t)
	unpatched.AddConfigMap(namespace, name, map[string]string{})
	unpatched.FailNth("configmaps/"+namespace+"/"+name, 500, 2)
	if _, err := newCatalogIn(t, unpatched, 60).(catalog.Leaser).Acquire(ctx, lease); err == nil {
		t.Fatal("a failed lease patch was reported as a taken claim")
	}

	// A quarantine the JSON encoder cannot render is refused, not silently
	// dropped from the record.
	unencodable := kubeapitest.New(t)
	unencodable.AddConfigMap(namespace, name, map[string]string{})
	_, err = newCatalogIn(t, unencodable, 60).(catalog.Leaser).Acquire(ctx, catalog.LeaseRequest{
		Holder: "run", Kind: model.RunKindManual, QuarantineUntil: farFuture})
	if err == nil || !strings.Contains(err.Error(), "encode lease") {
		t.Fatalf("Acquire with an unencodable quarantine = %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Acquire(cancelled, lease); err == nil {
		t.Fatal("Acquire took a claim on a cancelled context")
	}
}

// TestAcquireInheritsTheRecordsQuarantine: a released record keeps its deadline,
// and the acquisition that takes it up again inherits it even when its own
// request asks for an earlier one.
func TestAcquireInheritsTheRecordsQuarantine(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	l := newCatalogIn(t, srv, 60).(catalog.Leaser)
	ctx := context.Background()
	late := base.Add(time.Hour)

	first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", QuarantineUntil: late})
	if err != nil {
		t.Fatal(err)
	}
	if !first.QuarantineUntil.Equal(late) {
		t.Fatalf("quarantine = %s, want %s", first.QuarantineUntil, late)
	}
	if err := l.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", QuarantineUntil: base})
	if err != nil {
		t.Fatal(err)
	}
	if !second.QuarantineUntil.Equal(late) {
		t.Fatalf("the released record's quarantine was not inherited: %s", second.QuarantineUntil)
	}
}

// TestReleaseRefusesWhatItCannotRead: a failed read is surfaced and a corrupt
// lease key is refused; an absent ConfigMap is a no-op, because there is no
// claim left to drop.
func TestReleaseRefusesWhatItCannotRead(t *testing.T) {
	ctx := context.Background()
	held := catalog.Lease{Holder: "run", Token: "t"}

	absent := kubeapitest.New(t)
	if err := newCatalogIn(t, absent, 60).(catalog.Leaser).Release(ctx, held); err != nil {
		t.Fatalf("Release without a ConfigMap = %v, want a no-op", err)
	}

	denied := kubeapitest.New(t)
	denied.AddConfigMap(namespace, name, map[string]string{})
	denied.Deny("configmaps/" + namespace + "/" + name)
	if err := newCatalogIn(t, denied, 60).(catalog.Leaser).Release(ctx, held); err == nil {
		t.Fatal("a denied read was reported as a released claim")
	}

	corrupt := kubeapitest.New(t)
	corrupt.AddConfigMap(namespace, name, map[string]string{key + ".lease": "not a lease"})
	if err := newCatalogIn(t, corrupt, 60).(catalog.Leaser).Release(ctx, held); err == nil ||
		!strings.Contains(err.Error(), "not a lease document") {
		t.Fatalf("Release on a corrupt lease key = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	fresh := kubeapitest.New(t)
	fresh.AddConfigMap(namespace, name, map[string]string{})
	if err := newCatalogIn(t, fresh, 60).(catalog.Leaser).Release(cancelled, held); err == nil {
		t.Fatal("Release ran on a cancelled context")
	}
}
