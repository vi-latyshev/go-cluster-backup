package kubeapi

import "github.com/vi-latyshev/go-cluster-backup/internal/loader"

// Spec is the compiled plugin identity of this library: it contributes no
// factory, so every Kubernetes plugin imports it directly.
var Spec = loader.Spec{
	ID:      "kubeapi",
	Summary: "the minimal in-cluster Kubernetes REST client the Kubernetes plugins share",
}
