// Package k8s implements client behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClient performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewClient() (kubernetes.Interface, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// loadConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func loadConfig() (*rest.Config, error) {
	cfg, inClusterErr := rest.InClusterConfig()
	if inClusterErr == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, kubeconfigErr := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if kubeconfigErr != nil {
		return nil, fmt.Errorf("load k8s config: in-cluster config failed: %v; kubeconfig failed: %w", inClusterErr, kubeconfigErr)
	}
	return cfg, nil
}
