package k8s

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClient builds a kubernetes.Interface using in-cluster config when running
// inside a pod, falling back to kubeconfig for local dev.
//
// This is the standard k8s config loader for every service in this repo.
// Copy it verbatim into any new service that needs the API — do not invent
// variations. Order matters: rest.InClusterConfig() succeeds only when the
// process is running inside a pod with a mounted ServiceAccount token, so
// the kubeconfig fallback only fires for local dev.
func NewClient() (kubernetes.Interface, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

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
