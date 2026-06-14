// Package k8s implements ecr aws behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// awsECRClient groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type awsECRClient struct {
	api *ecr.Client
}

// CreateRepository applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *awsECRClient) CreateRepository(ctx context.Context, repositoryName string) error {
	_, err := c.api.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: &repositoryName,
	})
	return err
}

// DockerConfigJSON returns a docker config.json (as a string) that authorizes push/pull
// against ECR, using a fresh GetAuthorizationToken from the pod's IRSA credentials. The
// token is valid ~12h; callers should refresh it per build. Kaniko, Trivy and Syft all
// read this format (via /kaniko/.docker/config.json or $DOCKER_CONFIG).
func (c *awsECRClient) DockerConfigJSON(ctx context.Context) (string, error) {
	out, err := c.api.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return "", fmt.Errorf("ecr get authorization token: %w", err)
	}
	if len(out.AuthorizationData) == 0 || out.AuthorizationData[0].AuthorizationToken == nil {
		return "", fmt.Errorf("ecr returned no authorization data")
	}
	ad := out.AuthorizationData[0]
	host := ""
	if ad.ProxyEndpoint != nil {
		host = strings.TrimPrefix(*ad.ProxyEndpoint, "https://")
	}
	cfg := map[string]any{
		"auths": map[string]any{
			host: map[string]string{"auth": *ad.AuthorizationToken},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal docker config: %w", err)
	}
	return string(b), nil
}

// newAWSECRClient performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newAWSECRClient(ctx context.Context) (ECRRepositoryClient, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config for ECR: %w", err)
	}
	return &awsECRClient{api: ecr.NewFromConfig(cfg)}, nil
}
