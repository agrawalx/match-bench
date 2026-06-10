package k8s

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// awsECRClient adapts the aws-sdk-go-v2 ECR client to ECRRepositoryClient.
// Credentials come from the default chain (IRSA in-cluster, env/profile in
// dev). SDK errors are returned unwrapped so ensureRepository can match
// RepositoryAlreadyExistsException via the smithy ErrorCode interface.
type awsECRClient struct {
	api *ecr.Client
}

func (c *awsECRClient) CreateRepository(ctx context.Context, repositoryName string) error {
	_, err := c.api.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: &repositoryName,
	})
	return err
}

// newAWSECRClient is the production newECRClient implementation.
func newAWSECRClient(ctx context.Context) (ECRRepositoryClient, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config for ECR: %w", err)
	}
	return &awsECRClient{api: ecr.NewFromConfig(cfg)}, nil
}
