# infra/terraform/ecr.tf
#
# This Terraform file creates the ECR repositories used by service images.
# It belongs to the IICPC AWS infrastructure layer and should remain
# aligned with infra/README.md and the Kubernetes manifests under k8s/.
# Keep explanatory comments at this file header so resource blocks stay declarative.

resource "aws_ecr_repository" "service" {
  for_each = toset(var.service_images)

  name                 = "iicpc/${each.value}"
  image_tag_mutability = var.ecr_image_tag_mutability
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "service" {
  for_each = aws_ecr_repository.service

  repository = each.value.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 14 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 14
        }
        action = { type = "expire" }
      }
    ]
  })
}
