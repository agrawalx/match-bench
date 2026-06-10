# ecr.tf — one repository per service image.
#
# Problem: the committed manifests reference ghcr.io/...:dev with
# imagePullPolicy that does not work on autoscaled EKS nodes (DEPLOYMENT §1.2).
# Decision: create exactly the 12 ECR repos the Makefile pushes to, named
# iicpc/<service>, with IMMUTABLE tags by default. Why immutable: a git-SHA tag
# must never move under a running contest, or two contestants are scored against
# different platform binaries.
#
# Same-account ECR needs no imagePullSecret: the node IAM role gets
# AmazonEC2ContainerRegistryReadOnly from the eks module's managed node groups,
# so kubelet pulls authenticate automatically (DEPLOYMENT §1.2).

resource "aws_ecr_repository" "service" {
  for_each = toset(var.service_images)

  name                 = "iicpc/${each.value}"
  image_tag_mutability = var.ecr_image_tag_mutability
  force_delete         = true # allow `terraform destroy` to remove repos with images

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = var.tags
}

# Expire untagged layers so failed/aborted pushes don't accrue cost. Tagged
# (git-SHA) images are retained — they are the audit trail of what ran.
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
