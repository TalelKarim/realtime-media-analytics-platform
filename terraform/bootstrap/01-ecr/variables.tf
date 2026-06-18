variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "repository_name" {
  description = "ECR repository name for the ECS collector image."
  type        = string
  default     = "realtime-media-analytics-dev-collector"
}

variable "image_tag_mutability" {
  description = "ECR image tag mutability. IMMUTABLE is safer when using versioned tags."
  type        = string
  default     = "IMMUTABLE"

  validation {
    condition     = contains(["MUTABLE", "IMMUTABLE"], var.image_tag_mutability)
    error_message = "image_tag_mutability must be MUTABLE or IMMUTABLE."
  }
}

variable "tags" {
  description = "Common bootstrap tags."
  type        = map(string)
  default = {
    Project     = "realtime-media-analytics"
    Environment = "dev"
    ManagedBy   = "terraform"
    Owner       = "talel-karim"
    Component   = "container-registry"
  }
}