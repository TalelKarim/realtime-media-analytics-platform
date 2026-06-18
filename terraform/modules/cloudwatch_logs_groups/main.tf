locals {
  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "observability"
      Service     = "cloudwatch-logs"
    },
    var.tags
  )
}

resource "aws_cloudwatch_log_group" "this" {
  for_each = toset(var.log_group_names)

  name              = each.value
  retention_in_days = var.retention_in_days
  kms_key_id        = var.kms_key_arn

  tags = merge(local.common_tags, {
    Name = each.value
    Role = "application-logs"
  })
}