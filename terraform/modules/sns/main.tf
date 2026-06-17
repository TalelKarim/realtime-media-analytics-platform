locals {
  name_prefix = "${var.project}-${var.environment}"

  alerts_topic_name = "${local.name_prefix}-alerts"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "alerting"
      Service     = "sns"
    },
    var.tags
  )
}

resource "aws_sns_topic" "alerts" {
  name = local.alerts_topic_name

  kms_master_key_id = var.kms_key_id

  tags = merge(local.common_tags, {
    Name = local.alerts_topic_name
    Role = "alerts"
  })
}

resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.email_subscriptions)

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}