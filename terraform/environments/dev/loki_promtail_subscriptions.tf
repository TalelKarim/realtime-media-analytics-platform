data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_lambda_function" "grafana_promtail" {
  function_name = "GrafanaCloudLambdaPromtail"
}

locals {
  promtail_forwarded_log_groups = {
    broadcaster = "/aws/lambda/${var.project}-${var.environment}-broadcaster"

    realtime_processor = "/aws/lambda/${var.project}-${var.environment}-realtime-processor"
  }
}

resource "aws_lambda_permission" "allow_cloudwatch_logs_to_invoke_promtail" {
  for_each = local.promtail_forwarded_log_groups

  statement_id  = "AllowCWLogsPromtail${replace(each.key, "_", "")}"
  action        = "lambda:InvokeFunction"
  function_name = data.aws_lambda_function.grafana_promtail.function_name
  principal     = "logs.${data.aws_region.current.name}.amazonaws.com"

  source_arn = "${module.cloudwatch_log_groups.log_group_arns[each.value]}:*"
}

resource "aws_cloudwatch_log_subscription_filter" "to_grafana_promtail" {
  for_each = local.promtail_forwarded_log_groups

  name            = "grafana-promtail-${replace(each.key, "_", "-")}"
  log_group_name  = module.cloudwatch_log_groups.log_group_names[each.value]
  filter_pattern  = ""
  destination_arn = data.aws_lambda_function.grafana_promtail.arn

  depends_on = [
    aws_lambda_permission.allow_cloudwatch_logs_to_invoke_promtail
  ]
}