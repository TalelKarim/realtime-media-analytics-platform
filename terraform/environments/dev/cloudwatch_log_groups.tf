module "cloudwatch_log_groups" {
  source = "../../modules/cloudwatch_log_groups"

  project     = var.project
  environment = var.environment

  kms_key_arn       = module.kms.logs_key_arn
  retention_in_days = 7

  log_group_names = [
    "/ecs/${var.project}-${var.environment}-collector",

    "/aws/lambda/${var.project}-${var.environment}-realtime-processor",
    "/aws/lambda/${var.project}-${var.environment}-alert-processor",
    "/aws/lambda/${var.project}-${var.environment}-broadcaster",

    "/aws/lambda/${var.project}-${var.environment}-websocket-connect",
    "/aws/lambda/${var.project}-${var.environment}-websocket-disconnect",
    "/aws/lambda/${var.project}-${var.environment}-websocket-default"
  ]

  tags = var.tags
}