# ============================================================
# Alert Processor Lambda
# ============================================================

module "lambda_alert_processor" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-alert-processor"
  description   = "Consumes Wikimedia events from Kinesis and prepares realtime alert detection."

  runtime = "python3.11"
  handler = "handler.lambda_handler"

  # Use the same source_dir pattern as the realtime processor.
  # If your existing realtime processor uses another relative path,
  # copy that exact same pattern and replace the lambda name.
  source_dir = "${path.root}/../../../.build/lambdas/alert-processor"

  role_arn = module.iam.alert_processor_role_arn

  memory_size = 256
  timeout     = 30

  # The log group already exists and follows the Lambda naming convention:
  # /aws/lambda/realtime-media-analytics-dev-alert-processor
  create_log_group = false

  environment_variables = {
    ENVIRONMENT            = var.environment
    LOG_LEVEL              = "INFO"

    ALERT_STATE_TABLE_NAME = module.dynamodb.alert_state_table_name
    SNS_TOPIC_ARN          = module.sns.alerts_topic_arn

    ALERT_WINDOW_SECONDS   = "60"
    ALERT_TTL_DAYS         = "2"
  }

  tags = var.tags
}

# ============================================================
# Kinesis → Alert Processor Event Source Mapping
# ============================================================

resource "aws_lambda_event_source_mapping" "alert_processor_kinesis" {
  event_source_arn = module.kinesis.stream_arn
  function_name    = module.lambda_alert_processor.function_name

  starting_position = "LATEST"

  batch_size                         = 100
  maximum_batching_window_in_seconds = 5

  enabled = true

  depends_on = [
    module.iam,
    module.lambda_alert_processor
  ]
}