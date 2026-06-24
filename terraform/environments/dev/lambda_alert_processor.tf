# ============================================================
# Alert Processor Lambda
# ============================================================

module "lambda_alert_processor" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-alert-processor"
  description   = "Consumes Wikimedia events from Kinesis, writes alert_state counters, detects anomalies, and publishes SNS alerts."

  runtime = "python3.11"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/alert-processor"

  role_arn = module.iam.alert_processor_role_arn

  memory_size = 256
  timeout     = 60

  # The log group already exists and follows the Lambda naming convention:
  # /aws/lambda/realtime-media-analytics-dev-alert-processor
  create_log_group = false

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    # Required by the Python handler
    ALERT_STATE_TABLE_NAME = module.dynamodb.alert_state_table_name
    ALERT_TOPIC_ARN        = module.sns.alerts_topic_arn

    # Alert state retention
    # Contract: ttl = window_start + 35 minutes
    ALERT_TTL_MINUTES = "35"

    # Do not evaluate the still-open current minute.
    # Example: at 15:21:12, evaluate WINDOW#15:20:00Z.
    EVALUATION_DELAY_SECONDS = "10"

    # Baseline configuration
    BASELINE_WINDOW_MINUTES        = "30"
    MODERATION_WINDOW_MINUTES      = "5"
    MIN_BASELINE_POINTS            = var.environment == "dev" ? "8" : "10"
    MIN_MODERATION_BASELINE_POINTS = var.environment == "dev" ? "8" : "3"

    # Detection thresholds
    GLOBAL_Z_THRESHOLD               = "2.0"
    WIKI_Z_THRESHOLD                 = "2.0"
    MODERATION_BURST_RATIO_THRESHOLD = "3.0"

    # Minimum counts to avoid noisy alerts on tiny volumes.
    # Dev values are intentionally lower so you can observe behavior faster.
    GLOBAL_MIN_COUNT = var.environment == "dev" ? "40" : "50"
    WIKI_MIN_COUNT   = var.environment == "dev" ? "30" : "30"
    DELETE_MIN_COUNT = var.environment == "dev" ? "20" : "5"
    BLOCK_MIN_COUNT  = var.environment == "dev" ? "10" : "3"

    # Safety guard: do not evaluate unlimited wiki alert keys per invocation.
    MAX_WIKI_ALERT_KEYS_PER_INVOCATION = "20"
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