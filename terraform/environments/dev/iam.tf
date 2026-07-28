module "iam" {
  source = "../../modules/iam"

  project     = var.project
  environment = var.environment

  kinesis_key_arn  = module.kms.kinesis_key_arn
  s3_key_arn       = module.kms.s3_key_arn
  dynamodb_key_arn = module.kms.dynamodb_key_arn
  sqs_key_arn      = module.kms.sqs_key_arn
  logs_key_arn     = module.kms.logs_key_arn

  kinesis_stream_name = module.kinesis.stream_name

  realtime_aggregates_table_name     = module.dynamodb.realtime_aggregates_table_name
  websocket_connections_table_name   = module.dynamodb.websocket_connections_table_name
  websocket_subscriptions_table_name = module.dynamodb.websocket_subscriptions_table_name
  broadcast_snapshots_table_name     = module.dynamodb.broadcast_snapshots_table_name
  alert_state_table_name             = module.dynamodb.alert_state_table_name

  broadcast_queue_name      = module.sqs.broadcast_signal_queue_name
  broadcast_jobs_queue_name = module.sqs.broadcast_jobs_queue_name

  alerts_topic_name = module.sns.alerts_topic_name

  datalake_bucket_name = module.s3_datalake.bucket_name

  websocket_manage_connections_arn = module.apigw_websocket.manage_connections_arn

  tags = var.tags
}