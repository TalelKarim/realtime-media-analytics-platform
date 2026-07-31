module "sqs" {
  source = "../../modules/sqs"

  project     = var.project
  environment = var.environment

  kms_key_arn = module.kms.sqs_key_arn

  # Existing queue:
  # Realtime Processor -> Broadcaster / future Coordinator
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400
  max_receive_count          = 3

  # Broadcasting V2:
  # Coordinator -> Workers
  broadcast_jobs_visibility_timeout_seconds = var.broadcast_jobs_visibility_timeout_seconds
  broadcast_jobs_message_retention_seconds  = 86400
  broadcast_jobs_max_receive_count          = var.broadcast_jobs_max_receive_count
}