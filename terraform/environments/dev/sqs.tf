module "sqs" {
  source = "../../modules/sqs"

  project     = var.project
  environment = var.environment

  kms_key_arn = module.kms.sqs_key_arn

  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400
  max_receive_count          = 3
}