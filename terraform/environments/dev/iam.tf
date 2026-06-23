module "iam" {
  source = "../../modules/iam"

  project     = var.project
  environment = var.environment

  kinesis_key_arn  = module.kms.kinesis_key_arn
  s3_key_arn       = module.kms.s3_key_arn
  dynamodb_key_arn = module.kms.dynamodb_key_arn
  sqs_key_arn      = module.kms.sqs_key_arn
  logs_key_arn     = module.kms.logs_key_arn
}