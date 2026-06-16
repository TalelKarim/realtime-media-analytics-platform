module "dynamodb" {
  source = "../../modules/dynamodb"

  project     = var.project
  environment = var.environment

  kms_key_arn = module.kms.dynamodb_key_arn

  point_in_time_recovery_enabled = false
  deletion_protection_enabled    = false
}