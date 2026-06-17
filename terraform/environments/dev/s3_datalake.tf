module "s3_data_lake" {
  source = "../../modules/s3_data_lake"

  project     = var.project
  environment = var.environment

  kms_key_arn = module.kms.s3_key_arn

  force_destroy      = true
  versioning_enabled = true

  tags = var.tags
}