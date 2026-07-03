data "aws_caller_identity" "current" {}

module "quicksight" {
  source = "./modules/quicksight"

  name_prefix    = local.name_prefix
  aws_account_id = data.aws_caller_identity.current.account_id

  quicksight_principal_arn = var.quicksight_principal_arn

  glue_database_name   = module.glue_catalog.database_name
  datalake_bucket_name = module.data_lake.bucket_name
  s3_kms_key_arn       = module.kms.s3_kms_key_arn

  tags = local.tags
}