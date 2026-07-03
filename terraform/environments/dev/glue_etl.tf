module "glue_etl" {
  source = "./modules/glue_etl"

  name_prefix          = local.name_prefix
  datalake_bucket_name = module.data_lake.bucket_name
  datalake_bucket_arn  = module.data_lake.bucket_arn
  s3_kms_key_arn       = module.kms.s3_kms_key_arn

  enable_bronze_to_silver_schedule = true
  enable_silver_to_gold_schedule   = false

  tags = local.tags
}