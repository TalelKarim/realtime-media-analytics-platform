module "glue_catalog" {
  source = "../../modules/glue_catalog"

  database_name        = replace("${local.name_prefix}", "-", "_")
  datalake_bucket_name = module.s3_datalake.bucket_name

  tags = local.tags
}