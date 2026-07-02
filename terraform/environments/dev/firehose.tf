module "firehose_wikimedia_bronze" {
  source = "../../modules/firehose_kinesis_to_s3"

  name = "${local.name_prefix}-wikimedia-bronze"

  kinesis_stream_arn = module.kinesis.stream_arn

  s3_bucket_arn  = module.s3_datalake.bucket_arn
  s3_kms_key_arn = module.kms.s3_key_arn

  firehose_role_arn = module.iam.firehose_role_arn

  buffering_size_mb          = 64
  buffering_interval_seconds = 300
  log_retention_days         = 14

  tags = local.tags
}