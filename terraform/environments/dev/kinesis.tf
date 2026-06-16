module "kinesis" {
  source = "../../modules/kinesis"

  project     = var.project
  environment = var.environment

  kms_key_arn = module.kms.kinesis_key_arn


  shard_count            = var.kinesis_shard_count
  retention_period_hours = var.kinesis_retention_period_hours
  shard_level_metrics    = var.kinesis_shard_level_metrics
}