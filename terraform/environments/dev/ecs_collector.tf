locals {
  collector_log_group_name = "/ecs/${var.project}-${var.environment}-collector"

  collector_repository_url = data.terraform_remote_state.bootstrap_ecr.outputs.collector_repository_url
}

module "ecs_collector" {
  source = "../../modules/ecs-collector"

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  repository_url = local.collector_repository_url
  image_tag      = var.collector_image_tag

  execution_role_arn = module.iam.ecs_collector_execution_role_arn
  task_role_arn      = module.iam.ecs_collector_task_role_arn

  subnet_ids        = module.networking.private_subnet_ids
  security_group_id = module.networking.ecs_collector_sg_id
  assign_public_ip  = false

  log_group_name      = local.collector_log_group_name
  kinesis_stream_name = module.kinesis.stream_name

  task_cpu    = var.collector_task_cpu
  task_memory = var.collector_task_memory

  desired_count = var.collector_desired_count

  batch_size             = var.collector_batch_size
  flush_interval_seconds = var.collector_flush_interval_seconds
  sample_rate            = var.collector_sample_rate
  log_level              = var.collector_log_level

  container_insights_enabled = false
  enable_execute_command     = false

  tags = var.tags

  depends_on = [
    module.cloudwatch_log_groups
  ]
}