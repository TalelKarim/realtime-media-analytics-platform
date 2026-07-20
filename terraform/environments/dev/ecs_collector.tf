locals {
  collector_log_group_name = "/ecs/${var.project}-${var.environment}-collector"

  collector_repository_url = data.terraform_remote_state.bootstrap_ecr.outputs.collector_repository_url

  # The existing Python SDK header is formatted as:
  #   Authorization=Basic%20<base64>
  # Alloy needs the actual HTTP header value:
  #   Basic <base64>
  collector_grafana_otlp_authorization = replace(
    replace(var.grafana_otlp_headers, "Authorization=", ""),
    "Basic%20",
    "Basic "
  )
}

resource "aws_secretsmanager_secret" "collector_grafana_otlp_authorization" {
  name                    = "${var.project}/${var.environment}/collector/grafana-otlp-authorization"
  recovery_window_in_days = 0

  tags = merge(var.tags, {
    Name      = "${var.project}-${var.environment}-collector-grafana-otlp-authorization"
    Component = "collector-observability"
  })
}

resource "aws_secretsmanager_secret_version" "collector_grafana_otlp_authorization" {
  secret_id     = aws_secretsmanager_secret.collector_grafana_otlp_authorization.id
  secret_string = local.collector_grafana_otlp_authorization
}

resource "aws_iam_role_policy" "ecs_collector_execution_read_grafana_secret" {
  name = "${var.project}-${var.environment}-collector-read-grafana-otlp-secret"
  role = element(
    reverse(split("/", module.iam.ecs_collector_execution_role_arn)),
    0
  )

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadGrafanaOtlpAuthorization"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.collector_grafana_otlp_authorization.arn
      }
    ]
  })
}

module "ecs_collector" {
  source = "../../modules/ecs-collector"

  project     = var.project
  environment = var.environment
  aws_region  = var.aws_region

  repository_url = local.collector_repository_url
  image_tag      = var.collector_image_tag

  # The Alloy image is built from services/collector/Dockerfile.alloy and pushed
  # to the same ECR repository under this dedicated tag.
  alloy_image = "${local.collector_repository_url}:alloy-v1.17.0"

  collector_service_version = var.collector_image_tag

  grafana_otlp_endpoint                 = var.grafana_otlp_endpoint
  grafana_otlp_authorization_secret_arn = aws_secretsmanager_secret.collector_grafana_otlp_authorization.arn

  execution_role_arn = module.iam.ecs_collector_execution_role_arn
  task_role_arn      = module.iam.ecs_collector_task_role_arn

  subnet_ids        = module.networking.private_subnet_ids
  security_group_id = module.networking.ecs_collector_sg_id
  assign_public_ip  = false

  log_group_name      = local.collector_log_group_name
  kinesis_stream_name = module.kinesis.stream_name

  # 0.5 vCPU / 1 GiB is the first safe baseline for Python + Alloy.
  task_cpu    = 512
  task_memory = 1024

  collector_container_cpu                = 320
  collector_container_memory_reservation = 512
  collector_container_memory             = 640

  alloy_container_cpu                = 192
  alloy_container_memory_reservation = 256
  alloy_container_memory             = 384

  desired_count = var.collector_desired_count

  batch_size             = var.collector_batch_size
  flush_interval_seconds = var.collector_flush_interval_seconds
  sample_rate            = var.collector_sample_rate
  log_level              = var.collector_log_level

  kinesis_max_retries                  = 3
  kinesis_retry_base_sleep_seconds     = 0.5
  reconnect_sleep_seconds              = 5

  # Prevent two SSE collectors from running simultaneously during deployment.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  container_insights_enabled = false
  enable_execute_command     = false

  tags = var.tags

  depends_on = [
    module.cloudwatch_log_groups,
    aws_secretsmanager_secret_version.collector_grafana_otlp_authorization,
    aws_iam_role_policy.ecs_collector_execution_read_grafana_secret
  ]
}