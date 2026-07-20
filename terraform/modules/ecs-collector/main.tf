locals {
  name_prefix = "${var.project}-${var.environment}"

  cluster_name = coalesce(var.cluster_name, "${local.name_prefix}-collector-cluster")
  service_name = coalesce(var.service_name, "${local.name_prefix}-collector")
  task_family  = coalesce(var.task_family, "${local.name_prefix}-collector")

  collector_container_name  = "collector"
  collector_container_image = "${var.repository_url}:${var.image_tag}"
  alloy_container_name      = "alloy"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Module      = "ecs-collector"
    },
    var.tags
  )

  collector_environment_variables = {
    AWS_REGION             = var.aws_region
    ENVIRONMENT            = var.environment
    WIKIMEDIA_STREAM_URL   = var.wikimedia_stream_url
    KINESIS_STREAM_NAME    = var.kinesis_stream_name
    BATCH_SIZE             = tostring(var.batch_size)
    FLUSH_INTERVAL_SECONDS = tostring(var.flush_interval_seconds)
    SAMPLE_RATE            = tostring(var.sample_rate)
    LOG_LEVEL              = var.log_level

    KINESIS_MAX_RETRIES              = tostring(var.kinesis_max_retries)
    KINESIS_RETRY_BASE_SLEEP_SECONDS = tostring(var.kinesis_retry_base_sleep_seconds)
    RECONNECT_SLEEP_SECONDS          = tostring(var.reconnect_sleep_seconds)

    OTEL_ENABLED                   = "true"
    OTEL_SERVICE_NAME              = "realtime-media-analytics-${var.environment}-collector"
    SERVICE_VERSION                = var.collector_service_version
    OTEL_EXPORTER_OTLP_ENDPOINT    = "http://127.0.0.1:4318"
    OTEL_EXPORTER_OTLP_PROTOCOL    = "http/protobuf"
    OTEL_EXPORTER_OTLP_TIMEOUT     = "3"
    OTEL_TRACES_EXPORTER           = "otlp"
    OTEL_METRICS_EXPORTER          = "otlp"
    OTEL_LOGS_EXPORTER             = "none"
    OTEL_TRACES_SAMPLER            = "always_on"
    OTEL_METRIC_EXPORT_INTERVAL_MS = "10000"
    OTEL_METRIC_EXPORT_TIMEOUT_MS  = "3000"
    OTEL_BSP_SCHEDULE_DELAY_MS     = "5000"
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "256"
    OTEL_BSP_MAX_QUEUE_SIZE        = "2048"
    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.platform=aws_ecs",
      "cloud.region=${var.aws_region}"
    ])
  }

  alloy_environment_variables = {
    GRAFANA_OTLP_ENDPOINT = var.grafana_otlp_endpoint
  }
}

resource "aws_ecs_cluster" "this" {
  name = local.cluster_name

  setting {
    name  = "containerInsights"
    value = var.container_insights_enabled ? "enabled" : "disabled"
  }

  tags = merge(local.common_tags, {
    Name = local.cluster_name
    Role = "collector-runtime"
  })
}

resource "aws_ecs_task_definition" "this" {
  family                   = local.task_family
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"

  cpu    = tostring(var.task_cpu)
  memory = tostring(var.task_memory)

  execution_role_arn = var.execution_role_arn
  task_role_arn      = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  container_definitions = jsonencode([
    {
      name      = local.collector_container_name
      image     = local.collector_container_image
      essential = true

      cpu               = var.collector_container_cpu
      memoryReservation = var.collector_container_memory_reservation
      memory            = var.collector_container_memory

      stopTimeout = 30

      environment = [
        for key in sort(keys(local.collector_environment_variables)) : {
          name  = key
          value = local.collector_environment_variables[key]
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.collector_container_name
        }
      }
    },
    {
      name      = local.alloy_container_name
      image     = var.alloy_image
      essential = false

      cpu               = var.alloy_container_cpu
      memoryReservation = var.alloy_container_memory_reservation
      memory            = var.alloy_container_memory

      command = [
        "run",
        "--server.http.listen-addr=0.0.0.0:12345",
        "--storage.path=/var/lib/alloy/data",
        "/etc/alloy/config.alloy"
      ]

      stopTimeout = 30

      restartPolicy = {
        enabled              = true
        ignoredExitCodes     = [0]
        restartAttemptPeriod = 60
      }

      environment = [
        for key in sort(keys(local.alloy_environment_variables)) : {
          name  = key
          value = local.alloy_environment_variables[key]
        }
      ]

      secrets = [
        {
          name      = "GRAFANA_OTLP_AUTHORIZATION"
          valueFrom = var.grafana_otlp_authorization_secret_arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.alloy_container_name
        }
      }
    }
  ])

  tags = merge(local.common_tags, {
    Name = local.task_family
    Role = "collector-task-definition"
  })
}

resource "aws_ecs_service" "this" {
  name            = local.service_name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn

  launch_type      = "FARGATE"
  platform_version = "LATEST"

  desired_count = var.desired_count

  enable_execute_command = var.enable_execute_command

  # Stop the previous single collector before starting the replacement.
  # Two active SSE collectors would duplicate Wikimedia events and double-count
  # DynamoDB aggregates because the downstream processor uses ADD operations.
  deployment_minimum_healthy_percent = var.deployment_minimum_healthy_percent
  deployment_maximum_percent         = var.deployment_maximum_percent

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.security_group_id]
    assign_public_ip = var.assign_public_ip
  }

  propagate_tags = "SERVICE"

  tags = merge(local.common_tags, {
    Name = local.service_name
    Role = "collector-service"
  })
}