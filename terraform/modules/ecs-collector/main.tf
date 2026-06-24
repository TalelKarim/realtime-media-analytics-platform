locals {
  name_prefix = "${var.project}-${var.environment}"

  cluster_name = coalesce(var.cluster_name, "${local.name_prefix}-collector-cluster")
  service_name = coalesce(var.service_name, "${local.name_prefix}-collector")
  task_family  = coalesce(var.task_family, "${local.name_prefix}-collector")

  container_name  = "collector"
  container_image = "${var.repository_url}:${var.image_tag}"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Module      = "ecs-collector"
    },
    var.tags
  )

  environment_variables = {
    AWS_REGION             = var.aws_region
    WIKIMEDIA_STREAM_URL   = var.wikimedia_stream_url
    KINESIS_STREAM_NAME    = var.kinesis_stream_name
    BATCH_SIZE             = tostring(var.batch_size)
    FLUSH_INTERVAL_SECONDS = tostring(var.flush_interval_seconds)
    SAMPLE_RATE            = tostring(var.sample_rate)
    LOG_LEVEL              = var.log_level
  }
}

# ============================================================
# ECS CLUSTER
# ============================================================

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

# ============================================================
# ECS TASK DEFINITION
# ============================================================

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
      name      = local.container_name
      image     = local.container_image
      essential = true

      environment = [
        for key in sort(keys(local.environment_variables)) : {
          name  = key
          value = local.environment_variables[key]
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = local.container_name
        }
      }
    }
  ])

  tags = merge(local.common_tags, {
    Name = local.task_family
    Role = "collector-task-definition"
  })
}

# ============================================================
# ECS SERVICE
# ============================================================

resource "aws_ecs_service" "this" {
  name            = local.service_name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn

  launch_type      = "FARGATE"
  platform_version = "LATEST"

  desired_count = var.desired_count

  enable_execute_command = var.enable_execute_command

  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

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