locals {
  name_prefix = "${var.project}-${var.environment}"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Module      = "iam"
    },
    var.tags
  )

  kinesis_stream_name = coalesce(var.kinesis_stream_name, "${local.name_prefix}-wikimedia-events")

  realtime_aggregates_table_name   = coalesce(var.realtime_aggregates_table_name, "${local.name_prefix}-realtime-aggregates")
  websocket_connections_table_name = coalesce(var.websocket_connections_table_name, "${local.name_prefix}-websocket-connections")
  alert_state_table_name           = coalesce(var.alert_state_table_name, "${local.name_prefix}-alert-state")

  broadcast_queue_name = coalesce(var.broadcast_queue_name, "${local.name_prefix}-broadcast-signal.fifo")
  alerts_topic_name    = coalesce(var.alerts_topic_name, "${local.name_prefix}-alerts")
  datalake_bucket_name = coalesce(var.datalake_bucket_name, "${local.name_prefix}-datalake-${data.aws_caller_identity.current.account_id}")

  kinesis_stream_arn = "arn:${data.aws_partition.current.partition}:kinesis:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stream/${local.kinesis_stream_name}"

  realtime_aggregates_table_arn   = "arn:${data.aws_partition.current.partition}:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.realtime_aggregates_table_name}"
  websocket_connections_table_arn = "arn:${data.aws_partition.current.partition}:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.websocket_connections_table_name}"
  alert_state_table_arn           = "arn:${data.aws_partition.current.partition}:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.alert_state_table_name}"

  broadcast_queue_arn = "arn:${data.aws_partition.current.partition}:sqs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${local.broadcast_queue_name}"
  alerts_topic_arn    = "arn:${data.aws_partition.current.partition}:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${local.alerts_topic_name}"

  datalake_bucket_arn         = "arn:${data.aws_partition.current.partition}:s3:::${local.datalake_bucket_name}"
  datalake_bucket_objects_arn = "arn:${data.aws_partition.current.partition}:s3:::${local.datalake_bucket_name}/*"

  websocket_manage_connections_arn = coalesce(
    var.websocket_manage_connections_arn,
    "arn:${data.aws_partition.current.partition}:execute-api:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*/*/POST/@connections/*"
  )

  runtime_kms_key_arns = [
    var.kinesis_key_arn,
    var.s3_key_arn,
    var.dynamodb_key_arn,
    var.sqs_key_arn,
    var.logs_key_arn
  ]
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

data "aws_iam_policy_document" "ecs_tasks_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# ============================================================
# ECS COLLECTOR ROLES
# ============================================================

resource "aws_iam_role" "ecs_collector_execution" {
  name               = "${local.name_prefix}-ecs-collector-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-ecs-collector-execution-role" })
}

resource "aws_iam_role_policy_attachment" "ecs_collector_execution" {
  role       = aws_iam_role.ecs_collector_execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_collector_task" {
  name               = "${local.name_prefix}-ecs-collector-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-ecs-collector-task-role" })
}

resource "aws_iam_role_policy" "ecs_collector_task" {
  name = "${local.name_prefix}-ecs-collector-task-policy"
  role = aws_iam_role.ecs_collector_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteToKinesis"
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary"
        ]
        Resource = local.kinesis_stream_arn
      },
      {
        Sid    = "UseKinesisKmsKey"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey"
        ]
        Resource = var.kinesis_key_arn
      }
    ]
  })
}

# ============================================================
# LAMBDA ROLES
# ============================================================

resource "aws_iam_role" "realtime_processor" {
  name               = "${local.name_prefix}-realtime-processor-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-realtime-processor-role" })
}

resource "aws_iam_role" "broadcaster" {
  name               = "${local.name_prefix}-broadcaster-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-broadcaster-role" })
}

resource "aws_iam_role" "websocket_connect" {
  name               = "${local.name_prefix}-websocket-connect-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-websocket-connect-role" })
}

resource "aws_iam_role" "websocket_disconnect" {
  name               = "${local.name_prefix}-websocket-disconnect-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-websocket-disconnect-role" })
}

resource "aws_iam_role" "websocket_default" {
  name               = "${local.name_prefix}-websocket-default-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-websocket-default-role" })
}

resource "aws_iam_role" "alert_processor" {
  name               = "${local.name_prefix}-alert-processor-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-alert-processor-role" })
}

locals {
  lambda_roles = {
    realtime_processor   = aws_iam_role.realtime_processor.name
    broadcaster          = aws_iam_role.broadcaster.name
    websocket_connect    = aws_iam_role.websocket_connect.name
    websocket_disconnect = aws_iam_role.websocket_disconnect.name
    websocket_default    = aws_iam_role.websocket_default.name
    alert_processor      = aws_iam_role.alert_processor.name
  }
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  for_each = local.lambda_roles

  role       = each.value
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "realtime_processor" {
  name = "${local.name_prefix}-realtime-processor-policy"
  role = aws_iam_role.realtime_processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadFromKinesis"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]
        Resource = local.kinesis_stream_arn
      },
      {
        Sid    = "UpdateRealtimeAggregates"
        Effect = "Allow"
        Action = [
          "dynamodb:UpdateItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.realtime_aggregates_table_arn
      },
      {
        Sid    = "SendBroadcastSignal"
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = local.broadcast_queue_arn
      },
      {
        Sid    = "UseRuntimeKmsKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kinesis_key_arn,
          var.dynamodb_key_arn,
          var.sqs_key_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "broadcaster" {
  name = "${local.name_prefix}-broadcaster-policy"
  role = aws_iam_role.broadcaster.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ConsumeBroadcastQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]
        Resource = local.broadcast_queue_arn
      },
      {
        Sid    = "ReadRealtimeAggregates"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:BatchGetItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.realtime_aggregates_table_arn
      },
      {
        Sid    = "ReadAndCleanupWebsocketConnections"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.websocket_connections_table_arn
      },
      {
        Sid    = "ManageWebsocketConnections"
        Effect = "Allow"
        Action = [
          "execute-api:ManageConnections"
        ]
        Resource = local.websocket_manage_connections_arn
      },
      {
        Sid    = "UseRuntimeKmsKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          var.dynamodb_key_arn,
          var.sqs_key_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "websocket_connect" {
  name = "${local.name_prefix}-websocket-connect-policy"
  role = aws_iam_role.websocket_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StoreWebsocketConnection"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.websocket_connections_table_arn
      },
      {
        Sid    = "UseDynamoDbKmsKey"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = var.dynamodb_key_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "websocket_disconnect" {
  name = "${local.name_prefix}-websocket-disconnect-policy"
  role = aws_iam_role.websocket_disconnect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DeleteWebsocketConnection"
        Effect = "Allow"
        Action = [
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.websocket_connections_table_arn
      },
      {
        Sid    = "UseDynamoDbKmsKey"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = var.dynamodb_key_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "websocket_default" {
  name = "${local.name_prefix}-websocket-default-policy"
  role = aws_iam_role.websocket_default.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "UpdateWebsocketSubscriptions"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable"
        ]
        Resource = local.websocket_connections_table_arn
      },
      {
        Sid    = "SendSubscriptionAck"
        Effect = "Allow"
        Action = [
          "execute-api:ManageConnections"
        ]
        Resource = local.websocket_manage_connections_arn
      },
      {
        Sid    = "UseRuntimeKmsKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = var.dynamodb_key_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "alert_processor" {
  name = "${local.name_prefix}-alert-processor-policy"
  role = aws_iam_role.alert_processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadFromKinesis"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]
        Resource = local.kinesis_stream_arn
      },
      {
        Sid    = "PersistAlertState"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
          "dynamodb:DescribeTable"
        ]
        Resource = local.alert_state_table_arn
      },
      {
        Sid    = "PublishAlerts"
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = local.alerts_topic_arn
      },
      {
        Sid    = "UseRuntimeKmsKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kinesis_key_arn,
          var.dynamodb_key_arn
        ]
      }
    ]
  })
}

# ============================================================
# FIREHOSE ROLE
# ============================================================

resource "aws_iam_role" "firehose" {
  name               = "${local.name_prefix}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-firehose-role" })
}

resource "aws_iam_role_policy" "firehose" {
  name = "${local.name_prefix}-firehose-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadFromKinesis"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]
        Resource = local.kinesis_stream_arn
      },
      {
        Sid    = "WriteToS3DataLake"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          local.datalake_bucket_arn,
          local.datalake_bucket_objects_arn
        ]
      },
      {
        Sid    = "UseRuntimeKmsKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kinesis_key_arn,
          var.s3_key_arn
        ]
      }
    ]
  })
}

# ============================================================
# GLUE ROLE
# ============================================================

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = merge(local.common_tags, { Name = "${local.name_prefix}-glue-role" })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue" {
  name = "${local.name_prefix}-glue-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadWriteDataLake"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          local.datalake_bucket_arn,
          local.datalake_bucket_objects_arn
        ]
      },
      {
        Sid    = "UseS3KmsKey"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey"
        ]
        Resource = var.s3_key_arn
      }
    ]
  })
}