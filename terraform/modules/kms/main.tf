locals {
  name_prefix = "${var.project}-${var.environment}"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Module      = "kms"
    },
    var.tags
  )
}



data "aws_caller_identity" "current" {}

# ============================================================
# KMS KEY — KINESIS DATA STREAMS
# ============================================================

resource "aws_kms_key" "kinesis" {
  description             = "KMS key for Kinesis Data Streams - ${local.name_prefix}"
  key_usage               = "ENCRYPT_DECRYPT"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RootAccountAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-kms-kinesis"
    Service = "kinesis"
  })
}

resource "aws_kms_alias" "kinesis" {
  name          = "alias/${local.name_prefix}-kinesis"
  target_key_id = aws_kms_key.kinesis.key_id
}

# ============================================================
# KMS KEY — S3 DATA LAKE
# ============================================================

resource "aws_kms_key" "s3" {
  description             = "KMS key for S3 Data Lake - ${local.name_prefix}"
  key_usage               = "ENCRYPT_DECRYPT"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RootAccountAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-kms-s3"
    Service = "s3"
  })
}

resource "aws_kms_alias" "s3" {
  name          = "alias/${local.name_prefix}-s3"
  target_key_id = aws_kms_key.s3.key_id
}

# ============================================================
# KMS KEY — DYNAMODB
# Used by: realtime_aggregates + websocket_connections tables
# ============================================================

resource "aws_kms_key" "dynamodb" {
  description             = "KMS key for DynamoDB tables - ${local.name_prefix}"
  key_usage               = "ENCRYPT_DECRYPT"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RootAccountAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-kms-dynamodb"
    Service = "dynamodb"
  })
}

resource "aws_kms_alias" "dynamodb" {
  name          = "alias/${local.name_prefix}-dynamodb"
  target_key_id = aws_kms_key.dynamodb.key_id
}

# ============================================================
# KMS KEY — SQS FIFO
# ============================================================

resource "aws_kms_key" "sqs" {
  description             = "KMS key for SQS FIFO queue - ${local.name_prefix}"
  key_usage               = "ENCRYPT_DECRYPT"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RootAccountAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-kms-sqs"
    Service = "sqs"
  })
}

resource "aws_kms_alias" "sqs" {
  name          = "alias/${local.name_prefix}-sqs"
  target_key_id = aws_kms_key.sqs.key_id
}

# ============================================================
# KMS KEY — CLOUDWATCH LOGS
# Requires an explicit grant to logs.amazonaws.com because
# CloudWatch Logs acts as a service principal when encrypting
# log events — not as an IAM role.
# ============================================================

resource "aws_kms_key" "logs" {
  description             = "KMS key for CloudWatch Logs - ${local.name_prefix}"
  key_usage               = "ENCRYPT_DECRYPT"
  deletion_window_in_days = var.deletion_window_in_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RootAccountAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Principal = {
          Service = "logs.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-kms-logs"
    Service = "cloudwatch-logs"
  })
}

resource "aws_kms_alias" "logs" {
  name          = "alias/${local.name_prefix}-logs"
  target_key_id = aws_kms_key.logs.key_id
}