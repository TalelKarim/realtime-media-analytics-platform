data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"

  bucket_name = coalesce(
    var.bucket_name,
    "${local.name_prefix}-datalake-${data.aws_caller_identity.current.account_id}"
  )

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "historical-analytics"
      Service     = "s3"
    },
    var.tags
  )
}

resource "aws_s3_bucket" "data_lake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    Name = local.bucket_name
    Role = "data-lake"
  })
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = var.versioning_enabled ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_arn
      sse_algorithm     = "aws:kms"
    }

    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "data_lake" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  policy = data.aws_iam_policy_document.data_lake.json
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_upload_days
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = var.bronze_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.bronze_transition_to_glacier_ir_days
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = var.bronze_expiration_days
    }
  }

  rule {
    id     = "silver-lifecycle"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = var.silver_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = var.silver_expiration_days
    }
  }

  rule {
    id     = "gold-lifecycle"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    transition {
      days          = var.gold_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = var.gold_expiration_days
    }
  }
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(var.prefixes)

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value
  content = ""

  server_side_encryption = "aws:kms"
  kms_key_id             = var.kms_key_arn

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.data_lake
  ]
}