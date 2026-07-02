locals {
  bronze_base_path = "s3://${var.datalake_bucket_name}/bronze/wikimedia/recentchange"
  silver_base_path = "s3://${var.datalake_bucket_name}/silver/wikimedia/recentchange"

  script_s3_key      = "glue/scripts/bronze_to_silver.py"
  temp_dir           = "s3://${var.datalake_bucket_name}/glue/temp/"
  spark_event_logs   = "s3://${var.datalake_bucket_name}/glue/spark-event-logs/"
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = var.datalake_bucket_name
  key    = local.script_s3_key
  source = var.bronze_to_silver_script_path

  source_hash = filemd5(var.bronze_to_silver_script_path)

  server_side_encryption = "aws:kms"
  kms_key_id             = var.s3_kms_key_arn

  tags = var.tags
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

resource "aws_iam_role" "glue_etl" {
  name               = "${var.name_prefix}-glue-etl-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_etl_data_access" {
  statement {
    sid    = "ListDataLakeBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      var.datalake_bucket_arn
    ]
  }

  statement {
    sid    = "ReadBronze"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${var.datalake_bucket_arn}/bronze/*"
    ]
  }

  statement {
    sid    = "ReadGlueScripts"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${var.datalake_bucket_arn}/glue/scripts/*"
    ]
  }

  statement {
    sid    = "WriteSilverAndGlueRuntimeObjects"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject"
    ]

    resources = [
      "${var.datalake_bucket_arn}/silver/*",
      "${var.datalake_bucket_arn}/glue/temp/*",
      "${var.datalake_bucket_arn}/glue/spark-event-logs/*"
    ]
  }

  statement {
    sid    = "UseDataLakeKmsKey"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]

    resources = [
      var.s3_kms_key_arn
    ]
  }
}

resource "aws_iam_role_policy" "glue_etl_data_access" {
  name   = "${var.name_prefix}-glue-etl-data-access"
  role   = aws_iam_role.glue_etl.id
  policy = data.aws_iam_policy_document.glue_etl_data_access.json
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "${var.name_prefix}-bronze-to-silver"
  role_arn = aws_iam_role.glue_etl.arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.datalake_bucket_name}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = local.spark_event_logs
    "--TempDir"                          = local.temp_dir

    "--BRONZE_BASE_PATH" = local.bronze_base_path
    "--SILVER_BASE_PATH" = local.silver_base_path
    "--WRITE_MODE"       = "append"
  }

  tags = var.tags
}