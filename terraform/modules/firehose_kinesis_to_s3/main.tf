resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/kinesisfirehose/${var.name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

resource "aws_cloudwatch_log_stream" "s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.this.name
}

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = var.name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.kinesis_stream_arn
    role_arn           = var.firehose_role_arn
  }

  extended_s3_configuration {
    role_arn   = var.firehose_role_arn
    bucket_arn = var.s3_bucket_arn

    buffering_size     = var.buffering_size_mb
    buffering_interval = var.buffering_interval_seconds

    compression_format = "GZIP"
    kms_key_arn        = var.s3_kms_key_arn

    prefix = "bronze/wikimedia/recentchange/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/hour=!{partitionKeyFromQuery:hour}/"

    error_output_prefix = "errors/firehose/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.this.name
      log_stream_name = aws_cloudwatch_log_stream.s3_delivery.name
    }

    dynamic_partitioning_configuration {
      enabled        = true
      retry_duration = 300
    }

    processing_configuration {
      enabled = true

      processors {
        type = "MetadataExtraction"

        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }

        parameters {
          parameter_name = "MetadataExtractionQuery"

          # We extract the partition values directly from occurred_at:
          # occurred_at = 2026-07-02T16:37:22Z
          # year  = 2026
          # month = 07
          # day   = 02
          # hour  = 16
          parameter_value = <<-EOT
          {
            year: .occurred_at[0:4],
            month: .occurred_at[5:7],
            day: .occurred_at[8:10],
            hour: .occurred_at[11:13]
          }
          EOT
        }
      }

      processors {
        type = "AppendDelimiterToRecord"
      }
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.this,
    aws_cloudwatch_log_stream.s3_delivery
  ]

  tags = var.tags
}