output "delivery_stream_name" {
  description = "Firehose delivery stream name."
  value       = aws_kinesis_firehose_delivery_stream.this.name
}

output "delivery_stream_arn" {
  description = "Firehose delivery stream ARN."
  value       = aws_kinesis_firehose_delivery_stream.this.arn
}

output "log_group_name" {
  description = "CloudWatch log group used by Firehose."
  value       = aws_cloudwatch_log_group.this.name
}

output "s3_prefix" {
  description = "S3 Bronze prefix used by Firehose."
  value       = "bronze/wikimedia/recentchange/"
}

output "s3_error_prefix" {
  description = "S3 error prefix used by Firehose."
  value       = "errors/firehose/"
}