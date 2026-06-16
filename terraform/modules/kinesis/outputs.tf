output "stream_name" {
  description = "Kinesis stream name."
  value       = aws_kinesis_stream.this.name
}

output "stream_arn" {
  description = "Kinesis stream ARN."
  value       = aws_kinesis_stream.this.arn
}

output "stream_id" {
  description = "Kinesis stream ID."
  value       = aws_kinesis_stream.this.id
}

output "stream_mode" {
  description = "Kinesis stream capacity mode."
  value       = "PROVISIONED"
}

output "shard_count" {
  description = "Number of provisioned shards."
  value       = var.shard_count
}

output "retention_period_hours" {
  description = "Kinesis record retention period in hours."
  value       = var.retention_period_hours
}

output "kms_key_arn" {
  description = "KMS key ARN used by the Kinesis stream."
  value       = var.kms_key_arn
}