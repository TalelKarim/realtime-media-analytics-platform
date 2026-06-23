output "kinesis_key_arn" {
  description = "KMS key ARN for Kinesis Data Streams"
  value       = aws_kms_key.kinesis.arn
}

output "kinesis_key_id" {
  description = "KMS key ID for Kinesis Data Streams"
  value       = aws_kms_key.kinesis.key_id
}

output "s3_key_arn" {
  description = "KMS key ARN for S3 Data Lake"
  value       = aws_kms_key.s3.arn
}

output "s3_key_id" {
  description = "KMS key ID for S3 Data Lake"
  value       = aws_kms_key.s3.key_id
}



output "dynamodb_key_arn" {
  description = "KMS key ARN for DynamoDB tables"
  value       = aws_kms_key.dynamodb.arn
}

output "dynamodb_key_id" {
  description = "KMS key ID for DynamoDB tables"
  value       = aws_kms_key.dynamodb.key_id
}

output "sqs_key_arn" {
  description = "KMS key ARN for SQS FIFO queue"
  value       = aws_kms_key.sqs.arn
}

output "sqs_key_id" {
  description = "KMS key ID for SQS FIFO queue"
  value       = aws_kms_key.sqs.key_id
}




output "sns_key_arn" {
  description = "KMS key ARN for SQS FIFO queue"
  value       = aws_kms_key.sns.arn
}

output "sns_key_id" {
  description = "KMS key ID for SQS FIFO queue"
  value       = aws_kms_key.sns.key_id
}


output "logs_key_arn" {
  description = "KMS key ARN for CloudWatch Logs"
  value       = aws_kms_key.logs.arn
}

output "logs_key_id" {
  description = "KMS key ID for CloudWatch Logs"
  value       = aws_kms_key.logs.key_id
}