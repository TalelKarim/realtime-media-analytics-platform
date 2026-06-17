output "bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_id" {
  description = "ID of the S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.id
}

output "bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = aws_s3_bucket.data_lake.arn
}

output "bronze_prefix" {
  description = "Bronze layer prefix."
  value       = "bronze/"
}

output "silver_prefix" {
  description = "Silver layer prefix."
  value       = "silver/"
}

output "gold_prefix" {
  description = "Gold layer prefix."
  value       = "gold/"
}

output "athena_results_prefix" {
  description = "Athena query results prefix."
  value       = "athena-results/"
}