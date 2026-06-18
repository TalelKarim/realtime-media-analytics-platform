output "tfstate_bucket_name" {
  description = "Terraform state S3 bucket name."
  value       = aws_s3_bucket.tfstate.bucket
}

output "tfstate_bucket_arn" {
  description = "Terraform state S3 bucket ARN."
  value       = aws_s3_bucket.tfstate.arn
}