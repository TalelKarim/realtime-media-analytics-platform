output "dashboard_bucket_name" {
  value = aws_s3_bucket.dashboard.bucket
}

output "cloudfront_distribution_domain_name" {
  value = aws_cloudfront_distribution.dashboard.domain_name
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.dashboard.id
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions_deploy.arn
}



