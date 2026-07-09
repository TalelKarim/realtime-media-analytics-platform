output "role_name" {
  description = "IAM role name used by Grafana Cloud."
  value       = aws_iam_role.this.name
}

output "role_arn" {
  description = "IAM role ARN to paste into Grafana Cloud."
  value       = aws_iam_role.this.arn
}

output "policy_arn" {
  description = "IAM policy ARN attached to the Grafana role."
  value       = aws_iam_policy.this.arn
}