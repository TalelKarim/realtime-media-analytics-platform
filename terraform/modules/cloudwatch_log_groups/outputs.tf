output "log_group_names" {
  description = "CloudWatch log group names keyed by log group name."
  value = {
    for name, log_group in aws_cloudwatch_log_group.this :
    name => log_group.name
  }
}

output "log_group_arns" {
  description = "CloudWatch log group ARNs keyed by log group name."
  value = {
    for name, log_group in aws_cloudwatch_log_group.this :
    name => log_group.arn
  }
}