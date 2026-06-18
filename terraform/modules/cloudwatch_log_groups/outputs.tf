output "log_group_names" {
  description = "Created CloudWatch log group names."
  value       = [for lg in aws_cloudwatch_log_group.this : lg.name]
}

output "log_group_arns" {
  description = "Created CloudWatch log group ARNs."
  value = {
    for name, lg in aws_cloudwatch_log_group.this :
    name => lg.arn
  }
}