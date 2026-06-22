output "function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.this.function_name
}

output "function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.this.arn
}

output "invoke_arn" {
  description = "Lambda invoke ARN."
  value       = aws_lambda_function.this.invoke_arn
}

output "qualified_arn" {
  description = "Qualified Lambda ARN."
  value       = aws_lambda_function.this.qualified_arn
}

output "log_group_name" {
  description = "CloudWatch log group name."
  value       = local.log_group_name
}