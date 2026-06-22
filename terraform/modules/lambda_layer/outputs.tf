output "layer_arn" {
  description = "Lambda layer version ARN."
  value       = aws_lambda_layer_version.this.arn
}

output "layer_name" {
  description = "Lambda layer name."
  value       = aws_lambda_layer_version.this.layer_name
}

output "version" {
  description = "Lambda layer version."
  value       = aws_lambda_layer_version.this.version
}

