locals {
  log_group_name = "/aws/lambda/${var.function_name}"
  zip_path       = "${path.root}/.terraform/${var.function_name}.zip"
}

data "archive_file" "this" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = local.zip_path
  excludes    = var.package_excludes
}

resource "aws_cloudwatch_log_group" "this" {
  count = var.create_log_group ? 1 : 0

  name              = local.log_group_name
  retention_in_days = var.log_retention_in_days

  tags = var.tags
}

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  description   = var.description

  role    = var.role_arn
  runtime = var.runtime
  handler = var.handler

  filename         = data.archive_file.this.output_path
  source_code_hash = data.archive_file.this.output_base64sha256

  memory_size   = var.memory_size
  timeout       = var.timeout
  architectures = var.architectures
  layers        = var.layers

  dynamic "environment" {
    for_each = length(var.environment_variables) > 0 ? [1] : []

    content {
      variables = var.environment_variables
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.this
  ]

  tags = var.tags
}