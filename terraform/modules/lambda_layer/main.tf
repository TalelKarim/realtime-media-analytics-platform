locals {
  zip_path = "${path.root}/.terraform/${var.layer_name}.zip"
}

data "archive_file" "this" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = local.zip_path
  excludes    = var.package_excludes
}

resource "aws_lambda_layer_version" "this" {
  layer_name       = var.layer_name
  description      = var.description
  filename         = data.archive_file.this.output_path
  source_code_hash = data.archive_file.this.output_base64sha256

  compatible_runtimes      = var.compatible_runtimes
  compatible_architectures = var.compatible_architectures
}
