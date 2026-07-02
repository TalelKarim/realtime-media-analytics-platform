output "bronze_to_silver_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "glue_etl_role_arn" {
  value = aws_iam_role.glue_etl.arn
}

output "bronze_base_path" {
  value = local.bronze_base_path
}

output "silver_base_path" {
  value = local.silver_base_path
}