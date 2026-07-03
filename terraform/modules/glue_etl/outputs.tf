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


output "bronze_to_silver_trigger_name" {
  value = try(aws_glue_trigger.bronze_to_silver_hourly[0].name, null)
}



output "silver_to_gold_job_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "silver_to_gold_trigger_name" {
  value = try(aws_glue_trigger.silver_to_gold_hourly[0].name, null)
}

output "gold_base_path" {
  value = local.gold_base_path
}