output "database_name" {
  value = aws_glue_catalog_database.this.name
}

output "bronze_table_name" {
  value = aws_glue_catalog_table.bronze_recentchange.name
}

output "bronze_s3_location" {
  value = local.bronze_s3_location
}