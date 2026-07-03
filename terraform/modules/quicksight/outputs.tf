output "athena_workgroup_name" {
  value = aws_athena_workgroup.quicksight.name
}

output "athena_results_location" {
  value = local.athena_results_location
}

output "quicksight_data_source_arn" {
  value = aws_quicksight_data_source.athena.arn
}

output "quicksight_gold_dataset_arns" {
  value = {
    for key, dataset in aws_quicksight_data_set.gold :
    key => dataset.arn
  }
}

output "quicksight_gold_dataset_ids" {
  value = {
    for key, dataset in aws_quicksight_data_set.gold :
    key => dataset.data_set_id
  }
}