# Phase 1 outputs — networking
# Run `terraform output` after apply to validate

output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "vpc_cidr" {
  description = "VPC CIDR"
  value       = module.networking.vpc_cidr
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  description = "Private subnet IDs — ECS Fargate Collector"
  value       = module.networking.private_subnet_ids
}

output "ecs_collector_sg_id" {
  description = "ECS Collector security group ID"
  value       = module.networking.ecs_collector_sg_id
}

output "nat_gateway_public_ip" {
  description = "NAT Gateway public IP"
  value       = module.networking.nat_gateway_public_ip
}



# iam outputs 
# Phase 3 outputs — IAM

output "ecs_collector_execution_role_arn" {
  description = "ECS collector execution role ARN"
  value       = module.iam.ecs_collector_execution_role_arn
}

output "ecs_collector_task_role_arn" {
  description = "ECS collector task role ARN"
  value       = module.iam.ecs_collector_task_role_arn
}

output "lambda_role_arns" {
  description = "Lambda runtime role ARNs"
  value       = module.iam.lambda_role_arns
}

output "firehose_role_arn" {
  description = "Firehose role ARN"
  value       = module.iam.firehose_role_arn
}

output "glue_role_arn" {
  description = "Glue role ARN"
  value       = module.iam.glue_role_arn
}

output "iam_future_resource_names" {
  description = "Future resource names assumed by IAM"
  value       = module.iam.future_resource_names
}




# kinesis outputs
output "kinesis_stream_name" {
  description = "Name of the Wikimedia Kinesis event stream."
  value       = module.kinesis.stream_name
}

output "kinesis_stream_arn" {
  description = "ARN of the Wikimedia Kinesis event stream."
  value       = module.kinesis.stream_arn
}

output "kinesis_stream_id" {
  description = "ID of the Wikimedia Kinesis event stream."
  value       = module.kinesis.stream_id
}

output "kinesis_stream_mode" {
  description = "Capacity mode of the Wikimedia Kinesis event stream."
  value       = module.kinesis.stream_mode
}

output "kinesis_shard_count" {
  description = "Number of provisioned shards for the Wikimedia Kinesis event stream."
  value       = module.kinesis.shard_count
}

output "kinesis_retention_period_hours" {
  description = "Retention period in hours for the Wikimedia Kinesis event stream."
  value       = module.kinesis.retention_period_hours
}