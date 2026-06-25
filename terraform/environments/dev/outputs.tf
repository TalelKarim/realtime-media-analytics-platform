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



# dynamodb outputs
output "dynamodb_realtime_aggregates_table_name" {
  description = "Name of the realtime aggregates DynamoDB table."
  value       = module.dynamodb.realtime_aggregates_table_name
}

output "dynamodb_realtime_aggregates_table_arn" {
  description = "ARN of the realtime aggregates DynamoDB table."
  value       = module.dynamodb.realtime_aggregates_table_arn
}

output "dynamodb_websocket_connections_table_name" {
  description = "Name of the WebSocket connections DynamoDB table."
  value       = module.dynamodb.websocket_connections_table_name
}

output "dynamodb_websocket_connections_table_arn" {
  description = "ARN of the WebSocket connections DynamoDB table."
  value       = module.dynamodb.websocket_connections_table_arn
}

output "dynamodb_alert_state_table_name" {
  description = "Name of the alert state DynamoDB table."
  value       = module.dynamodb.alert_state_table_name
}

output "dynamodb_alert_state_table_arn" {
  description = "ARN of the alert state DynamoDB table."
  value       = module.dynamodb.alert_state_table_arn
}


# SQS AND SNS outputs

output "sqs_broadcast_signal_queue_name" {
  description = "Name of the broadcast signal FIFO queue."
  value       = module.sqs.broadcast_signal_queue_name
}

output "sqs_broadcast_signal_queue_url" {
  description = "URL of the broadcast signal FIFO queue."
  value       = module.sqs.broadcast_signal_queue_url
}

output "sqs_broadcast_signal_queue_arn" {
  description = "ARN of the broadcast signal FIFO queue."
  value       = module.sqs.broadcast_signal_queue_arn
}

output "sqs_broadcast_signal_dlq_name" {
  description = "Name of the broadcast signal dead-letter FIFO queue."
  value       = module.sqs.broadcast_signal_dlq_name
}

output "sqs_broadcast_signal_dlq_arn" {
  description = "ARN of the broadcast signal dead-letter FIFO queue."
  value       = module.sqs.broadcast_signal_dlq_arn
}

output "sns_alerts_topic_name" {
  description = "Name of the alerts SNS topic."
  value       = module.sns.alerts_topic_name
}

output "sns_alerts_topic_arn" {
  description = "ARN of the alerts SNS topic."
  value       = module.sns.alerts_topic_arn
}



# S3 Datalake


output "s3_datalake_bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = module.s3_datalake.bucket_name
}

output "s3_datalake_bucket_arn" {
  description = "ARN of the S3 data lake bucket."
  value       = module.s3_datalake.bucket_arn
}

output "s3_datalake_bronze_prefix" {
  description = "Bronze layer prefix."
  value       = module.s3_datalake.bronze_prefix
}

output "s3_datalake_silver_prefix" {
  description = "Silver layer prefix."
  value       = module.s3_datalake.silver_prefix
}

output "s3_datalake_gold_prefix" {
  description = "Gold layer prefix."
  value       = module.s3_datalake.gold_prefix
}

output "s3_datalake_athena_results_prefix" {
  description = "Athena query results prefix."
  value       = module.s3_datalake.athena_results_prefix
}



# ECS Outputs 
# Phase 10 outputs — ECS Fargate Collector

output "ecs_collector_cluster_name" {
  description = "ECS collector cluster name"
  value       = module.ecs_collector.cluster_name
}

output "ecs_collector_cluster_arn" {
  description = "ECS collector cluster ARN"
  value       = module.ecs_collector.cluster_arn
}

output "ecs_collector_service_name" {
  description = "ECS collector service name"
  value       = module.ecs_collector.service_name
}

output "ecs_collector_service_arn" {
  description = "ECS collector service ARN"
  value       = module.ecs_collector.service_arn
}

output "ecs_collector_task_definition_arn" {
  description = "ECS collector task definition ARN"
  value       = module.ecs_collector.task_definition_arn
}

output "ecs_collector_container_image" {
  description = "Collector container image configured in task definition"
  value       = module.ecs_collector.container_image
}


# websocket outputs 

output "websocket_api_id" {
  description = "API Gateway WebSocket API ID."
  value       = module.apigw_websocket.api_id
}

output "websocket_api_endpoint" {
  description = "API Gateway WebSocket API endpoint without stage."
  value       = module.apigw_websocket.api_endpoint
}

output "websocket_url" {
  description = "API Gateway WebSocket URL including stage."
  value       = module.apigw_websocket.invoke_url
}

output "websocket_manage_connections_arn" {
  description = "ARN used by Lambdas to manage WebSocket connections."
  value       = module.apigw_websocket.manage_connections_arn
}