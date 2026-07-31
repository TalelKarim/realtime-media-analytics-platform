output "ecs_collector_execution_role_arn" {
  description = "ECS collector task execution role ARN"
  value       = aws_iam_role.ecs_collector_execution.arn
}

output "ecs_collector_task_role_arn" {
  description = "ECS collector task role ARN"
  value       = aws_iam_role.ecs_collector_task.arn
}

output "realtime_processor_role_arn" {
  description = "Realtime processor Lambda role ARN"
  value       = aws_iam_role.realtime_processor.arn
}



output "broadcast_coordinator_role_arn" {
  description = "Broadcast Coordinator Lambda role ARN"
  value       = aws_iam_role.broadcast_coordinator.arn
}

output "broadcast_worker_role_arn" {
  description = "Broadcast Worker Lambda role ARN"
  value       = aws_iam_role.broadcast_worker.arn
}


output "websocket_connect_role_arn" {
  description = "WebSocket connect Lambda role ARN"
  value       = aws_iam_role.websocket_connect.arn
}

output "websocket_disconnect_role_arn" {
  description = "WebSocket disconnect Lambda role ARN"
  value       = aws_iam_role.websocket_disconnect.arn
}

output "websocket_default_role_arn" {
  description = "WebSocket default Lambda role ARN"
  value       = aws_iam_role.websocket_default.arn
}

output "alert_processor_role_arn" {
  description = "Alert processor Lambda role ARN"
  value       = aws_iam_role.alert_processor.arn
}

output "firehose_role_arn" {
  description = "Firehose delivery role ARN"
  value       = aws_iam_role.firehose.arn
}

output "glue_role_arn" {
  description = "Glue ETL role ARN"
  value       = aws_iam_role.glue.arn
}

output "lambda_role_arns" {
  description = "Map of Lambda role ARNs"

  value = {
    realtime_processor    = aws_iam_role.realtime_processor.arn
    broadcast_coordinator = aws_iam_role.broadcast_coordinator.arn
    broadcast_worker      = aws_iam_role.broadcast_worker.arn
    websocket_connect     = aws_iam_role.websocket_connect.arn
    websocket_disconnect  = aws_iam_role.websocket_disconnect.arn
    websocket_default     = aws_iam_role.websocket_default.arn
    alert_processor       = aws_iam_role.alert_processor.arn
  }
}

output "future_resource_names" {
  description = "Resource names assumed by IAM policies before the resources are created"

  value = {
    kinesis_stream                = local.kinesis_stream_name
    realtime_aggregates_table     = local.realtime_aggregates_table_name
    websocket_connections_table   = local.websocket_connections_table_name
    websocket_subscriptions_table = local.websocket_subscriptions_table_name
    broadcast_snapshots_table     = local.broadcast_snapshots_table_name
    alert_state_table             = local.alert_state_table_name
    broadcast_queue               = local.broadcast_queue_name
    broadcast_jobs_queue          = local.broadcast_jobs_queue_name
    alerts_topic                  = local.alerts_topic_name
    datalake_bucket               = local.datalake_bucket_name
  }
}