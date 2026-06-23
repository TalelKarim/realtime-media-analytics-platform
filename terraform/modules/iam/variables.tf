variable "project" {
  description = "Project name used as prefix for all resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "kinesis_key_arn" {
  description = "KMS key ARN for Kinesis"
  type        = string
}

variable "s3_key_arn" {
  description = "KMS key ARN for S3 Data Lake"
  type        = string
}

variable "dynamodb_key_arn" {
  description = "KMS key ARN for DynamoDB"
  type        = string
}

variable "sqs_key_arn" {
  description = "KMS key ARN for SQS"
  type        = string
}

variable "logs_key_arn" {
  description = "KMS key ARN for CloudWatch Logs"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Future Kinesis stream name"
  type        = string
  default     = null
}



variable "realtime_aggregates_table_name" {
  description = "Future DynamoDB realtime aggregates table name"
  type        = string
  default     = null
}

variable "websocket_connections_table_name" {
  description = "Future DynamoDB websocket connections table name"
  type        = string
  default     = null
}

variable "alert_state_table_name" {
  description = "Future DynamoDB alert processor state table name"
  type        = string
  default     = null
}

variable "broadcast_queue_name" {
  description = "Future SQS FIFO broadcast signal queue name"
  type        = string
  default     = null
}

variable "alerts_topic_name" {
  description = "Future SNS alerts topic name"
  type        = string
  default     = null
}

variable "datalake_bucket_name" {
  description = "Future S3 Data Lake bucket name"
  type        = string
  default     = null
}

variable "websocket_manage_connections_arn" {
  description = "API Gateway WebSocket Management API ARN. Wildcard by default until API is created."
  type        = string
  default     = null
}

variable "tags" {
  description = "Additional tags to merge into all resources"
  type        = map(string)
  default     = {}
}