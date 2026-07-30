aws_region  = "us-east-1"
project     = "realtime-media-analytics"
environment = "dev"


# kinesis variables

kinesis_shard_count            = 1
kinesis_retention_period_hours = 48
kinesis_shard_level_metrics    = []

sns_email_subscriptions = [
  "talelkarimc@gmail.com"
]


tags = {
  Project     = "realtime-media-analytics"
  Environment = "dev"
  ManagedBy   = "terraform"
  Owner       = "talel-karim"
}



# ECS Fargate Collector 
collector_image_tag              = "otel-alloy-v1"
collector_desired_count          = 1
collector_task_cpu               = 256
collector_task_memory            = 512
collector_sample_rate            = 0.05
collector_batch_size             = 100
collector_flush_interval_seconds = 1
collector_log_level              = "INFO"



# grafana 
grafana_otlp_endpoint = "https://otlp-gateway-prod-eu-west-6.grafana.net/otlp"



# quicksight
quicksight_principal_arn = "arn:aws:quicksight:us-east-1:156358246560:user/default/156358246560"


grafana_aws_account_id = "008923505280"
grafana_external_id    = "3355686"




# enhaned broadcasting 

subscription_shard_count                  = 10
broadcast_worker_max_concurrency          = 20
broadcast_shard_jobs_enabled              = true
broadcast_snapshot_ttl_seconds            = 900
broadcast_jobs_visibility_timeout_seconds = 180
broadcast_jobs_max_receive_count          = 5
enhaned_broadcasting_enabled              = true

# Fixed public WebSocket URL
websocket_custom_domain_name = "stream-websocket.talelkarimchebbi.com"
