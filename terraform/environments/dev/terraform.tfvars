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
collector_image_tag                = "dev-001"
collector_desired_count            = 0
collector_task_cpu                 = 256
collector_task_memory              = 512
collector_sample_rate              = 0.01
collector_batch_size               = 100
collector_flush_interval_seconds   = 2
collector_log_level                = "INFO"