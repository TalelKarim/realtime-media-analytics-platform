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