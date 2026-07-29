# =============================================================================
# Broadcasting V2 event source mappings
#
# Realtime Processor
#   -> broadcast-signal.fifo
#   -> Broadcast Coordinator
#   -> broadcast-jobs.fifo
#   -> Broadcast Workers
# =============================================================================

# -----------------------------------------------------------------------------
# broadcast-signal.fifo -> Broadcast Coordinator
#
# The signal queue currently uses a FIFO MessageGroupId shared by broadcast
# signals. Signals are therefore processed sequentially by the Coordinator,
# which is the desired behavior: one coordination cycle at a time.
# -----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "broadcast_signal_to_coordinator" {
  event_source_arn = module.sqs.broadcast_signal_queue_arn
  function_name    = module.lambda_broadcast_coordinator.function_arn

  batch_size                         = 1
  maximum_batching_window_in_seconds = 0

  function_response_types = [
    "ReportBatchItemFailures"
  ]

  enabled = var.enhaned_broadcasting_enabled

  depends_on = [
    module.iam,
    module.lambda_broadcast_coordinator
  ]
}

# -----------------------------------------------------------------------------
# broadcast-jobs.fifo -> Broadcast Worker
#
# Phase 2 contract: one SQS message represents one complete connection shard.
#
# Example MessageGroupIds:
#   SHARD#00
#   SHARD#01
#   ...
#   SHARD#19
#
# Phase 1 keeps this mapping disabled because the current Worker still expects
# the old topic-shard contract. Different connection shards will be processed
# concurrently after Phase 2 is enabled.
# -----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "broadcast_jobs_to_worker" {
  event_source_arn = module.sqs.broadcast_jobs_queue_arn
  function_name    = module.lambda_broadcast_worker.function_arn

  # One message already represents one complete shard job.
  batch_size                         = 1
  maximum_batching_window_in_seconds = 0

  function_response_types = [
    "ReportBatchItemFailures"
  ]

  scaling_config {
    maximum_concurrency = var.broadcast_worker_max_concurrency
  }

  enabled = (
    var.enhaned_broadcasting_enabled &&
    var.broadcast_shard_jobs_enabled
  )

  depends_on = [
    module.iam,
    module.lambda_broadcast_worker
  ]
}
