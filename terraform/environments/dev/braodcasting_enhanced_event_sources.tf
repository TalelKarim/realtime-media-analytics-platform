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
# One SQS message represents one topic-shard job.
#
# Example MessageGroupIds:
#   TOPIC#global#SHARD#00
#   TOPIC#global#SHARD#01
#   ...
#   TOPIC#global#SHARD#19
#
# Different groups can be processed concurrently. Messages belonging to the
# same topic-shard remain ordered.
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

  enabled = var.enhaned_broadcasting_enabled

  depends_on = [
    module.iam,
    module.lambda_broadcast_worker
  ]
}
