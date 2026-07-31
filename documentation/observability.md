# Observability

## 1. Telemetry topology

```text
Collector Python SDK
→ Grafana Alloy sidecar
→ Grafana Cloud OTLP

Lambda Python SDKs
→ local OpenTelemetry Collector Lambda Extension
→ Grafana Cloud OTLP

CloudWatch Logs
→ GrafanaCloudLambdaPromtail subscription
→ Loki

AWS service metrics
→ Grafana AWS integration
```

Backends:

```text
Mimir  metrics
Loki   logs
Tempo  traces
```

## 2. Trace propagation

W3C trace context flows through:

```text
Collector envelope.trace_context
→ Realtime Processor
→ broadcast-signal SQS MessageAttributes
→ Coordinator
→ broadcast-jobs SQS MessageAttributes
→ Worker
```

Kinesis batches can combine several producer traces. The Processor uses one valid context as parent and attaches other unique contexts as span links.

Per-connection `postToConnection` spans are disabled by default to prevent enormous trace volume. High-level Worker fan-out spans and structured result logs remain available.

## 3. Collector metrics

Counters:

```text
collector_sse_events_received_total
collector_sse_parse_failures_total
collector_events_invalid_total
collector_events_canary_dropped_total
collector_events_sampled_out_total
collector_events_kept_total
collector_sse_reconnects_total
collector_batch_flushes_total
collector_kinesis_records_sent_total
collector_kinesis_records_failed_total
collector_kinesis_retry_attempts_total
collector_kinesis_partial_failures_total
```

Histograms:

```text
collector_batch_size
collector_batch_flush_duration
collector_kinesis_put_records_duration
collector_event_to_kinesis_latency
```

Gauges:

```text
collector_sse_connection_state
collector_buffer_size
collector_seconds_since_last_event
collector_seconds_since_last_successful_put
collector_process_uptime_seconds
```

## 4. Realtime Processor metrics

```text
realtime_processor_batches_total
realtime_processor_records_received_total
realtime_processor_records_decoded_total
realtime_processor_records_valid_total
realtime_processor_records_skipped_total
realtime_processor_records_failed_total
dynamodb_aggregate_updates_total
dynamodb_aggregate_update_failure_total
broadcast_signals_sent_total
broadcast_signal_failure_total
broadcast_signal_skipped_total
processor_batch_duration_ms
dynamodb_update_batch_duration_ms
broadcast_signal_duration_ms
```

## 5. Coordinator metrics

```text
broadcast_coordinator_signals_total
broadcast_coordinator_signals_failed_total
broadcast_snapshots_created_total
broadcast_jobs_created_total
broadcast_jobs_planned_total
broadcast_manifests_created_total
broadcast_coordinator_duplicates_total
broadcast_coordinator_duration_ms
broadcast_coordinator_aggregate_read_duration_ms
broadcast_coordinator_payload_build_duration_ms
```

## 6. Worker metrics-lite surface

Only these instruments currently create Mimir series:

```text
websocket_delivery_total{result=success|failure|gone|retry_exhausted}
broadcast_worker_jobs_total{result=success|failed}
event_to_dashboard_latency_ms
```

All other Worker instruments are intentional no-ops. Their values remain available in:

- `broadcast_worker_job_completed` logs;
- failure logs;
- Tempo spans;
- native Lambda/SQS/API Gateway metrics.

This reduction was applied after Grafana Cloud ingestion reached roughly 1,500 samples/s and started rejecting metrics.

Worker freshness buckets:

```text
0, 2000, 4000, 6000, 8000, 10000, 15000, 30000 ms
```

Non-stale jobs force-flush metrics and traces at invocation end. Stale jobs skip metric flush but still flush traces.

## 7. Native AWS metrics to retain

### Kinesis

```text
IncomingRecords
IncomingBytes
WriteProvisionedThroughputExceeded
ReadProvisionedThroughputExceeded
GetRecords.IteratorAgeMilliseconds per consumer
```

Shard-level metrics are currently disabled in Terraform and should be enabled before capacity changes.

### Lambda

```text
Invocations
Errors
Duration p50/p95/max
Throttles
ConcurrentExecutions
IteratorAge for stream consumers
DeadLetterErrors where applicable
```

### SQS

```text
ApproximateNumberOfMessagesVisible
ApproximateNumberOfMessagesNotVisible
ApproximateAgeOfOldestMessage
NumberOfMessagesSent
NumberOfMessagesReceived
NumberOfMessagesDeleted
DLQ visible count
```

### DynamoDB

```text
ConsumedReadCapacityUnits
ConsumedWriteCapacityUnits
ReadThrottleEvents
WriteThrottleEvents
SuccessfulRequestLatency
SystemErrors
UserErrors
```

### API Gateway WebSocket

```text
ConnectCount
MessageCount
IntegrationError
ClientError
ExecutionError
Latency
IntegrationLatency
```

### Firehose

```text
DeliveryToS3.Success
DeliveryToS3.DataFreshness
DeliveryToS3.Records
DeliveryToS3.Bytes
ThrottledRecords
```

## 8. Primary dashboards

### Pipeline health

- Collector connected state;
- source events retained/sampled;
- Kinesis incoming rate and IteratorAge;
- Processor errors/duration;
- aggregate update failures;
- signal queue age.

### Broadcasting

- Coordinator max/p95 duration;
- snapshots/manifests/jobs created;
- jobs queue age and visible count;
- Worker Lambda duration/max and concurrency;
- `websocket_delivery_total` outcome rate;
- freshness p95 and SLO compliance below 10 seconds;
- API Gateway 429/5xx indicators.

### Historical

- Firehose success and DataFreshness;
- Glue job success/duration;
- Athena scanned bytes and failures.

## 9. Recommended PromQL

Freshness p95:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(event_to_dashboard_latency_ms_milliseconds_bucket[5m])
  )
)
```

SLO success ratio under 10 seconds:

```promql
sum(rate(event_to_dashboard_latency_ms_milliseconds_bucket{le="10000"}[5m]))
/
sum(rate(event_to_dashboard_latency_ms_milliseconds_count[5m]))
```

Delivery failures:

```promql
sum by (result) (rate(websocket_delivery_total[5m]))
```

Use the exact exported metric suffixes visible in Grafana Explore because OTel/Prometheus unit translation can add `_milliseconds`.

## 10. Structured logs

High-value messages include:

```text
collector_started
collector_batch_flushed
realtime_processor_batch_processed
broadcast_signal_sent
broadcast_coordinator_signal_completed
broadcast_coordinator_duplicate_signal_skipped
broadcast_worker_job_completed
broadcast_worker_job_failed
websocket_chunk_delivery_failed
broadcast_worker_invocation_completed
```

Worker completion logs contain durations, counts, queue delay, retries, stale status and OTel flush duration.

## 11. Alerting recommendations

Warning:

- Kinesis IteratorAge increasing for 5 minutes;
- signal/jobs queue age above one broadcast interval;
- Worker max duration above broadcast interval;
- freshness p95 above 8 seconds;
- any Grafana `rate_limited` rejection;
- Firehose DataFreshness above 450 seconds for 5 minutes.

Critical:

- freshness SLO ratio below 95%;
- API Gateway 429 sustained;
- DLQ visible messages;
- Kinesis write/read throttling;
- no successful Collector put for more than 60 seconds;
- Firehose DataFreshness above 600 seconds for 5 minutes.
