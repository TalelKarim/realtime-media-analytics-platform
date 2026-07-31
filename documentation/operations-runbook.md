# Operations Runbook

## 1. Deployment model

Infrastructure is managed by Terraform Cloud from the repository branch. Lambda packages are built under `.build/lambdas` before Terraform packages them.

```bash
./scripts/build_all.sh
```

The frontend deploy workflow runs on branch `v2` changes under `frontend/dashboard/**`.

Do not use local `terraform output` as the operating workflow for this project.

## 2. Post-deploy smoke test

### WebSocket connection

Use the stable endpoint:

```text
wss://stream-websocket.talelkarimchebbi.com
```

Verify:

- `$connect` access log status 200;
- an item appears in `websocket_connections`;
- `connection_shard` is present;
- topics contains `global`;
- TTL is in the future.

Exact table count:

```bash
python3 - <<'PYCODE'
import boto3

client = boto3.client('dynamodb', region_name='us-east-1')
table = 'realtime-media-analytics-dev-websocket-connections'
total = 0
for page in client.get_paginator('scan').paginate(
    TableName=table,
    Select='COUNT',
    ConsistentRead=True,
):
    total += page['Count']
print(total)
PYCODE
```

### Subscription

Send:

```json
{"action":"subscribe","topic":"wiki:frwiki"}
```

Expect `subscription.ack` and the connection topic list to update.

### Broadcast

Verify in order:

1. Realtime Processor logs `broadcast_signal_sent`;
2. signal queue receives/deletes messages;
3. Coordinator creates snapshot/manifest and advances `LATEST`;
4. ten shard jobs are sent;
5. Worker logs success or stale skip;
6. browser receives `stats.batch_update`.

## 3. Queue inspection

Primary queues:

```text
realtime-media-analytics-dev-broadcast-signal.fifo
realtime-media-analytics-dev-broadcast-jobs.fifo
```

DLQs:

```text
realtime-media-analytics-dev-broadcast-signal-dlq.fifo
realtime-media-analytics-dev-broadcast-jobs-dlq.fifo
```

Investigate when visible count or oldest age rises continuously. A short temporary jobs backlog can be absorbed by stale skipping; a continuously rising signal backlog indicates Coordinator throughput is insufficient.

## 4. Freshness incident

When p95 exceeds 10 seconds, inspect in this order:

1. Kinesis `IteratorAgeMilliseconds`;
2. Realtime Processor max duration/errors;
3. signal queue oldest age;
4. Coordinator max duration and duplicate rate;
5. jobs queue oldest age;
6. Worker max duration/throttles/concurrency;
7. `websocket_delivery_total` failures/retry exhaustion;
8. API Gateway 429/5xx;
9. Grafana `rate_limited` ingestion;
10. k6/client freshness if available.

Do not conclude that Grafana throttling caused business latency. It can distort the displayed metric; compare AWS logs/timings and client measurements.

## 5. API Gateway 429

Symptoms:

- `TooManyRequestsException`;
- Worker retries rise;
- fan-out duration and freshness rise;
- jobs queue can accumulate.

Actions:

- verify stage rate/burst limits;
- verify account quota;
- reduce Worker concurrency or per-Worker threads if burst is too sharp;
- increase connection shards only after checking total parallel `postToConnection` pressure;
- request a quota increase where justified.

Remember:

```text
more connection shards
→ fewer recipients per Worker
→ more simultaneous Workers
→ same total number of calls, potentially larger burst
```

## 6. Stale jobs

Short Worker executions that only check `LATEST` are expected stale skips. They are healthy when newer jobs complete real fan-out.

Investigate when a connection shard almost never completes a real job. Check FIFO group backlog, Worker errors and shard distribution.

## 7. Gone connections

A 410/Gone response means API Gateway no longer owns that connection. The Worker should delete the connection item. Cleanup failure is logged but does not replay the complete fan-out.

TTL is a final safety net and is not immediate.

## 8. Coordinator duplicate/idempotency behavior

A repeated signal can be skipped because:

- SQS deduplicated it before delivery;
- Coordinator idempotency record is active/completed.

Check `source_signal_id`, idempotency item status and lease expiry. A failed/expired lease can be taken over.

## 9. DLQ handling

Before redrive:

1. read the message body;
2. identify whether the problem is permanent contract corruption or temporary infrastructure;
3. fix the root cause;
4. ensure replay cannot create harmful duplicate aggregate updates;
5. redrive a controlled subset;
6. verify queue age and application logs.

Broadcast jobs are Latest State Wins. Old DLQ jobs are usually obsolete and should not be blindly redriven after a newer manifest exists.

## 10. Collector incident

Check:

```text
collector_sse_connection_state
collector_seconds_since_last_event
collector_seconds_since_last_successful_put
ECS desired/running task count
ECS logs
NAT/network path
Kinesis PutRecords errors
```

A task restart can lose the in-memory buffer and create a brief ingestion gap. Wikimedia source replay is not guaranteed by the current collector.

## 11. Kinesis incident

Check:

```text
IncomingRecords/Bytes
WriteProvisionedThroughputExceeded
ReadProvisionedThroughputExceeded
IteratorAge per consumer
```

Do not increase shard count based only on WebSocket connection tests. Ingestion and fan-out are different capacity planes.

## 12. Firehose incident

Healthy dev behavior can show DataFreshness around 300–450 seconds because the configured buffer interval is 300 seconds.

Warning: above 450 seconds for 5 minutes.

Critical: above 600 seconds for 5 minutes or continuously increasing.

Also inspect delivery success, KMS/IAM errors and Firehose logs.

## 13. Grafana ingestion throttling

The Worker uses a metrics-lite surface. If `rate_limited` returns:

- verify no legacy high-cardinality Worker package was redeployed;
- confirm export interval is 60 seconds;
- confirm stale jobs skip metric flush;
- inspect the number of active Lambda environments;
- prefer native AWS metrics/logs for diagnostics instead of adding many histograms.

## 14. Safe destroy/recreate

The custom WebSocket domain is in the same workspace. Destroy causes downtime and removes the current API/domain mapping; apply recreates the same FQDN.

After recreation:

- validate ACM and Route 53;
- open a new connection;
- do not expect old connection IDs to remain valid;
- rerun smoke tests;
- verify frontend local storage replaced any old generated `execute-api` URL with the stable hostname.
