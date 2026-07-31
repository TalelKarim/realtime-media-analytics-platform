# Freshness and SLO — Exact Meaning and Reliability

## 1. Executive definition

The current metric `event_to_dashboard_latency_ms` is a real backend end-to-end measurement, but its name is broader than what it proves.

It measures:

> Time from the Wikimedia source event timestamp to successful return of the Worker `postToConnection` call for a WebSocket chunk.

It does not yet measure:

> Time until the browser receives, parses and visibly renders the update.

A more precise name would be:

```text
source_event_to_apigw_post_success_latency_ms
```

## 2. Start timestamp

The Collector copies Wikimedia:

```text
raw_event.meta.dt
```

into:

```text
envelope.occurred_at
```

The Realtime Processor converts it to `source_event_timestamp_ms`. If a future contract provides an explicit numeric source timestamp, it is preferred; otherwise `occurred_at` is used.

This means the timer includes:

```text
Wikimedia → Collector transport
Collector buffer
Kinesis delivery/batching
Processor execution
DynamoDB updates
SQS signal wait
Coordinator reads/builds
SQS job wait
Worker preparation
fan-out and postToConnection
```

## 3. Timestamp propagation

The Processor computes:

```text
oldest/latest per aggregation window
oldest/latest per topic per aggregation window
```

The topic map is transferred in the SQS signal. The Coordinator attaches the matching pair to every immutable topic snapshot and repeats it in the manifest reference. The Worker embeds those values in every topic update.

## 4. Chunk reference

A chunk can contain several topics. Suppose:

```text
global latest       = 16:00:01.800
wiki:frwiki latest  = 16:00:01.200
wiki:enwiki latest  = 16:00:01.600
```

The Worker uses:

```text
latest_reference_ms = min(01.800, 01.200, 01.600)
                    = 01.200
```

This is conservative: the chunk is considered only as fresh as its least-fresh topic.

## 5. End timestamp

Immediately before each attempt, the Worker adds:

```text
server_send_attempt_at_ms
```

After `postToConnection` returns successfully, it captures an internal `success_at_ms` and records:

```text
event_to_dashboard_latency_ms
= max(0, success_at_ms - latest_reference_ms)
```

The metric is emitted once per successfully delivered chunk.

## 6. What p95 means

A p95 near 7 seconds means:

> Over the dashboard query window, approximately 95% of successful WebSocket chunks were accepted by API Gateway while the least-fresh topic in the chunk had a source timestamp about 7 seconds old or less.

It does not mean:

> 95% of source events were displayed in browsers within 7 seconds.

It is weighted by delivered chunks/connections. One broadcast to 10,000 single-chunk clients contributes about 10,000 observations.

## 7. Histogram accuracy

Worker buckets are:

```text
0, 2s, 4s, 6s, 8s, 10s, 15s, 30s
```

Grafana/Prometheus `histogram_quantile` interpolates within buckets. A displayed p95 of 7.1 seconds is an approximation between the 6- and 8-second boundaries, not a raw percentile calculated from every exact millisecond value.

The exact 10-second boundary is intentionally preserved for the SLO.

## 8. Why the metric is operationally useful

It responds to real degradation in:

- Collector/Kinesis delay;
- Kinesis IteratorAge;
- DynamoDB update/read time;
- SQS backlog;
- Coordinator duration;
- stale work;
- Worker preparation;
- fan-out duration;
- API Gateway throttling/retries.

It is therefore not decorative.

## 9. Main semantic weakness: timestamp and snapshot state are not atomic

The Coordinator receives timestamp bounds in SQS and separately reads mutable counters from `realtime_aggregates`.

```text
signal timestamp
+
later DynamoDB read
→ snapshot
```

These two pieces were not created in one atomic transaction.

### Pessimistic case

```text
Batch A updates data and sends signal timestamp A.
Batch B updates newer data but its similar signal is FIFO-deduplicated.
Coordinator reads data A+B but only has timestamp A.
```

The payload is newer than declared, so measured freshness looks worse than reality.

### Optimistic case

DynamoDB aggregate reads are eventually consistent and multiple items are read separately. A read can theoretically return state older or mixed relative to the signal timestamp. The timestamp can then claim freshness that all payload counters do not yet fully represent.

V2 fixes the later part of the path: after a snapshot is created, payload and timestamps are immutable and every Worker reads the same pair. It does not make the initial aggregate read and timestamp watermark atomic.

## 10. Other limitations

### Successful deliveries only

Failures, Gone connections and retry-exhausted chunks do not enter the freshness histogram. Always read freshness with `websocket_delivery_total`.

### Negative values are clamped

The Worker uses `max(0, ...)`. A future source timestamp or clock anomaly becomes zero instead of an explicit invalid sample. A better implementation would log/count clock-skew errors and skip the observation.

### Source timestamp parsing

The Processor parser falls back to current time for invalid ISO timestamps. The Collector checks presence but not strict parseability. Invalid source time can therefore appear artificially fresh.

### Chunk semantics

The metric is per chunk, not “complete connection update.” A connection receiving three chunks contributes three observations. A separate last-chunk freshness would be useful for multi-topic clients.

### Grafana throttling

When Grafana Cloud rejected metric samples, the observed population was incomplete. The metrics-lite patch greatly reduced this issue, but `rate_limited` must remain zero during formal SLO tests.

## 11. Correct documentation statement

Use:

> Backend push freshness p95 is about 7 seconds: 95% of successfully delivered WebSocket chunks were accepted by API Gateway while the least-fresh topic timestamp in the chunk was about 7 seconds old or less.

Do not yet use:

> 95% of users see the update in 7 seconds.

## 12. Independent ground truth to add

k6 should calculate on every received `stats.batch_update`:

```javascript
clientReceiveAtMs = Date.now();
latestReferenceMs = Math.min(
  ...message.updates
    .map((u) => Number(u.latest_event_timestamp_ms))
    .filter((v) => Number.isFinite(v) && v > 0)
);
clientFreshnessMs = clientReceiveAtMs - latestReferenceMs;
```

Metric:

```text
client_event_to_receive_latency_ms
```

Threshold:

```text
p(95) < 10000 ms
```

Also calculate:

```text
client_receive_at_ms - server_send_attempt_at_ms
```

This approximates post-attempt-to-client network/API Gateway delay, although `server_send_attempt_at_ms` precedes retries and is not the success timestamp.

## 13. Browser-visible validation

A Playwright synthetic should:

1. open the deployed dashboard;
2. wait for a new sequence;
3. capture the source timestamp from the received update;
4. wait until the corresponding value is present in the DOM;
5. calculate source-to-DOM latency;
6. fail when p95/individual threshold exceeds the SLO.

Real User Monitoring can later report the same measurement from actual browsers.

## 14. Clock requirements

Backend timestamps use AWS-managed runtime clocks. The k6 EC2 clock must be synchronized through chrony/NTP. Before formal validation:

```bash
timedatectl status
chronyc tracking
```

Clock offset directly biases source-to-client subtraction.

## 15. Reliability verdict

| Question | Verdict |
|---|---|
| Is it based on real source timestamps? | Yes |
| Does it cover nearly the whole backend? | Yes |
| Does it measure fan-out position per client/chunk? | Yes |
| Is it exact payload ground truth? | Not perfectly |
| Does it prove browser receipt? | No |
| Does it prove DOM display? | No |
| Is it useful for operations? | Strongly yes |
| Is it sufficient alone for the user SLO? | No |
