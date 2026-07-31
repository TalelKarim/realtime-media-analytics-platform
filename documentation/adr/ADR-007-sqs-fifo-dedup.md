# ADR-007 — Use SQS FIFO for Broadcast Control and Shard Jobs

- Status: Accepted

## Decision

Use two FIFO queues:

```text
broadcast-signal.fifo: sequential Coordinator signals
broadcast-jobs.fifo: ordered jobs per connection shard
```

Signal deduplication coalesces the same 3-second sequence/window/topic set. Job deduplication is `manifest_id + connection_shard`.

## Consequences

Positive: ordering, retry/DLQ, controlled fan-out and reduced duplicate work.

Negative: the first accepted signal timestamp can lag later aggregate writes; FIFO group count bounds parallelism.
