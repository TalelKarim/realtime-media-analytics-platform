# Scaling and Capacity

## 1. Independent scaling planes

The platform has distinct capacity dimensions:

```text
Wikimedia ingestion
Kinesis stream throughput
Realtime aggregate write throughput
Coordinator control-plane throughput
WebSocket fan-out compute
API Gateway Management API throughput
historical delivery and ETL
```

A successful 10,000-socket test validates primarily the fan-out plane, not Kinesis input capacity.

## 2. Collector

Current: one 256/512 Fargate task.

The single task avoids duplicate SSE ingestion but creates a restart gap. Horizontal Collector scaling requires leader election or event-id deduplication because two tasks would receive the same public stream.

## 3. Kinesis

Current: one provisioned shard, 48-hour retention, `PartitionKey=event_id`.

A larger shard count increases:

- write capacity;
- aggregate consumer concurrency;
- Alert Processor concurrency;
- Firehose source throughput.

Every consumer reads all shards. With four shards, each Lambda consumer can process up to roughly four shard batches concurrently at the default parallelization factor of one.

Consequences:

- more concurrent DynamoDB updates;
- more signals generated in parallel;
- no global order across shards;
- a larger chance of timestamp/state misalignment;
- more total read competition unless Enhanced Fan-Out is used.

Enable shard-level metrics and measure before scaling.

## 4. DynamoDB aggregate write sharding

High-contention global and top-N models are divided across ten write shards. Coordinator reads fan in across those shards.

Trade-off:

```text
more write shards
→ lower hot-partition risk
→ more parallel reads and merge work
```

Wiki-specific exact counters are not write-sharded because the wiki key already distributes the workload.

## 5. Signal coalescing

The Processor creates a broadcast sequence every three seconds. Similar batches in the same sequence/window/topic combination share a FIFO deduplication ID.

This controls Coordinator frequency but is not a strict debounce that always retains the last timestamp in the window. It usually retains the first accepted signal while later aggregate writes still occur.

## 6. Coordinator

All signals use one FIFO MessageGroupId, so Coordinator processing is serial.

Capacity condition:

```text
steady Coordinator duration
< average interval between accepted signals
```

If duration is longer, the signal queue accumulates. A future optimization could coalesce pending state or trigger from a compacted pointer instead of processing every accepted signal.

## 7. Connection sharding

Current: 10 logical shards.

```text
connections per Worker ≈ active connections / shard count
```

Examples:

| Connections | Shards | Approx. recipients/job |
|---:|---:|---:|
| 5,000 | 10 | 500 |
| 10,000 | 10 | 1,000 |
| 10,000 | 12 | 833 |
| 100,000 | 100 | 1,000 |

Increasing shards reduces each Worker duration but increases:

- SQS jobs per broadcast;
- Lambda invocations;
- concurrent DynamoDB/API Gateway calls;
- repeated manifest/LATEST reads;
- telemetry environments;
- burst pressure.

## 8. Worker concurrency

Current limits:

```text
reserved Lambda concurrency = 20
event source maximum concurrency = 20
threads per Worker = 16
HTTP pool per Worker = 24
```

With ten active shard groups, the normal maximum is ten concurrent Workers, giving approximately 160 in-flight post calls. Twenty Workers would allow approximately 320.

Actual throughput is bounded by the minimum of:

```text
active MessageGroupIds
available Lambda concurrency
event source max concurrency
API Gateway throttle/quota
DynamoDB capacity
network and runtime resources
```

## 9. API Gateway fan-out complexity

The architecture requires one Management API call per delivered connection/chunk.

```text
work = O(active connections × chunks per connection)
```

Connection sharding parallelizes this work; it does not make it multicast.

At much larger scale, evaluate whether a dedicated WebSocket fleet or managed pub/sub product that owns sockets directly would be more efficient.

## 10. Multi-topic impact

A connection with several topics increases:

- snapshot topics loaded;
- JSON serialization work;
- payload bytes;
- chunk count;
- total `postToConnection` calls when chunking occurs.

Capacity tests must include realistic topic distributions, not only `global`.

## 11. Latest State Wins stability

If Worker duration temporarily exceeds the broadcast interval, FIFO and two `LATEST` checks skip obsolete jobs. This prevents an unbounded data-plane backlog.

It does not remove control-plane work already performed by the Coordinator, and it intentionally drops intermediate dashboard states.

## 12. Scaling decision checklist

Before increasing Kinesis shards:

- enable shard metrics;
- verify write/read throttling and IteratorAge;
- test DynamoDB concurrency;
- review aggregate idempotence;
- review alert late-event handling.

Before increasing connection shards:

- measure Worker duration distribution;
- check jobs queue age;
- check API Gateway 429;
- check Lambda concurrency;
- check Grafana metric ingestion;
- run multi-topic tests.

Before moving Workers to ECS:

- prove Lambda cost/concurrency/cold-start is the bottleneck;
- model permanent task cost;
- design SQS long polling, visibility extension, shutdown and autoscaling;
- remember ECS does not reduce the total Management API calls.
