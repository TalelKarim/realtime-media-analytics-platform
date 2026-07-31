# Load Testing

## 1. Goals

The load suite validates four independent dimensions:

1. connection establishment;
2. connection stability and heartbeat;
3. broadcast delivery volume;
4. source-to-client freshness.

The current script validates the first three. Client-side freshness is the next required addition.

## 2. Generator architecture

Local/corporate testing could not open the requested 5,000 sockets; AWS logs showed only about 1,193 successful connects with no API errors. The bottleneck was the generator/network path, not the platform.

A dedicated EC2 generator is therefore used:

```text
Amazon Linux 2023
m7i.4xlarge
16 vCPU
64 GiB RAM
50 GiB gp3
public subnet us-east-1a
k6
```

The custom WebSocket URL is stable across infrastructure recreation.

## 3. k6 metrics

Current custom metrics include connection attempts/success/failure, messages sent/received, ping latency and active connections. `ws_active_connections = -1` in historical output is a script accounting bug, not negative AWS connections.

The total received message count is cumulative across all clients:

```text
one broadcast delivered to 10,000 sockets
→ approximately 10,000 received messages
```

For the 10,000-connection test:

```text
2,544,698 received / 10,000 clients
≈ 254.5 frames per connection
```

## 4. Validated milestones

| Milestone | Main result |
|---|---|
| 100 connections | p95 freshness about 2.4–3 s; no durable backlog |
| 500 connections V1 | exposed API Gateway throttle and single-Broadcaster limits |
| 2,000 connections V2 | stable validation milestone |
| 5,000 connections / 6 shards | 100% opened, no error, freshness p95 about 6.8–7 s |
| 5,000 connections / 10 shards | lower real Worker fan-out spans, about 1.8–2.4 s |
| 10,000 connections / 10 shards | 100%, 0 error, ~20 min, 2.54M frames, backend p95 ~7 s |

The 10,000 test was primarily `global` topic. It validates the connection-shard fan-out path, not the worst-case multi-topic/chunking path.

## 5. Recommended command shape

```bash
ulimit -n 250000

k6 run \
  -e WS_URL="wss://stream-websocket.talelkarimchebbi.com" \
  -e CONNECTIONS=10000 \
  -e HOLD_SECONDS=1200 \
  -e RAMP_SECONDS=300 \
  -e HEARTBEAT_SECONDS=30 \
  load-tests/websocket-connections.js
```

The exact environment names must match the current script before execution.

## 6. Multi-topic scenario

Each connection should keep `global` and subscribe to two deterministic rotating topics, for example:

```text
top_pages
wiki:frwiki
wiki:enwiki
wiki:commonswiki
wiki:wikidatawiki
wiki:dewiki
wiki:eswiki
wiki:itwiki
wiki:ptwiki
wiki:ruwiki
wiki:arwiki
wiki:jawiki
```

For 5,000 clients and two extra subscriptions, expect about 10,000 subscription acknowledgements.

Multi-topic validation must check:

- payload size distribution;
- chunks per connection;
- equal-cursor chunk acceptance;
- per-topic correctness;
- client freshness using the least-fresh topic in each chunk;
- Worker memory and serialization time.

## 7. Thresholds to enforce

```javascript
thresholds: {
  ws_connection_failures: ['count==0'],
  unexpected_disconnects: ['count==0'],
  client_event_to_receive_latency_ms: ['p(95)<10000'],
}
```

Also assert a minimum number of broadcast frames per connection. A test that opens sockets but receives no business messages must fail.

## 8. Ground-truth client freshness

For every `stats.batch_update`:

```text
client_receive_time
-
minimum latest_event_timestamp_ms across updates
```

Record the value in a k6 `Trend`. Synchronize the EC2 clock first.

Compare in the same test window:

```text
Worker backend freshness p95
k6 client receive freshness p95
```

A small stable delta validates the backend metric. A large delta identifies API Gateway/network/client delay or an instrumentation error.

## 9. Test taxonomy

### Ingestion test

Raise sampling/input volume and observe Collector/Kinesis/Processor capacity independently from WebSockets.

### Fan-out test

Preload/hold many WebSocket connections and measure Worker/API Gateway behavior.

### End-to-end test

Run real Wikimedia ingestion while clients calculate source-to-receive latency.

### Resilience test

Inject/reproduce:

- Worker throttling;
- API Gateway 429;
- Coordinator delay;
- SQS backlog;
- Lambda error and message retry;
- expired/Gone connections;
- stale job skipping;
- Collector restart;
- Kinesis consumer lag.

### Soak test

Run for several hours to detect memory/socket leaks, metric cardinality, TTL cleanup issues and gradual backlog.

## 10. Evidence to capture

For each formal test save:

```text
Git commit/tag
Terraform variables
connection shard count
Worker concurrency and threads
API Gateway limits
k6 command
k6 summary JSON
CloudWatch/Grafana screenshots
SQS queue age
Lambda duration/throttles
API Gateway errors
freshness backend and client
```

A capacity claim without the exact configuration is not reproducible.
