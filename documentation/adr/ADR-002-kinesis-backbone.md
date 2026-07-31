# ADR-002 — Use Kinesis Data Streams as the Event Backbone

- Status: Accepted

## Context

The same normalized event is needed by realtime aggregation, alerting and historical delivery. Consumers require independent speed and failure isolation.

## Decision

Publish once to Kinesis and attach independent consumers.

## Consequences

Positive: fan-out, 48-hour replay buffer, backpressure isolation, native Lambda/Firehose integration.

Negative: shard capacity planning, at-least-once replay, per-shard ordering only and shared read throughput without Enhanced Fan-Out.

## Current scope

Dev uses one provisioned shard. This is a cost choice, not a proven production capacity claim.
