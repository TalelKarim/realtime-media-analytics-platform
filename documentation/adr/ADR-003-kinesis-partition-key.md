# ADR-003 — Partition Kinesis by `event_id`

- Status: Accepted

## Context

A hot wiki key could overload one shard. Realtime aggregates do not require strict wiki/global processing order.

## Decision

Use normalized `event_id` as `PartitionKey`.

## Consequences

Positive: high-cardinality distribution and low hot-shard risk.

Negative: no order guarantee by wiki and no global order when multiple shards exist.

## Validity condition

Processing must remain order-insensitive: atomic additions, event-time window assignment and min/max bounds.
