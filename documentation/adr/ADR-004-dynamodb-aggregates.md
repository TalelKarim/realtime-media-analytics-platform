# ADR-004 — Store Realtime Materialized Aggregates in DynamoDB

- Status: Accepted

## Decision

Persist minute read models in `realtime_aggregates` using atomic `UpdateItem ADD` and TTL.

## Rationale

The dashboard needs precomputed low-latency counters, not scans of raw events.

## Consequences

Positive: serverless scaling, atomic increments, predictable keys and fast Coordinator reads.

Negative: many item updates, eventual multi-item snapshot consistency and replay double-count risk without idempotency.
