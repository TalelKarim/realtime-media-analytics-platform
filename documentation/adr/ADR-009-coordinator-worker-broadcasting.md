# ADR-009 — Replace the Single Broadcaster with Coordinator and Sharded Workers

- Status: Accepted

## Context

V1 scanned every connection and performed all aggregate reads and fan-out in one Lambda. Load exposed long fan-out, API throttling and backlog.

## Decision

Create immutable topic snapshots once, publish a manifest, then send one job per connection shard to parallel Workers querying a GSI.

## Consequences

Positive: horizontal fan-out, no table Scan, reused payloads, isolated shard failures and testable scaling.

Negative: more resources, jobs, reads and cursor logic; total post calls remain O(N).
