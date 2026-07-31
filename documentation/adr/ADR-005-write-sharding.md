# ADR-005 — Write-shard Hot Aggregate Keys

- Status: Accepted

## Decision

Split global activity, top wikis and top pages across ten deterministic DynamoDB write shards.

## Consequences

Positive: reduces hot partition contention under concurrent processors.

Negative: Coordinator must read/merge multiple shards, increasing read concurrency and snapshot build complexity.

## Notes

Connection shards are unrelated to aggregate write shards.
