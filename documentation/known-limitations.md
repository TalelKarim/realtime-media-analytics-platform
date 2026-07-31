# Known Limitations and Senior Architecture Critique

## Priority 0 — correctness

### 1. Aggregate updates are not fully idempotent

Kinesis/Lambda is at-least-once. The Processor performs many independent DynamoDB `ADD` updates. If some succeed and a later update fails, the entire Kinesis batch can be replayed and successful increments can be applied again.

Impact: inflated realtime counters and alert counters.

Remediation options:

- event-id deduplication with bounded retention;
- transactional/idempotency ledger per batch/event;
- stateful streaming engine with checkpointed state;
- redesign writes so replay is deterministic and overwrite-based.

### 2. Freshness watermark is not atomically tied to aggregate state

The signal transports timestamps while the Coordinator separately reads mutable aggregate items. The snapshot can be slightly newer or older than the timestamp describes.

Remediation: persist a watermark/version with each aggregate update and derive the snapshot reference from the exact read model version, or build snapshots directly from a compacted/versioned state stream.

## Priority 1 — SLO proof

### 3. No client-side freshness ground truth

The current p95 ends at `postToConnection` success. Add k6 receive-time and Playwright DOM-time metrics.

### 4. Histogram observes successful chunks only

Always pair freshness with delivery outcomes. Define a composite SLI that treats missing delivery as failure rather than omitting it.

### 5. Clock anomalies are hidden

Negative freshness is clamped to zero and invalid source timestamps can fall back to now. Reject or separately count invalid timestamps.

## Priority 1 — resilience

### 6. Single Collector

One Collector avoids duplicates but creates brief outage and in-memory-buffer loss during restart. Decide whether source ingestion is best-effort or implement active/passive leadership and deduplication.

### 7. Alert watermark is pragmatic

A fixed evaluation delay does not formally close event-time windows. Late events can arrive after alert evaluation. Define allowed lateness and reevaluation behavior.

### 8. Old broadcast jobs are not guaranteed delivery

This is intentional Latest State Wins. It must remain explicit in product requirements.

## Priority 2 — capacity evidence

### 9. One Kinesis shard lacks quantitative proof

Shard-level metrics are disabled. Enable and retain capacity evidence before claiming headroom.

### 10. 10,000 test is mainly global-topic fan-out

Repeat with realistic multi-topic subscriptions, chunking and true source-to-client freshness.

### 11. Fan-out remains O(N)

At hundreds of thousands of clients, API Gateway Management API calls, cost and quotas may dominate. Evaluate dedicated socket-owning infrastructure.

## Priority 2 — maintainability

### 12. Legacy V1 Broadcaster remains

The source package, Terraform Lambda, IAM role, log group and Promtail mapping are dead/migration residue.

### 13. Transitional `websocket_subscriptions` table remains

Lifecycle handlers still dual-write it even though V2 recipient discovery uses `websocket_connections` GSI. This doubles write/delete complexity.

### 14. Rollout flags and misspelled names remain

Examples:

```text
enhaned_broadcasting_enabled
braodcasting_enhanced_event_sources.tf
Phase 1 / Phase 2 comments and scripts
```

Final V2 should remove the rollout switches, hard-enable the active topology and rename files/variables.

### 15. Build artifacts are repository-coupled

Terraform packages from `.build`. This is practical for Terraform Cloud but requires deterministic Linux-target builds and strict discipline to keep source and artifacts identical.

## Overall verdict

The V2 broadcasting design is technically credible and has strong observed behavior at 10,000 sockets. Its largest remaining gaps are not basic scalability; they are correctness guarantees, independent SLO proof and removal of migration residue.
