# ADR-010 — Adopt Latest State Wins for WebSocket Delivery

- Status: Accepted

## Decision

Maintain a conditional `LATEST` pointer. Workers check it before loading and immediately before fan-out; stale jobs succeed without sending. The frontend rejects older cursors.

## Rationale

The product is a live dashboard. A recent state is more valuable than replaying every intermediate snapshot.

## Consequences

Positive: bounded recovery from backlog and lower obsolete delivery work.

Negative: intermediate states are intentionally skipped; this model cannot serve event-ledger requirements.
