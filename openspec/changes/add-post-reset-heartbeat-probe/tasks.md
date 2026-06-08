## 1. Spec

- [x] 1.1 Add a usage-refresh-policy requirement for post-reset heartbeat probes after repeated stale reset observations.

## 2. Persistence

- [x] 2.1 Add a persisted table/model for per-account, per-window, per-reset heartbeat observation state.
- [x] 2.2 Add repository methods to record stale reset observations, mark heartbeats sent, and clear observations once a window advances.

## 3. Usage refresh behavior

- [x] 3.1 During usage refresh, record expired reset observations for refreshed windows regardless of account status.
- [x] 3.2 Send one separate real `hi` heartbeat after the configured observation threshold without changing the existing Force probe endpoint.
- [x] 3.3 Ensure already-sent heartbeats are not repeated for the same account/window/reset timestamp.
- [x] 3.4 Record heartbeat attempts in Request Logs with account, model, upstream status, source, and transport metadata.
- [x] 3.5 Dispatch automatic heartbeat requests outside the main usage refresh path so cache invalidation and status reconciliation are not blocked by upstream heartbeat latency.

## 4. Tests and validation

- [x] 4.1 Add unit tests for observation thresholding, one-shot heartbeat behavior, Request Logs recording, and clearing after reset advances.
- [x] 4.2 Add regression coverage proving heartbeat dispatch does not block refresh completion.
- [x] 4.3 Run focused usage updater tests and lint/type checks for touched modules.
