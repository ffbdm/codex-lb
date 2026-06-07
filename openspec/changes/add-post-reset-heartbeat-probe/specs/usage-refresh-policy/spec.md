## ADDED Requirements

### Requirement: Post-reset heartbeat probes nudge stale upstream windows

The usage refresh scheduler MUST detect refreshed account quota windows whose `reset_at` remains expired across repeated refresh observations. After the same account, window, and expired reset timestamp has been observed at least three times, the scheduler MUST send one real lightweight upstream `responses.create` heartbeat pinned to that account, separate from the manual Force probe endpoint, MUST mark that heartbeat as sent so it is not repeated for the same reset timestamp, and MUST record the attempt in Request Logs with a system source that identifies the automatic post-reset heartbeat.

#### Scenario: Stale primary reset triggers one heartbeat
- **GIVEN** an account refresh returns a primary window with `reset_at` in the past
- **AND** the same account/window/reset timestamp has been observed on two prior refreshes without advancing
- **WHEN** the current refresh records the third observation
- **THEN** the scheduler sends one non-streaming heartbeat request with input `hi`, `store=false`, and a small output cap pinned to that account
- **AND** the heartbeat is marked sent for that account/window/reset timestamp
- **AND** a Request Logs entry is recorded with `source` set to `post_reset_heartbeat` and `transport` set to `http`

#### Scenario: Already-sent heartbeat is not repeated
- **GIVEN** a heartbeat has already been sent for an account/window/reset timestamp
- **WHEN** later refreshes keep returning that same expired reset timestamp
- **THEN** the scheduler does not send another heartbeat for that tuple

#### Scenario: Window advance clears stale observations
- **GIVEN** a stale reset observation exists for an account/window
- **WHEN** a later refresh returns no reset timestamp or a reset timestamp in the future
- **THEN** the stale observation state for that account/window is cleared
