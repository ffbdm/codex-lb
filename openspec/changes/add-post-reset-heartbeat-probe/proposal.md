## Why

Some upstream quota windows reset in ChatGPT while `/wham/usage` continues reporting the previous reset timestamp for one or more refresh cycles. This can happen even when the account is not explicitly `rate_limited` or `quota_exceeded`; the visible symptom is a 5h or weekly window whose `reset_at` is already in the past or does not roll forward after repeated usage refreshes. A real lightweight request against the account can nudge the upstream limiter to start the next window, but the existing Force probe is manual and tied to operator action.

## What Changes

- Add an automatic post-reset heartbeat path during usage refresh.
- For each account and usage window (`primary`, `secondary`, and monthly where applicable), persist observations when the refreshed upstream payload still reports an expired `reset_at`.
- After the same expired reset has been observed for three refreshes, send one real non-streaming `responses.create` heartbeat pinned to that account with input `hi`, `store=false`, and a small output cap.
- Mark the heartbeat as sent for that account/window/reset tuple so the scheduler does not spend quota repeatedly if upstream remains stale.
- Record each heartbeat attempt in Request Logs with `source=post_reset_heartbeat` so operators can audit the automatic call.
- Keep the existing manual Force probe endpoint unchanged.

## Impact

- Stale post-reset windows can recover without an operator clicking Force probe.
- The automatic request consumes a tiny amount of upstream quota, but only after repeated evidence that the window has not rolled forward and only once per observed reset timestamp.
- The behavior is scoped to the usage refresh scheduler; public OpenAI-compatible proxy APIs do not change.
