from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Protocol, cast

import aiohttp

from app.core import usage as usage_core
from app.core.auth.refresh import RefreshError
from app.core.balancer import (
    PERMANENT_FAILURE_CODES,
    QUOTA_EXCEEDED_COOLDOWN_SECONDS,
    account_status_for_permanent_failure,
)
from app.core.clients.http import lease_http_session
from app.core.clients.usage import UsageFetchError, fetch_usage
from app.core.config.settings import get_settings
from app.core.crypto import TokenEncryptor
from app.core.plan_types import coerce_account_plan_type
from app.core.upstream_proxy import ResolvedUpstreamRoute, UpstreamProxyRouteError, resolve_upstream_route
from app.core.usage.models import AdditionalRateLimitPayload, UsagePayload, UsageWindow
from app.core.utils.request_id import get_request_id
from app.core.utils.time import utcnow
from app.db.models import Account, AccountStatus, UsageHistory
from app.db.session import get_background_session
from app.modules.accounts.auth_manager import AccountsRepositoryPort, AuthManager
from app.modules.usage.additional_quota_keys import canonicalize_additional_quota_key
from app.modules.usage.repository import AdditionalUsageRepository

logger = logging.getLogger(__name__)


POST_RESET_HEARTBEAT_OBSERVATION_THRESHOLD = 3
POST_RESET_HEARTBEAT_MODEL = "gpt-5.5"
POST_RESET_HEARTBEAT_MAX_OUTPUT_TOKENS = 8
POST_RESET_HEARTBEAT_TIMEOUT_SECONDS = 30.0
POST_RESET_HEARTBEAT_CONNECT_TIMEOUT_SECONDS = 10.0


class UsageRepositoryPort(Protocol):
    async def latest_entry_for_account(
        self,
        account_id: str,
        *,
        window: str | None = None,
    ) -> UsageHistory | None: ...

    async def record_post_reset_observation(
        self,
        *,
        account_id: str,
        window: str,
        stalled_reset_at: int,
        observed_at: datetime | None = None,
    ) -> PostResetHeartbeatObservationLike: ...

    async def mark_post_reset_heartbeat_sent(
        self,
        observation_id: int,
        *,
        sent_at: datetime | None = None,
    ) -> bool: ...

    async def clear_post_reset_observations(
        self,
        *,
        account_id: str,
        window: str,
    ) -> int: ...

    async def add_entry(
        self,
        account_id: str,
        used_percent: float,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        recorded_at: datetime | None = None,
        window: str | None = None,
        reset_at: int | None = None,
        window_minutes: int | None = None,
        credits_has: bool | None = None,
        credits_unlimited: bool | None = None,
        credits_balance: float | None = None,
    ) -> UsageHistory | None: ...


class PostResetHeartbeatObservationLike(Protocol):
    id: int
    account_id: str
    window: str
    stalled_reset_at: int
    observed_count: int
    heartbeat_sent_at: datetime | None


class AdditionalUsageRepositoryPort(Protocol):
    async def add_entry(
        self,
        account_id: str,
        limit_name: str,
        metered_feature: str,
        window: str,
        used_percent: float,
        reset_at: int | None = None,
        window_minutes: int | None = None,
        recorded_at: datetime | None = None,
        quota_key: str | None = None,
    ) -> None: ...

    async def delete_for_account(self, account_id: str) -> None: ...

    async def delete_for_account_and_quota_key(self, account_id: str, quota_key: str) -> None: ...

    async def delete_for_account_and_limit(self, account_id: str, limit_name: str) -> None: ...

    async def delete_for_account_quota_key_window(
        self,
        account_id: str,
        quota_key: str,
        window: str,
    ) -> None: ...

    async def delete_for_account_limit_window(
        self,
        account_id: str,
        limit_name: str,
        window: str,
    ) -> None: ...

    async def list_quota_keys(
        self,
        *,
        account_ids: Collection[str] | None = None,
        since: datetime | None = None,
    ) -> list[str]: ...

    async def list_limit_names(
        self,
        *,
        account_ids: Collection[str] | None = None,
        since: datetime | None = None,
    ) -> list[str]: ...

    async def latest_recorded_at_for_account(self, account_id: str) -> datetime | None: ...


class RequestLogsRepositoryPort(Protocol):
    async def add_log(
        self,
        account_id: str | None,
        request_id: str,
        model: str,
        input_tokens: int | None,
        output_tokens: int | None,
        latency_ms: int | None,
        status: str,
        error_code: str | None,
        **kwargs: object,
    ) -> object: ...


class AccountsRepositoryWithStatusComparePort(AccountsRepositoryPort, Protocol):
    async def update_status_if_current(
        self,
        account_id: str,
        status: AccountStatus,
        deactivation_reason: str | None = None,
        reset_at: int | None = None,
        blocked_at: int | None = None,
        *,
        expected_status: AccountStatus,
        expected_deactivation_reason: str | None = None,
        expected_reset_at: int | None = None,
        expected_blocked_at: int | None = None,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class AccountRefreshResult:
    usage_written: bool
    fetch_succeeded: bool = True


@dataclass(frozen=True, slots=True)
class _MergedAdditionalWindow:
    limit_name: str
    metered_feature: str
    used_percent: float
    reset_at: int | None
    window_minutes: int | None


# Module-level freshness cache for additional-only accounts (no main UsageHistory
# entry). Used as a fast path to avoid DB queries on every pass within the same
# process. Updated only after a successful refresh that wrote data.
_last_successful_refresh: dict[str, datetime] = {}
_usage_refresh_auth_cooldowns: dict[str, float] = {}


class _UsageRefreshSingleflight:
    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task[AccountRefreshResult]] = {}
        self._lock = asyncio.Lock()

    async def run(
        self,
        account_id: str,
        factory: Callable[[], Awaitable[AccountRefreshResult]],
        *,
        join_existing: bool = True,
    ) -> AccountRefreshResult:
        while True:
            wait_for_existing: asyncio.Task[AccountRefreshResult] | None = None
            async with self._lock:
                task = self._inflight.get(account_id)
                if task is None or task.done():
                    task = asyncio.create_task(self._run_factory(factory))
                    self._inflight[account_id] = task
                    task.add_done_callback(
                        lambda done, *, key=account_id: self._clear_if_current(key, done),
                    )
                    break
                if join_existing:
                    break
                wait_for_existing = task
            if wait_for_existing is None:
                break
            try:
                await asyncio.shield(wait_for_existing)
            except asyncio.CancelledError:
                current_task = asyncio.current_task()
                if current_task is not None and current_task.cancelling():
                    raise
            except Exception:
                pass
        return await asyncio.shield(task)

    async def _run_factory(
        self,
        factory: Callable[[], Awaitable[AccountRefreshResult]],
    ) -> AccountRefreshResult:
        return await factory()

    def _clear_if_current(
        self,
        key: str,
        task: asyncio.Task[AccountRefreshResult],
    ) -> None:
        current = self._inflight.get(key)
        if current is task:
            self._inflight.pop(key, None)
        if task.cancelled():
            return
        with contextlib.suppress(BaseException):
            task.exception()

    def clear(self) -> None:
        self._inflight.clear()

    async def cancel_all(self) -> None:
        async with self._lock:
            tasks = list(self._inflight.values())
            self._inflight.clear()
        for task in tasks:
            task.cancel()
        if not tasks:
            return
        with contextlib.suppress(BaseException):
            await asyncio.gather(*tasks, return_exceptions=True)


_USAGE_REFRESH_SINGLEFLIGHT = _UsageRefreshSingleflight()


class UsageUpdater:
    def __init__(
        self,
        usage_repo: UsageRepositoryPort,
        accounts_repo: AccountsRepositoryPort | None = None,
        additional_usage_repo: AdditionalUsageRepositoryPort | AdditionalUsageRepository | None = None,
        request_logs_repo: RequestLogsRepositoryPort | None = None,
    ) -> None:
        self._usage_repo = usage_repo
        self._accounts_repo = accounts_repo
        self._additional_usage_repo = additional_usage_repo
        self._request_logs_repo = request_logs_repo
        self._encryptor = TokenEncryptor()
        self._auth_manager = AuthManager(accounts_repo) if accounts_repo else None

    async def refresh_accounts(
        self,
        accounts: list[Account],
        latest_usage: Mapping[str, UsageHistory],
    ) -> bool:
        """Refresh usage for all accounts. Returns True if usage rows were written."""
        settings = get_settings()
        if not settings.usage_refresh_enabled:
            return False

        refreshed = False
        now = utcnow()
        interval = settings.usage_refresh_interval_seconds
        _prune_usage_refresh_auth_cooldowns()
        for account in accounts:
            if account.status in (AccountStatus.REAUTH_REQUIRED, AccountStatus.DEACTIVATED):
                continue
            if _is_usage_refresh_in_cooldown(account.id):
                continue
            latest = await self._freshness_usage_entry(account, latest_usage.get(account.id))
            bypass_freshness = _quota_recovery_should_bypass_freshness(account, latest=latest)
            if not bypass_freshness and _latest_usage_is_fresh(latest, now=now, interval_seconds=interval):
                continue
            # Additional-only accounts have no main UsageHistory entry.
            # Check DB-backed freshness (works across workers/restarts)
            # with process-local cache as a fast path.
            # NOTE: When a successful fetch returns empty additional data
            # (all rows deleted), the DB has no timestamp to consult.
            # Cross-worker may re-fetch; process-local cache (line ~138)
            # prevents redundant calls within the same worker.
            if latest is None:
                last_ok = _last_successful_refresh.get(account.id)
                if not bypass_freshness and last_ok and (now - last_ok).total_seconds() < interval:
                    continue
                if self._additional_usage_repo is not None:
                    additional_fresh_at = await self._additional_usage_repo.latest_recorded_at_for_account(
                        account.id,
                    )
                    if (
                        not bypass_freshness
                        and additional_fresh_at
                        and (now - additional_fresh_at).total_seconds() < interval
                    ):
                        _last_successful_refresh[account.id] = additional_fresh_at
                        continue
            # NOTE: AsyncSession is not safe for concurrent use. Run sequentially
            # within the request-scoped session to avoid PK collisions and
            # flush-time warnings (SAWarning: Session.add during flush).
            try:
                result = await _USAGE_REFRESH_SINGLEFLIGHT.run(
                    account.id,
                    lambda account=account: self._refresh_account_if_stale(
                        account,
                        usage_account_id=account.chatgpt_account_id,
                        interval_seconds=interval,
                    ),
                )
                await self._sync_account_from_repo(account)
                refreshed = refreshed or result.usage_written
                # Only cache when the upstream fetch actually succeeded.
                # Transient errors (401 retry failure, 5xx, etc.) must not
                # suppress retries within the interval.
                if result.fetch_succeeded:
                    _last_successful_refresh[account.id] = now
                    _clear_usage_refresh_auth_cooldown(account.id)
            except Exception as exc:
                logger.warning(
                    "Usage refresh failed account_id=%s request_id=%s error=%s",
                    account.id,
                    get_request_id(),
                    exc,
                    exc_info=True,
                )
                # swallow per-account failures so the whole refresh loop keeps going
                continue
        return refreshed

    async def force_refresh(self, account: Account) -> bool:
        """Refresh one account regardless of cached/fresh usage rows."""
        settings = get_settings()
        if not settings.usage_refresh_enabled:
            return False
        if account.status in (AccountStatus.REAUTH_REQUIRED, AccountStatus.DEACTIVATED):
            return False
        try:
            result = await _USAGE_REFRESH_SINGLEFLIGHT.run(
                account.id,
                lambda: self._refresh_account(
                    account,
                    usage_account_id=account.chatgpt_account_id,
                ),
                join_existing=False,
            )
            await self._sync_account_from_repo(account)
            if result.fetch_succeeded:
                _last_successful_refresh[account.id] = utcnow()
                _clear_usage_refresh_auth_cooldown(account.id)
            return result.usage_written
        except Exception as exc:
            logger.warning(
                "Forced usage refresh failed account_id=%s request_id=%s error=%s",
                account.id,
                get_request_id(),
                exc,
                exc_info=True,
            )
            return False

    async def _refresh_account_if_stale(
        self,
        account: Account,
        *,
        usage_account_id: str | None,
        interval_seconds: int,
    ) -> AccountRefreshResult:
        primary_latest = await self._usage_repo.latest_entry_for_account(account.id, window="primary")
        latest = await self._freshness_usage_entry(account, primary_latest)
        if not _quota_recovery_should_bypass_freshness(account, latest=latest) and _latest_usage_is_fresh(
            latest,
            now=utcnow(),
            interval_seconds=interval_seconds,
        ):
            return AccountRefreshResult(usage_written=False)
        return await self._refresh_account(
            account,
            usage_account_id=usage_account_id,
        )

    async def _freshness_usage_entry(self, account: Account, latest: UsageHistory | None) -> UsageHistory | None:
        if latest is not None:
            return latest
        if usage_core.capacity_for_plan(account.plan_type, "monthly") is None:
            return None
        return await self._usage_repo.latest_entry_for_account(account.id, window="monthly")

    async def _refresh_account(
        self,
        account: Account,
        *,
        usage_account_id: str | None,
    ) -> AccountRefreshResult:
        access_token = self._encryptor.decrypt(account.access_token_encrypted)
        payload: UsagePayload | None = None
        try:
            route = await _resolve_upstream_route_for_account(account, operation="usage_refresh")
            payload = await fetch_usage(
                access_token=access_token,
                account_id=usage_account_id,
                route=route,
                allow_direct_egress=route is None,
            )
        except UpstreamProxyRouteError as exc:
            logger.warning(
                "Usage refresh upstream proxy route unavailable account_id=%s reason=%s",
                account.id,
                exc.reason,
            )
            _mark_usage_refresh_auth_cooldown(account.id, 0)
            return AccountRefreshResult(usage_written=False, fetch_succeeded=False)
        except UsageFetchError as exc:
            if _should_deactivate_for_usage_error(exc):
                await self._deactivate_for_client_error(account, exc)
                return AccountRefreshResult(usage_written=False, fetch_succeeded=False)
            if exc.status_code != 401 or not self._auth_manager:
                _mark_usage_refresh_auth_cooldown(account.id, exc.status_code)
                return AccountRefreshResult(usage_written=False, fetch_succeeded=False)
            try:
                account = await self._auth_manager.ensure_fresh(account, force=True)
            except RefreshError:
                _mark_usage_refresh_auth_cooldown(account.id, exc.status_code)
                return AccountRefreshResult(usage_written=False, fetch_succeeded=False)
            access_token = self._encryptor.decrypt(account.access_token_encrypted)
            try:
                route = await _resolve_upstream_route_for_account(account, operation="usage_refresh")
                payload = await fetch_usage(
                    access_token=access_token,
                    account_id=usage_account_id,
                    route=route,
                    allow_direct_egress=route is None,
                )
            except UpstreamProxyRouteError as route_exc:
                logger.warning(
                    "Usage refresh retry upstream proxy route unavailable account_id=%s reason=%s",
                    account.id,
                    route_exc.reason,
                )
                _mark_usage_refresh_auth_cooldown(account.id, 0)
                return AccountRefreshResult(usage_written=False, fetch_succeeded=False)
            except UsageFetchError as retry_exc:
                if _should_deactivate_for_usage_error(retry_exc):
                    await self._deactivate_for_client_error(account, retry_exc)
                else:
                    _mark_usage_refresh_auth_cooldown(account.id, retry_exc.status_code)
                return AccountRefreshResult(usage_written=False, fetch_succeeded=False)

        if payload is None:
            return AccountRefreshResult(usage_written=False, fetch_succeeded=False)

        if _payload_mismatches_account_slot(account, payload):
            logger.warning(
                "Usage refresh payload identity mismatch; skipping account mutation "
                "account_id=%s stored_workspace_id=%s payload_workspace_id=%s stored_plan_type=%s "
                "payload_plan_type=%s stored_seat_type=%s payload_seat_type=%s request_id=%s",
                account.id,
                account.workspace_id,
                payload.workspace_id,
                account.plan_type,
                payload.plan_type,
                account.seat_type,
                payload.seat_type,
                get_request_id(),
            )
            return AccountRefreshResult(usage_written=False, fetch_succeeded=False)

        identity_matches_slot = await self._sync_identity_metadata(account, payload)
        if not identity_matches_slot:
            logger.warning(
                "Usage refresh payload reported a workspace slot owned by another account; "
                "skipping account usage mutation account_id=%s payload_workspace_id=%s request_id=%s",
                account.id,
                payload.workspace_id,
                get_request_id(),
            )
            return AccountRefreshResult(usage_written=False, fetch_succeeded=False)

        now_epoch = _now_epoch()
        if self._additional_usage_repo is not None:
            if payload.additional_rate_limits:
                merged_limits = _merge_additional_rate_limits(
                    payload.additional_rate_limits,
                    account_id=account.id,
                    now_epoch=now_epoch,
                )
                current_entries: set[tuple[str, str]] = set()
                for quota_key, windows in merged_limits.items():
                    for window, merged_window in windows.items():
                        current_entries.add((quota_key, window))
                        await _add_additional_usage_entry(
                            self._additional_usage_repo,
                            account_id=account.id,
                            limit_name=merged_window.limit_name,
                            metered_feature=merged_window.metered_feature,
                            quota_key=quota_key,
                            window=window,
                            used_percent=merged_window.used_percent,
                            reset_at=merged_window.reset_at,
                            window_minutes=merged_window.window_minutes,
                        )
                current_quota_keys = {name for name, _ in current_entries}
                existing_quota_keys = await _list_additional_usage_quota_keys(
                    self._additional_usage_repo,
                    account_ids=[account.id],
                )
                for stale_key in existing_quota_keys:
                    if stale_key not in current_quota_keys:
                        await _delete_additional_usage_quota_key(
                            self._additional_usage_repo,
                            account.id,
                            stale_key,
                        )
                        continue
                    for window in ("primary", "secondary"):
                        if (stale_key, window) not in current_entries:
                            await _delete_additional_usage_quota_key_window(
                                self._additional_usage_repo,
                                account.id,
                                stale_key,
                                window,
                            )
            elif payload.additional_rate_limits is not None:
                await self._additional_usage_repo.delete_for_account(account.id)

        rate_limit = payload.rate_limit
        if rate_limit is None:
            additional_synced = self._additional_usage_repo is not None and payload.additional_rate_limits is not None
            return AccountRefreshResult(usage_written=additional_synced)
        # Treat both None and empty rate_limit (both windows absent) as
        # additional-only to avoid falling through to window processing.
        normalized_windows = usage_core.normalize_rate_limit_windows(
            rate_limit.primary_window,
            rate_limit.secondary_window,
        )
        primary = normalized_windows.primary
        secondary = normalized_windows.secondary
        monthly = normalized_windows.monthly
        if primary is None and secondary is None:
            if monthly is None:
                additional_synced = (
                    self._additional_usage_repo is not None and payload.additional_rate_limits is not None
                )
                return AccountRefreshResult(usage_written=additional_synced)
        if primary is None and secondary is None and monthly is None:
            additional_synced = self._additional_usage_repo is not None and payload.additional_rate_limits is not None
            return AccountRefreshResult(usage_written=additional_synced)
        credits_has, credits_unlimited, credits_balance = _credits_snapshot(payload)
        usage_written = False

        if primary and primary.used_percent is not None:
            entry = await self._usage_repo.add_entry(
                account_id=account.id,
                used_percent=float(primary.used_percent),
                input_tokens=None,
                output_tokens=None,
                window="primary",
                reset_at=_reset_at(primary.reset_at, primary.reset_after_seconds, now_epoch),
                window_minutes=_window_minutes(primary.limit_window_seconds),
                credits_has=credits_has,
                credits_unlimited=credits_unlimited,
                credits_balance=credits_balance,
            )
            usage_written = usage_written or _usage_entry_written(entry)

        if secondary and secondary.used_percent is not None:
            entry = await self._usage_repo.add_entry(
                account_id=account.id,
                used_percent=float(secondary.used_percent),
                input_tokens=None,
                output_tokens=None,
                window="secondary",
                reset_at=_reset_at(secondary.reset_at, secondary.reset_after_seconds, now_epoch),
                window_minutes=_window_minutes(secondary.limit_window_seconds),
            )
            usage_written = usage_written or _usage_entry_written(entry)

        if monthly and monthly.used_percent is not None:
            entry = await self._usage_repo.add_entry(
                account_id=account.id,
                used_percent=float(monthly.used_percent),
                input_tokens=None,
                output_tokens=None,
                window="monthly",
                reset_at=_reset_at(monthly.reset_at, monthly.reset_after_seconds, now_epoch),
                window_minutes=_window_minutes(monthly.limit_window_seconds),
                credits_has=credits_has,
                credits_unlimited=credits_unlimited,
                credits_balance=credits_balance,
            )
            usage_written = usage_written or _usage_entry_written(entry)

        await self._process_post_reset_heartbeats(
            account,
            primary=primary,
            secondary=secondary,
            monthly=monthly,
            now_epoch=now_epoch,
        )
        await self._recover_quota_status_from_usage(account, primary=primary, secondary=secondary, monthly=monthly)
        return AccountRefreshResult(usage_written=usage_written)

    async def _process_post_reset_heartbeats(
        self,
        account: Account,
        *,
        primary: UsageWindow | None,
        secondary: UsageWindow | None,
        monthly: UsageWindow | None,
        now_epoch: int,
    ) -> None:
        if account.status == AccountStatus.PAUSED:
            return
        for window_name, window in (
            ("primary", primary),
            ("secondary", secondary),
            ("monthly", monthly),
        ):
            await self._process_post_reset_heartbeat_window(
                account,
                window_name=window_name,
                window=window,
                now_epoch=now_epoch,
            )

    async def _process_post_reset_heartbeat_window(
        self,
        account: Account,
        *,
        window_name: str,
        window: UsageWindow | None,
        now_epoch: int,
    ) -> None:
        reset_at = _window_reset_at(window, now_epoch)
        if reset_at is None or reset_at > now_epoch:
            await self._usage_repo.clear_post_reset_observations(
                account_id=account.id,
                window=window_name,
            )
            return

        observation = await self._usage_repo.record_post_reset_observation(
            account_id=account.id,
            window=window_name,
            stalled_reset_at=reset_at,
        )
        if observation.heartbeat_sent_at is not None:
            return
        if observation.observed_count < POST_RESET_HEARTBEAT_OBSERVATION_THRESHOLD:
            return

        marked = await self._usage_repo.mark_post_reset_heartbeat_sent(observation.id)
        if not marked:
            return
        await self._send_post_reset_heartbeat(account, window_name=window_name, stalled_reset_at=reset_at)

    async def _send_post_reset_heartbeat(
        self,
        account: Account,
        *,
        window_name: str,
        stalled_reset_at: int,
    ) -> int:
        access_token = self._encryptor.decrypt(account.access_token_encrypted)
        settings = get_settings()
        base = settings.upstream_base_url.rstrip("/")
        if "/backend-api" not in base:
            base = f"{base}/backend-api"
        url = f"{base}/codex/responses"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if account.chatgpt_account_id and not account.chatgpt_account_id.startswith(("email_", "local_")):
            headers["chatgpt-account-id"] = account.chatgpt_account_id
        body = {
            "model": POST_RESET_HEARTBEAT_MODEL,
            "instructions": "Reply with hi.",
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hi"}],
                }
            ],
            "max_output_tokens": POST_RESET_HEARTBEAT_MAX_OUTPUT_TOKENS,
            "stream": False,
            "store": False,
        }
        timeout = aiohttp.ClientTimeout(
            total=POST_RESET_HEARTBEAT_TIMEOUT_SECONDS,
            sock_connect=POST_RESET_HEARTBEAT_CONNECT_TIMEOUT_SECONDS,
        )
        started_at = utcnow()
        started_monotonic = time.monotonic()
        status_code = 0
        error_message: str | None = None
        try:
            async with lease_http_session() as session:
                async with session.post(url, headers=headers, json=body, timeout=timeout) as resp:
                    await resp.read()
                    status_code = resp.status
                    logger.info(
                        "Post-reset heartbeat sent account_id=%s window=%s stalled_reset_at=%s status=%s",
                        account.id,
                        window_name,
                        stalled_reset_at,
                        resp.status,
                    )
                    return resp.status
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            error_message = str(exc)
            logger.warning(
                "Post-reset heartbeat failed account_id=%s window=%s stalled_reset_at=%s error=%s",
                account.id,
                window_name,
                stalled_reset_at,
                exc,
            )
            return 0
        finally:
            latency_ms = max(0, int((time.monotonic() - started_monotonic) * 1000))
            await self._log_post_reset_heartbeat_request(
                account,
                window_name=window_name,
                stalled_reset_at=stalled_reset_at,
                requested_at=started_at,
                latency_ms=latency_ms,
                status_code=status_code,
                error_message=error_message,
            )

    async def _log_post_reset_heartbeat_request(
        self,
        account: Account,
        *,
        window_name: str,
        stalled_reset_at: int,
        requested_at: datetime,
        latency_ms: int,
        status_code: int,
        error_message: str | None,
    ) -> None:
        if self._request_logs_repo is None:
            return
        success = 200 <= status_code < 400
        try:
            await self._request_logs_repo.add_log(
                account_id=account.id,
                request_id=f"post-reset-heartbeat-{account.id}-{window_name}-{stalled_reset_at}",
                model=POST_RESET_HEARTBEAT_MODEL,
                input_tokens=1,
                output_tokens=None,
                latency_ms=latency_ms,
                status="success" if success else "error",
                error_code=None if success else "post_reset_heartbeat_failed",
                error_message=error_message,
                requested_at=requested_at,
                transport="http",
                plan_type=account.plan_type,
                source="post_reset_heartbeat",
                useragent_group="system",
                failure_phase=None if success else "post_reset_heartbeat",
                failure_detail=None if success else f"window={window_name} stalled_reset_at={stalled_reset_at}",
                upstream_status_code=status_code if status_code > 0 else None,
            )
        except Exception as exc:
            logger.warning(
                "Failed to record post-reset heartbeat request log "
                "account_id=%s window=%s stalled_reset_at=%s error=%s",
                account.id,
                window_name,
                stalled_reset_at,
                exc,
            )

    async def _deactivate_for_client_error(self, account: Account, exc: UsageFetchError) -> None:
        if not self._auth_manager:
            return
        reason = f"Usage API error: HTTP {exc.status_code} - {exc.message}"
        status = (
            account_status_for_permanent_failure(exc.code)
            if exc.code in PERMANENT_FAILURE_CODES
            else AccountStatus.DEACTIVATED
        )
        logger.warning(
            "Marking account unavailable due to client error account_id=%s account_status=%s status=%s "
            "message=%s request_id=%s",
            account.id,
            status.value,
            exc.status_code,
            exc.message,
            get_request_id(),
        )
        await self._auth_manager._repo.update_status(account.id, status, reason)
        account.status = status
        account.deactivation_reason = reason

    async def _sync_identity_metadata(self, account: Account, payload: UsagePayload) -> bool:
        next_plan_type = coerce_account_plan_type(payload.plan_type, account.plan_type or "free")
        payload_workspace_id = _clean_optional(payload.workspace_id)
        next_workspace_id = payload_workspace_id or account.workspace_id
        next_workspace_label = _clean_optional(payload.workspace_label) or account.workspace_label
        next_seat_type = _clean_optional(payload.seat_type) or account.seat_type
        if self._auth_manager and payload_workspace_id and not account.workspace_id:
            slot_taken = await self._auth_manager._repo.workspace_slot_taken(
                account_id=account.id,
                email=account.email,
                chatgpt_account_id=account.chatgpt_account_id,
                workspace_id=payload_workspace_id,
            )
            if slot_taken:
                logger.warning(
                    "Usage payload reported workspace_id=%s for legacy account_id=%s, but that slot "
                    "is already owned by another account; skipping usage payload",
                    payload_workspace_id,
                    account.id,
                )
                return False
        if (
            next_plan_type == account.plan_type
            and next_workspace_id == account.workspace_id
            and next_workspace_label == account.workspace_label
            and next_seat_type == account.seat_type
        ):
            return True

        account.plan_type = next_plan_type
        account.workspace_id = next_workspace_id
        account.workspace_label = next_workspace_label
        account.seat_type = next_seat_type
        if not self._auth_manager:
            return True

        await self._auth_manager._repo.update_tokens(
            account.id,
            access_token_encrypted=account.access_token_encrypted,
            refresh_token_encrypted=account.refresh_token_encrypted,
            id_token_encrypted=account.id_token_encrypted,
            last_refresh=account.last_refresh,
            plan_type=account.plan_type,
            email=account.email,
            chatgpt_account_id=account.chatgpt_account_id,
            workspace_id=account.workspace_id,
            workspace_label=account.workspace_label,
            seat_type=account.seat_type,
        )
        return True

    async def _recover_quota_status_from_usage(
        self,
        account: Account,
        *,
        primary: UsageWindow | None,
        secondary: UsageWindow | None,
        monthly: UsageWindow | None = None,
    ) -> None:
        if not self._auth_manager:
            return
        if account.status == AccountStatus.RATE_LIMITED:
            long_window = monthly or secondary
            if primary is None and monthly is None:
                return
            if primary is not None and not _window_has_available_quota(primary):
                return
            if primary is None and (long_window is None or not _window_has_available_quota(long_window)):
                return
            if long_window is not None and not _window_has_available_quota(long_window):
                return
            target_status = AccountStatus.ACTIVE
            target_reset_at = None
            expected_status = AccountStatus.RATE_LIMITED
        elif account.status == AccountStatus.QUOTA_EXCEEDED:
            if account.blocked_at is not None and time.time() < account.blocked_at + QUOTA_EXCEEDED_COOLDOWN_SECONDS:
                return
            long_window = monthly or secondary
            windows = [window for window in (primary, long_window) if window is not None]
            if long_window is None or not _window_has_available_quota(long_window):
                return
            if primary is not None and _window_is_exhausted(primary):
                target_status = AccountStatus.RATE_LIMITED
                target_reset_at = _reset_at(primary.reset_at, primary.reset_after_seconds, _now_epoch())
            else:
                if any(_window_is_exhausted(window) for window in windows):
                    return
                target_status = AccountStatus.ACTIVE
                target_reset_at = None
            if not any(_window_has_available_quota(window) for window in windows):
                return
            expected_status = AccountStatus.QUOTA_EXCEEDED
        else:
            return

        repo = cast(AccountsRepositoryWithStatusComparePort, self._auth_manager._repo)
        updated = await repo.update_status_if_current(
            account.id,
            target_status,
            None,
            target_reset_at,
            blocked_at=None,
            expected_status=expected_status,
            expected_deactivation_reason=account.deactivation_reason,
            expected_reset_at=account.reset_at,
            expected_blocked_at=account.blocked_at,
        )
        if not updated:
            await self._sync_account_from_repo(account)
            return
        account.status = target_status
        account.deactivation_reason = None
        account.reset_at = target_reset_at
        account.blocked_at = None

    async def _sync_account_from_repo(self, account: Account) -> None:
        if not self._accounts_repo:
            return
        stored = await self._accounts_repo.get_by_id(account.id)
        if stored is None:
            return
        account.chatgpt_account_id = stored.chatgpt_account_id
        account.email = stored.email
        account.workspace_id = stored.workspace_id
        account.workspace_label = stored.workspace_label
        account.seat_type = stored.seat_type
        account.plan_type = stored.plan_type
        account.access_token_encrypted = stored.access_token_encrypted
        account.refresh_token_encrypted = stored.refresh_token_encrypted
        account.id_token_encrypted = stored.id_token_encrypted
        account.last_refresh = stored.last_refresh
        account.status = stored.status
        account.deactivation_reason = stored.deactivation_reason
        account.reset_at = stored.reset_at
        account.blocked_at = stored.blocked_at


def _window_reset_at(window: UsageWindow | None, now_epoch: int) -> int | None:
    if window is None:
        return None
    return _reset_at(window.reset_at, window.reset_after_seconds, now_epoch)


def _credits_snapshot(payload: UsagePayload) -> tuple[bool | None, bool | None, float | None]:
    credits = payload.credits
    if credits is None:
        return None, None, None
    credits_has = credits.has_credits
    credits_unlimited = credits.unlimited
    balance_value = credits.balance
    return credits_has, credits_unlimited, _parse_credits_balance(balance_value)


def _payload_mismatches_account_slot(account: Account, payload: UsagePayload) -> bool:
    payload_workspace_id = _clean_optional(payload.workspace_id)
    if account.workspace_id and payload_workspace_id and account.workspace_id != payload_workspace_id:
        return True
    if not payload_workspace_id:
        payload_plan_type = coerce_account_plan_type(payload.plan_type, account.plan_type or "free")
        stored_plan_type = coerce_account_plan_type(account.plan_type, "free")
        if payload.plan_type and stored_plan_type not in {"unknown", ""} and payload_plan_type != stored_plan_type:
            return True
    return False


def _clean_optional(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _usage_entry_written(entry: UsageHistory | None) -> bool:
    return entry is not None


def _window_has_available_quota(window: UsageWindow) -> bool:
    used_percent = window.used_percent
    return used_percent is not None and float(used_percent) < 100.0


def _window_is_exhausted(window: UsageWindow) -> bool:
    used_percent = window.used_percent
    return used_percent is not None and float(used_percent) >= 100.0


def _prefer_merged_additional_window(
    existing: _MergedAdditionalWindow,
    candidate: _MergedAdditionalWindow,
    *,
    quota_key: str,
    window: str,
) -> _MergedAdditionalWindow:
    if candidate.used_percent > existing.used_percent:
        logger.warning(
            "Additional usage refresh saw conflicting aliases for the same canonical quota window; "
            "keeping the higher usage sample account_quota=%s window=%s existing_limit=%s candidate_limit=%s "
            "request_id=%s",
            quota_key,
            window,
            existing.limit_name,
            candidate.limit_name,
            get_request_id(),
        )
        return candidate
    if candidate.used_percent < existing.used_percent:
        logger.warning(
            "Additional usage refresh saw conflicting aliases for the same canonical quota window; "
            "keeping the higher usage sample account_quota=%s window=%s existing_limit=%s candidate_limit=%s "
            "request_id=%s",
            quota_key,
            window,
            existing.limit_name,
            candidate.limit_name,
            get_request_id(),
        )
        return existing
    preferred = sorted(
        (existing, candidate),
        key=lambda entry: (entry.limit_name, entry.metered_feature),
    )[0]
    if preferred != existing or existing != candidate:
        logger.info(
            "Additional usage refresh coalesced duplicate aliases for canonical quota window "
            "account_quota=%s window=%s chosen_limit=%s request_id=%s",
            quota_key,
            window,
            preferred.limit_name,
            get_request_id(),
        )
    return preferred


def _merge_additional_rate_limits(
    additional_rate_limits: Collection[AdditionalRateLimitPayload],
    *,
    account_id: str,
    now_epoch: int,
) -> dict[str, dict[str, _MergedAdditionalWindow]]:
    merged: dict[str, dict[str, _MergedAdditionalWindow]] = {}
    for additional in additional_rate_limits:
        limit_name = getattr(additional, "limit_name", None)
        metered_feature = getattr(additional, "metered_feature", None)
        quota_key = canonicalize_additional_quota_key(
            limit_name=limit_name,
            metered_feature=metered_feature,
        )
        if quota_key is None:
            logger.warning(
                "Skipping additional usage item without resolvable quota key "
                "account_id=%s limit_name=%s metered_feature=%s request_id=%s",
                account_id,
                limit_name,
                metered_feature,
                get_request_id(),
            )
            continue
        rate_limit = getattr(additional, "rate_limit", None)
        if rate_limit is None:
            continue
        for window_name, usage_window in (
            ("primary", getattr(rate_limit, "primary_window", None)),
            ("secondary", getattr(rate_limit, "secondary_window", None)),
        ):
            if usage_window is None or usage_window.used_percent is None:
                continue
            candidate = _MergedAdditionalWindow(
                limit_name=str(limit_name),
                metered_feature=str(metered_feature),
                used_percent=float(usage_window.used_percent),
                reset_at=_reset_at(usage_window.reset_at, usage_window.reset_after_seconds, now_epoch),
                window_minutes=_window_minutes(usage_window.limit_window_seconds),
            )
            windows = merged.setdefault(quota_key, {})
            existing = windows.get(window_name)
            windows[window_name] = (
                candidate
                if existing is None
                else _prefer_merged_additional_window(
                    existing,
                    candidate,
                    quota_key=quota_key,
                    window=window_name,
                )
            )
    return merged


async def _add_additional_usage_entry(
    repo: AdditionalUsageRepositoryPort | AdditionalUsageRepository,
    *,
    account_id: str,
    limit_name: str,
    metered_feature: str,
    quota_key: str,
    window: str,
    used_percent: float,
    reset_at: int | None,
    window_minutes: int | None,
) -> None:
    await repo.add_entry(
        account_id=account_id,
        limit_name=limit_name,
        metered_feature=metered_feature,
        quota_key=quota_key,
        window=window,
        used_percent=used_percent,
        reset_at=reset_at,
        window_minutes=window_minutes,
    )


async def _list_additional_usage_quota_keys(
    repo: AdditionalUsageRepositoryPort | AdditionalUsageRepository,
    *,
    account_ids: Collection[str] | None = None,
) -> list[str]:
    return await repo.list_quota_keys(account_ids=account_ids)


async def _delete_additional_usage_quota_key(
    repo: AdditionalUsageRepositoryPort | AdditionalUsageRepository,
    account_id: str,
    quota_key: str,
) -> None:
    await repo.delete_for_account_and_quota_key(account_id, quota_key)


async def _delete_additional_usage_quota_key_window(
    repo: AdditionalUsageRepositoryPort | AdditionalUsageRepository,
    account_id: str,
    quota_key: str,
    window: str,
) -> None:
    await repo.delete_for_account_quota_key_window(account_id, quota_key, window)


def _latest_usage_is_fresh(
    latest: UsageHistory | None,
    *,
    now: datetime,
    interval_seconds: int,
) -> bool:
    if latest is None:
        return False
    recorded_at = latest.recorded_at
    comparison_now = now
    if recorded_at.tzinfo is None and comparison_now.tzinfo is None:
        comparison_now = datetime.now()
    elif recorded_at.tzinfo is not None and comparison_now.tzinfo is None:
        comparison_now = comparison_now.replace(tzinfo=timezone.utc)
    elif recorded_at.tzinfo is None and comparison_now.tzinfo is not None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    if (comparison_now - recorded_at).total_seconds() >= interval_seconds:
        return False
    if latest.reset_at is not None:
        now_epoch = int(now.replace(tzinfo=timezone.utc).timestamp())
        if now_epoch >= latest.reset_at:
            return False
    return True


def _quota_recovery_should_bypass_freshness(account: Account, *, latest: UsageHistory | None) -> bool:
    if _account_needs_post_reset_refresh(account, latest=latest):
        return True
    if account.status != AccountStatus.QUOTA_EXCEEDED:
        return False
    if account.blocked_at is None:
        return latest is None
    cooldown_expires_at = account.blocked_at + QUOTA_EXCEEDED_COOLDOWN_SECONDS
    if time.time() < cooldown_expires_at:
        return False
    if latest is None:
        return True
    recorded_at = latest.recorded_at
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    return recorded_at.timestamp() < cooldown_expires_at


def _account_needs_post_reset_refresh(account: Account, *, latest: UsageHistory | None) -> bool:
    if account.status not in (AccountStatus.RATE_LIMITED, AccountStatus.QUOTA_EXCEEDED):
        return False
    if account.reset_at is None:
        return False
    if time.time() < account.reset_at:
        return False
    if latest is None:
        return True
    recorded_at = latest.recorded_at
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    return recorded_at.timestamp() < float(account.reset_at)


def _parse_credits_balance(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _window_minutes(limit_seconds: int | None) -> int | None:
    if not limit_seconds or limit_seconds <= 0:
        return None
    return max(1, math.ceil(limit_seconds / 60))


def _now_epoch() -> int:
    return int(utcnow().replace(tzinfo=timezone.utc).timestamp())


def _reset_at(reset_at: int | None, reset_after_seconds: int | None, now_epoch: int) -> int | None:
    if reset_at is not None:
        return int(reset_at)
    if reset_after_seconds is None:
        return None
    return now_epoch + max(0, int(reset_after_seconds))


# The usage endpoint can return 403 for accounts that are still otherwise usable
# for proxy traffic, so treat it as a refresh failure instead of a permanent
# account-level deactivation signal.
_DEACTIVATING_USAGE_STATUS_CODES = {402, 404}
_DEACTIVATING_USAGE_MESSAGE_HINTS = (
    "your openai account has been deactivated",
    "account has been deactivated",
)


def _should_deactivate_for_usage_error(exc: UsageFetchError) -> bool:
    if exc.status_code in _DEACTIVATING_USAGE_STATUS_CODES:
        return True
    if exc.code in PERMANENT_FAILURE_CODES:
        return True
    lowered = exc.message.lower()
    return any(hint in lowered for hint in _DEACTIVATING_USAGE_MESSAGE_HINTS)


async def _resolve_upstream_route_for_account(account: Account, *, operation: str) -> ResolvedUpstreamRoute | None:
    async with get_background_session() as session:
        return await resolve_upstream_route(
            session,
            account_id=account.id,
            operation=operation,
            scope="account",
        )


def _mark_usage_refresh_auth_cooldown(account_id: str, status_code: int) -> None:
    if status_code not in {401, 403}:
        return
    cooldown_seconds = max(0.0, float(get_settings().usage_refresh_auth_failure_cooldown_seconds))
    if cooldown_seconds <= 0:
        return
    _usage_refresh_auth_cooldowns[account_id] = time.monotonic() + cooldown_seconds


def _is_usage_refresh_in_cooldown(account_id: str) -> bool:
    expires_at = _usage_refresh_auth_cooldowns.get(account_id)
    if expires_at is None:
        return False
    if expires_at > time.monotonic():
        return True
    _usage_refresh_auth_cooldowns.pop(account_id, None)
    return False


def _clear_usage_refresh_auth_cooldown(account_id: str) -> None:
    _usage_refresh_auth_cooldowns.pop(account_id, None)


def _prune_usage_refresh_auth_cooldowns() -> None:
    now = time.monotonic()
    stale = [account_id for account_id, expires_at in _usage_refresh_auth_cooldowns.items() if expires_at <= now]
    for account_id in stale:
        _usage_refresh_auth_cooldowns.pop(account_id, None)


def _clear_usage_refresh_state() -> None:
    _usage_refresh_auth_cooldowns.clear()
    _last_successful_refresh.clear()
    _USAGE_REFRESH_SINGLEFLIGHT.clear()
