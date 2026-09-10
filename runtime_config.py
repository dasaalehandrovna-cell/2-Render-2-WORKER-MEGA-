"""vys-262 R48 FINAL HEAVY internal runtime configuration.

All non-secret operational tunables that used to be Render environment variables
live here.  Render ENV is intentionally reserved for credentials, remote
addresses and external account/resource identifiers.

Values in this file are authoritative: install_internal_runtime_config() uses
assignment (not setdefault), so stale numeric/tuning variables left in Render do
not override the packaged configuration.
"""
from __future__ import annotations

import os
from typing import Dict

CONFIG_VERSION = "vys-262-r54-preboot-handoff-rootfix"

# Render #1 / FAST.  These values were the R13 recommended deployment values.
FRONT_INTERNAL_ENV: Dict[str, str] = {
    # Process / role
    "PORT": "5000",
    "BOT_SPLIT_ROLE": "front",
    "RENDER_TELEGRAM_ONLY": "1",
    "MALLOC_ARENA_MAX": "2",

    # R25: FAST owns interaction; HEAVY remains background-only for split/Google/MEGA.
    "UI_WORKERS": "6",
        "FAST_UI_WORKERS": "6",
        "FAST_UI_MAX_PENDING": "900",
        "WINDOW_RENDER_WORKERS": "6",
        "WINDOW_RENDER_MAX_PENDING_KEYS": "256",
    "UI_MAX_PENDING": "800",
    "CALLBACK_ACK_WORKERS": "2",
    "UI_CLEANUP_WORKERS": "2",
    "UI_DELETE_WORKERS": "2",
    "UI_DELETE_MAX_PENDING": "1200",
    "R26_TRACE_RING_ROWS": "4000",
    "R26_TRACE_EXPORT_ROWS": "4000",
    "UI_CLEANUP_MAX_PENDING": "1200",
    "WEBHOOK_WORKERS": "3",
    "DELTA_WORKERS": "2",
    "BACKGROUND_WORKERS": "2",

    # Small user-facing runtime constants
    "QUICK_EXPENSE_REMINDER_MINUTES": "60",

    # Shared Redis layout (names/limits are implementation details, not secrets)
    "REDIS_RUNTIME_DEFAULT": "0",
    "WORKER_REDIS_SNAPSHOT_KEY": "vys262:bot_state:latest_gz",
    "WORKER_REDIS_SNAPSHOT_MAX_MB": "16",
    "WORKER_REDIS_EVENT_PREFIX": "vys262:tg_events:v1",
    "WORKER_REDIS_CAPSULE_KEY": "vys262:durable_capsule:r20",
        "WORKER_CAPSULE_MEGA_ENABLED": "1",
        "WORKER_CAPSULE_MEGA_KEEP": "10",
    "WORKER_REDIS_CAPSULE_MAX_MB": "8",
    "WORKER_EVENT_RETENTION_SEC": "604800",

    # Peer / event-journal transport
    "PEER_PING_ENABLED": "1",
    "PEER_PING_INTERVAL_SEC": "120",
    "SPLIT_WORKER_SYNC_ENABLED": "1",
    "SPLIT_STATE_SYNC_DELAY_SEC": "1.2",
    "SPLIT_STATE_SYNC_MIN_INTERVAL_SEC": "30",
    "SPLIT_FINANCE_SYNC_DELAY_SEC": "0.8",
    "SPLIT_CONTINUITY_FINANCE_DELAY_SEC": "4.0",
    "SPLIT_CONTINUITY_OTHER_DELAY_SEC": "2.5",
    "SPLIT_CONTINUITY_MAX_LATENCY_SEC": "5.0",
    "SPLIT_SYNC_MAX_LATENCY_SEC": "3.0",
    "SPLIT_FULL_RECONCILE_QUIET_SEC": "20",
    "SPLIT_DELTA_MAX_PAGES": "256",
    "SPLIT_DELTA_MAX_BYTES": "524288",
    "SPLIT_EVENT_RECEIPT_TIMEOUT_SEC": "1.2",
    "SPLIT_REDIS_FALLBACK_CONNECT_TIMEOUT_SEC": "0.7",
    "SPLIT_REDIS_FALLBACK_SOCKET_TIMEOUT_SEC": "1.2",

    # Boot / rolling deploy recovery
    "SPLIT_BOOT_ALWAYS_RESTORE": "1",
    "SPLIT_BOOT_HANDOFF_GRACE_SEC": "16",
    "SPLIT_PREBOOT_CAPTURE_WAIT_SEC": "4.0",
    "SPLIT_BOOT_WORKER_ATTEMPTS": "3",
    "SPLIT_BOOT_WORKER_TIMEOUT": "12",
    "SPLIT_RESTORE_RETRY_SEC": "5",
    "SPLIT_RESTORE_BOOT_ATTEMPTS": "3",
    "SPLIT_FORCE_BOOT_RESTORE": "0",
    "SPLIT_ALLOW_EMPTY_BOOT": "0",
    "SPLIT_EMERGENCY_MEGA": "1",

    # Heavy services are remote on Front.  MEGA credentials may still exist only
    # for emergency boot restore, but normal MEGA runtime stays disabled here.
    "MEGA_ENABLED": "0",
    "MEGA_AUTORESTORE": "0",
    "TG_DURABLE_ENABLED": "0",
    "MEGA_TIMEOUT": "120",
    "MEGA_LOGIN_TIMEOUT": "120",
    "MEGA_AUTOCREATE_LAYOUT": "1",
    "SPLIT_GOOGLE_REMOTE_ENABLED": "1",
}

# Render #2 / HEAVY.
WORKER_INTERNAL_ENV: Dict[str, str] = {
    "R43_DIRECT_HEAVY": "1",
    "HEAVY_HTTP_THREADS": "4",
    "PORT": "10000",
    "PEER_PING_ENABLED": "1",
    "PEER_PING_INTERVAL_SEC": "120",

    # Redis keys / retention
    "REDIS_RUNTIME_DEFAULT": "0",
    "WORKER_REDIS_SNAPSHOT_KEY": "vys262:bot_state:latest_gz",
    "WORKER_REDIS_SNAPSHOT_MAX_MB": "16",
    "WORKER_REDIS_DELTA_KEY": "vys262:bot_state:latest_gz:deltas_v1",
    "WORKER_REDIS_DELTA_MAX_ITEMS": "2000",
    "WORKER_REDIS_EVENT_PREFIX": "vys262:tg_events:v1",
    "WORKER_REDIS_CAPSULE_KEY": "vys262:durable_capsule:r20",
        "WORKER_CAPSULE_MEGA_ENABLED": "1",
        "WORKER_CAPSULE_MEGA_KEEP": "10",
    "WORKER_REDIS_CAPSULE_MAX_MB": "8",
    "WORKER_EVENT_RETENTION_SEC": "604800",
    "WORKER_EVENT_MAX_WIRE_KB": "512",
    "WORKER_EVENT_REDIS_QUEUE_MAX": "1024",
    "WORKER_EVENT_REDIS_RETRY_MS": "250",
    "WORKER_EVENT_REDIS_RECONCILE_SEC": "5",
    "WORKER_R32_EVENT_RETENTION_SEC": "2592000",
    "WORKER_R32_EVENT_MAX_WIRE_KB": "8192",
    "WORKER_R34_EVENT_LARGE_MAX_MB": "64",
    "WORKER_R34_EXPORT_REVISION_WAIT_SEC": "180",
    "WORKER_R34_RESULT_RETRY_SEC": "8",
    "WORKER_R34_RESULT_RETRY_WINDOW_SEC": "900",
    # R36 durable transport when REDIS_URL is absent.
    "WORKER_R36_RESULT_ATTEMPT_WINDOW_SEC": "45",
    "R36_MEGA_JOB_TIMEOUT": "180",
    "R36_MEGA_RECOVERY_SCAN_SEC": "45",
    "WORKER_R32_MEGA_SEGMENT_EVENTS": "128",
    "WORKER_R32_MEGA_FLUSH_SEC": "30",

    # Worker local cache / transport limits
    "WORKER_CACHE_DIR": "/tmp/vys262_worker",
    "WORKER_RESTORE_CACHE_MAX_AGE_SEC": "120",
    "WORKER_SNAPSHOT_UPLOAD_MAX_MB": "64",
    "WORKER_DELTA_MAX_WIRE_KB": "2048",
    "WORKER_DELTA_MAX_JSON_MB": "16",
    "WORKER_DELTA_MAX_PAGES": "4096",
    "WORKER_DELTA_MAX_DB_MB": "128",
    "WORKER_FRONT_FETCH_TIMEOUT": "30",
    "WORKER_FULL_REBASE_MIN_INTERVAL_SEC": "45",

    # Local full checkpoint / reconcile cadence
    "WORKER_FULL_CHECKPOINT_SEC": "21600",
    "WORKER_FULL_CHECKPOINT_MAX_DELTAS": "1000",
    "WORKER_FULL_CHECKPOINT_MAX_DELTA_MB": "16",
    "WORKER_MEGA_CHECKPOINT_SEC": "86400",
    "WORKER_RECONCILE_SEC": "21600",

    # R38 Google/Drive resilience and durable Google recovery
    "R38_GOOGLE_RETRY_WINDOW_SEC": "600",
    "R38_GOOGLE_CALLBACK_WINDOW_SEC": "60",
    "R38_GOOGLE_MEGA_SCAN_SEC": "45",
    "R38_GOOGLE_MEGA_TIMEOUT": "180",

    # MEGA command timeouts
    "MEGA_TIMEOUT": "180",
    "MEGA_LOGIN_TIMEOUT": "120",
    "MEGA_AUTOCREATE_LAYOUT": "1",
}



# R49 root-fix: keep the externally supplied Redis URL private while runtime Redis
# is OFF by default.  The owner can enable it explicitly from Info without a
# restart; a restart always returns to the packaged OFF default.
_REDIS_EXTERNAL_URL = str(os.environ.get("REDIS_URL", "") or "").strip()
_REDIS_RUNTIME_ENABLED = False
_REDIS_RUNTIME_INITIALIZED = False

def _apply_redis_runtime_state(enabled: bool) -> None:
    global _REDIS_RUNTIME_ENABLED
    requested = bool(enabled)
    _REDIS_RUNTIME_ENABLED = bool(requested and _REDIS_EXTERNAL_URL)
    os.environ["REDIS_URL"] = _REDIS_EXTERNAL_URL if _REDIS_RUNTIME_ENABLED else ""
    os.environ["REDIS_RUNTIME_ENABLED"] = "1" if _REDIS_RUNTIME_ENABLED else "0"

def redis_runtime_state() -> Dict[str, object]:
    return {
        "configured": bool(_REDIS_EXTERNAL_URL),
        "enabled": bool(_REDIS_RUNTIME_ENABLED and _REDIS_EXTERNAL_URL),
        "default_enabled": False,
    }

def set_redis_runtime_enabled(enabled: bool) -> Dict[str, object]:
    _apply_redis_runtime_state(bool(enabled))
    state = redis_runtime_state()
    if bool(enabled) and not state["configured"]:
        state["error"] = "REDIS_URL is not configured in Render"
    return state

def _init_redis_runtime_default() -> None:
    global _REDIS_RUNTIME_INITIALIZED
    if not _REDIS_RUNTIME_INITIALIZED:
        default_enabled = str(os.environ.get("REDIS_RUNTIME_DEFAULT", "0") or "0").strip().lower() in {"1", "true", "yes", "on", "да"}
        _apply_redis_runtime_state(default_enabled)
        _REDIS_RUNTIME_INITIALIZED = True
    else:
        _apply_redis_runtime_state(_REDIS_RUNTIME_ENABLED)

def install_internal_runtime_config(role: str) -> Dict[str, str]:
    """Install packaged tunables before the rest of the service reads os.environ."""
    role = str(role or "").strip().lower()
    values = FRONT_INTERNAL_ENV if role == "front" else WORKER_INTERNAL_ENV if role == "worker" else {}
    for key, value in values.items():
        os.environ[str(key)] = str(value)
    _init_redis_runtime_default()
    os.environ["VYS262_INTERNAL_CONFIG_VERSION"] = CONFIG_VERSION
    return dict(values)


def internal_runtime_config(role: str) -> Dict[str, str]:
    role = str(role or "").strip().lower()
    return dict(FRONT_INTERNAL_ENV if role == "front" else WORKER_INTERNAL_ENV if role == "worker" else {})
