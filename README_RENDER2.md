# Render #2 — HEAVY worker — R6 FULL STATE

Deploy this ZIP as the heavy worker. Start command: `python worker_service.py`.

R6 durability order:
1. Fetch + validate full SQLite from Render #1.
2. Atomically replace worker restore cache (only if not stale).
3. Store the exact gzip in shared Redis/Key Value.
4. Archive/promote it in MEGA in the heavy path.

Restore order:
1. Worker local cache.
2. Shared Redis durable snapshot (survives worker deploy/restart).
3. MEGA archive fallback.

An older delayed Front GET, Redis value, or MEGA snapshot cannot overwrite a newer cached revision. This prevents rollback to factory/default settings after deployments.

Google service-account JSON belongs only on Render #2.
The `REDIS_URL` must be the same Redis/Key Value used by Render #1.
