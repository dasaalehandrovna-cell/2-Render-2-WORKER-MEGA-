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


## R7 system polish
- finance derived calculations are coalesced after the local SQLite commit;
- deep chat audit moves confirmed left/kicked chats to removed;
- Google setup is a 3-step menu and auto-tests the pasted table;
- CSV/XLSX serialization and Drive upload execute on Render #2.

### R7.1 finance/chat/google cleanup
- Owns normal CSV/XLSX serialization and Google Drive upload jobs.
- Google Sheets tabs use deterministic titles so repeat exports refresh the same tab instead of creating timestamp clutter.
- Exposes service-account email to the Front for the guided `/google` setup.
