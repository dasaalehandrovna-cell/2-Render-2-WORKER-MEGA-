# Render #2 HEAVY — R13 Event Journal

R13 adds a pre-commit remote Telegram event witness and monotonic operation states `RECEIVED → COMMITTED → MIRRORED`. Normal changes use compact SQLite page deltas. A full database moves from Front only after a rare hash mismatch or explicit deploy/shutdown checkpoint. Worker creates its own periodic checkpoints from the mirrored SQLite.

Recommended cadence: hash reconciliation every 6h, Worker Redis checkpoint every 6h/threshold, MEGA full checkpoint once per 24h.

---

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

R8 paired release: worker logic is unchanged from R7; this archive is version-paired with the R8 Front chat lifecycle fix.

## R9 Google style
- Restores original vys-262 Google Sheets cell colors, borders, wrapping, bold summary rows and number formatting.
- Existing R8 worker/restore/export behavior is unchanged.

## R10 unified UI / contours / Excel
- Service progress is human-readable; internal W/Ф232/Ф233 ids are hidden from ordinary users.
- All XLSX/Google Sheet outputs use the colored vys-262 financial palette.
- /ok is blocked in contour 1/2; disabled business-mode callbacks return to the mode menu.
- Owner UI has Google Excel and Render #2 health controls; peer health is bidirectional.
- Contour toggles force immediate keyboard redraw after state changes.


## R11 fast finance handoff
Worker не участвует в первичном финансовом commit. Он получает coalesced уведомление после локального commit/завершения update, скачивает консистентный полный SQLite, валидирует PRAGMA quick_check, сначала обновляет restore cache/Redis и затем архивирует в MEGA.

## R13 event journal + delta durability
Normal state changes arrive through `/internal/delta` as changed SQLite pages. Worker reconstructs the exact database, validates `PRAGMA quick_check` and SHA256, journals the delta to Redis, and keeps a local exact restore gzip. Worker creates a full Redis checkpoint from its own mirror every 6 hours (or safety threshold) by default; MEGA full checkpoint is limited to about once per 24 hours. A hash-only reconciliation with Front runs every 6 hours, and the full Front database is fetched only when hashes really differ. On Worker restart: Redis full checkpoint -> Redis delta replay -> MEGA fallback.


## R14 internal configuration
All runtime tuning values (intervals, limits, ports, feature switches and internal Redis key names) are packaged in `runtime_config.py`. Render Environment should contain only credentials, remote addresses and external Telegram/Google/MEGA identifiers. Stale tuning variables left in Render are ignored/overwritten at service startup.

## R15 FAST HOTPATH
- RAW event receipt acknowledges after Worker-local SQLite fsync; Redis event persistence is handled by a dedicated retrying queue/reconcile loop so Front is not held by Redis latency.
- Worker remains owner of Redis/MEGA/Google/heavy checkpoints. No new numeric Render ENV is required; R14 runtime_config.py remains authoritative.


## R16 FAST FINANCE + ALL COLOR XLSX
See FIXES_R16_FAST_FINANCE_ALL_COLOR_XLSX.txt.
