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

## R17 FAST TERMINAL CHAT + FULL RESTORE
See FIXES_R17_FAST_TERMINAL_CHAT_FULL_RESTORE.txt.

Render #2 remains the heavy durability/restore side. It receives/coalesces state from FAST; no new user-button dependency on HEAVY was introduced in R17.


## R18 EXACT RESTORE REBASE
See `FIXES_R18_INSTANT_CALLBACK_EXACT_DEPLOY_RESTORE.txt`.

HEAVY now accepts immediate full-rebase requests when the delta base mismatches, preserves a newer Redis restore point, and can capture the still-live old FAST during the next FAST preboot. Full snapshot validation, Redis/MEGA promotion and other heavy durability remain outside the FAST user-button path.


## R19 FAST CALLBACK + AUTHORITATIVE RESTORE
See `FIXES_R19_FAST_CALLBACK_AUTHORITATIVE_RESTORE.txt`.

R19 removes the second legacy boot restore on FAST, gives lightweight navigation callbacks their own dedicated FAST UI lane, moves post-update cleanup off the UI lane, fixes the repeated full-state rebase loop by promoting the exact served full snapshot as the next delta baseline, and pauses automatic Google sync cleanly when no target table is configured.

## R20 DURABLE CONFIG + TRUE FAST LANE
See `FIXES_R20_DURABLE_CONFIG_FAST_LANE.txt`.

HEAVY owns the R20 durable config/user-state capsule: local cache + Redis + asynchronous versioned MEGA archive. A deep capsule request can heal Redis/local cache from MEGA during FAST preboot.

## Пер-R21 — HEAVY SECOND STAGE
See `FIXES_R21_EVERY_BUTTON_FAST_HEAVY_STAGE.txt`.

Render #2 remains the heavy execution/durable service. R21 changes the Front boundary: every button is accepted by FAST first, while split-capable heavy work is dispatched here as a second stage. R20 durable capsule format/key stays compatible across the upgrade.
