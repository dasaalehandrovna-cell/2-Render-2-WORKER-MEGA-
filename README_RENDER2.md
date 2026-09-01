# Render #2 — HEAVY worker

Deploy this folder/ZIP as the second Render service.

- Start: `python worker_service.py` (Dockerfile already uses it).
- Receives state-sync jobs from Render #1.
- Fetches a consistent SQLite snapshot, runs `PRAGMA quick_check`, then promotes it to MEGA.
- Keeps a restore cache for fast boot recovery of Render #1.
- Runs Google OAuth + Google Sheets API here, never on the Telegram front.
- Google jobs are asynchronous and callback Render #1 with the final link/error.

Google setup:
1. Put your existing `GOOGLE_SERVICE_ACCOUNT_JSON` only in this Render Environment.
2. In Telegram open `/google` and choose the destination spreadsheet.
3. Share that spreadsheet with the service account `client_email` as Editor.

## r3 fixes from 2026-08-31 logs
- Google refresh reuses an existing tab with the same period title instead of failing on duplicate `addSheet`.
- Automatic v167/v261 refreshes now execute here using this service account.
- State-sync coalescing is dirty-aware: if front changes while a snapshot is being uploaded, one follow-up snapshot is guaranteed.
