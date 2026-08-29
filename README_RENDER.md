# выс-260 WORKER — Render №2

Start command:

    python worker_service.py

Required ENV:

- `FRONT_SERVICE_URL=https://<render-1>.onrender.com`
- `PEER_SHARED_SECRET=<same-long-random-secret-on-both-renders>`
- `MEGA_EMAIL=...`
- `MEGA_PASSWORD=...`
- `MEGA_BACKUP_DIR=TelegramBotBackups`

Optional:

- `MEGA_TIMEOUT=180`
- `WORKER_FRONT_FETCH_TIMEOUT=30`
- `WORKER_RESTORE_CACHE_MAX_AGE_SEC=120`

The worker does NOT need `B_T` and must not receive Telegram webhook traffic.

MEGAcmd must be installed in this Render service (use the same MEGAcmd build/install step as the current monolithic bot).

Pilot responsibilities in v260:
1. mutual keepalive every 600s;
2. receive `sync_state` jobs;
3. download a consistent SQLite snapshot from Render №1;
4. run `PRAGMA quick_check`;
5. archive/promote `/TelegramBotBackups/database/latest_bot_state.sqlite3.gz` in MEGA;
6. return that snapshot to Render №1 during deploy restore.

The generic `/internal/job` envelope is intentionally ready for the next stage (Excel/ZIP/journals), but v260 only accepts `sync_state` so that the first split deployment can be tested safely before moving user-facing file generators.
