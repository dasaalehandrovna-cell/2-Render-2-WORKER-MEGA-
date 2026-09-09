# ПРАВИЛА HEAVY — R49

**PATCH → FINALIZE → TEST → PACKAGE.**

- Один file owner: `process_file_job_r43`.
- Один Google owner: `process_google_job_r43`.
- Worker threads запускаются только после определения final owners.
- Job idempotency/exact-once сохраняются.
- Redis snapshot — быстрый emergency cache, MEGA — disaster/durable backup.
- Пересоздание MEGA root только явно и наблюдаемо через `MEGA_AUTOCREATE_LAYOUT`/STATE/log.
