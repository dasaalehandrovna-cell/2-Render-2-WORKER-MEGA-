# R60 — Redis logical modes + inspector

- `REDIS_ENABLED=0`: hard OFF.
- `REDIS_ENABLED=1`: Redis starts ON after deploy.
- Runtime menu changes are temporary until restart.
- `REDIS_START_ENABLED` is ignored in R60.
- HEAVY `/internal/runtime/redis` applies explicit ON/OFF transitions with fresh client sockets and PING verification.
- HEAVY `/internal/runtime/redis/inspect` is read-only, authenticated, bounded to 500 keys and masks sensitive previews.
- SQLite/local ledgers remain the correctness layer; Redis is background cache/outbox acceleration.
