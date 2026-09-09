# FINALIZATION REPORT HEAVY — R50 ROOTFIX

- Redis runtime по умолчанию OFF на каждом старте; `/internal/runtime/redis` позволяет владельцу синхронно включать/выключать его через FAST Info.
- HEAVY остаётся единственным runtime-владельцем MEGA после старта FAST.
- `/internal/state/events`: ACK выдаётся только после долговечной архивации batch в MEGA (`mega-direct`).
- Exact snapshot handoff умеет синхронно продвинуть snapshot в canonical MEGA при manual restore/re-anchor.
- `/internal/restore/failed-tasks` восстанавливает failed-task объекты в MEGA без runtime MEGA credentials на FAST.
- `MEGA_AUTOCREATE_LAYOUT=1` сохранён.

Проверки: `FINALIZATION_GATE.py` PASS 29/29; Redis runtime OFF/ON/OFF test PASS.

- R50: если `_download_mega_latest()` нашёл валидный snapshot только в legacy root, HEAVY сразу вызывает canonical promote и seed-ит configured root; следующий FAST restart уже не зависит от legacy.
