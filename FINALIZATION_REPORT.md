# R65 HEAVY — Manual whole-account MEGA recovery browser

- Added authenticated read-only manual recovery endpoints:
  - `GET /internal/r65/mega/list`
  - `GET /internal/r65/mega/file`
- Folder listing uses immediate `mega-ls -l` output instead of recursively scanning the whole account.
- Manual browser may read arbitrary folders in the logged-in MEGA account for owner-triggered recovery.
- Automatic production MEGA paths remain strictly locked to Render `MEGA_BACKUP_DIR` via existing `mega_root()` policy.
- No automatic write outside the strict root was added.
- No PREV/ORIG/BASE compatibility chain added.
