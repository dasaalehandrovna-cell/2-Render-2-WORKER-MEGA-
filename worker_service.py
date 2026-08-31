#!/usr/bin/env python3
"""выс-260 WORKER service.

No Telegram token is required. Responsibilities in the pilot split:
- mutual 10-minute peer ping;
- authenticated job endpoint;
- fetch consistent SQLite snapshots from the front;
- SQLite quick_check before durability promotion;
- upload/archive latest SQLite snapshot in MEGA;
- serve latest durable snapshot back to the front for deploy restore.

The API intentionally includes a small generic job envelope so Excel/ZIP/journals can
be moved here incrementally without changing the front<->worker trust model.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import queue
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, request, Response

app = Flask(__name__)
VERSION = "выс-260-worker"


def env_bool(name: str, default: bool = False) -> bool:
    return str(os.getenv(name, "1" if default else "0") or "").strip().lower() in {"1", "true", "yes", "on", "да"}


def env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.getenv(name, str(default)) or str(default))))
    except Exception:
        return default


def peer_secret() -> str:
    return str(os.getenv("PEER_SHARED_SECRET", "") or "").strip()


def authorized() -> bool:
    expected = peer_secret()
    supplied = str(request.headers.get("X-Peer-Secret", "") or "")
    return bool(expected and secrets.compare_digest(expected, supplied))


def front_base() -> str:
    raw = str(os.getenv("FRONT_SERVICE_URL", os.getenv("PEER_SERVICE_URL", "")) or "").strip().rstrip("/")
    if raw and not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


def mega_root() -> str:
    return "/" + str(os.getenv("MEGA_BACKUP_DIR", "TelegramBotBackups2-2") or "TelegramBotBackups2-2").strip("/")


def remote_db_dir() -> str:
    return mega_root().rstrip("/") + "/database"


def remote_latest() -> str:
    return remote_db_dir().rstrip("/") + "/latest_bot_state.sqlite3.gz"


STATE_LOCK = threading.RLock()
MEGA_LOCK = threading.RLock()
STATE = {
    "started_at": time.time(),
    "peer_last_attempt": 0.0,
    "peer_last_ok": 0.0,
    "peer_last_error": "",
    "peer_status": None,
    "job_last_id": "",
    "job_last_type": "",
    "job_last_reason": "",
    "job_last_started": 0.0,
    "job_last_done": 0.0,
    "job_last_error": "",
    "sync_count": 0,
    "sync_failures": 0,
    "last_snapshot_sha256": "",
    "last_snapshot_size": 0,
    "last_snapshot_at": 0.0,
    "last_mega_upload_at": 0.0,
    "last_history_at": 0.0,
    "last_restore_download_at": 0.0,
    "mega_layout_ok": False,
    "mega_layout_at": 0.0,
    "mega_root": "",
    "mega_snapshot_present": False,
    "mega_migrated_from": "",
    "mega_migrated_at": 0.0,
}
JOB_Q: queue.Queue[dict] = queue.Queue(maxsize=32)
SYNC_PENDING_LOCK = threading.RLock()
SYNC_PENDING = False
CACHE_DIR = Path(os.getenv("WORKER_CACHE_DIR", "/tmp/vys260_worker") or "/tmp/vys260_worker")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_LATEST = CACHE_DIR / "latest_bot_state.sqlite3.gz"


def run_cmd(args, timeout=120):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, check=False)


def mega_login() -> tuple[bool, str]:
    login_timeout = env_int("MEGA_LOGIN_TIMEOUT", 120, 30, 300)
    try:
        who = run_cmd(["mega-whoami"], timeout=min(20, login_timeout))
    except FileNotFoundError:
        return False, "MEGAcmd not installed"
    except subprocess.TimeoutExpired:
        return False, "mega-whoami timeout"
    except Exception as exc:
        return False, f"mega-whoami {type(exc).__name__}"
    if who.returncode == 0:
        return True, "already logged in"
    email = str(os.getenv("MEGA_EMAIL", "") or "").strip()
    password = str(os.getenv("MEGA_PASSWORD", "") or "").strip()
    if not email or not password:
        return False, "MEGA_EMAIL/MEGA_PASSWORD missing"
    try:
        login = run_cmd(["mega-login", email, password], timeout=login_timeout)
    except subprocess.TimeoutExpired:
        # Never stringify TimeoutExpired: its command can contain the password.
        return False, f"mega-login timeout after {login_timeout}s"
    except Exception as exc:
        return False, f"mega-login {type(exc).__name__}"
    if login.returncode != 0:
        return False, (login.stderr or login.stdout or "mega-login failed")[:240]
    return True, "login OK"


def mega_path_exists(path: str) -> bool:
    try:
        r = run_cmd(["mega-ls", path], timeout=30)
        return r.returncode == 0
    except Exception:
        return False


def ensure_mega_dir(path: str) -> tuple[bool, str]:
    # Create sequentially (root -> database -> history) so this also works on
    # MEGAcmd builds where `mega-mkdir -p` is unavailable.
    if mega_path_exists(path):
        return True, "exists"
    try:
        r = run_cmd(["mega-mkdir", path], timeout=45)
    except subprocess.TimeoutExpired:
        return False, f"mega-mkdir timeout: {path}"
    except Exception as exc:
        return False, f"mega-mkdir {type(exc).__name__}: {path}"
    if r.returncode == 0 or mega_path_exists(path):
        return True, "created"
    return False, f"mega-mkdir failed {path}: {(r.stderr or r.stdout or '')[:180]}"


def mega_legacy_roots() -> list[str]:
    raw = str(os.getenv("MEGA_LEGACY_BACKUP_DIRS", "/TelegramBotBackups-2T,/TelegramBotBackups") or "")
    out = []
    current = mega_root()
    for item in raw.split(","):
        item = "/" + str(item or "").strip().strip("/")
        if item != "/" and item != current and item not in out:
            out.append(item)
    return out


def _mega_prepare_layout_locked() -> tuple[bool, str]:
    ok, detail = mega_login()
    if not ok:
        return False, detail
    for path in (mega_root(), remote_db_dir(), remote_db_dir() + "/history"):
        ok_dir, dir_detail = ensure_mega_dir(path)
        if not ok_dir:
            return False, dir_detail
    with STATE_LOCK:
        STATE["mega_layout_ok"] = True
        STATE["mega_layout_at"] = time.time()
        STATE["mega_root"] = mega_root()
    return True, f"MEGA layout ready: {mega_root()}"


def _mega_promote_legacy_remote_locked(src: str, root: str) -> tuple[bool, str]:
    """Copy an old remote gzip into the new root without deleting the old copy."""
    name = src.rsplit("/", 1)[-1]
    try:
        cp = run_cmd(["mega-cp", src, remote_db_dir()], timeout=120)
    except subprocess.TimeoutExpired:
        return False, f"legacy copy timeout from {root}"
    if cp.returncode != 0:
        return False, f"legacy copy failed from {root}"
    copied = remote_db_dir().rstrip("/") + "/" + name
    if copied != remote_latest():
        try:
            mv = run_cmd(["mega-mv", copied, remote_latest()], timeout=60)
        except subprocess.TimeoutExpired:
            return False, f"legacy promote timeout from {root}"
        if mv.returncode != 0 and not mega_path_exists(remote_latest()):
            return False, f"legacy promote failed from {root}"
    if mega_path_exists(remote_latest()):
        with STATE_LOCK:
            STATE["mega_migrated_from"] = root
            STATE["mega_migrated_at"] = time.time()
        return True, f"copied legacy snapshot from {root}"
    return False, f"legacy snapshot not visible after copy from {root}"


def _mega_manifest_generation_remote_locked(root: str) -> str:
    manifest_remote = root.rstrip("/") + "/database/current_manifest.json"
    if not mega_path_exists(manifest_remote):
        return ""
    workdir = Path(tempfile.mkdtemp(prefix="v260_worker_legacy_manifest_"))
    try:
        get = run_cmd(["mega-get", manifest_remote, str(workdir)], timeout=90)
        if get.returncode != 0:
            return ""
        rows = list(workdir.rglob("current_manifest.json")) + list(workdir.rglob("*.json"))
        if not rows:
            return ""
        try:
            payload = json.loads(rows[0].read_text(encoding="utf-8")) or {}
        except Exception:
            return ""
        remote_generation = str(payload.get("remote_generation") or "").strip()
        if remote_generation:
            return remote_generation
        generation = str(payload.get("generation") or "").strip()
        if generation:
            return root.rstrip("/") + "/database/generations/" + generation
        return ""
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _mega_try_migrate_legacy_latest_locked() -> tuple[bool, str]:
    # First launch helper: when the new root is empty, copy (never move/delete)
    # from the old layout. Prefer the legacy mirror, then the v242 immutable
    # current_manifest -> generation path used by recent bot versions.
    if mega_path_exists(remote_latest()):
        return True, "latest already present"
    for root in mega_legacy_roots():
        legacy_latest = root.rstrip("/") + "/database/latest_bot_state.sqlite3.gz"
        if mega_path_exists(legacy_latest):
            ok, detail = _mega_promote_legacy_remote_locked(legacy_latest, root)
            if ok:
                return ok, detail
        generation_remote = _mega_manifest_generation_remote_locked(root)
        if generation_remote and mega_path_exists(generation_remote):
            ok, detail = _mega_promote_legacy_remote_locked(generation_remote, root)
            if ok:
                return ok, detail
    return False, "no legacy latest/current_manifest snapshot found"


def quick_check_gzip(gz_path: Path) -> tuple[bool, str, dict]:
    workdir = Path(tempfile.mkdtemp(prefix="v260_worker_check_"))
    raw = workdir / "state.sqlite3"
    try:
        with gzip.open(gz_path, "rb") as src, open(raw, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        con = sqlite3.connect(str(raw))
        try:
            row = con.execute("PRAGMA quick_check").fetchone()
            if not row or str(row[0]).lower() != "ok":
                return False, f"SQLite quick_check={row}", {}
            meta = {}
            try:
                mrow = con.execute("SELECT v FROM meta WHERE kind='db_snapshot' AND k='main'").fetchone()
                if mrow and mrow[0]:
                    meta = json.loads(mrow[0]) or {}
            except Exception:
                meta = {}
        finally:
            con.close()
        sha = hashlib.sha256(gz_path.read_bytes()).hexdigest()
        return True, "ok", {"sha256_gz": sha, "size": gz_path.stat().st_size, "db_snapshot": meta}
    except Exception as exc:
        return False, f"{type(exc).__name__}: {str(exc)[:220]}", {}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def fetch_front_snapshot() -> tuple[Path | None, str, dict]:
    base, secret = front_base(), peer_secret()
    if not base or not secret:
        return None, "FRONT_SERVICE_URL/secret not configured", {}
    tmp = CACHE_DIR / f"incoming_{int(time.time()*1000)}.sqlite3.gz"
    try:
        r = requests.get(
            base + "/internal/split/state",
            headers={"X-Peer-Secret": secret, "User-Agent": "vys-260-worker-state-fetch"},
            timeout=env_int("WORKER_FRONT_FETCH_TIMEOUT", 30, 5, 120),
            stream=True,
        )
        if r.status_code != 200:
            return None, f"front HTTP {r.status_code}: {r.text[:180]}", {}
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    fh.write(chunk)
        ok, detail, meta = quick_check_gzip(tmp)
        if not ok:
            tmp.unlink(missing_ok=True)
            return None, detail, {}
        return tmp, "front snapshot OK", meta
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        return None, f"front {type(exc).__name__}: {str(exc)[:220]}", {}


def _mega_promote_snapshot_locked(local_gz: Path) -> tuple[bool, str]:
    ok, detail = mega_login()
    if not ok:
        return False, detail
    dbdir = remote_db_dir()
    layout_ok, layout_detail = _mega_prepare_layout_locked()
    if not layout_ok:
        return False, layout_detail
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tmp_name = f"incoming_{stamp}_{os.getpid()}.sqlite3.gz"
    tmp_remote = dbdir + "/" + tmp_name
    # Upload under a unique name first, then promote. Existing latest remains intact
    # until the new object is fully in MEGA.
    put = run_cmd(["mega-put", str(local_gz), dbdir], timeout=env_int("MEGA_TIMEOUT", 180, 30, 900))
    if put.returncode != 0:
        return False, f"mega-put failed: {(put.stderr or put.stdout)[:220]}"
    uploaded_default = dbdir + "/" + local_gz.name
    # local file may not have the unique filename; rename uploaded object to unique tmp.
    if uploaded_default != tmp_remote:
        mvtmp = run_cmd(["mega-mv", uploaded_default, tmp_remote], timeout=45)
        if mvtmp.returncode != 0:
            return False, f"mega-mv incoming failed: {(mvtmp.stderr or mvtmp.stdout)[:220]}"
    # Keep a periodic history copy, not one copy per finance event. This avoids
    # recreating the old high-MEGA-traffic problem on the worker.
    now_ts = time.time()
    history_every = env_int("WORKER_HISTORY_INTERVAL_SEC", 3600, 300, 86400)
    with STATE_LOCK:
        last_history = float(STATE.get("last_history_at") or 0.0)
    if now_ts - last_history >= history_every:
        hist = dbdir + f"/history/latest_{stamp}.sqlite3.gz"
        cp = run_cmd(["mega-cp", remote_latest(), hist], timeout=45)
        if cp.returncode == 0:
            with STATE_LOCK:
                STATE["last_history_at"] = now_ts
    run_cmd(["mega-rm", remote_latest()], timeout=30)
    mv = run_cmd(["mega-mv", tmp_remote, remote_latest()], timeout=45)
    if mv.returncode != 0:
        # best effort: leave incoming object so the snapshot is not lost
        return False, f"mega promote failed; incoming retained: {(mv.stderr or mv.stdout)[:220]}"
    return True, "MEGA latest promoted"



def mega_promote_snapshot(local_gz: Path) -> tuple[bool, str]:
    with MEGA_LOCK:
        try:
            return _mega_promote_snapshot_locked(local_gz)
        except subprocess.TimeoutExpired:
            return False, "MEGA upload command timeout"
        except Exception as exc:
            return False, f"MEGA upload {type(exc).__name__}"

def sync_state_job(job: dict) -> tuple[bool, str]:
    snap, detail, meta = fetch_front_snapshot()
    if not snap:
        return False, detail
    try:
        ok, detail = mega_promote_snapshot(snap)
        if not ok:
            return False, detail
        shutil.copy2(snap, CACHE_LATEST)
        with STATE_LOCK:
            STATE["last_snapshot_sha256"] = str(meta.get("sha256_gz") or "")
            STATE["last_snapshot_size"] = int(meta.get("size") or 0)
            STATE["last_snapshot_at"] = time.time()
            STATE["last_mega_upload_at"] = time.time()
            STATE["sync_count"] += 1
        return True, f"synced {meta.get('size', 0)} bytes sha={str(meta.get('sha256_gz') or '')[:12]}"
    finally:
        snap.unlink(missing_ok=True)


def _mega_download_latest_locked() -> tuple[Path | None, str]:
    ok, detail = mega_login()
    if not ok:
        return None, detail
    workdir = Path(tempfile.mkdtemp(prefix="v260_worker_mega_get_"))
    try:
        get = run_cmd(["mega-get", remote_latest(), str(workdir)], timeout=env_int("MEGA_TIMEOUT", 180, 30, 900))
        if get.returncode != 0:
            return None, f"mega-get failed: {(get.stderr or get.stdout)[:220]}"
        rows = list(workdir.rglob("latest_bot_state.sqlite3.gz"))
        if not rows:
            return None, "MEGA latest snapshot not found"
        ok, detail, meta = quick_check_gzip(rows[0])
        if not ok:
            return None, detail
        shutil.copy2(rows[0], CACHE_LATEST)
        with STATE_LOCK:
            STATE["last_snapshot_sha256"] = str(meta.get("sha256_gz") or "")
            STATE["last_snapshot_size"] = int(meta.get("size") or 0)
            STATE["last_snapshot_at"] = time.time()
            STATE["last_restore_download_at"] = time.time()
        return CACHE_LATEST, "MEGA latest OK"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)



def mega_download_latest() -> tuple[Path | None, str]:
    with MEGA_LOCK:
        try:
            return _mega_download_latest_locked()
        except subprocess.TimeoutExpired:
            return None, "MEGA download command timeout"
        except Exception as exc:
            return None, f"MEGA download {type(exc).__name__}"

def process_job(job: dict) -> None:
    global SYNC_PENDING
    job_id = str(job.get("id") or "")
    kind = str(job.get("type") or "")
    with STATE_LOCK:
        STATE["job_last_id"] = job_id
        STATE["job_last_type"] = kind
        STATE["job_last_reason"] = str(job.get("reason") or "")[:180]
        STATE["job_last_started"] = time.time()
        STATE["job_last_error"] = ""
    try:
        if kind == "sync_state":
            ok, detail = sync_state_job(job)
        else:
            ok, detail = False, f"unsupported pilot job type: {kind}"
        with STATE_LOCK:
            STATE["job_last_done"] = time.time()
            STATE["job_last_error"] = "" if ok else detail[:260]
            if not ok:
                STATE["sync_failures"] += 1
        print(f"[WORKER JOB] {job_id} {kind} ok={ok} {detail}", flush=True)
    finally:
        if kind == "sync_state":
            with SYNC_PENDING_LOCK:
                SYNC_PENDING = False


def worker_loop() -> None:
    while True:
        job = JOB_Q.get()
        try:
            process_job(job)
        except Exception as exc:
            with STATE_LOCK:
                STATE["job_last_error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
                STATE["sync_failures"] += 1
            print(f"[WORKER JOB ERROR] {exc}", flush=True)
        finally:
            JOB_Q.task_done()



def mega_warmup_once() -> None:
    # On a clean MEGA account/root, create the complete directory structure first.
    # Absence of latest_bot_state.sqlite3.gz is normal on first launch.
    time.sleep(1.0)
    with MEGA_LOCK:
        try:
            layout_ok, detail = _mega_prepare_layout_locked()
            path = None
            if layout_ok:
                if not mega_path_exists(remote_latest()):
                    _mega_try_migrate_legacy_latest_locked()
                if mega_path_exists(remote_latest()):
                    path, detail = _mega_download_latest_locked()
                else:
                    detail = f"MEGA layout ready; no latest snapshot yet in {mega_root()}"
            with STATE_LOCK:
                STATE["mega_warmup_ok"] = bool(layout_ok)
                STATE["mega_snapshot_present"] = bool(path)
                STATE["mega_warmup_detail"] = str(detail)[:220]
                STATE["mega_warmup_at"] = time.time()
            print(f"[WORKER MEGA] warmup layout={layout_ok} snapshot={bool(path)} {detail}", flush=True)
        except Exception as exc:
            with STATE_LOCK:
                STATE["mega_warmup_ok"] = False
                STATE["mega_warmup_detail"] = f"{type(exc).__name__}: {str(exc)[:180]}"
                STATE["mega_warmup_at"] = time.time()
            print(f"[WORKER MEGA] warmup failed {type(exc).__name__}", flush=True)

def peer_loop() -> None:
    time.sleep(5)
    while True:
        base = front_base()
        with STATE_LOCK:
            STATE["peer_last_attempt"] = time.time()
        if base:
            try:
                r = requests.get(base + "/peer/health", headers={"User-Agent": "vys-260-worker-peer-keepalive"}, timeout=12)
                with STATE_LOCK:
                    STATE["peer_status"] = int(r.status_code)
                    if 200 <= r.status_code < 300:
                        STATE["peer_last_ok"] = time.time(); STATE["peer_last_error"] = ""
                    else:
                        STATE["peer_last_error"] = f"HTTP {r.status_code}"
            except Exception as exc:
                with STATE_LOCK:
                    STATE["peer_status"] = None; STATE["peer_last_error"] = str(exc)[:220]
        else:
            with STATE_LOCK:
                STATE["peer_last_error"] = "FRONT_SERVICE_URL empty"
        time.sleep(600)  # fixed 10-minute mutual keepalive


@app.route("/", methods=["GET", "HEAD"])
@app.route("/healthz", methods=["GET", "HEAD"])
@app.route("/peer/health", methods=["GET", "HEAD"])
def health():
    if request.method == "HEAD":
        return "", 200
    with STATE_LOCK:
        state = dict(STATE)
    return {
        "ok": True,
        "role": "worker",
        "version": VERSION,
        "front_configured": bool(front_base()),
        "mega_configured": bool(os.getenv("MEGA_EMAIL") and os.getenv("MEGA_PASSWORD")),
        "queue_size": JOB_Q.qsize(),
        "state": state,
    }, 200


@app.route("/internal/job", methods=["POST"])
def internal_job():
    global SYNC_PENDING
    if not authorized():
        return {"ok": False}, 404
    body = request.get_json(silent=True) or {}
    kind = str(body.get("type") or "").strip()
    if kind != "sync_state":
        return {"ok": False, "error": "unsupported job type in pilot", "supported": ["sync_state"]}, 400
    with SYNC_PENDING_LOCK:
        if SYNC_PENDING:
            return {"ok": True, "status": "coalesced", "queue_size": JOB_Q.qsize()}, 202
        SYNC_PENDING = True
    job = {
        "id": secrets.token_hex(8),
        "type": kind,
        "reason": str(body.get("reason") or "")[:180],
        "created_at": time.time(),
    }
    try:
        JOB_Q.put_nowait(job)
    except queue.Full:
        with SYNC_PENDING_LOCK:
            SYNC_PENDING = False
        return {"ok": False, "error": "worker queue full"}, 503
    return {"ok": True, "status": "queued", "job_id": job["id"], "queue_size": JOB_Q.qsize()}, 202


@app.route("/internal/restore/latest", methods=["GET"])
def internal_restore_latest():
    if not authorized():
        return {"ok": False}, 404
    cache_max_age = env_int("WORKER_RESTORE_CACHE_MAX_AGE_SEC", 120, 0, 3600)
    use_cache = CACHE_LATEST.exists() and cache_max_age > 0 and time.time() - CACHE_LATEST.stat().st_mtime <= cache_max_age
    path = CACHE_LATEST if use_cache else None
    detail = "worker cache"
    if path is None:
        with MEGA_LOCK:
            layout_ok, layout_detail = _mega_prepare_layout_locked()
            if layout_ok and not mega_path_exists(remote_latest()):
                _mega_try_migrate_legacy_latest_locked()
            if layout_ok and mega_path_exists(remote_latest()):
                path, detail = _mega_download_latest_locked()
            else:
                path, detail = None, (layout_detail if not layout_ok else f"no latest snapshot in {mega_root()}")
    if not path or not path.exists():
        return {"ok": False, "error": detail}, 503
    payload = path.read_bytes()
    resp = Response(payload, status=200, mimetype="application/gzip")
    resp.headers["Content-Disposition"] = 'attachment; filename="latest_bot_state.sqlite3.gz"'
    resp.headers["X-Worker-Version"] = VERSION
    resp.headers["X-Restore-Source"] = detail
    resp.headers["X-SHA256"] = hashlib.sha256(payload).hexdigest()
    return resp


@app.route("/internal/status", methods=["GET"])
def internal_status():
    if not authorized():
        return {"ok": False}, 404
    with STATE_LOCK:
        return {"ok": True, "version": VERSION, "queue_size": JOB_Q.qsize(), "state": dict(STATE)}, 200


threading.Thread(target=worker_loop, name="v260-worker-jobs", daemon=True).start()
threading.Thread(target=peer_loop, name="v260-worker-peer", daemon=True).start()
threading.Thread(target=mega_warmup_once, name="v260-worker-mega-warmup", daemon=True).start()

if __name__ == "__main__":
    port = env_int("PORT", 10000, 1, 65535)
    app.run(host="0.0.0.0", port=port, threaded=True)
