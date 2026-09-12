# v262
#!/usr/bin/env python3
"""vys-262 Render #2 heavy worker · Пер-R45 STABLE direct transport.

Responsibilities:
- mutual peer health ping with Render #1;
- receive compact SQLite page deltas from front and validate reconstructed state;
- keep a Redis delta journal plus local exact restore cache;
- generate periodic full Redis/MEGA checkpoints locally;
- serve cached latest snapshot to Render #1 for fast deploy restore;
- execute Google Sheets creation (OAuth + Sheets API) away from Telegram frontend.
"""
from __future__ import annotations
import base64
import csv
import mimetypes
import gzip
import hashlib
import json
import io
import os
import queue
import re
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
try:
    import redis as _redis
except Exception:
    _redis = None
from flask import Flask, request, Response, send_file
from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from runtime_config import install_internal_runtime_config, CONFIG_VERSION as INTERNAL_CONFIG_VERSION, redis_runtime_state, set_redis_runtime_enabled, redis_effective_url, redis_render_url
install_internal_runtime_config("worker")

app = Flask(__name__)
VERSION = 'vys-262-worker-r63-shared-redis-recovery'
TRANSPORT_VERSION = 'vys-262-worker-r52-forensic-transport'
R52_FORENSIC_LOG = str(os.getenv('R52_FORENSIC_LOG','1') or '1').strip().lower() not in {'0','false','no','off'}

def _r52h(event, **fields):
    if not R52_FORENSIC_LOG: return
    try:
        parts=[f'R52DIAG HEAVY event={str(event or "?")[:80]}',f'mono={time.monotonic():.3f}',f'thread={threading.current_thread().name}',f'tid={threading.get_ident()}']
        for k,v in fields.items():
            low=str(k).casefold()
            if any(x in low for x in ('token','secret','password','authorization','cookie','session','credential','private_key')): v='<redacted>'
            if isinstance(v,(dict,list,tuple,set)): v=json.dumps(v,ensure_ascii=False,separators=(',',':'),default=str)
            text=str(v if v is not None else '').replace('\n','\\n').replace('\r','\\r')
            parts.append(f'{str(k)[:60]}={text[:900]}')
        print(' '.join(parts),flush=True)
    except Exception: pass

@app.before_request
def _r52h_http_enter():
    if not R52_FORENSIC_LOG: return None
    try:
        request.environ['r52.started_mono']=time.monotonic()
        _r52h('HTTP_IN',method=request.method,path=request.path,content_length=request.content_length or 0,user_agent=str(request.headers.get('User-Agent') or '')[:180],job_q=JOB_Q.qsize() if 'JOB_Q' in globals() else -1,google_q=GOOGLE_Q.qsize() if 'GOOGLE_Q' in globals() else -1)
    except Exception: pass
    return None

@app.after_request
def _r52h_http_exit(response):
    if R52_FORENSIC_LOG:
        try:
            started=float(request.environ.get('r52.started_mono') or time.monotonic())
            _r52h('HTTP_OUT',method=request.method,path=request.path,status=getattr(response,'status_code',0),elapsed=time.monotonic()-started,content_length=getattr(response,'content_length',None),job_q=JOB_Q.qsize() if 'JOB_Q' in globals() else -1,google_q=GOOGLE_Q.qsize() if 'GOOGLE_Q' in globals() else -1)
        except Exception: pass
    return response


def env_bool(name, default=False):
    return str(os.getenv(name, '1' if default else '0') or '').strip().lower() in {'1','true','yes','on','да'}

def mega_enabled():
    return env_bool('MEGA_ENABLED', True)


def env_int(name, default, lo, hi):
    try: return max(lo, min(hi, int(os.getenv(name, str(default)) or str(default))))
    except Exception: return default

_R43_DIRECT_HEAVY = env_bool('R43_DIRECT_HEAVY', True)
_R43_JOB_LOCK = threading.RLock()
_R43_SNAPSHOT_LOCK = threading.RLock()
_R43_SNAPSHOT_STATE = {'last_ok':0.0,'last_error':'','last_token':'','fetches':0,'bytes':0,'covered_revision':0}

def peer_secret(): return str(os.getenv('PEER_SHARED_SECRET','') or '').strip()
def authorized():
    expected, supplied = peer_secret(), str(request.headers.get('X-Peer-Secret','') or '')
    return bool(expected and secrets.compare_digest(expected, supplied))
def front_base():
    raw = str(os.getenv('FRONT_PRIVATE_URL','') or '').strip().rstrip('/')
    private = bool(raw)
    if not raw:
        raw = str(os.getenv('FRONT_SERVICE_URL', os.getenv('PEER_SERVICE_URL','')) or '').strip().rstrip('/')
    if raw and not raw.startswith(('http://','https://')):
        looks_private = private or raw.endswith('.internal') or '.internal:' in raw or (raw.startswith('render-') and ':' in raw)
        raw = ('http://' if looks_private else 'https://') + raw
    return raw
def mega_root():
    """R58: the only permitted MEGA namespace is Render MEGA_BACKUP_DIR."""
    raw = str(os.getenv('MEGA_BACKUP_DIR','') or '').strip().replace('\\','/')
    if not raw.strip('/'):
        if mega_enabled():
            raise RuntimeError('MEGA_ENABLED=1 requires MEGA_BACKUP_DIR in Render; strict root policy refuses defaults')
        return '/__MEGA_DISABLED__'
    return '/' + raw.strip('/')


def _mega_path_within_root(path):
    root = mega_root().rstrip('/')
    value = '/' + str(path or '').strip().strip('/')
    return value == root or value.startswith(root + '/')
def remote_db_dir(): return mega_root().rstrip('/') + '/database'
def remote_latest(): return remote_db_dir().rstrip('/') + '/latest_bot_state.sqlite3.gz'
def remote_history_dir(): return remote_db_dir().rstrip('/') + '/history'

# R58 fail closed: with MEGA enabled there must be exactly one explicit Render root.
if mega_enabled():
    _R58_STRICT_MEGA_ROOT = mega_root()
    print(f'[R58 MEGA] STRICT ROOT locked to {_R58_STRICT_MEGA_ROOT}', flush=True)
else:
    _R58_STRICT_MEGA_ROOT = str(os.getenv('MEGA_BACKUP_DIR','') or '').strip()

STATE_LOCK = threading.RLock()
MEGA_LOCK = threading.RLock()
GOOGLE_LOCK = threading.RLock()
STATE = {
    'started_at': time.time(), 'peer_last_attempt':0.0, 'peer_last_ok':0.0, 'peer_last_error':'', 'peer_status':None,
    'front_health':{},
    'job_last_id':'', 'job_last_type':'', 'job_last_reason':'', 'job_last_started':0.0, 'job_last_done':0.0, 'job_last_error':'',
    'sync_count':0, 'sync_failures':0, 'last_snapshot_sha256':'', 'last_snapshot_size':0, 'last_snapshot_at':0.0, 'last_full_fetch_started':0.0, 'full_fetch_suppressed':0,
    'last_mega_upload_at':0.0, 'last_restore_download_at':0.0, 'mega_layout_ok':False, 'mega_warmup_ok':False,
    'mega_root_recreated':False, 'mega_layout_created_dirs':[],
    'google_jobs':0, 'google_failures':0, 'google_last_ok':0.0, 'google_last_error':'',
    'cache_revision':0.0,
    'last_state_token':'', 'active_state_token':'', 'dirty_state_token':'', 'deduped_sync_requests':0,
    'redis_cache_ok':False, 'redis_last_write':0.0, 'redis_last_read':0.0, 'redis_last_error':'',
    'delta_count':0, 'delta_since_checkpoint':0, 'delta_bytes':0, 'delta_last_at':0.0, 'delta_last_pages':0, 'delta_last_error':'',
    'delta_replayed':0, 'full_checkpoint_at':0.0, 'full_checkpoint_count':0, 'last_state_sha256':'',
    'delta_bytes_since_checkpoint':0,
    'event_received':0, 'event_committed':0, 'event_mirrored':0, 'event_pending':0, 'event_last_at':0.0, 'event_last_error':'',
    'reconcile_last_at':0.0, 'reconcile_last_ok':0.0, 'reconcile_last_error':'', 'reconcile_full_resyncs':0,
}
JOB_Q = queue.Queue(maxsize=32)
GOOGLE_Q = queue.Queue(maxsize=16)
GOOGLE_JOB_LOCK = threading.RLock()
GOOGLE_JOB_STATUS = {}
SYNC_PENDING_LOCK = threading.RLock(); SYNC_PENDING = False; SYNC_DIRTY = False; SYNC_DIRTY_REASON = ''
RESTORE_REFRESH_LOCK = threading.RLock(); RESTORE_REFRESH_RUNNING = False
CACHE_DIR = Path(os.getenv('WORKER_CACHE_DIR','/tmp/vys262_worker') or '/tmp/vys262_worker'); CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_LATEST = CACHE_DIR / 'latest_bot_state.sqlite3.gz'
CACHE_DB = CACHE_DIR / 'latest_bot_state.sqlite3'
_GOOGLE_TOKEN = {'token':'','expires_at':0.0}
_REDIS_CLIENT = None
_REDIS_LOCK = threading.RLock()
_REDIS_SNAPSHOT_KEY = str(os.getenv('WORKER_REDIS_SNAPSHOT_KEY','vys262:bot_state:latest_gz') or 'vys262:bot_state:latest_gz').strip()
_REDIS_META_KEY = _REDIS_SNAPSHOT_KEY + ':meta'
_REDIS_DELTA_KEY = str(os.getenv('WORKER_REDIS_DELTA_KEY', _REDIS_SNAPSHOT_KEY + ':deltas_v1') or (_REDIS_SNAPSHOT_KEY + ':deltas_v1')).strip()
_REDIS_DELTA_META_KEY = _REDIS_DELTA_KEY + ':meta'
_REDIS_EVENT_PREFIX = str(os.getenv('WORKER_REDIS_EVENT_PREFIX','vys262:tg_events:v1') or 'vys262:tg_events:v1').strip()
_REDIS_CAPSULE_KEY = str(os.getenv('WORKER_REDIS_CAPSULE_KEY','vys262:durable_capsule:r20') or 'vys262:durable_capsule:r20').strip()
CAPSULE_LOCAL = CACHE_DIR / 'durable_capsule_r20.json.gz'
CAPSULE_LOCK = threading.RLock()
CAPSULE_MEGA_Q = queue.Queue(maxsize=1)
CAPSULE_MEGA_STATE = {'last_ok':0.0,'last_error':'','uploads':0,'restores':0}
EVENT_DB = CACHE_DIR / 'event_journal.sqlite3'
EVENT_LOCK = threading.RLock()
EVENT_REDIS_Q = queue.Queue(maxsize=env_int('WORKER_EVENT_REDIS_QUEUE_MAX',2048,64,20000))

def _event_db_init_v268():
    with EVENT_LOCK:
        conn=sqlite3.connect(EVENT_DB,timeout=10)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=FULL')
            conn.execute("CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, update_id TEXT, chat_id TEXT, update_type TEXT, payload_json TEXT, payload_sha256 TEXT, state TEXT, received_at REAL, committed_at REAL, mirrored_at REAL, state_token TEXT, last_error TEXT, updated_at REAL)")
            conn.execute('CREATE INDEX IF NOT EXISTS idx_events_state_time ON events(state,updated_at)')
            conn.commit()
        finally: conn.close()

def _event_local_upsert_v268(row:dict):
    if not isinstance(row,dict): return False
    eid=str(row.get('event_id') or row.get('update_id') or '')
    if not eid: return False
    _event_db_init_v268()
    with EVENT_LOCK:
        conn=sqlite3.connect(EVENT_DB,timeout=10)
        try:
            old=conn.execute('SELECT payload_json,payload_sha256,state,received_at,committed_at,mirrored_at,state_token,last_error FROM events WHERE event_id=?',(eid,)).fetchone()
            payload=row.get('payload')
            payload_json=json.dumps(payload,ensure_ascii=False,separators=(',',':'),default=str) if isinstance(payload,dict) else (old[0] if old else '{}')
            incoming_state=str(row.get('state') or (old[2] if old else 'received'))
            old_state=str(old[2] if old else '')
            _rank={'received':1,'failed_retry':1,'committed':2,'mirrored':3,'checkpointed':4,'done':4}
            state=old_state if _rank.get(old_state,0)>_rank.get(incoming_state,0) else incoming_state
            received=float(row.get('received_at') or (old[3] if old else time.time()) or time.time())
            committed=float(row.get('committed_at') or (old[4] if old else 0.0) or 0.0)
            mirrored=float(row.get('mirrored_at') or (old[5] if old else 0.0) or 0.0)
            token=str(row.get('state_token') or (old[6] if old else '') or '')[:120]
            err=str(row.get('last_error') or (old[7] if old else '') or '')[:300]
            conn.execute("INSERT INTO events(event_id,update_id,chat_id,update_type,payload_json,payload_sha256,state,received_at,committed_at,mirrored_at,state_token,last_error,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET update_id=excluded.update_id,chat_id=excluded.chat_id,update_type=excluded.update_type,payload_json=CASE WHEN excluded.payload_json!='{}' THEN excluded.payload_json ELSE events.payload_json END,payload_sha256=CASE WHEN excluded.payload_sha256!='' THEN excluded.payload_sha256 ELSE events.payload_sha256 END,state=excluded.state,received_at=events.received_at,committed_at=CASE WHEN excluded.committed_at>0 THEN excluded.committed_at ELSE events.committed_at END,mirrored_at=CASE WHEN excluded.mirrored_at>0 THEN excluded.mirrored_at ELSE events.mirrored_at END,state_token=CASE WHEN excluded.state_token!='' THEN excluded.state_token ELSE events.state_token END,last_error=excluded.last_error,updated_at=excluded.updated_at",
                (eid,str(row.get('update_id') or eid),str(row.get('chat_id') if row.get('chat_id') is not None else ''),str(row.get('update_type') or 'other')[:40],payload_json,str(row.get('payload_sha256') or (old[1] if old else '') or ''),state,received,committed,mirrored,token,err,time.time()))
            conn.commit(); return True
        finally: conn.close()

def _event_redis_store_v268(row:dict):
    client=_redis_client()
    if client is None: return False,'REDIS_URL not configured'
    eid=str(row.get('event_id') or row.get('update_id') or '')
    if not eid: return False,'event id empty'
    try:
        key=f'{_REDIS_EVENT_PREFIX}:event:{eid}'; pending=f'{_REDIS_EVENT_PREFIX}:pending'
        current={}
        raw=client.get(key)
        if raw:
            try: current=json.loads(raw.decode('utf-8') if isinstance(raw,(bytes,bytearray)) else raw)
            except Exception: current={}
        merged=dict(current or {}); merged.update(row)
        _rank={'received':1,'failed_retry':1,'committed':2,'mirrored':3,'checkpointed':4,'done':4}
        old_state=str((current or {}).get('state') or ''); new_state=str((row or {}).get('state') or '')
        if _rank.get(old_state,0)>_rank.get(new_state,0): merged['state']=old_state
        merged['updated_at']=time.time()
        ttl=env_int('WORKER_EVENT_RETENTION_SEC',604800,86400,2592000)
        pipe=client.pipeline(transaction=True)
        pipe.set(key,json.dumps(merged,ensure_ascii=False,separators=(',',':'),default=str),ex=ttl)
        if str(merged.get('state') or '') in {'mirrored','checkpointed','done'}: pipe.zrem(pending,eid)
        else: pipe.zadd(pending,{eid:float(merged.get('received_at') or time.time())})
        pipe.execute(); return True,'Redis event stored'
    except Exception as exc: return False,f'{type(exc).__name__}: {str(exc)[:220]}'



def _event_redis_enqueue_v270(row:dict) -> bool:
    """R15: acknowledge the remote witness after Worker-local fsync; Redis flush is detached.

    This keeps Render #1 fast while retaining a second-service copy immediately.  The
    reconcile loop retries every unmirrored local event into Redis until it succeeds.
    """
    try:
        EVENT_REDIS_Q.put_nowait(dict(row or {}))
        return True
    except queue.Full:
        return False

def _event_redis_flush_loop_v270():
    while True:
        row = EVENT_REDIS_Q.get()
        try:
            ok, detail = _event_redis_store_v268(row)
            if not ok:
                with STATE_LOCK:
                    STATE['event_last_error'] = ('async redis: ' + str(detail))[:220]
                # Small retry without blocking the HTTP receipt path.
                time.sleep(env_int('WORKER_EVENT_REDIS_RETRY_MS',250,10,5000)/1000.0)
                try: EVENT_REDIS_Q.put_nowait(row)
                except queue.Full: pass
        except Exception as exc:
            with STATE_LOCK:
                STATE['event_last_error'] = f'async redis {type(exc).__name__}: {str(exc)[:180]}'
        finally:
            EVENT_REDIS_Q.task_done()

def _event_hydrate_pending_from_redis_v268(limit=250):
    client=_redis_client()
    if client is None: return 0
    loaded=0
    try:
        ids=client.zrange(f'{_REDIS_EVENT_PREFIX}:pending',0,max(0,min(999,int(limit)-1))) or []
        for raw_id in ids:
            eid=raw_id.decode() if isinstance(raw_id,(bytes,bytearray)) else str(raw_id)
            raw=client.get(f'{_REDIS_EVENT_PREFIX}:event:{eid}')
            if not raw: continue
            try: row=json.loads(raw.decode('utf-8') if isinstance(raw,(bytes,bytearray)) else raw)
            except Exception: continue
            if _event_local_upsert_v268(row): loaded+=1
        return loaded
    except Exception: return loaded

def _event_mark_mirrored_v268(event_ids,state_token=''):
    ids=[str(x) for x in (event_ids or []) if str(x)]
    if not ids: return 0
    n=0
    for eid in ids:
        row={'event_id':eid,'update_id':eid,'state':'mirrored','mirrored_at':time.time(),'state_token':str(state_token or '')[:120],'last_error':''}
        if _event_local_upsert_v268(row): n+=1
        _event_redis_store_v268(row)
    with STATE_LOCK:
        STATE['event_mirrored']=int(STATE.get('event_mirrored') or 0)+n; STATE['event_last_at']=time.time()
        try:
            conn=sqlite3.connect(EVENT_DB); STATE['event_pending']=int(conn.execute("SELECT COUNT(*) FROM events WHERE state IN ('received','failed_retry','committed')").fetchone()[0]); conn.close()
        except Exception: pass
    return n

def _event_pending_rows_v268(limit=100):
    _event_hydrate_pending_from_redis_v268(limit*2)
    _event_db_init_v268()
    with EVENT_LOCK:
        conn=sqlite3.connect(EVENT_DB,timeout=10)
        try:
            rows=conn.execute("SELECT event_id,update_id,chat_id,update_type,payload_json,payload_sha256,state,received_at,committed_at,state_token,last_error FROM events WHERE state IN ('received','failed_retry','committed') ORDER BY received_at ASC LIMIT ?",(max(1,min(250,int(limit))),)).fetchall()
        finally: conn.close()
    out=[]
    for r in rows:
        try: payload=json.loads(r[4] or '{}')
        except Exception: payload={}
        chat=None
        try: chat=int(r[2]) if str(r[2]).strip() else None
        except Exception: chat=r[2]
        out.append({'event_id':r[0],'update_id':r[1],'chat_id':chat,'update_type':r[3],'payload':payload,'payload_sha256':r[5],'state':r[6],'received_at':r[7],'committed_at':r[8],'state_token':r[9],'last_error':r[10]})
    with STATE_LOCK: STATE['event_pending']=len(out)
    return out

def _event_reconcile_loop_v268():
    while True:
        time.sleep(env_int('WORKER_EVENT_REDIS_RECONCILE_SEC',5,1,300))
        try:
            if not str(redis_effective_url() or '').strip() or _redis is None:
                try:
                    _event_db_init_v268()
                    with EVENT_LOCK:
                        conn=sqlite3.connect(EVENT_DB,timeout=10)
                        pending=int(conn.execute("SELECT COUNT(*) FROM events WHERE state IN ('received','failed_retry','committed')").fetchone()[0])
                        conn.close()
                    with STATE_LOCK:
                        STATE['event_pending']=pending; STATE['event_last_error']=''
                except Exception:
                    pass
                continue
            _event_hydrate_pending_from_redis_v268(500)
            # R15: any Worker-local event not yet guaranteed in Redis is retried here.
            _event_db_init_v268()
            with EVENT_LOCK:
                conn=sqlite3.connect(EVENT_DB,timeout=10)
                rows=conn.execute("SELECT event_id,update_id,chat_id,update_type,payload_json,payload_sha256,state,received_at,committed_at,state_token,last_error FROM events WHERE state IN ('received','failed_retry','committed') ORDER BY updated_at ASC LIMIT 200").fetchall()
                conn.close()
            for r in rows:
                try: payload=json.loads(r[4] or '{}')
                except Exception: payload={}
                row={'event_id':r[0],'update_id':r[1],'chat_id':r[2],'update_type':r[3],'payload':payload,'payload_sha256':r[5],'state':r[6],'received_at':r[7],'committed_at':r[8],'state_token':r[9],'last_error':r[10]}
                _event_redis_store_v268(row)
            # prune old mirrored local diagnostics; Redis keys expire independently.
            cutoff=time.time()-env_int('WORKER_EVENT_RETENTION_SEC',604800,86400,2592000)
            with EVENT_LOCK:
                conn=sqlite3.connect(EVENT_DB,timeout=10); conn.execute("DELETE FROM events WHERE state='mirrored' AND updated_at<?",(cutoff,)); conn.commit(); conn.close()
        except Exception as exc:
            with STATE_LOCK: STATE['event_last_error']=f'{type(exc).__name__}: {str(exc)[:180]}'

def _redis_client():
    global _REDIS_CLIENT, _R44_TEST_REDIS_CLIENT
    if _redis is None:
        return None
    url = str(redis_effective_url() or '').strip()
    if not url:
        return None
    with _REDIS_LOCK:
        if _REDIS_CLIENT is None:
            _REDIS_CLIENT = _redis.Redis.from_url(url, socket_connect_timeout=5, socket_timeout=12, health_check_interval=30)
        return _REDIS_CLIENT

def redis_store_snapshot(local_gz: Path, meta: dict, clear_deltas: bool=False):
    client = _redis_client()
    if client is None:
        return False, 'REDIS_URL not configured'
    try:
        payload = local_gz.read_bytes()
        max_bytes = env_int('WORKER_REDIS_SNAPSHOT_MAX_MB',16,1,128) * 1024 * 1024
        if len(payload) > max_bytes:
            return False, f'snapshot too large for Redis: {len(payload)} > {max_bytes}'
        incoming_revision = float((meta or {}).get('revision') or 0.0)
        existing_revision = 0.0
        try:
            existing_raw = client.get(_REDIS_META_KEY)
            if isinstance(existing_raw, (bytes, bytearray)):
                existing_raw = existing_raw.decode('utf-8', 'replace')
            existing_meta = json.loads(existing_raw) if isinstance(existing_raw, str) and existing_raw else {}
            existing_revision = float((existing_meta or {}).get('revision') or 0.0)
        except Exception:
            existing_revision = 0.0
        if existing_revision > incoming_revision + 0.000001:
            with STATE_LOCK:
                STATE['redis_cache_ok'] = True; STATE['redis_last_write'] = time.time(); STATE['redis_last_error'] = 'newer Redis snapshot preserved'
            return True, f'Redis newer snapshot preserved existing={existing_revision} incoming={incoming_revision}'
        row = {
            'revision': incoming_revision,
            'sha256_gz': str((meta or {}).get('sha256_gz') or hashlib.sha256(payload).hexdigest()),
            'size': len(payload), 'saved_at': time.time(), 'version': TRANSPORT_VERSION,
        }
        pipe = client.pipeline(transaction=True)
        pipe.set(_REDIS_SNAPSHOT_KEY, payload)
        pipe.set(_REDIS_META_KEY, json.dumps(row, separators=(',',':')))
        if clear_deltas:
            pipe.delete(_REDIS_DELTA_KEY)
            pipe.delete(_REDIS_DELTA_META_KEY)
        pipe.execute()
        with STATE_LOCK:
            STATE['redis_cache_ok'] = True; STATE['redis_last_write'] = time.time(); STATE['redis_last_error'] = ''
        return True, 'Redis snapshot cached'
    except Exception as exc:
        with STATE_LOCK:
            STATE['redis_cache_ok'] = False; STATE['redis_last_error'] = f'{type(exc).__name__}: {str(exc)[:220]}'
        return False, STATE['redis_last_error']

def redis_load_snapshot_to_cache():
    client = _redis_client()
    if client is None:
        return False, 'REDIS_URL not configured'
    fd, name = tempfile.mkstemp(prefix='vys262_redis_', suffix='.sqlite3.gz'); os.close(fd)
    tmp = Path(name)
    try:
        payload = client.get(_REDIS_SNAPSHOT_KEY)
        if not payload:
            return False, 'Redis snapshot missing'
        tmp.write_bytes(payload)
        ok, detail, meta = quick_check_gzip(tmp)
        if not ok:
            return False, 'Redis snapshot invalid: ' + str(detail)[:240]
        incoming_revision = float(meta.get('revision') or 0.0)
        with STATE_LOCK:
            current_revision = float(STATE.get('cache_revision') or 0.0)
        cache_exists = CACHE_LATEST.exists()
        accept = (not cache_exists) or current_revision <= 0.0 or (incoming_revision > 0.0 and incoming_revision >= current_revision)
        if accept:
            tmp_cache = CACHE_DIR / f'.redis_{secrets.token_hex(6)}.tmp'
            shutil.copy2(tmp, tmp_cache); os.replace(tmp_cache, CACHE_LATEST)
            with STATE_LOCK:
                STATE['cache_revision'] = max(current_revision, incoming_revision)
                STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or '')
                STATE['last_snapshot_size'] = int(meta.get('size') or len(payload))
        elif cache_exists:
            with STATE_LOCK:
                STATE['redis_cache_ok'] = True; STATE['redis_last_read'] = time.time(); STATE['redis_last_error'] = 'older Redis snapshot ignored'
            return True, f'Redis snapshot older than live cache; kept cache revision={current_revision}'
        with STATE_LOCK:
            STATE['redis_cache_ok'] = True; STATE['redis_last_read'] = time.time(); STATE['redis_last_error'] = ''
        return True, 'Redis latest OK'
    except Exception as exc:
        with STATE_LOCK:
            STATE['redis_cache_ok'] = False; STATE['redis_last_error'] = f'{type(exc).__name__}: {str(exc)[:220]}'
        return False, STATE['redis_last_error']
    finally:
        tmp.unlink(missing_ok=True)


def run_cmd(args, timeout=120):
    if args and str(args[0]).startswith('mega-') and not mega_enabled():
        return subprocess.CompletedProcess(args, 90, '', 'MEGA disabled by MEGA_ENABLED=0')
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, check=False)


def mega_login():
    if not mega_enabled():
        return False, 'MEGA disabled by MEGA_ENABLED=0'
    login_timeout = env_int('MEGA_LOGIN_TIMEOUT',120,30,300)
    try:
        who = run_cmd(['mega-whoami'], timeout=min(20,login_timeout))
        if who.returncode == 0: return True, 'already logged in'
    except FileNotFoundError: return False, 'MEGAcmd not installed'
    except Exception: pass
    session = str(os.getenv('MEGA_SESSION','') or '').strip(); email = str(os.getenv('MEGA_EMAIL','') or '').strip(); password = str(os.getenv('MEGA_PASSWORD','') or '').strip()
    if not session and (not email or not password): return False, 'MEGA_SESSION or MEGA_EMAIL/MEGA_PASSWORD missing'
    args = ['mega-login', session] if session else ['mega-login', email, password]
    try: run_cmd(['mega-logout'], timeout=20)
    except Exception: pass
    for attempt in (1,2):
        try:
            p = run_cmd(args, timeout=login_timeout)
            if p.returncode == 0: return True, 'login OK'
            err = (p.stderr or p.stdout or 'mega-login rejected')[:220]
        except subprocess.TimeoutExpired: return False, f'mega-login timeout after {login_timeout}s'
        except Exception as exc: return False, f'mega-login {type(exc).__name__}'
        if attempt == 1:
            try: run_cmd(['mega-logout'], timeout=20)
            except Exception: pass
            time.sleep(.8)
    return False, err


def mega_exists(path):
    try: return run_cmd(['mega-ls', path], timeout=30).returncode == 0
    except Exception: return False


def ensure_mega_dir(path):
    if mega_exists(path): return True
    try: p = run_cmd(['mega-mkdir', path], timeout=45)
    except Exception: return False
    return p.returncode == 0 or mega_exists(path)


def prepare_mega_layout():
    """R49: make MEGA layout recreation explicit and observable.

    Redis/HEAVY cache are allowed to restore the bot without MEGA.  When a later
    disaster-checkpoint needs MEGA and its root was manually removed, HEAVY may
    recreate it only when MEGA_AUTOCREATE_LAYOUT=1 (default).
    """
    ok, detail = mega_login()
    if not ok: return False, detail
    paths = (mega_root(), remote_db_dir(), remote_history_dir())
    missing = [p for p in paths if not mega_exists(p)]
    if missing and not env_bool('MEGA_AUTOCREATE_LAYOUT', True):
        with STATE_LOCK:
            STATE['mega_layout_ok'] = False
            STATE['mega_root_recreated'] = False
            STATE['mega_layout_created_dirs'] = []
        return False, 'MEGA layout missing; autocreate disabled: ' + ', '.join(missing)
    created = []
    for p in paths:
        if mega_exists(p):
            continue
        if not ensure_mega_dir(p):
            return False, f'cannot create MEGA dir {p}'
        created.append(p)
    root_recreated = mega_root() in created
    with STATE_LOCK:
        STATE['mega_layout_ok'] = True
        STATE['mega_root_recreated'] = bool(root_recreated)
        STATE['mega_layout_created_dirs'] = list(created)
    if root_recreated:
        print('[R49] MEGA ROOT RECREATED ' + mega_root(), flush=True)
    return True, ('MEGA layout ready' if not created else 'MEGA layout ready; created=' + ','.join(created))


def mega_legacy_roots():
    # R58: legacy roots are intentionally disabled.
    return []


def mega_restore_candidates():
    # R58: restore may read only the configured Render root.
    return [remote_latest()]


def quick_check_gzip(gz_path: Path):
    work = Path(tempfile.mkdtemp(prefix='vys262_check_')); raw = work / 'state.sqlite3'
    try:
        with gzip.open(gz_path,'rb') as src, open(raw,'wb') as dst: shutil.copyfileobj(src,dst,1024*1024)
        con = sqlite3.connect(str(raw))
        continuity_revision = 0.0
        try:
            row = con.execute('PRAGMA quick_check').fetchone()
            user_state_seq = 0
            try:
                for kind in ('split_state_revision_r18','runtime_continuity_v263','user_state_shadow_v265'):
                    meta_row = con.execute("SELECT v FROM meta WHERE kind=? AND k=?", (kind,'latest')).fetchone()
                    if not meta_row:
                        continue
                    meta_payload = json.loads(meta_row[0]) if isinstance(meta_row[0], str) else (meta_row[0] or {})
                    continuity_revision = max(continuity_revision, float((meta_payload or {}).get('saved_at') or 0.0))
                    if kind == 'user_state_shadow_v265':
                        user_state_seq = int((meta_payload or {}).get('seq') or 0)
            except Exception:
                pass
        finally: con.close()
        if not row or str(row[0]).lower() != 'ok': return False, f'quick_check={row}', {}
        payload = gz_path.read_bytes(); return True, 'OK', {'sha256_gz':hashlib.sha256(payload).hexdigest(),'size':len(payload),'revision':continuity_revision,'user_state_seq':user_state_seq}
    except Exception as exc: return False, f'{type(exc).__name__}: {str(exc)[:180]}', {}
    finally: shutil.rmtree(work, ignore_errors=True)



DELTA_APPLY_LOCK = threading.RLock()

def _sha256_file_v267(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def _sqlite_page_size_v267(path: Path) -> int:
    with open(path, 'rb') as fh:
        head = fh.read(100)
    if len(head) < 18 or head[:16] != b'SQLite format 3\x00':
        raise RuntimeError('not a SQLite database')
    size = int.from_bytes(head[16:18], 'big')
    if size == 1:
        size = 65536
    if size < 512 or size > 65536 or (size & (size - 1)):
        raise RuntimeError(f'invalid SQLite page size {size}')
    return size

def _quick_check_db_v267(path: Path):
    try:
        con = sqlite3.connect(str(path))
        try:
            row = con.execute('PRAGMA quick_check').fetchone()
            revision = 0.0
            user_state_seq = 0
            try:
                for kind in ('split_state_revision_r18','runtime_continuity_v263','user_state_shadow_v265'):
                    meta_row = con.execute("SELECT v FROM meta WHERE kind=? AND k=?", (kind,'latest')).fetchone()
                    if not meta_row:
                        continue
                    payload = json.loads(meta_row[0]) if isinstance(meta_row[0], str) else (meta_row[0] or {})
                    revision = max(revision, float((payload or {}).get('saved_at') or 0.0))
                    if kind == 'user_state_shadow_v265':
                        user_state_seq = int((payload or {}).get('seq') or 0)
            except Exception:
                pass
        finally:
            con.close()
        if not row or str(row[0]).lower() != 'ok':
            return False, f'quick_check={row}', {}
        return True, 'OK', {'revision':revision,'user_state_seq':user_state_seq,'sha256_db':_sha256_file_v267(path),'size_db':path.stat().st_size}
    except Exception as exc:
        return False, f'{type(exc).__name__}: {str(exc)[:220]}', {}

def _gzip_cache_db_v267():
    if not CACHE_DB.exists():
        return False, 'CACHE_DB missing', {}
    fd, name = tempfile.mkstemp(prefix='vys262_cache_', suffix='.sqlite3.gz'); os.close(fd)
    tmp = Path(name)
    try:
        with open(CACHE_DB,'rb') as src, gzip.open(tmp,'wb',compresslevel=9) as dst:
            shutil.copyfileobj(src,dst,1024*1024)
        ok, detail, meta = quick_check_gzip(tmp)
        if not ok:
            return False, detail, {}
        target = CACHE_DIR / f'.latest_gz_{secrets.token_hex(6)}.tmp'
        shutil.copy2(tmp,target); os.replace(target,CACHE_LATEST)
        return True, 'OK', meta
    finally:
        tmp.unlink(missing_ok=True)

def _ensure_cache_db_v267():
    if CACHE_DB.exists():
        ok, detail, meta = _quick_check_db_v267(CACHE_DB)
        if ok:
            with STATE_LOCK:
                STATE['last_state_sha256'] = str(meta.get('sha256_db') or '')
            return True, 'cache DB OK', meta
        CACHE_DB.unlink(missing_ok=True)
    if not CACHE_LATEST.exists():
        return False, 'CACHE_LATEST missing', {}
    fd, name = tempfile.mkstemp(prefix='vys262_cache_db_', suffix='.sqlite3'); os.close(fd)
    tmp = Path(name)
    try:
        with gzip.open(CACHE_LATEST,'rb') as src, open(tmp,'wb') as dst:
            shutil.copyfileobj(src,dst,1024*1024)
        ok, detail, meta = _quick_check_db_v267(tmp)
        if not ok:
            return False, 'decompressed cache invalid: '+detail, {}
        target = CACHE_DIR / f'.latest_db_{secrets.token_hex(6)}.tmp'
        shutil.copy2(tmp,target); os.replace(target,CACHE_DB)
        with STATE_LOCK:
            STATE['last_state_sha256'] = str(meta.get('sha256_db') or '')
        return True, 'cache DB hydrated', meta
    finally:
        tmp.unlink(missing_ok=True)

def _redis_append_delta_v267(wire: bytes, payload: dict):
    client = _redis_client()
    if client is None:
        return False, 'REDIS_URL not configured'
    try:
        max_items = env_int('WORKER_REDIS_DELTA_MAX_ITEMS',2000,10,20000)
        row = {'new_sha256':str(payload.get('new_sha256') or ''), 'state_token':str(payload.get('state_token') or ''), 'saved_at':time.time(), 'count':int(STATE.get('delta_since_checkpoint') or 0)}
        pipe=client.pipeline(transaction=True)
        pipe.rpush(_REDIS_DELTA_KEY, wire)
        pipe.ltrim(_REDIS_DELTA_KEY, -max_items, -1)
        pipe.set(_REDIS_DELTA_META_KEY, json.dumps(row,separators=(',',':')))
        pipe.execute()
        with STATE_LOCK:
            STATE['redis_cache_ok']=True; STATE['redis_last_write']=time.time(); STATE['redis_last_error']=''
        return True, 'Redis delta appended'
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:220]}'
        with STATE_LOCK:
            STATE['redis_cache_ok']=False; STATE['redis_last_error']=detail
        return False,detail

def _apply_delta_payload_v267(payload: dict, *, journal_wire: bytes|None=None, replay=False):
    if not isinstance(payload,dict) or int(payload.get('schema') or 0) != 1:
        return False, 'invalid delta schema', 'invalid'
    ok, detail, meta = _ensure_cache_db_v267()
    if not ok:
        return False, detail, 'need_full'
    base_sha=str(payload.get('base_sha256') or '').lower()
    new_sha=str(payload.get('new_sha256') or '').lower()
    current_sha=str(meta.get('sha256_db') or _sha256_file_v267(CACHE_DB)).lower()
    if new_sha and current_sha == new_sha:
        _event_mark_mirrored_v268(payload.get('event_ids') or [], payload.get('state_token') or '')
        return True, 'already applied', 'up_to_date'
    if not base_sha or current_sha != base_sha:
        return False, f'base mismatch worker={current_sha[:16]} front={base_sha[:16]}', 'need_full'
    page_size=int(payload.get('page_size') or 0)
    db_size=int(payload.get('db_size') or 0)
    pages=payload.get('pages') or []
    if page_size != _sqlite_page_size_v267(CACHE_DB) or db_size < 100 or db_size > env_int('WORKER_DELTA_MAX_DB_MB',128,1,1024)*1024*1024:
        return False, 'delta database geometry invalid', 'need_full'
    if not isinstance(pages,list) or len(pages) > env_int('WORKER_DELTA_MAX_PAGES',4096,8,65536):
        return False, 'too many delta pages', 'invalid'
    tmp=CACHE_DIR / f'.delta_{secrets.token_hex(8)}.sqlite3'
    shutil.copy2(CACHE_DB,tmp)
    try:
        with open(tmp,'r+b') as fh:
            for row in pages:
                if not isinstance(row,list) or len(row)!=2:
                    return False,'invalid delta page row','invalid'
                idx=int(row[0]); data=base64.b64decode(str(row[1] or ''),validate=True)
                if idx < 0 or len(data) > page_size:
                    return False,'invalid delta page geometry','invalid'
                fh.seek(idx*page_size); fh.write(data)
            fh.truncate(db_size)
            fh.flush(); os.fsync(fh.fileno())
        ok2, detail2, meta2=_quick_check_db_v267(tmp)
        if not ok2:
            return False,'patched DB invalid: '+detail2,'need_full'
        actual=str(meta2.get('sha256_db') or '').lower()
        if new_sha and actual != new_sha:
            return False,f'patched sha mismatch expected={new_sha[:16]} actual={actual[:16]}','need_full'
        # Durability first: after the patch is fully validated, append the tiny delta to
        # Redis BEFORE replacing the local /tmp cache. If the Worker is killed in the
        # next millisecond, startup can replay this journal onto the last checkpoint.
        if (not replay) and journal_wire is not None and _redis_client() is not None:
            redis_ok,redis_detail=_redis_append_delta_v267(journal_wire,payload)
            if not redis_ok:
                return False,'Redis delta append failed: '+str(redis_detail)[:180],'redis_unavailable'
        os.replace(tmp,CACHE_DB)
        gz_ok,gz_detail,gz_meta=_gzip_cache_db_v267()
        if not gz_ok:
            return False,'cache gzip failed: '+gz_detail,'need_full'
        with STATE_LOCK:
            STATE['cache_revision']=max(float(STATE.get('cache_revision') or 0.0),float(meta2.get('revision') or 0.0))
            STATE['last_snapshot_sha256']=str(gz_meta.get('sha256_gz') or '')
            STATE['last_snapshot_size']=int(gz_meta.get('size') or 0)
            STATE['last_snapshot_at']=time.time()
            STATE['last_state_sha256']=actual
            STATE['last_state_token']=str(payload.get('state_token') or '')[:120]
            STATE['delta_last_at']=time.time(); STATE['delta_last_pages']=len(pages); STATE['delta_last_error']=''
            if replay:
                STATE['delta_replayed']=int(STATE.get('delta_replayed') or 0)+1
            else:
                STATE['delta_count']=int(STATE.get('delta_count') or 0)+1
                STATE['delta_since_checkpoint']=int(STATE.get('delta_since_checkpoint') or 0)+1
                STATE['delta_bytes']=int(STATE.get('delta_bytes') or 0)+int(len(journal_wire or b''))
                STATE['delta_bytes_since_checkpoint']=int(STATE.get('delta_bytes_since_checkpoint') or 0)+int(len(journal_wire or b''))
        _event_mark_mirrored_v268(payload.get('event_ids') or [], payload.get('state_token') or '')
        return True,f'applied pages={len(pages)} bytes={len(journal_wire or b"")}', 'applied'
    finally:
        tmp.unlink(missing_ok=True)

def redis_replay_deltas_v267():
    client=_redis_client()
    if client is None:
        return False,'REDIS_URL not configured'
    try:
        rows=client.lrange(_REDIS_DELTA_KEY,0,-1) or []
        if not rows:
            _ensure_cache_db_v267()
            return True,'no Redis deltas'
        applied=0
        for wire in rows:
            try:
                raw=gzip.decompress(wire)
                payload=json.loads(raw.decode('utf-8'))
            except Exception as exc:
                return False,f'delta decode failed at {applied}: {exc}'
            ok,detail,status=_apply_delta_payload_v267(payload,journal_wire=None,replay=True)
            if not ok and status!='up_to_date':
                return False,f'delta replay failed at {applied}: {detail}'
            applied+=1
        with STATE_LOCK:
            STATE['delta_since_checkpoint']=max(int(STATE.get('delta_since_checkpoint') or 0), applied)
        return True,f'replayed {applied} Redis deltas'
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:220]}'

def _full_checkpoint_v267(reason='periodic'):
    with DELTA_APPLY_LOCK:
        ok,detail,meta=_ensure_cache_db_v267()
        if not ok:
            return False,detail
        gz_ok,gz_detail,gz_meta=_gzip_cache_db_v267()
        if not gz_ok:
            return False,gz_detail
        redis_configured=_redis_client() is not None
        redis_ok,redis_detail=redis_store_snapshot(CACHE_LATEST,gz_meta,clear_deltas=True)
        with STATE_LOCK: last_mega=float(STATE.get('last_mega_upload_at') or 0.0)
        mega_every=env_int('WORKER_MEGA_CHECKPOINT_SEC',86400,3600,604800)
        # R36: with no Redis, MEGA becomes the checkpoint durability backend. A
        # threshold checkpoint is therefore not allowed to be a permanent false/spam
        # loop merely because REDIS_URL is intentionally absent.
        force_mega=(not redis_ok) or str(reason).startswith(('manual','shutdown','reconcile'))
        mega_due=(force_mega or last_mega<=0.0 or time.time()-last_mega>=mega_every)
        if mega_due:
            mega_ok,mega_detail=mega_promote_snapshot(CACHE_LATEST)
        else:
            mega_ok,mega_detail=True,f'deferred until {mega_every}s interval'
        durable_ok=bool(redis_ok or (mega_due and mega_ok))
        with STATE_LOCK:
            if mega_due and mega_ok: STATE['last_mega_upload_at']=time.time()
            STATE['full_checkpoint_at']=time.time(); STATE['full_checkpoint_count']=int(STATE.get('full_checkpoint_count') or 0)+1
            if redis_ok or ((not redis_ok) and mega_due and mega_ok):
                STATE['delta_since_checkpoint']=0; STATE['delta_bytes_since_checkpoint']=0
        backend='redis' if redis_ok else ('mega' if mega_due and mega_ok else 'none')
        return durable_ok,f'{reason}: backend={backend}; redis={redis_ok} {redis_detail}; mega={mega_ok} {mega_detail}'

def _checkpoint_loop_v267():
    while True:
        time.sleep(30)
        try:
            with STATE_LOCK:
                count=int(STATE.get('delta_since_checkpoint') or 0)
                last=float(STATE.get('full_checkpoint_at') or STATE.get('started_at') or time.time())
            if count <= 0:
                continue
            seconds=env_int('WORKER_FULL_CHECKPOINT_SEC',21600,300,86400)
            max_deltas=env_int('WORKER_FULL_CHECKPOINT_MAX_DELTAS',1000,10,20000)
            with STATE_LOCK: delta_bytes_since=int(STATE.get('delta_bytes_since_checkpoint') or 0)
            max_delta_bytes=env_int('WORKER_FULL_CHECKPOINT_MAX_DELTA_MB',16,1,128)*1024*1024
            if count >= max_deltas or delta_bytes_since >= max_delta_bytes or time.time()-last >= seconds:
                ok,detail=_full_checkpoint_v267('threshold')
                print(f'[R12 CHECKPOINT] ok={ok} {detail}',flush=True)
        except Exception as exc:
            print(f'[R12 CHECKPOINT ERROR] {type(exc).__name__}: {str(exc)[:240]}',flush=True)


def fetch_front_snapshot():
    base, secret = front_base(), peer_secret()
    if not base or not secret: return None, 'front URL/secret not configured', {}
    timeout = env_int('WORKER_FRONT_FETCH_TIMEOUT',30,5,180)
    work = Path(tempfile.mkdtemp(prefix='vys262_front_fetch_')); path = work / 'snapshot.sqlite3.gz'
    try:
        r = requests.get(base + '/internal/split/state', headers={'X-Peer-Secret':secret,'User-Agent':'vys-262-worker-fetch-r19','X-Split-Rebase':'1'}, timeout=timeout, stream=True)
        if r.status_code != 200: return None, f'front HTTP {r.status_code}: {r.text[:180]}', {}
        with open(path,'wb') as fh:
            for chunk in r.iter_content(1024*1024):
                if chunk: fh.write(chunk)
        ok, detail, meta = quick_check_gzip(path)
        if not ok: return None, detail, meta
        meta['state_token'] = str(r.headers.get('X-Split-State-Token') or '')[:120]
        # Caller owns the copied temp path, so move it out of disposable dir.
        fd, durable_name = tempfile.mkstemp(prefix='vys262_snapshot_', suffix='.sqlite3.gz'); os.close(fd)
        durable = Path(durable_name); shutil.copy2(path, durable)
        return durable, 'front snapshot OK', meta
    except Exception as exc: return None, f'front {type(exc).__name__}: {str(exc)[:180]}', {}
    finally: shutil.rmtree(work, ignore_errors=True)


def mega_promote_snapshot(local_gz: Path):
    if not mega_enabled():
        return True, 'MEGA disabled by MEGA_ENABLED=0; local cache only'
    with MEGA_LOCK:
        ok, detail = prepare_mega_layout()
        if not ok: return False, detail
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        # Keep the old latest as history before replacement. Failure to archive does not delete latest.
        if mega_exists(remote_latest()):
            hist_tmp = remote_history_dir().rstrip('/') + '/latest_bot_state.sqlite3.gz'
            try:
                cp = run_cmd(['mega-cp', remote_latest(), remote_history_dir()], timeout=120)
                if cp.returncode == 0 and mega_exists(hist_tmp):
                    run_cmd(['mega-mv', hist_tmp, remote_history_dir().rstrip('/') + f'/state_{stamp}.sqlite3.gz'], timeout=60)
            except Exception: pass
        work = Path(tempfile.mkdtemp(prefix='vys262_mega_put_'))
        try:
            incoming_name = f'incoming_{stamp}_{secrets.token_hex(4)}.sqlite3.gz'; incoming_local = work / incoming_name; shutil.copy2(local_gz, incoming_local)
            put = run_cmd(['mega-put', str(incoming_local), remote_db_dir()], timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if put.returncode != 0: return False, 'mega-put failed: ' + (put.stderr or put.stdout or '')[:180]
            incoming_remote = remote_db_dir().rstrip('/') + '/' + incoming_name
            rollback_remote = ''
            if mega_exists(remote_latest()):
                rollback_remote = remote_history_dir().rstrip('/') + f'/rollback_{stamp}_{secrets.token_hex(3)}.sqlite3.gz'
                old_mv = run_cmd(['mega-mv', remote_latest(), rollback_remote], timeout=90)
                if old_mv.returncode != 0 and mega_exists(remote_latest()):
                    return False, 'could not move old latest to rollback slot'
            mv = run_cmd(['mega-mv', incoming_remote, remote_latest()], timeout=90)
            if mv.returncode != 0 and not mega_exists(remote_latest()):
                if rollback_remote and mega_exists(rollback_remote):
                    try: run_cmd(['mega-mv', rollback_remote, remote_latest()], timeout=90)
                    except Exception: pass
                return False, 'mega-mv promote failed; rollback attempted'
            # The rollback object is useful history; rename it to a normal timestamped history name.
            if rollback_remote and mega_exists(rollback_remote):
                try: run_cmd(['mega-mv', rollback_remote, remote_history_dir().rstrip('/') + f'/state_{stamp}_previous.sqlite3.gz'], timeout=60)
                except Exception: pass
            return True, 'MEGA latest promoted'
        finally: shutil.rmtree(work, ignore_errors=True)


def sync_state_job(job):
    with STATE_LOCK:
        STATE['last_full_fetch_started'] = time.time()
    snap, detail, meta = fetch_front_snapshot()
    if not snap:
        return False, detail
    try:
        incoming_revision = float(meta.get('revision') or 0.0)
        with STATE_LOCK:
            current_revision = float(STATE.get('cache_revision') or 0.0)
        cache_exists = CACHE_LATEST.exists()
        # Never let a delayed GET overwrite a newer direct-shutdown upload/cache.
        if cache_exists and current_revision > 0.0 and (incoming_revision <= 0.0 or incoming_revision < current_revision):
            fetched_token = str(meta.get('state_token') or job.get('state_token') or '')[:120]
            with STATE_LOCK:
                if fetched_token: STATE['last_state_token'] = fetched_token
                STATE['deduped_sync_requests'] += 1
            return True, f'stale snapshot ignored revision={incoming_revision} < cache={current_revision}'
        # R6 durability order: validated front state becomes the restore source FIRST.
        tmp_cache = CACHE_DIR / f'.sync_{secrets.token_hex(6)}.tmp'
        shutil.copy2(snap, tmp_cache)
        os.replace(tmp_cache, CACHE_LATEST)
        with STATE_LOCK:
            STATE['cache_revision'] = max(current_revision, incoming_revision)
            STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or '')
            STATE['last_snapshot_size'] = int(meta.get('size') or 0)
            STATE['last_snapshot_at'] = time.time()
            fetched_token = str(meta.get('state_token') or job.get('state_token') or '')[:120]
            if fetched_token:
                STATE['last_state_token'] = fetched_token
        # Full resync establishes a fresh delta base and clears the Redis delta journal.
        try:
            CACHE_DB.unlink(missing_ok=True)
            _ensure_cache_db_v267()
            with STATE_LOCK:
                STATE['last_state_sha256'] = _sha256_file_v267(CACHE_DB) if CACHE_DB.exists() else ''
                STATE['delta_since_checkpoint'] = 0
                STATE['delta_bytes_since_checkpoint'] = 0
        except Exception:
            pass
        # Fast durable cache is written before slow archival MEGA.
        redis_ok, redis_detail = redis_store_snapshot(CACHE_LATEST, meta, clear_deltas=True)
        mega_ok, mega_detail = mega_promote_snapshot(CACHE_LATEST)
        with STATE_LOCK:
            if mega_ok:
                STATE['last_mega_upload_at'] = time.time()
            else:
                STATE['sync_failures'] += 1
            STATE['sync_count'] += 1
        return True, f"cached {meta.get('size',0)} bytes; redis={redis_ok}; mega={mega_ok}; {redis_detail}; {mega_detail}"
    finally:
        snap.unlink(missing_ok=True)


def _download_mega_latest():
    with MEGA_LOCK:
        ok, detail = prepare_mega_layout()
        if not ok:
            return None, detail
        last_detail = 'MEGA latest snapshot not found'
        for idx, remote in enumerate(mega_restore_candidates()):
            if not mega_exists(remote):
                continue
            work = Path(tempfile.mkdtemp(prefix=f'vys262_mega_get_{idx}_'))
            try:
                p = run_cmd(['mega-get', remote, str(work)], timeout=env_int('MEGA_TIMEOUT',180,30,900))
                if p.returncode != 0:
                    last_detail = 'mega-get failed: ' + (p.stderr or p.stdout or '')[:180]
                    continue
                rows = list(work.rglob('latest_bot_state.sqlite3.gz')) + list(work.rglob('*.sqlite3.gz'))
                if not rows:
                    last_detail = 'MEGA download has no SQLite gzip'
                    continue
                check_ok, check_detail, meta = quick_check_gzip(rows[0])
                if not check_ok:
                    last_detail = check_detail
                    continue
                incoming_revision = float(meta.get('revision') or 0.0)
                with STATE_LOCK:
                    current_revision = float(STATE.get('cache_revision') or 0.0)
                cache_exists = CACHE_LATEST.exists()
                accept = (not cache_exists) or current_revision <= 0.0 or (incoming_revision > 0.0 and incoming_revision >= current_revision)
                if accept:
                    tmp_cache = CACHE_DIR / f'.mega_{secrets.token_hex(6)}.tmp'
                    shutil.copy2(rows[0], tmp_cache); os.replace(tmp_cache, CACHE_LATEST)
                    with STATE_LOCK:
                        STATE['cache_revision'] = max(current_revision, incoming_revision)
                        STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or '')
                        STATE['last_snapshot_size'] = int(meta.get('size') or 0)
                    try:
                        CACHE_DB.unlink(missing_ok=True)
                        _ensure_cache_db_v267()
                        redis_store_snapshot(CACHE_LATEST, meta)
                    except Exception:
                        pass
                with STATE_LOCK:
                    STATE['last_restore_download_at'] = time.time()
                if not accept and cache_exists:
                    return CACHE_LATEST, f'MEGA snapshot older than restore cache; kept cache rev={current_revision}'
                return CACHE_LATEST, f'MEGA configured-root latest OK: {remote}'
            finally:
                shutil.rmtree(work, ignore_errors=True)
        return None, last_detail


def process_job(job):
    global SYNC_PENDING, SYNC_DIRTY, SYNC_DIRTY_REASON
    jid, kind = str(job.get('id') or ''), str(job.get('type') or '')
    with STATE_LOCK:
        STATE['job_last_id']=jid; STATE['job_last_type']=kind; STATE['job_last_reason']=str(job.get('reason') or '')[:180]; STATE['job_last_started']=time.time(); STATE['job_last_error']=''
    try:
        if kind == 'sync_state':
            ok, detail = sync_state_job(job)
        elif kind == 'promote_uploaded':
            path = Path(str(job.get('path') or ''))
            try:
                if not path.exists():
                    ok, detail = False, 'uploaded snapshot missing before promote'
                else:
                    ok, detail = mega_promote_snapshot(path)
                    if ok:
                        with STATE_LOCK:
                            STATE['last_mega_upload_at'] = time.time()
                            STATE['sync_count'] += 1
            finally:
                try: path.unlink(missing_ok=True)
                except Exception: pass
        else:
            ok, detail = False, f'unsupported job type: {kind}'
        with STATE_LOCK:
            STATE['job_last_done']=time.time(); STATE['job_last_error']='' if ok else detail[:260]
            if not ok: STATE['sync_failures'] += 1
        print(f'[WORKER JOB] {jid} {kind} ok={ok} {detail}', flush=True)
    finally:
        if kind == 'sync_state':
            followup = None
            with STATE_LOCK:
                fetched_token = str(STATE.get('last_state_token') or '')
            with SYNC_PENDING_LOCK:
                dirty_token = str(STATE.get('dirty_state_token') or '')
                # A duplicate request that arrived while the first GET was running must
                # not trigger a second full SQLite download. Only a genuinely newer
                # front state token deserves one follow-up fetch.
                if SYNC_DIRTY and dirty_token and dirty_token != fetched_token:
                    # R26: never chain another full Front snapshot just because state
                    # changed while this full snapshot was in flight. FAST will send
                    # compact deltas from the newly promoted baseline. If those deltas
                    # truly diverge, the Front schedules one idle full reconcile.
                    with STATE_LOCK:
                        STATE['full_fetch_suppressed'] = int(STATE.get('full_fetch_suppressed') or 0) + 1
                        STATE['last_full_suppressed_token'] = dirty_token
                    SYNC_DIRTY = False
                    SYNC_DIRTY_REASON = ''
                    SYNC_PENDING = False
                    with STATE_LOCK:
                        STATE['active_state_token'] = ''
                        STATE['dirty_state_token'] = ''
                else:
                    SYNC_DIRTY = False
                    SYNC_DIRTY_REASON = ''
                    SYNC_PENDING = False
                    with STATE_LOCK:
                        STATE['active_state_token'] = ''
                        STATE['dirty_state_token'] = ''
            if followup is not None:
                try:
                    JOB_Q.put_nowait(followup)
                except queue.Full:
                    with SYNC_PENDING_LOCK:
                        SYNC_PENDING = False
                        SYNC_DIRTY = True
                        SYNC_DIRTY_REASON = str(followup.get('reason') or 'queue_full')
                    with STATE_LOCK:
                        STATE['dirty_state_token'] = str(followup.get('state_token') or '')[:120]


def worker_loop():
    while True:
        job = JOB_Q.get()
        try: process_job(job)
        except Exception as exc:
            with STATE_LOCK: STATE['job_last_error']=f'{type(exc).__name__}: {str(exc)[:240]}'; STATE['sync_failures'] += 1
        finally: JOB_Q.task_done()


def _restore_refresh_background():
    global RESTORE_REFRESH_RUNNING
    try:
        _download_mega_latest()
        try:
            if '_r32_replay_state_events_from_mega' in globals():
                ok32,detail32=_r32_replay_state_events_from_mega()
                print(f'[R32 MEGA EVENT REPLAY] ok={ok32} {detail32}',flush=True)
                if ok32 and CACHE_DB.exists():
                    with DELTA_APPLY_LOCK: _gzip_cache_db_v267()
        except Exception as exc:
            print(f'[R32 MEGA EVENT REPLAY ERROR] {type(exc).__name__}: {str(exc)[:220]}',flush=True)
    finally:
        with RESTORE_REFRESH_LOCK: RESTORE_REFRESH_RUNNING = False

def _schedule_restore_refresh():
    global RESTORE_REFRESH_RUNNING
    with RESTORE_REFRESH_LOCK:
        if RESTORE_REFRESH_RUNNING: return False
        RESTORE_REFRESH_RUNNING = True
    threading.Thread(target=_restore_refresh_background, name='vys262-worker-restore-refresh', daemon=True).start(); return True


def _reconcile_hash_loop_v268():
    """Every ~6h compare hashes; transfer a full database only on real divergence."""
    while True:
        interval=env_int('WORKER_RECONCILE_SEC',21600,900,86400)
        time.sleep(interval)
        base,secret=front_base(),peer_secret()
        if not base or not secret: continue
        with STATE_LOCK: STATE['reconcile_last_at']=time.time()
        try:
            r=requests.get(base+'/internal/split/hash',headers={'X-Peer-Secret':secret,'User-Agent':'vys-262-worker-reconcile-r13'},timeout=30)
            if r.status_code!=200: raise RuntimeError(f'hash HTTP {r.status_code}: {r.text[:160]}')
            body=r.json() if r.content else {}; front_sha=str(body.get('sha256') or '')
            with STATE_LOCK: worker_sha=str(STATE.get('last_state_sha256') or '')
            if front_sha and worker_sha and front_sha==worker_sha:
                with STATE_LOCK: STATE['reconcile_last_ok']=time.time(); STATE['reconcile_last_error']=''
                print(f'[R13 RECONCILE] hash OK {front_sha[:16]}',flush=True); continue
            # R32: binary SQLite hashes can differ after replaying equivalent row events.
            # Never pull a full Front database for this; Redis/MEGA event journals are the recovery path.
            with STATE_LOCK:
                STATE['reconcile_last_ok']=time.time(); STATE['reconcile_last_error']='R32 logical event stream; binary mismatch ignored'
            print(f'[R32 RECONCILE] logical event mode; no full fetch front={front_sha[:16]} worker={worker_sha[:16]}',flush=True)
        except Exception as exc:
            with STATE_LOCK: STATE['reconcile_last_error']=f'{type(exc).__name__}: {str(exc)[:220]}'
            print(f'[R13 RECONCILE ERROR] {type(exc).__name__}: {str(exc)[:220]}',flush=True)

def peer_loop():
    time.sleep(5)
    while True:
        if not env_bool('PEER_PING_ENABLED', True):
            time.sleep(env_int('PEER_PING_INTERVAL_SEC',120,30,1800))
            continue
        base = front_base()
        with STATE_LOCK: STATE['peer_last_attempt'] = time.time()
        if base:
            try:
                headers={'User-Agent':'vys-262-worker-peer-r11'}
                if peer_secret(): headers['X-Peer-Secret']=peer_secret()
                r = requests.get(base + '/peer/health', headers=headers, timeout=12)
                payload = {}
                try: payload = r.json() if r.content else {}
                except Exception: payload = {}
                with STATE_LOCK:
                    STATE['peer_status']=int(r.status_code)
                    if 200 <= r.status_code < 300:
                        STATE['peer_last_ok']=time.time(); STATE['peer_last_error']=''
                        STATE['front_health']={
                            'ok':bool(payload.get('ok', True)), 'version':str(payload.get('version') or ''),
                            'bot_version':str(payload.get('bot_version') or ''), 'ready':bool(payload.get('ready', False)),
                            'phase':str(payload.get('phase') or ''), 'seen_at':time.time(),
                        }
                    else: STATE['peer_last_error']=f'HTTP {r.status_code}'
            except Exception as exc:
                with STATE_LOCK: STATE['peer_status']=None; STATE['peer_last_error']=str(exc)[:220]
        else:
            with STATE_LOCK: STATE['peer_last_error']='FRONT_SERVICE_URL empty'
        time.sleep(env_int('PEER_PING_INTERVAL_SEC',120,30,1800))


def _b64url(raw: bytes): return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')
def _google_info():
    raw = str(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON','') or '').strip()
    if not raw: raise RuntimeError('GOOGLE_SERVICE_ACCOUNT_JSON is not configured on worker')
    try: info = json.loads(raw) if raw.lstrip().startswith('{') else json.loads(base64.b64decode(raw).decode('utf-8'))
    except Exception as exc: raise RuntimeError(f'GOOGLE_SERVICE_ACCOUNT_JSON invalid: {exc}')
    for key in ('client_email','private_key','token_uri'):
        if not info.get(key): raise RuntimeError(f'GOOGLE_SERVICE_ACCOUNT_JSON missing {key}')
    return info

def _google_sign(message: bytes, private_key: str):
    key_path = msg_path = sig_path = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', delete=False) as fh: fh.write(private_key); key_path = fh.name
        with tempfile.NamedTemporaryFile('wb', delete=False) as fh: fh.write(message); msg_path = fh.name
        fd, sig_path = tempfile.mkstemp(prefix='google_jwt_', suffix='.sig'); os.close(fd)
        p = subprocess.run(['openssl','dgst','-sha256','-sign',key_path,'-out',sig_path,msg_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
        if p.returncode != 0: raise RuntimeError(p.stderr.decode('utf-8','replace')[-400:])
        return Path(sig_path).read_bytes()
    finally:
        for p in (key_path,msg_path,sig_path):
            if p:
                try: os.remove(p)
                except Exception: pass

def _google_token():
    with GOOGLE_LOCK:
        now = time.time()
        if _GOOGLE_TOKEN['token'] and now < float(_GOOGLE_TOKEN['expires_at']) - 120: return _GOOGLE_TOKEN['token']
        info = _google_info(); header={'alg':'RS256','typ':'JWT'}; claims={'iss':info['client_email'],'scope':'https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive','aud':info.get('token_uri') or 'https://oauth2.googleapis.com/token','iat':int(now),'exp':int(now)+3600}
        signing = (_b64url(json.dumps(header,separators=(',',':')).encode()) + '.' + _b64url(json.dumps(claims,separators=(',',':')).encode())).encode('ascii')
        assertion = signing.decode('ascii') + '.' + _b64url(_google_sign(signing, info['private_key']))
        r = _r38_google_http('post', info.get('token_uri') or 'https://oauth2.googleapis.com/token', data={'grant_type':'urn:ietf:params:oauth:grant-type:jwt-bearer','assertion':assertion}, timeout=30)
        if r.status_code >= 300: raise RuntimeError(f'Google OAuth {r.status_code}: {r.text[:400]}')
        payload = r.json(); token = str(payload.get('access_token') or '')
        if not token: raise RuntimeError('Google OAuth returned no access_token')
        _GOOGLE_TOKEN.update(token=token, expires_at=now+int(payload.get('expires_in',3600) or 3600)); return token

def _sheet_id(value=None):
    raw = str(value if value is not None and str(value).strip() else os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID','') or '').strip()
    m = re.search(r'/spreadsheets/d/([A-Za-z0-9_-]+)', raw)
    if m: raw = m.group(1)
    raw = raw.split('?')[0].split('#')[0].strip().strip('/')
    if not re.fullmatch(r'[A-Za-z0-9_-]{20,}', raw):
        raise RuntimeError('Google spreadsheet ID is missing/invalid. Choose a table in /google on Render #1.')
    return raw

def _tab_title(title):
    # R7: deterministic title. Same period/chat updates the same tab instead of creating clutter.
    base = re.sub(r'[\\/?*\[\]:]', ' ', str(title or 'Статьи'))
    base = re.sub(r'\s+', ' ', base).strip(" ' ") or 'Статьи'
    return base[:100].rstrip()

def _cell_value(v):
    if isinstance(v,dict) and v.get('formula'): return {'formulaValue':'='+str(v.get('formula') or '').lstrip('=')}
    if isinstance(v,bool): return {'boolValue':v}
    if isinstance(v,(int,float)) and not isinstance(v,bool): return {'numberValue':float(v)}
    return {'stringValue':str(v or '')}

def _cat_fill(idx):
    # Original vys-262 Google palette/indexing. Columns before the category
    # block use the same pale green-gray header fill as the monolith.
    palette=[(0.78,0.94,0.81),(0.87,0.92,0.97),(0.99,0.89,0.84),(0.89,0.87,0.93),(1.0,0.95,0.8),(0.85,0.92,0.83),(0.81,0.89,0.95),(0.96,0.8,0.8),(0.82,0.88,0.89),(0.92,0.82,0.86),(0.85,0.82,0.91)]
    idx=int(idx or 0)
    if idx >= 3:
        rgb=palette[(idx-3)%len(palette)]
        return {'red':rgb[0],'green':rgb[1],'blue':rgb[2]}
    return {'red':0.92,'green':0.95,'blue':0.9}


def _v262_google_cell_format(row, r_idx, c_idx, max_cols, layout):
    """Google cell format from the final vys-262 monolith."""
    value = row[c_idx - 1] if c_idx - 1 < len(row) else ''
    row_is_blank = not any(str(v if v is not None else '').strip() for v in row)
    first = str(row[0] if row else '').strip().casefold()
    second = str(row[1] if len(row) > 1 else '').strip().casefold()
    fmt = {
        'verticalAlignment': 'TOP',
        'wrapStrategy': 'CLIP' if c_idx == 2 else 'WRAP',
        'borders': {side: {'style': 'SOLID', 'color': {'red': 0.65, 'green': 0.65, 'blue': 0.65}}
                    for side in ('top', 'bottom', 'left', 'right')},
    }
    if isinstance(value, (int, float)) and not isinstance(value, bool) or (isinstance(value, dict) and value.get('formula')):
        fmt['numberFormat'] = {'type': 'NUMBER', 'pattern': '#,##0'}
    if first == 'ars':
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.78, 'green': 0.94, 'blue': 0.81}})
    elif first == 'usd':
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.72, 'green': 0.86, 'blue': 1.0}})
    elif first in {'дата', 'date'}:
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': _cat_fill(c_idx - 1)})
    elif row_is_blank and layout in {'category', 'category_compact'}:
        fmt['backgroundColor'] = {'red': 1.0, 'green': 0.6, 'blue': 0.0}
    elif first in {'расход', 'сумма по статьям'} or second in {'расход', 'сумма по статьям'}:
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 1.0, 'green': 0.55, 'blue': 0.55}})
    elif first in {'приход', 'приход за период'} or second in {'приход', 'приход за период'}:
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.55, 'green': 0.78, 'blue': 1.0}})
    elif first in {'остаток с прошлого раза', 'остаток на руках', 'на руках:', 'гомонковые', 'остаток в обороте'} or second in {'остаток с прошлого раза', 'остаток на руках', 'на руках:', 'гомонковые', 'остаток в обороте'}:
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.55, 'green': 0.85, 'blue': 0.55}})
    elif first == 'расход еды на человека в сутки' or second == 'расход еды на человека в сутки':
        fmt.update({'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.74, 'green': 0.82, 'blue': 1.0}})
    elif layout == 'compact' and c_idx in {2, 3} and value not in ('', None):
        fmt['backgroundColor'] = _cat_fill(3 if c_idx == 3 else 2)
    elif layout == 'category_compact' and c_idx >= 3 and value not in ('', None):
        fmt['backgroundColor'] = _cat_fill(c_idx)
    elif layout == 'category' and c_idx >= 4 and value not in ('', None):
        # Exact final vys-262 category indexing.
        fmt['backgroundColor'] = _cat_fill(c_idx - 1)
    return fmt

def create_google_sheet(body):
    """Create or refresh a named tab in the selected owner spreadsheet.

    r2 always tried addSheet, so the next automatic Thu-Wed refresh failed with
    "already exists". r3 reuses an existing tab with the same title and replaces
    only that tab's managed grid. All Google network work stays on Render #2.
    """
    rows = body.get('rows') or []
    layout = str(body.get('layout') or 'category').lower()
    notes_raw = body.get('annotations') or {}
    notes = {}
    for key, val in notes_raw.items():
        try:
            r, c = key.split(',', 1); notes[(int(r), int(c))] = str(val)
        except Exception:
            pass
    token = _google_token()
    spreadsheet_id = _sheet_id(body.get('spreadsheet_id'))
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    info = _google_info()
    meta = _r38_google_http('get',
        f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}',
        headers=headers,
        params={'fields': 'spreadsheetId,properties.title,sheets.properties(sheetId,title,gridProperties)'},
        timeout=45,
    )
    if meta.status_code >= 300:
        if meta.status_code in (401, 403):
            raise RuntimeError(f'Google Sheets target access {meta.status_code}; share table with {info.get("client_email")}: {meta.text[:400]}')
        raise RuntimeError(f'Google Sheets metadata {meta.status_code}: {meta.text[:400]}')
    payload = meta.json() or {}
    max_cols = max((len(r) for r in rows), default=1)
    row_count = max(100, len(rows) + 20)
    col_count = max(26, max_cols + 3)
    title = _tab_title(body.get('title'))
    existing = None
    for sh in payload.get('sheets') or []:
        props = (sh or {}).get('properties') or {}
        if str(props.get('title') or '') == title:
            existing = props
            break
    if existing:
        sheet_id = int(existing.get('sheetId'))
        grid = existing.get('gridProperties') or {}
        clear_rows = max(row_count, int(grid.get('rowCount') or 0), len(rows) + 5)
        clear_cols = max(col_count, int(grid.get('columnCount') or 0), max_cols + 2)
        # Empty updateCells over a range clears the listed fields. This prevents
        # stale values/notes from a longer previous export from surviving.
        clear_req = {'updateCells': {'range': {
            'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': clear_rows,
            'startColumnIndex': 0, 'endColumnIndex': clear_cols,
        }, 'fields': 'userEnteredValue,note,userEnteredFormat'}}
    else:
        add = _r38_google_http('post',
            f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate',
            headers=headers,
            json={'requests': [{'addSheet': {'properties': {'title': title, 'gridProperties': {
                'rowCount': row_count, 'columnCount': col_count,
                'frozenRowCount': 1 if layout in {'compact', 'category_compact'} else 2,
            }}}}]},
            timeout=60,
        )
        if add.status_code >= 300:
            raise RuntimeError(f'Google Sheets add tab {add.status_code}: {add.text[:500]}')
        try:
            sheet_id = int(add.json()['replies'][0]['addSheet']['properties']['sheetId'])
        except Exception as exc:
            raise RuntimeError(f'Google Sheets missing sheetId: {exc}')
        clear_req = None

    cell_rows = []
    for r_idx, row in enumerate(rows, start=1):
        values = []
        for c_idx in range(1, max_cols + 1):
            value = row[c_idx - 1] if c_idx - 1 < len(row) else ''
            cell = {'userEnteredValue': _cell_value(value)}
            note = str(notes.get((r_idx, c_idx)) or '').strip()
            if note:
                cell['note'] = note
            # R9: use the full original vys-262 formatting, not the reduced
            # worker-only coloring introduced by the split.
            cell['userEnteredFormat'] = _v262_google_cell_format(row, r_idx, c_idx, max_cols, layout)
            values.append(cell)
        cell_rows.append({'values': values})

    reqs = []
    if clear_req:
        reqs.append(clear_req)
    reqs.extend([
        {'updateCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'startColumnIndex': 0},
                         'rows': cell_rows, 'fields': 'userEnteredValue,note,userEnteredFormat'}},
        {'autoResizeDimensions': {'dimensions': {'sheetId': sheet_id, 'dimension': 'COLUMNS',
                                                 'startIndex': 0, 'endIndex': max_cols}}},
    ])
    upd = _r38_google_http('post',
        f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate',
        headers=headers, json={'requests': reqs}, timeout=90,
    )
    if upd.status_code >= 300:
        raise RuntimeError(f'Google Sheets update {upd.status_code}: {upd.text[:500]}')

    expected = {(r, c): n.strip() for (r, c), n in notes.items() if n.strip()}
    if expected:
        escaped = title.replace("'", "''")
        verify = _r38_google_http('get',
            f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}', headers=headers,
            params={'includeGridData': 'true', 'ranges': f"'{escaped}'!A1:ZZ{max(1, len(rows))}",
                    'fields': 'sheets(data(rowData(values(note))))'}, timeout=60,
        )
        if verify.status_code >= 300:
            raise RuntimeError(f'Google Sheets note verify {verify.status_code}: {verify.text[:500]}')
        actual = {}
        try:
            row_data = ((verify.json().get('sheets') or [{}])[0].get('data') or [{}])[0].get('rowData') or []
            for r0, row_obj in enumerate(row_data, start=1):
                for c0, cell in enumerate(row_obj.get('values') or [], start=1):
                    note = str(cell.get('note') or '').strip()
                    if note:
                        actual[(r0, c0)] = note
        except Exception as exc:
            raise RuntimeError(f'Google Sheets note verify parse: {exc}')
        missing = [f'{r}:{c}' for (r, c), note in expected.items() if actual.get((r, c)) != note]
        if missing:
            raise RuntimeError(f'Google Sheets notes not confirmed: {missing[:12]}')
    return f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}'



# ---------------------------------------------------------------------------
# R32 logical state-event stream.
# FAST sends only committed SQLite row mutations. HEAVY stores each immutable
# event in Redis, applies it to its local exact restore cache, archives batches to
# MEGA, and serves the assembled SQLite to FAST on deploy.
_R32_STATE_PREFIX = str(os.getenv('WORKER_R32_STATE_EVENT_PREFIX','vys262:state_events:r32') or 'vys262:state_events:r32').strip()
_R32_STATE_LOCK = threading.RLock()
_R32_MEGA_WAKE = threading.Event()
_R32_STATE_REPLAY_LOCK = threading.RLock()
STATE.update({
    'r32_event_stream': True, 'r32_state_events_received':0, 'r32_state_events_applied':0,
    'r32_state_events_stale':0, 'r32_state_event_bytes':0, 'r32_state_last_at':0.0,
    'r32_state_last_error':'', 'r32_mega_segments':0, 'r32_mega_last_ok':0.0,
    'r32_mega_last_error':'', 'r32_baseline_ready':False,
})

def _r32_event_key(eid): return f'{_R32_STATE_PREFIX}:event:{eid}'
def _r32_index_key(): return f'{_R32_STATE_PREFIX}:index'
def _r32_pending_key(): return f'{_R32_STATE_PREFIX}:mega_pending'
def _r32_archived_key(): return f'{_R32_STATE_PREFIX}:mega_archived'
def _r32_migration_key(): return f'{_R32_STATE_PREFIX}:migration_seeded'

def _r32_state_schema(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS r32_state_revisions (shard_key TEXT PRIMARY KEY, revision INTEGER NOT NULL, event_id TEXT NOT NULL, updated_at REAL NOT NULL)')

def _r32_event_valid(ev):
    if not isinstance(ev,dict) or int(ev.get('schema') or 0) != 32: return False
    if not str(ev.get('event_id') or '') or int(ev.get('revision') or 0) <= 0: return False
    return str(ev.get('kind') or '') in {'set_kv','save_chat','prune_chats','delete_chat','set_meta','set_cold','set_cold_many','delete_cold'}

def _r32_redis_store_events(events):
    client=_redis_client()
    if client is None: return False,'REDIS_URL not configured',[]
    ttl=env_int('WORKER_R32_EVENT_RETENTION_SEC',2592000,604800,7776000)
    new_ids=[]
    try:
        # First phase: SET NX lets retries be idempotent.
        pipe=client.pipeline(transaction=False)
        packed=[]
        for ev in events:
            eid=str(ev.get('event_id') or '')
            raw=json.dumps(ev,ensure_ascii=False,separators=(',',':'),default=str)
            packed.append((eid,raw,float(ev.get('created_at') or time.time())))
            pipe.set(_r32_event_key(eid),raw,nx=True,ex=ttl)
        results=pipe.execute()
        pipe=client.pipeline(transaction=True)
        for (eid,raw,score),created in zip(packed,results):
            if created:
                new_ids.append(eid)
                pipe.zadd(_r32_index_key(),{eid:score})
                pipe.rpush(_r32_pending_key(),eid)
            else:
                pipe.expire(_r32_event_key(eid),ttl)
        # Index/pending metadata live longer than event rows but are compact.
        pipe.expire(_r32_index_key(),ttl)
        pipe.expire(_r32_pending_key(),ttl)
        pipe.execute()
        return True,f'Redis state events stored new={len(new_ids)} total={len(events)}',new_ids
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:220]}',[]

def _r32_apply_event_tx(conn,ev):
    kind=str(ev.get('kind') or ''); key=str(ev.get('key') or '')[:220]
    rev=int(ev.get('revision') or 0); eid=str(ev.get('event_id') or '')
    payload=ev.get('payload') if isinstance(ev.get('payload'),dict) else {}
    row=conn.execute('SELECT revision FROM r32_state_revisions WHERE shard_key=?',(key,)).fetchone()
    if row and int(row[0] or 0) >= rev:
        return 'stale'
    if kind=='set_kv':
        k=str(payload.get('k') or '')
        conn.execute('INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,json.dumps(payload.get('v'),ensure_ascii=False,separators=(',',':'),default=str)))
    elif kind=='save_chat':
        cid=str(payload.get('chat_id') or '')
        conn.execute('INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v',(cid,json.dumps(payload.get('v') or {},ensure_ascii=False,separators=(',',':'),default=str)))
    elif kind=='prune_chats':
        keep={str(x) for x in (payload.get('keep') or [])}
        rows=conn.execute('SELECT chat_id FROM chats').fetchall()
        for r in rows:
            if str(r[0]) not in keep: conn.execute('DELETE FROM chats WHERE chat_id=?',(str(r[0]),))
    elif kind=='delete_chat':
        cid=str(payload.get('chat_id') or ''); conn.execute('DELETE FROM chats WHERE chat_id=?',(cid,)); conn.execute('DELETE FROM cold_fields WHERE chat_id=?',(cid,))
    elif kind=='set_meta':
        mk=str(payload.get('kind') or ''); kk=str(payload.get('k') or '')
        conn.execute('INSERT INTO meta(kind,k,v) VALUES(?,?,?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v',(mk,kk,json.dumps(payload.get('v'),ensure_ascii=False,separators=(',',':'),default=str)))
    elif kind=='set_cold':
        cid=str(payload.get('chat_id') or ''); kk=str(payload.get('k') or ''); stamp=datetime.now(timezone.utc).isoformat(timespec='seconds')
        conn.execute('INSERT INTO cold_fields(chat_id,k,v,updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id,k) DO UPDATE SET v=excluded.v,updated_at=excluded.updated_at',(cid,kk,json.dumps(payload.get('v'),ensure_ascii=False,separators=(',',':'),default=str),stamp))
    elif kind=='set_cold_many':
        cid=str(payload.get('chat_id') or ''); stamp=datetime.now(timezone.utc).isoformat(timespec='seconds')
        for kk,vv in (payload.get('items') or {}).items():
            conn.execute('INSERT INTO cold_fields(chat_id,k,v,updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id,k) DO UPDATE SET v=excluded.v,updated_at=excluded.updated_at',(cid,str(kk),json.dumps(vv,ensure_ascii=False,separators=(',',':'),default=str),stamp))
    elif kind=='delete_cold':
        conn.execute('DELETE FROM cold_fields WHERE chat_id=? AND k=?',(str(payload.get('chat_id') or ''),str(payload.get('k') or '')))
    else:
        return 'invalid'
    conn.execute('INSERT INTO r32_state_revisions(shard_key,revision,event_id,updated_at) VALUES(?,?,?,?) ON CONFLICT(shard_key) DO UPDATE SET revision=excluded.revision,event_id=excluded.event_id,updated_at=excluded.updated_at',(key,rev,eid,time.time()))
    return 'applied'

def _r32_apply_events(events):
    if not events: return True,'no events',0,0
    with _R32_STATE_REPLAY_LOCK, DELTA_APPLY_LOCK:
        ok,detail,_meta=_ensure_cache_db_v267()
        if not ok: return False,detail,0,0
        applied=stale=0
        conn=sqlite3.connect(str(CACHE_DB),timeout=20)
        try:
            conn.execute('PRAGMA journal_mode=WAL'); conn.execute('PRAGMA synchronous=FULL')
            # Baseline R31 already has these tables; keep this defensive for disaster rebuilds.
            conn.execute('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)')
            conn.execute('CREATE TABLE IF NOT EXISTS chats (chat_id TEXT PRIMARY KEY, v TEXT NOT NULL)')
            conn.execute('CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, PRIMARY KEY(kind,k))')
            conn.execute("CREATE TABLE IF NOT EXISTS cold_fields (chat_id TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(chat_id,k))")
            _r32_state_schema(conn)
            conn.execute('BEGIN IMMEDIATE')
            for ev in sorted(events,key=lambda x:int(x.get('revision') or 0)):
                status=_r32_apply_event_tx(conn,ev)
                if status=='applied': applied+=1
                elif status=='stale': stale+=1
            conn.commit()
            try: conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            except Exception: pass
        except Exception:
            try: conn.rollback()
            except Exception: pass
            raise
        finally: conn.close()
        with STATE_LOCK:
            STATE['r32_state_events_applied']=int(STATE.get('r32_state_events_applied') or 0)+applied
            STATE['r32_state_events_stale']=int(STATE.get('r32_state_events_stale') or 0)+stale
            STATE['r32_state_last_at']=time.time(); STATE['r32_state_last_error']=''; STATE['r32_baseline_ready']=True
            STATE['delta_since_checkpoint']=int(STATE.get('delta_since_checkpoint') or 0)+applied
            STATE['delta_bytes_since_checkpoint']=int(STATE.get('delta_bytes_since_checkpoint') or 0)+sum(len(json.dumps(x,ensure_ascii=False,default=str)) for x in events)
            try: STATE['last_state_sha256']=_sha256_file_v267(CACHE_DB)
            except Exception: pass
        return True,f'applied={applied} stale={stale}',applied,stale

def _r32_replay_state_events_from_redis(limit=50000):
    client=_redis_client()
    if client is None: return False,'REDIS_URL not configured'
    try:
        ids=client.zrange(_r32_index_key(),-max(1,int(limit)),-1) or []
        if not ids: return True,'no R32 Redis events'
        events=[]
        pipe=client.pipeline(transaction=False)
        for eid in ids:
            eid=eid.decode() if isinstance(eid,(bytes,bytearray)) else str(eid); pipe.get(_r32_event_key(eid))
        raws=pipe.execute()
        for raw in raws:
            if not raw: continue
            try:
                if isinstance(raw,(bytes,bytearray)): raw=raw.decode('utf-8')
                ev=json.loads(raw)
                if _r32_event_valid(ev): events.append(ev)
            except Exception: pass
        ok,detail,applied,stale=_r32_apply_events(events)
        return ok,f'{detail}; replay_rows={len(events)}'
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:220]}'

def _r32_events_mega_dir(): return mega_root().rstrip('/') + '/events_r32'


def _r32_mega_event_rows(limit=2000):
    root=_r32_events_mega_dir()
    try:
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok: return []
            if not ensure_mega_dir(root): return []
            res=run_cmd(['mega-find',root,'--pattern=events_*.json.gz','--type=f'],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if res.returncode!=0: return []
            rows=sorted({x.strip() for x in (res.stdout or '').splitlines() if x.strip().endswith('.json.gz')})
            return rows[-max(1,int(limit)):]
    except Exception:
        return []

def _r32_replay_state_events_from_mega(limit_segments=1500):
    """Disaster path: rebuild post-checkpoint changes from immutable MEGA pieces.

    Normal deploys replay Redis events and do not pay this network cost.  This path is
    only needed when Redis/current worker cache is unavailable or explicitly deep-recovered.
    """
    rows=_r32_mega_event_rows(limit_segments)
    if not rows: return True,'no R32 MEGA event segments'
    events=[]; downloaded=0
    work=Path(tempfile.mkdtemp(prefix='r32_mega_replay_'))
    try:
        with MEGA_LOCK:
            for remote in rows:
                dl=work/secrets.token_hex(4); dl.mkdir(parents=True,exist_ok=True)
                get=run_cmd(['mega-get',remote,str(dl)],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                if get.returncode!=0: continue
                files=list(dl.rglob('events_*.json.gz')) or list(dl.rglob('*.json.gz'))
                if not files: continue
                try:
                    obj=json.loads(gzip.decompress(files[0].read_bytes()).decode('utf-8'))
                    chunk=(obj or {}).get('events') or []
                    for ev in chunk:
                        if _r32_event_valid(ev): events.append(ev)
                    downloaded+=1
                except Exception: pass
        if not events: return False,f'MEGA segments downloaded={downloaded} but no valid events'
        ok,detail,applied,stale=_r32_apply_events(events)
        return ok,f'{detail}; mega_segments={downloaded}; mega_events={len(events)}'
    finally:
        shutil.rmtree(work,ignore_errors=True)

def _r32_upload_pending_segment():
    client=_redis_client()
    if client is None: return False,'Redis unavailable'
    ids=client.lrange(_r32_pending_key(),0,max(0,env_int('WORKER_R32_MEGA_SEGMENT_EVENTS',128,8,1000)-1)) or []
    ids=[x.decode() if isinstance(x,(bytes,bytearray)) else str(x) for x in ids]
    if not ids: return True,'no pending R32 events'
    raws=client.mget([_r32_event_key(x) for x in ids]) or []
    events=[]; valid_ids=[]
    for eid,raw in zip(ids,raws):
        if not raw: continue
        try:
            if isinstance(raw,(bytes,bytearray)): raw=raw.decode('utf-8')
            ev=json.loads(raw)
            if _r32_event_valid(ev): events.append(ev); valid_ids.append(eid)
        except Exception: pass
    if not events:
        client.ltrim(_r32_pending_key(),len(ids),-1)
        return True,'dropped missing pending rows'
    packed=gzip.compress(json.dumps({'schema':32,'created_at':time.time(),'events':events},ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    day=datetime.now(timezone.utc).strftime('%Y%m%d')
    work=Path(tempfile.mkdtemp(prefix='r32_events_')); local=work/f'events_{stamp}_{valid_ids[0][-8:]}_{valid_ids[-1][-8:]}.json.gz'; local.write_bytes(packed)
    try:
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok: return False,detail
            root=_r32_events_mega_dir()
            if not ensure_mega_dir(root): return False,'cannot create events_r32'
            remote_day=root.rstrip('/')+'/'+day
            if not ensure_mega_dir(remote_day): return False,'cannot create events day dir'
            put=run_cmd(['mega-put',str(local),remote_day],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if put.returncode!=0: return False,'mega-put R32 events failed: '+(put.stderr or put.stdout or '')[:180]
        pipe=client.pipeline(transaction=True)
        pipe.ltrim(_r32_pending_key(),len(ids),-1)
        if valid_ids: pipe.sadd(_r32_archived_key(),*valid_ids)
        pipe.execute()
        with STATE_LOCK:
            STATE['r32_mega_segments']=int(STATE.get('r32_mega_segments') or 0)+1; STATE['r32_mega_last_ok']=time.time(); STATE['r32_mega_last_error']=''
        return True,f'uploaded events={len(events)} bytes={len(packed)}'
    finally:
        shutil.rmtree(work,ignore_errors=True)

def _r32_mega_event_loop():
    while True:
        _R32_MEGA_WAKE.wait(timeout=max(5,env_int('WORKER_R32_MEGA_FLUSH_SEC',30,5,600)))
        _R32_MEGA_WAKE.clear()
        try:
            for _ in range(8):
                ok,detail=_r32_upload_pending_segment()
                if not ok:
                    with STATE_LOCK: STATE['r32_mega_last_error']=str(detail)[:220]
                    break
                if 'no pending' in detail: break
        except Exception as exc:
            with STATE_LOCK: STATE['r32_mega_last_error']=f'{type(exc).__name__}: {str(exc)[:220]}'

@app.route('/internal/state/events',methods=['POST'])
def internal_r32_state_events():
    if not authorized(): return {'ok':False},404
    wire=request.get_data(cache=False,as_text=False) or b''
    max_wire=env_int('WORKER_R32_EVENT_MAX_WIRE_KB',1024,32,8192)*1024
    if not wire or len(wire)>max_wire: return {'ok':False,'error':'R32 event batch size invalid'},413
    try:
        raw=gzip.decompress(wire) if str(request.headers.get('Content-Encoding') or '').lower()=='gzip' else wire
        body=json.loads(raw.decode('utf-8')); events=body.get('events') if isinstance(body,dict) else None
        if not isinstance(events,list) or not events or len(events)>512: raise ValueError('events invalid')
        events=[x for x in events if _r32_event_valid(x)]
        if not events: raise ValueError('no valid events')
    except Exception as exc:
        return {'ok':False,'error':f'R32 event decode: {type(exc).__name__}: {str(exc)[:180]}'},400
    rok,rdetail,new_ids=_r32_redis_store_events(events)
    if not rok:
        with STATE_LOCK: STATE['r32_state_last_error']=str(rdetail)[:220]
        return {'ok':False,'error':'R32 Redis durability failed: '+str(rdetail)[:180]},503
    try:
        ok,detail,applied,stale=_r32_apply_events(events)
    except Exception as exc:
        ok=False; detail=f'{type(exc).__name__}: {str(exc)[:220]}'; applied=stale=0
    if not ok:
        with STATE_LOCK: STATE['r32_state_last_error']=str(detail)[:220]
        # Events are already durable in Redis; a retry/replay can apply them later.
        return {'ok':False,'durable':True,'error':'R32 cache apply failed: '+str(detail)[:180]},503
    with STATE_LOCK:
        STATE['r32_state_events_received']=int(STATE.get('r32_state_events_received') or 0)+len(events)
        STATE['r32_state_event_bytes']=int(STATE.get('r32_state_event_bytes') or 0)+len(wire)
    if new_ids: _R32_MEGA_WAKE.set()
    return {'ok':True,'durable':'redis','events':len(events),'new':len(new_ids),'applied':applied,'stale':stale},200

@app.route('/internal/r32/status',methods=['GET'])
def internal_r32_status():
    if not authorized(): return {'ok':False},404
    client=_redis_client(); pending=0; total=0
    try:
        if client is not None:
            pending=int(client.llen(_r32_pending_key()) or 0); total=int(client.zcard(_r32_index_key()) or 0)
    except Exception: pass
    with STATE_LOCK: st={k:v for k,v in STATE.items() if str(k).startswith('r32_')}
    return {'ok':True,'event_stream':True,'redis_events':total,'mega_pending':pending,'state':st},200


@app.route('/', methods=['GET','HEAD'])
@app.route('/healthz', methods=['GET','HEAD'])
@app.route('/peer/health', methods=['GET','HEAD'])
def health():
    if request.method == 'HEAD': return '',200
    with STATE_LOCK: state=dict(STATE)
    return {'ok':True,'role':'worker','version':VERSION,'front_configured':bool(front_base()),'mega_enabled':mega_enabled(),'mega_configured':bool(mega_enabled() and (os.getenv('MEGA_SESSION') or (os.getenv('MEGA_EMAIL') and os.getenv('MEGA_PASSWORD')))),'google_configured':bool(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')),'queue_size':JOB_Q.qsize(),'google_queue_size':GOOGLE_Q.qsize(),'state':state},200

@app.route('/internal/event/receipt', methods=['POST'])
def internal_event_receipt_v268():
    if not authorized(): return {'ok':False},404
    wire=request.get_data(cache=False,as_text=False) or b''
    if not wire or len(wire)>env_int('WORKER_EVENT_MAX_WIRE_KB',512,16,4096)*1024: return {'ok':False,'error':'event wire invalid'},413
    try:
        raw=gzip.decompress(wire) if str(request.headers.get('Content-Encoding') or '').lower()=='gzip' else wire
        row=json.loads(raw.decode('utf-8'))
    except Exception as exc: return {'ok':False,'error':f'event decode: {type(exc).__name__}: {str(exc)[:180]}'},400
    if not isinstance(row,dict) or int(row.get('schema') or 0)!=1 or not str(row.get('event_id') or ''): return {'ok':False,'error':'event schema invalid'},400
    eid=str(row.get('event_id'))
    row['state']='received'; row['received_at']=float(row.get('received_at') or time.time()); row['updated_at']=time.time()
    if not _event_local_upsert_v268(row):
        return {'ok':False,'error':'worker local event journal failed'},503
    # R54 Redis-OFF contract: the user's explicit Redis switch must not eventually
    # fill EVENT_REDIS_Q and turn every normal Telegram message into HTTP 503.
    # Worker-local SQLite is FULL-synchronous and is enough for raw-update witness;
    # committed business state is independently mirrored through the R32 MEGA stream.
    redis_enabled = bool(str(redis_effective_url() or '').strip()) and (_redis is not None)
    if redis_enabled:
        queued=_event_redis_enqueue_v270(row)
        if not queued:
            rok,rdetail=_event_redis_store_v268(row)
            if not rok:
                with STATE_LOCK: STATE['event_last_error']=str(rdetail)[:220]
                return {'ok':False,'error':'event witness queue+Redis failed: '+str(rdetail)[:180]},503
        durability='worker_local+redis_async'
    else:
        durability='worker_local_redis_off'
    with STATE_LOCK:
        STATE['event_received']=int(STATE.get('event_received') or 0)+1; STATE['event_last_at']=time.time(); STATE['event_last_error']=''
    return {'ok':True,'event_id':eid,'state':'received','durable':durability},200

@app.route('/internal/event/commit', methods=['POST'])
def internal_event_commit_v268():
    if not authorized(): return {'ok':False},404
    row=request.get_json(silent=True) or {}; eid=str(row.get('event_id') or row.get('update_id') or '')
    if not eid: return {'ok':False,'error':'event id empty'},400
    state='committed' if str(row.get('state') or '')=='committed' else 'failed_retry'
    row.update({'event_id':eid,'update_id':str(row.get('update_id') or eid),'state':state,'updated_at':time.time()})
    if state=='committed' and not float(row.get('committed_at') or 0.0): row['committed_at']=time.time()
    if not _event_local_upsert_v268(row):
        return {'ok':False,'error':'worker local event commit journal failed'},503
    redis_enabled = bool(str(redis_effective_url() or '').strip()) and (_redis is not None)
    if redis_enabled:
        rok,rdetail=_event_redis_store_v268(row)
        if not rok: return {'ok':False,'error':'Redis event update failed: '+str(rdetail)[:180]},503
        durability='worker_local+redis'
    else:
        durability='worker_local_redis_off'
    with STATE_LOCK:
        if state=='committed': STATE['event_committed']=int(STATE.get('event_committed') or 0)+1
        STATE['event_last_at']=time.time(); STATE['event_last_error']=''
    return {'ok':True,'event_id':eid,'state':state,'durable':durability},200

@app.route('/internal/events/pending', methods=['GET'])
def internal_events_pending_v268():
    if not authorized(): return {'ok':False},404
    try: limit=max(1,min(250,int(request.args.get('limit','100') or '100')))
    except Exception: limit=100
    rows=_event_pending_rows_v268(limit)
    return {'ok':True,'events':rows,'count':len(rows)},200

@app.route('/internal/delta', methods=['POST'])
def internal_delta_v267():
    """Apply a compact page delta to the Worker's exact SQLite cache.

    Normal Front updates use this endpoint. A 409 means the Front and Worker bases
    diverged and the Front should perform one full /internal/snapshot/upload rebase.
    """
    if not authorized():
        return {'ok':False},404
    max_wire=env_int('WORKER_DELTA_MAX_WIRE_KB',2048,32,16384)*1024
    wire=request.get_data(cache=False,as_text=False) or b''
    if not wire:
        return {'ok':False,'error':'empty delta'},400
    if len(wire)>max_wire:
        return {'ok':False,'error':f'delta too large: {len(wire)} > {max_wire}'},413
    try:
        raw=gzip.decompress(wire) if str(request.headers.get('Content-Encoding') or '').lower()=='gzip' else wire
        if len(raw)>env_int('WORKER_DELTA_MAX_JSON_MB',16,1,64)*1024*1024:
            return {'ok':False,'error':'delta JSON too large'},413
        payload=json.loads(raw.decode('utf-8'))
    except Exception as exc:
        return {'ok':False,'error':f'delta decode: {type(exc).__name__}: {str(exc)[:240]}'},400
    with DELTA_APPLY_LOCK:
        ok,detail,status=_apply_delta_payload_v267(payload,journal_wire=wire,replay=False)
    if ok:
        with STATE_LOCK:
            return {'ok':True,'status':status,'detail':detail,'state_token':STATE.get('last_state_token'),'state_sha256':STATE.get('last_state_sha256'),'delta_since_checkpoint':STATE.get('delta_since_checkpoint')},200
    with STATE_LOCK:
        STATE['delta_last_error']=str(detail)[:240]
    code=409 if status=='need_full' else 400
    return {'ok':False,'status':status,'error':detail,'worker_sha256':str(STATE.get('last_state_sha256') or '')},code

@app.route('/internal/job', methods=['POST'])
def internal_job():
    global SYNC_PENDING, SYNC_DIRTY, SYNC_DIRTY_REASON
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}; kind=str(body.get('type') or '').strip()
    if kind != 'sync_state': return {'ok':False,'error':'unsupported job type','supported':['sync_state']},400
    token = str(body.get('state_token') or '')[:120]
    with STATE_LOCK:
        last_token = str(STATE.get('last_state_token') or '')
        active_token = str(STATE.get('active_state_token') or '')
        dirty_token = str(STATE.get('dirty_state_token') or '')
        last_full_started = float(STATE.get('last_full_fetch_started') or 0.0)
    try:
        min_full_interval = max(10, min(300, int(os.getenv('WORKER_FULL_REBASE_MIN_INTERVAL_SEC','45') or '45')))
    except Exception:
        min_full_interval = 45
    reason_l = str(body.get('reason') or '').lower()
    force_full = any(x in reason_l for x in ('boot_ready_exact_rebase','shutdown','manual_force'))
    if (not force_full) and last_full_started > 0 and time.time() - last_full_started < min_full_interval:
        with STATE_LOCK:
            STATE['full_fetch_suppressed'] = int(STATE.get('full_fetch_suppressed') or 0) + 1
        return {'ok':True,'status':'full_rebase_rate_limited','retry_after':max(1,int(min_full_interval-(time.time()-last_full_started))),'state_token':token,'queue_size':JOB_Q.qsize()},202
    with SYNC_PENDING_LOCK:
        if not SYNC_PENDING and token and token == last_token:
            with STATE_LOCK: STATE['deduped_sync_requests'] += 1
            return {'ok':True,'status':'up_to_date','state_token':token,'queue_size':JOB_Q.qsize()},200
        if SYNC_PENDING:
            if token and token in {active_token, dirty_token, last_token}:
                with STATE_LOCK: STATE['deduped_sync_requests'] += 1
                return {'ok':True,'status':'coalesced_same','state_token':token,'queue_size':JOB_Q.qsize()},202
            SYNC_DIRTY = True
            SYNC_DIRTY_REASON = str(body.get('reason') or 'coalesced_changes')[:180]
            with STATE_LOCK: STATE['dirty_state_token'] = token
            return {'ok':True,'status':'coalesced_dirty','state_token':token,'queue_size':JOB_Q.qsize()},202
        SYNC_PENDING=True
        with STATE_LOCK: STATE['active_state_token'] = token
    job={'id':secrets.token_hex(8),'type':kind,'reason':str(body.get('reason') or '')[:180],'state_token':token,'created_at':time.time()}
    try: JOB_Q.put_nowait(job)
    except queue.Full:
        with SYNC_PENDING_LOCK: SYNC_PENDING=False
        return {'ok':False,'error':'worker queue full'},503
    return {'ok':True,'status':'queued','job_id':job['id'],'queue_size':JOB_Q.qsize()},202

def _google_status_put(job_id, **values):
    now = time.time()
    with GOOGLE_JOB_LOCK:
        stale = [k for k, row in GOOGLE_JOB_STATUS.items() if now - float((row or {}).get('updated_at') or now) > 86400]
        for key in stale:
            GOOGLE_JOB_STATUS.pop(key, None)
        row = dict(GOOGLE_JOB_STATUS.get(job_id) or {})
        row.update(values)
        row['updated_at'] = now
        GOOGLE_JOB_STATUS[job_id] = row
        return dict(row)


def _notify_front_google_result(job, ok, url='', error=''):
    base, secret = front_base(), peer_secret()
    if not base or not secret:
        return False
    payload = {
        'job_id': str(job.get('id') or ''),
        'ok': bool(ok),
        'url': str(url or ''),
        'error': str(error or '')[:900],
        'title': str((job.get('payload') or {}).get('title') or 'Google Excel')[:220],
        'recipient_chat_id': (job.get('payload') or {}).get('recipient_chat_id'),
        'target_chat_id': (job.get('payload') or {}).get('target_chat_id'),
        'tenant_id': (job.get('payload') or {}).get('tenant_id'),
        'notify_result': bool((job.get('payload') or {}).get('notify_result', True)),
    }
    for attempt in range(1, 4):
        try:
            r = requests.post(base + '/internal/split/google-result', json=payload, headers={'X-Peer-Secret':secret,'User-Agent':'vys-262-worker-google-result'}, timeout=15)
            if 200 <= r.status_code < 300:
                return True
        except Exception:
            pass
        if attempt < 3:
            time.sleep(float(attempt))
    return False



@app.route('/internal/google/sheet', methods=['POST'])
def internal_google_sheet():
    if not authorized():
        return {'ok':False},404
    body = request.get_json(silent=True) or {}
    try:
        spreadsheet_id = _sheet_id(body.get('spreadsheet_id'))
    except Exception as exc:
        return {'ok':False,'error':str(exc)[:600]},400
    body['spreadsheet_id'] = spreadsheet_id
    try:
        recipient_chat_id = int(body.get('recipient_chat_id') or 0)
    except Exception:
        recipient_chat_id = 0
    if not recipient_chat_id:
        return {'ok':False,'error':'recipient_chat_id required'},400
    job_id = str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]
    with GOOGLE_JOB_LOCK:
        existing = dict(GOOGLE_JOB_STATUS.get(job_id) or {})
    if existing:
        status = str(existing.get('status') or 'queued')
        if status == 'done' and existing.get('ok') and existing.get('url'):
            return {'ok':True,'status':'done','job_id':job_id,'url':existing.get('url')},200
        if status == 'done' and not existing.get('ok'):
            return {'ok':False,'status':'done','job_id':job_id,'error':existing.get('error') or 'Google job failed'},409
        return {'ok':True,'status':status,'job_id':job_id,'queue_size':GOOGLE_Q.qsize()},202
    job = {'id':job_id,'type':'google_sheet','created_at':time.time(),'payload':body}
    _google_status_put(job_id, status='queued', ok=None)
    try:
        GOOGLE_Q.put_nowait(job)
    except queue.Full:
        with GOOGLE_JOB_LOCK:
            GOOGLE_JOB_STATUS.pop(job_id, None)
        return {'ok':False,'error':'Google worker queue full'},503
    return {'ok':True,'status':'queued','job_id':job_id,'queue_size':GOOGLE_Q.qsize()},202


@app.route('/internal/google/test', methods=['POST'])
def internal_google_test():
    if not authorized():
        return {'ok':False},404
    body = request.get_json(silent=True) or {}
    try:
        spreadsheet_id = _sheet_id(body.get('spreadsheet_id'))
        token = _google_token()
        info = _google_info()
        headers = {'Authorization':f'Bearer {token}'}
        meta = _r38_google_http('get', f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}', headers=headers, params={'fields':'spreadsheetId,properties.title'}, timeout=30)
        if meta.status_code >= 300:
            if meta.status_code in (401,403):
                raise RuntimeError(f'Нет доступа к таблице. Расшарьте её {info.get("client_email")} как Редактору. Google: {meta.text[:300]}')
            raise RuntimeError(f'Google Sheets HTTP {meta.status_code}: {meta.text[:350]}')
        title = str((meta.json().get('properties') or {}).get('title') or '')
        return {'ok':True,'spreadsheet_id':spreadsheet_id,'title':title,'service_email':info.get('client_email')},200
    except Exception as exc:
        return {'ok':False,'error':str(exc)[:700]},502

@app.route('/internal/snapshot/upload', methods=['POST'])
def internal_snapshot_upload():
    """Accept the final front SQLite snapshot directly during graceful deploy shutdown.

    Cache is replaced immediately after validation, so a new front instance can restore
    the exact last user state even while MEGA promotion continues in the worker queue.
    """
    if not authorized():
        return {'ok':False},404
    max_bytes = env_int('WORKER_SNAPSHOT_UPLOAD_MAX_MB',64,4,512) * 1024 * 1024
    raw = request.get_data(cache=False, as_text=False) or b''
    if not raw:
        return {'ok':False,'error':'empty snapshot'},400
    if len(raw) > max_bytes:
        return {'ok':False,'error':f'snapshot too large: {len(raw)} > {max_bytes}'},413
    DELTA_APPLY_LOCK.acquire()
    fd, name = tempfile.mkstemp(prefix='vys262_uploaded_', suffix='.sqlite3.gz'); os.close(fd)
    incoming = Path(name)
    try:
        incoming.write_bytes(raw)
        ok, detail, meta = quick_check_gzip(incoming)
        if not ok:
            incoming.unlink(missing_ok=True)
            return {'ok':False,'error':'invalid SQLite snapshot: '+str(detail)[:400]},400
        incoming_revision = float(meta.get('revision') or 0.0)
        with STATE_LOCK:
            current_revision = float(STATE.get('cache_revision') or 0.0)
        cache_exists = CACHE_LATEST.exists()
        if cache_exists and current_revision > 0.0 and (incoming_revision <= 0.0 or incoming_revision < current_revision):
            incoming.unlink(missing_ok=True)
            return {'ok':True,'cached':False,'status':'stale_ignored','revision':incoming_revision,'cache_revision':current_revision},202
        tmp_cache = CACHE_DIR / f'.latest_{secrets.token_hex(6)}.tmp'
        shutil.copy2(incoming, tmp_cache)
        os.replace(tmp_cache, CACHE_LATEST)
        with STATE_LOCK:
            STATE['cache_revision'] = max(current_revision, incoming_revision)
            STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or '')
            STATE['last_snapshot_size'] = int(meta.get('size') or len(raw))
            STATE['last_snapshot_at'] = time.time()
            STATE['last_state_token'] = str(request.headers.get('X-Split-State-Token') or STATE.get('last_state_token') or '')[:120]
            STATE['r34_seeded'] = True
            STATE['r34_seeded_at'] = time.time()
        try:
            CACHE_DB.unlink(missing_ok=True)
            _ensure_cache_db_v267()
            with STATE_LOCK:
                STATE['last_state_sha256'] = _sha256_file_v267(CACHE_DB) if CACHE_DB.exists() else ''
                STATE['delta_since_checkpoint'] = 0
            redis_store_snapshot(CACHE_LATEST, meta, clear_deltas=True)
        except Exception:
            pass
        promote_sync = str(request.headers.get('X-Snapshot-Promote-Mode') or 'async').strip().lower() == 'sync'
        if promote_sync:
            mega_ok, mega_detail = mega_promote_snapshot(incoming)
            incoming.unlink(missing_ok=True)
            if not mega_ok:
                return {'ok':False,'cached':True,'mega_promoted':False,'error':str(mega_detail)[:500],
                        'revision':float(meta.get('revision') or 0.0)},502
            return {'ok':True,'cached':True,'queued':False,'mega_promoted':True,'mega_detail':str(mega_detail)[:300],
                    'size':len(raw),'sha256':str(meta.get('sha256_gz') or ''),
                    'revision':float(meta.get('revision') or 0.0)},200

        job = {
            'id': secrets.token_hex(8), 'type': 'promote_uploaded',
            'reason': str(request.headers.get('X-Snapshot-Reason') or 'front_direct_upload')[:180],
            'created_at': time.time(), 'path': str(incoming),
        }
        try:
            JOB_Q.put_nowait(job)
        except queue.Full:
            # Cache is already exact; keep the file for a short best-effort promoter thread.
            def _late_promote(path=str(incoming)):
                p = Path(path)
                try:
                    mega_promote_snapshot(p)
                finally:
                    p.unlink(missing_ok=True)
            threading.Thread(target=_late_promote, name='vys262-upload-promote', daemon=True).start()
        return {'ok':True,'cached':True,'queued':True,'size':len(raw),'sha256':str(meta.get('sha256_gz') or ''),'revision':float(meta.get('revision') or 0.0)},202
    except Exception as exc:
        incoming.unlink(missing_ok=True)
        return {'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:400]}'},500
    finally:
        try: DELTA_APPLY_LOCK.release()
        except Exception: pass



def _capsule_mega_dir_r20():
    return mega_root().rstrip('/') + '/config_r20'

def _capsule_mega_filename_r20(obj):
    seq,gen,_=_capsule_tuple_r20(obj) if '_capsule_tuple_r20' in globals() else (0,0,0.0)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    return f'capsule_g{int(gen):010d}_s{int(seq):010d}_{stamp}.json.gz'

def _capsule_mega_rows_r20(limit=30):
    path=_capsule_mega_dir_r20()
    try:
        res=run_cmd(['mega-find',path,'--pattern=capsule_*.json.gz','--type=f'],timeout=60)
        if res.returncode!=0: return []
        rows=[x.strip() for x in (res.stdout or '').splitlines() if x.strip().endswith('.json.gz')]
        return sorted(set(rows),reverse=True)[:max(1,int(limit))]
    except Exception:
        return []

def _capsule_mega_upload_latest_r20():
    if not env_bool('WORKER_CAPSULE_MEGA_ENABLED',True): return True,'MEGA capsule disabled'
    try:
        with CAPSULE_LOCK:
            if not CAPSULE_LOCAL.is_file(): return False,'local capsule missing'
            packed=CAPSULE_LOCAL.read_bytes()
        obj,err=_capsule_decode_r20(packed)
        if not obj: return False,err or 'invalid local capsule'
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok: return False,detail
            cdir=_capsule_mega_dir_r20()
            if not ensure_mega_dir(cdir): return False,'cannot create capsule MEGA dir'
            work=Path(tempfile.mkdtemp(prefix='vys262_capsule_mega_'))
            try:
                local=work/_capsule_mega_filename_r20(obj); local.write_bytes(packed)
                put=run_cmd(['mega-put',str(local),cdir],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                if put.returncode!=0: return False,'mega-put capsule failed: '+(put.stderr or put.stdout or '')[:180]
                keep=env_int('WORKER_CAPSULE_MEGA_KEEP',10,3,50)
                rows=_capsule_mega_rows_r20(keep+30)
                for old in rows[keep:]:
                    try: run_cmd(['mega-rm',old],timeout=45)
                    except Exception: pass
            finally:
                shutil.rmtree(work,ignore_errors=True)
        CAPSULE_MEGA_STATE['last_ok']=time.time(); CAPSULE_MEGA_STATE['last_error']=''; CAPSULE_MEGA_STATE['uploads']=int(CAPSULE_MEGA_STATE.get('uploads') or 0)+1
        return True,f'MEGA capsule stored gen={_capsule_tuple_r20(obj)[1]} seq={_capsule_tuple_r20(obj)[0]}'
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:200]}'; CAPSULE_MEGA_STATE['last_error']=detail; return False,detail

def _capsule_mega_load_r20():
    if not env_bool('WORKER_CAPSULE_MEGA_ENABLED',True): return {},'MEGA capsule disabled'
    try:
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok: return {},detail
            cdir=_capsule_mega_dir_r20()
            if not ensure_mega_dir(cdir): return {},'capsule MEGA dir unavailable'
            rows=_capsule_mega_rows_r20(20)
            if not rows: return {},'no MEGA capsule'
            # Filenames are zero-padded by generation then user seq, so reverse sort
            # gives the strongest recent configuration first. Validate more than one
            # file so a partial/corrupt upload cannot break recovery.
            work=Path(tempfile.mkdtemp(prefix='vys262_capsule_get_'))
            try:
                candidates=[]
                for remote in rows[:5]:
                    dl=work/secrets.token_hex(3); dl.mkdir(parents=True,exist_ok=True)
                    get=run_cmd(['mega-get',remote,str(dl)],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                    if get.returncode!=0: continue
                    files=list(dl.rglob('*.json.gz'))+list(dl.rglob('*.gz'))
                    for f in files:
                        obj,err=_capsule_decode_r20(f.read_bytes())
                        if obj: candidates.append(obj)
                obj=_capsule_merge_r20(*candidates) if '_capsule_merge_r20' in globals() else (candidates[0] if candidates else {})
                if obj:
                    CAPSULE_MEGA_STATE['last_ok']=time.time(); CAPSULE_MEGA_STATE['last_error']=''; CAPSULE_MEGA_STATE['restores']=int(CAPSULE_MEGA_STATE.get('restores') or 0)+1
                    return obj,f'MEGA capsule OK gen={_capsule_tuple_r20(obj)[1]} seq={_capsule_tuple_r20(obj)[0]}'
                return {},'MEGA capsule files invalid'
            finally:
                shutil.rmtree(work,ignore_errors=True)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:200]}'; CAPSULE_MEGA_STATE['last_error']=detail; return {},detail

def _capsule_mega_enqueue_r20():
    if not env_bool('WORKER_CAPSULE_MEGA_ENABLED',True): return False
    try:
        CAPSULE_MEGA_Q.put_nowait(1); return True
    except queue.Full:
        # A queued job reads CAPSULE_LOCAL only when it runs, therefore it naturally
        # uploads the newest coalesced generation even if several changes arrived.
        return True

def _capsule_mega_loop_r20():
    while True:
        CAPSULE_MEGA_Q.get()
        try:
            time.sleep(0.25)
            # Collapse any duplicate wakeups; the file itself is the latest generation.
            try:
                while True: CAPSULE_MEGA_Q.get_nowait(); CAPSULE_MEGA_Q.task_done()
            except queue.Empty: pass
            ok,detail=_capsule_mega_upload_latest_r20()
            if not ok: print('[R20 CAPSULE MEGA] '+detail,flush=True)
        except Exception as exc:
            CAPSULE_MEGA_STATE['last_error']=f'{type(exc).__name__}: {str(exc)[:180]}'
        finally:
            CAPSULE_MEGA_Q.task_done()

def _capsule_decode_r20(raw: bytes):
    try:
        if raw[:2] == b'\x1f\x8b': raw = gzip.decompress(raw)
        obj=json.loads(raw.decode('utf-8'))
        if not isinstance(obj,dict) or str(obj.get('kind') or '')!='vys262_durable_capsule_r20':
            return {}, 'invalid capsule kind'
        return obj, ''
    except Exception as exc:
        return {}, f'{type(exc).__name__}: {str(exc)[:180]}'

def _capsule_tuple_r20(obj):
    return (int((obj or {}).get('user_state_seq') or ((obj or {}).get('user_state') or {}).get('seq') or 0),
            int((obj or {}).get('config_generation') or ((obj or {}).get('config_checkpoint') or {}).get('generation') or 0),
            float((obj or {}).get('saved_at') or 0.0))

def _capsule_merge_r20(*rows):
    rows=[x for x in rows if isinstance(x,dict) and x]
    if not rows: return {}
    merged=dict(max(rows,key=lambda x: float(x.get('saved_at') or 0.0)))
    best_us=max(rows,key=lambda x:int((x.get('user_state') or {}).get('seq') or x.get('user_state_seq') or 0))
    best_cp=max(rows,key=lambda x:int((x.get('config_checkpoint') or {}).get('generation') or x.get('config_generation') or 0))
    merged['user_state']=best_us.get('user_state') or {}; merged['user_state_seq']=int((merged['user_state'] or {}).get('seq') or best_us.get('user_state_seq') or 0)
    merged['config_checkpoint']=best_cp.get('config_checkpoint') or {}; merged['config_generation']=int((merged['config_checkpoint'] or {}).get('generation') or best_cp.get('config_generation') or 0)
    try:
        best_rev=max(rows,key=lambda x:float(((x.get('state_revision') or {}).get('saved_at') or 0.0))); merged['state_revision']=best_rev.get('state_revision') or {}
    except Exception: pass
    merged['saved_at']=max(float(x.get('saved_at') or 0.0) for x in rows)
    merged['kind']='vys262_durable_capsule_r20'; merged['schema']=1
    return merged

def _capsule_load_latest_r20(deep_mega=False):
    rows=[]; detail=[]
    client=_redis_client()
    if client is not None:
        try:
            raw=client.get(_REDIS_CAPSULE_KEY)
            if raw:
                obj,err=_capsule_decode_r20(bytes(raw))
                if obj: rows.append(obj); detail.append('redis')
                elif err: detail.append('redis:'+err)
        except Exception as exc: detail.append('redis:'+str(exc)[:100])
    try:
        if CAPSULE_LOCAL.is_file():
            obj,err=_capsule_decode_r20(CAPSULE_LOCAL.read_bytes())
            if obj: rows.append(obj); detail.append('local')
            elif err: detail.append('local:'+err)
    except Exception as exc: detail.append('local:'+str(exc)[:100])
    if deep_mega or not rows:
        obj,why=_capsule_mega_load_r20()
        if obj: rows.append(obj); detail.append('mega')
        elif why: detail.append('mega:'+why[:100])
    return _capsule_merge_r20(*rows), ','.join(detail)

def _capsule_store_r20(obj:dict, packed:bytes):
    with CAPSULE_LOCK:
        if _R43_DIRECT_HEAVY:
            merged=dict(obj or {})
            packed=gzip.compress(json.dumps(merged,ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
            new_t=_capsule_tuple_r20(merged)
            tmp=CAPSULE_LOCAL.with_suffix('.tmp'); tmp.write_bytes(packed); os.replace(tmp,CAPSULE_LOCAL)
            with STATE_LOCK:
                STATE['capsule_seq']=new_t[0]; STATE['capsule_generation']=new_t[1]; STATE['capsule_saved_at']=new_t[2]; STATE['capsule_last_error']=''
            return True, f'R43 local mirror seq={new_t[0]} gen={new_t[1]}'
        old,_=_capsule_load_latest_r20()
        merged=dict(obj or {})
        if isinstance(old,dict) and old:
            old_seq,old_gen,_old_at=_capsule_tuple_r20(old)
            new_seq,new_gen,_new_at=_capsule_tuple_r20(merged)
            if old_seq > new_seq:
                merged['user_state']=old.get('user_state') or {}; merged['user_state_seq']=old_seq
            if old_gen > new_gen:
                merged['config_checkpoint']=old.get('config_checkpoint') or {}; merged['config_generation']=old_gen
            try:
                if float(((old.get('state_revision') or {}).get('saved_at') or 0.0)) > float(((merged.get('state_revision') or {}).get('saved_at') or 0.0)):
                    merged['state_revision']=old.get('state_revision') or {}
            except Exception: pass
            merged['saved_at']=max(float(old.get('saved_at') or 0.0),float(merged.get('saved_at') or 0.0))
        packed=gzip.compress(json.dumps(merged,ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
        new_t=_capsule_tuple_r20(merged)
        tmp=CAPSULE_LOCAL.with_suffix('.tmp'); tmp.write_bytes(packed); os.replace(tmp,CAPSULE_LOCAL)
        client=_redis_client()
        if client is not None:
            meta={'user_state_seq':new_t[0],'config_generation':new_t[1],'saved_at':new_t[2],'size':len(packed),'source':'worker-r20'}
            pipe=client.pipeline(transaction=True); pipe.set(_REDIS_CAPSULE_KEY,packed); pipe.set(_REDIS_CAPSULE_KEY+':meta',json.dumps(meta,separators=(',',':'))); pipe.execute()
        with STATE_LOCK:
            STATE['capsule_seq']=new_t[0]; STATE['capsule_generation']=new_t[1]; STATE['capsule_saved_at']=new_t[2]; STATE['capsule_last_error']=''
        _capsule_mega_enqueue_r20()
        return True, f'stored seq={new_t[0]} gen={new_t[1]}'

@app.route('/internal/capsule',methods=['POST'])
def internal_capsule_r20():
    if not authorized(): return {'ok':False},404
    max_bytes=env_int('WORKER_REDIS_CAPSULE_MAX_MB',8,1,32)*1024*1024
    raw=request.get_data(cache=False)
    if not raw or len(raw)>max_bytes: return {'ok':False,'error':'invalid capsule size'},413
    obj,err=_capsule_decode_r20(raw)
    if not obj: return {'ok':False,'error':err},400
    # Canonical storage remains gzip independent of HTTP server decompression behavior.
    packed=gzip.compress(json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
    try:
        ok,detail=_capsule_store_r20(obj,packed)
        return {'ok':ok,'detail':detail,'user_state_seq':_capsule_tuple_r20(obj)[0],'config_generation':_capsule_tuple_r20(obj)[1]},200 if ok else 503
    except Exception as exc:
        with STATE_LOCK: STATE['capsule_last_error']=f'{type(exc).__name__}: {str(exc)[:180]}'
        return {'ok':False,'error':STATE['capsule_last_error']},500

@app.route('/internal/capsule/latest',methods=['GET'])
def internal_capsule_latest_r20():
    if not authorized(): return {'ok':False},404
    deep=str(request.args.get('deep') or '').strip().lower() in {'1','true','yes','on'}
    obj,detail=_capsule_load_latest_r20(deep_mega=(deep and not _R43_DIRECT_HEAVY))
    if not obj: return {'ok':False,'error':'capsule not available','detail':detail},404
    # A deep boot restore also heals local/Redis from MEGA without waiting for the next change.
    if deep:
        try:
            packed0=gzip.compress(json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
            with CAPSULE_LOCK:
                tmp=CAPSULE_LOCAL.with_suffix('.tmp'); tmp.write_bytes(packed0); os.replace(tmp,CAPSULE_LOCAL)
            client=_redis_client()
            if client is not None:
                pipe=client.pipeline(transaction=True); pipe.set(_REDIS_CAPSULE_KEY,packed0); pipe.set(_REDIS_CAPSULE_KEY+':meta',json.dumps({'user_state_seq':_capsule_tuple_r20(obj)[0],'config_generation':_capsule_tuple_r20(obj)[1],'saved_at':_capsule_tuple_r20(obj)[2],'size':len(packed0),'source':'worker-r20-deep'},separators=(',',':'))); pipe.execute()
        except Exception: pass
    packed=gzip.compress(json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
    resp=Response(packed,status=200,mimetype='application/gzip')
    resp.headers['X-Capsule-User-Seq']=str(_capsule_tuple_r20(obj)[0]); resp.headers['X-Capsule-Config-Generation']=str(_capsule_tuple_r20(obj)[1])
    return resp

@app.route('/internal/restore/latest', methods=['GET'])
def internal_restore_latest():
    if not authorized(): return {'ok':False},404
    # R32: assemble the newest restore image from the latest checkpoint + immutable events.
    try:
        _r32_replay_state_events_from_redis()
        with DELTA_APPLY_LOCK:
            _ok32,_detail32,_meta32=_ensure_cache_db_v267()
            if _ok32:
                _gzip_cache_db_v267()
    except Exception as _r32_restore_exc:
        with STATE_LOCK: STATE['r32_state_last_error']=f'restore assemble: {type(_r32_restore_exc).__name__}: {str(_r32_restore_exc)[:180]}'
    cache_max_age=env_int('WORKER_RESTORE_CACHE_MAX_AGE_SEC',120,0,3600)
    if not CACHE_LATEST.exists():
        redis_ok, redis_detail = redis_load_snapshot_to_cache()
        if not redis_ok:
            started=_schedule_restore_refresh()
            return {'ok':False,'error':'restore cache not ready','redis':redis_detail,'refresh_queued':bool(started)},503
    # Never serve corrupt cache just because the file exists.
    ok, detail, meta = quick_check_gzip(CACHE_LATEST)
    if not ok:
        CACHE_LATEST.unlink(missing_ok=True)
        redis_ok, redis_detail = redis_load_snapshot_to_cache()
        if not redis_ok:
            started=_schedule_restore_refresh()
            return {'ok':False,'error':'restore cache invalid','detail':detail,'redis':redis_detail,'refresh_queued':bool(started)},503
        ok, detail, meta = quick_check_gzip(CACHE_LATEST)
        if not ok:
            return {'ok':False,'error':'Redis restore cache invalid after reload'},503
    age=max(0.0,time.time()-CACHE_LATEST.stat().st_mtime)
    if cache_max_age <= 0 or age > cache_max_age:
        _schedule_restore_refresh()
    payload=CACHE_LATEST.read_bytes()
    resp=Response(payload,status=200,mimetype='application/gzip')
    resp.headers['Content-Disposition']='attachment; filename="latest_bot_state.sqlite3.gz"'
    resp.headers['X-Worker-Version']=TRANSPORT_VERSION
    resp.headers['X-SHA256']=hashlib.sha256(payload).hexdigest()
    resp.headers['X-Cache-Age-Sec']=str(int(age))
    resp.headers['X-State-Revision']=str(float(meta.get('revision') or 0.0))
    return resp

@app.route('/internal/r34/seed-status',methods=['GET'])
def internal_r34_seed_status():
    if not authorized(): return {'ok':False},404
    with STATE_LOCK:
        return {'ok':True,'seeded':bool(STATE.get('r34_seeded')),'seeded_at':float(STATE.get('r34_seeded_at') or 0.0),'max_applied_revision':int(STATE.get('r34_max_applied_revision') or 0)},200

@app.route('/internal/status', methods=['GET'])
def internal_status():
    if not authorized(): return {'ok':False},404
    with STATE_LOCK: return {'ok':True,'version':VERSION,'queue_size':JOB_Q.qsize(),'google_queue_size':GOOGLE_Q.qsize(),'state':dict(STATE)},200

# ---------------------------------------------------------------------------
# R7 heavy file exports: CSV/XLSX/Drive live here, away from Telegram webhooks.
FILE_Q = queue.Queue(maxsize=12)
FILE_JOB_LOCK = threading.RLock()
FILE_JOB_STATUS = {}
FILE_DIR = CACHE_DIR / 'exports'
FILE_DIR.mkdir(parents=True, exist_ok=True)
try:
    STATE.update({'file_jobs':0, 'file_failures':0, 'file_last_ok':0.0, 'file_last_error':''})
except Exception:
    pass


def _file_status_put_memory(job_id, **values):
    now = time.time()
    with FILE_JOB_LOCK:
        for key, row in list(FILE_JOB_STATUS.items()):
            if now - float((row or {}).get('updated_at') or now) > 7200:
                old = FILE_JOB_STATUS.pop(key, {}) or {}
                try:
                    path = Path(str(old.get('path') or ''))
                    if path.is_file(): path.unlink(missing_ok=True)
                except Exception: pass
        row = dict(FILE_JOB_STATUS.get(job_id) or {})
        row.update(values); row['updated_at'] = now
        FILE_JOB_STATUS[job_id] = row
        return dict(row)


def _safe_export_name(value, ext):
    raw = str(value or f'export.{ext}').replace('\\','_').replace('/','_').strip()
    raw = re.sub(r'[\x00-\x1f<>:"|?*]+','_',raw)[:180] or f'export.{ext}'
    if not raw.lower().endswith('.'+ext): raw += '.'+ext
    return raw


def _xlsx_value(value):
    if isinstance(value, dict):
        formula = str(value.get('formula') or '').strip()
        if formula:
            return '=' + formula.lstrip('=')
        if 'value' in value:
            return value.get('value')
    return value


def _r10_rgb_hex(rgb):
    if not isinstance(rgb, dict): return None
    try:
        vals=[]
        for key in ('red','green','blue'):
            raw=float(rgb.get(key,0.0) or 0.0)
            vals.append(max(0,min(255,round(raw*255))))
        return ''.join(f'{v:02X}' for v in vals)
    except Exception:
        return None

def _r10_apply_v262_xlsx_style(cell, row, r_idx, c_idx, max_cols, layout):
    """R16 canonical palette for every XLSX file and Google Sheets path."""
    try:
        fmt=_v262_google_cell_format(row,r_idx,c_idx,max_cols,layout) or {}
    except Exception:
        fmt={}
    fill=_r10_rgb_hex(fmt.get('backgroundColor'))
    if fill: cell.fill=PatternFill('solid',fgColor=fill)
    tf=fmt.get('textFormat') or {}
    if tf.get('bold'): cell.font=Font(bold=True)
    wrap = str(fmt.get('wrapStrategy') or '').upper() != 'CLIP'
    h=str(fmt.get('horizontalAlignment') or '').lower() or None
    v=str(fmt.get('verticalAlignment') or 'TOP').lower()
    cell.alignment=Alignment(horizontal=h, vertical=v, wrap_text=wrap)
    nf=fmt.get('numberFormat') or {}
    if nf.get('pattern'): cell.number_format=str(nf.get('pattern'))
    borders=fmt.get('borders') or {}
    def side(name):
        spec=borders.get(name) or {}
        if not spec: return Side(style=None)
        color=_r10_rgb_hex(spec.get('color')) or 'A6A6A6'
        style='thin' if str(spec.get('style') or '').upper() != 'NONE' else None
        return Side(style=style,color=color)
    if borders:
        cell.border=Border(left=side('left'),right=side('right'),top=side('top'),bottom=side('bottom'))

def _render_export_file(body, job_id):
    ftype = 'xlsx' if str(body.get('file_type') or '').lower() in {'xlsx','xlsxstat','excel'} else 'csv'
    filename = _safe_export_name(body.get('filename'), ftype)
    path = FILE_DIR / f'{job_id}.{ftype}'
    rows = body.get('rows') or []
    if ftype == 'csv':
        with open(path,'w',newline='',encoding='utf-8-sig') as fh:
            w=csv.writer(fh)
            for row in rows: w.writerow(list(row or []))
        return path, filename
    wb=Workbook(); ws=wb.active; ws.title=str(body.get('sheet_name') or 'Экспорт')[:31] or 'Экспорт'
    annotations={}
    for key,val in (body.get('annotations') or {}).items():
        try:
            rr,cc=str(key).split(',',1); annotations[(int(rr),int(cc))]=str(val)
        except Exception: pass
    raw_layout = body.get('category_layout')
    layout = 'category' if raw_layout is True else (str(raw_layout or body.get('layout') or 'category').lower())
    style = str(body.get('style') or 'old')
    max_cols=max((len(list(row or [])) for row in rows), default=1)
    for r_idx,row in enumerate(rows,start=1):
        vals=list(row or [])
        padded=vals + [''] * max(0,max_cols-len(vals))
        for c_idx,value in enumerate(padded,start=1):
            cell=ws.cell(r_idx,c_idx,value=_xlsx_value(value))
            _r10_apply_v262_xlsx_style(cell,padded,r_idx,c_idx,max_cols,layout)
            note=annotations.get((r_idx,c_idx),'').strip()
            if note and style in {'new_comments','new_notes','old','new_plain'}:
                cell.comment=Comment(note,'Telegram Finance Bot')
    ws.freeze_panes='A2'
    for col in range(1,max_cols+1):
        letter=get_column_letter(col)
        width=10
        for cell in ws[letter][:min(ws.max_row,300)]:
            try: width=max(width,min(48,len(str(cell.value or ''))+2))
            except Exception: pass
        ws.column_dimensions[letter].width=width
    wb.save(path)
    return path, filename


def _drive_upload_file(path: Path, filename: str, folder_id: str=''):
    token=_google_token(); headers={'Authorization':f'Bearer {token}'}
    metadata={'name':filename}
    folder=str(folder_id or '').strip()
    if folder:
        folder=_sheet_id(folder) if '/spreadsheets/' in folder else re.sub(r'^.*/folders/','',folder).split('?')[0].split('#')[0].strip('/')
        if not re.fullmatch(r'[A-Za-z0-9_-]{10,}',folder): raise RuntimeError('Google Drive folder ID invalid')
        metadata['parents']=[folder]
    mime=mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    with open(path,'rb') as fh:
        r=_r38_google_http('post','https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,webViewLink', headers=headers,
            files={'metadata':('metadata',json.dumps(metadata),'application/json; charset=UTF-8'),'file':(filename,fh,mime)}, timeout=120)
    if r.status_code >= 300: raise RuntimeError(f'Google Drive upload {r.status_code}: {r.text[:500]}')
    payload=r.json() or {}; fid=str(payload.get('id') or '')
    return str(payload.get('webViewLink') or (f'https://drive.google.com/file/d/{fid}/view' if fid else ''))


def _notify_front_export_result(job, ok, **extra):
    """R36 bounded callback attempt; recovery queue owns long-term retry.

    Result workers must never sit blocked for 15 minutes each. They try for a short
    bounded window, then release the worker; the durable job ledger requeues delivery.
    """
    base,secret=front_base(),peer_secret()
    if not base or not secret: return False
    body=dict(job.get('payload') or {})
    payload={'job_id':str(job.get('id') or ''),'ok':bool(ok),'error':str(extra.get('error') or '')[:900],
        'url':str(extra.get('url') or ''),'filename':str(extra.get('filename') or body.get('filename') or ''),
        'file_type':str(body.get('file_type') or ''),'delivery':str(body.get('delivery') or 'chat'),
        'recipient_chat_id':body.get('recipient_chat_id'),'target_chat_id':body.get('target_chat_id'),
        'tenant_id':body.get('tenant_id'),'label':str(body.get('label') or ''),'chat_name':str(body.get('chat_name') or ''),'caption':str(body.get('caption') or '')[:1000]}
    deadline=time.time()+max(10,min(180,env_int('WORKER_R36_RESULT_ATTEMPT_WINDOW_SEC',45,10,180)))
    delay=1.0
    while time.time()<deadline:
        try:
            r=requests.post(base+'/internal/split/export-result',json=payload,headers={'X-Peer-Secret':secret,'User-Agent':'per-r36-worker-export-result'},timeout=15)
            data={}
            if r.content:
                try: data=r.json()
                except Exception: data={}
            if r.status_code==200 and bool(data.get('delivered')): return True
            if 400<=r.status_code<500 and r.status_code not in {408,409,425,429}: return False
        except Exception: pass
        time.sleep(delay); delay=min(max(2.0,env_int('WORKER_R36_RESULT_RETRY_SEC',6,1,30)),delay*1.5)
    return False

def _process_file_job_core(job):
    jid=str(job.get('id') or ''); body=dict(job.get('payload') or {})
    _file_status_put(jid,status='running')
    try:
        path,filename=_render_export_file(body,jid)
        delivery=str(body.get('delivery') or 'chat')
        url=''
        if delivery == 'drive':
            url=_drive_upload_file(path,filename,str(body.get('drive_folder_id') or ''))
        with STATE_LOCK:
            STATE['file_jobs']=int(STATE.get('file_jobs') or 0)+1; STATE['file_last_ok']=time.time(); STATE['file_last_error']=''
        _file_status_put(jid,status='done',ok=True,path=str(path),filename=filename,url=url,delivery=delivery)
        delivered=_notify_front_export_result(job,True,url=url,filename=filename)
        _file_status_put(jid,callback_delivered=bool(delivered))
        if delivery == 'drive': path.unlink(missing_ok=True)
        print(f'[EXPORT JOB] {jid} ok=True delivery={delivery} callback={delivered}',flush=True)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:700]}'
        with STATE_LOCK:
            STATE['file_failures']=int(STATE.get('file_failures') or 0)+1; STATE['file_last_error']=detail[:240]
        _file_status_put(jid,status='done',ok=False,error=detail)
        _notify_front_export_result(job,False,error=detail)
        print(f'[EXPORT JOB] {jid} ok=False {detail}',flush=True)


@app.route('/internal/google/info',methods=['GET'])
def internal_google_info_r7():
    if not authorized(): return {'ok':False},404
    try:
        info=_google_info()
        return {'ok':True,'service_email':str(info.get('client_email') or ''),'configured':True},200
    except Exception as exc:
        return {'ok':False,'configured':False,'error':str(exc)[:500]},502


@app.route('/internal/export/file',methods=['POST'])
def internal_export_file_r7():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    try: cid=int(body.get('recipient_chat_id') or 0)
    except Exception: cid=0
    if not cid: return {'ok':False,'error':'recipient_chat_id required'},400
    rows=body.get('rows') or []
    if not isinstance(rows,list) or len(rows)>100000: return {'ok':False,'error':'invalid/too many rows'},400
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]
    with FILE_JOB_LOCK: existing=dict(FILE_JOB_STATUS.get(jid) or {})
    if existing:
        return {'ok':bool(existing.get('ok',True)),'status':existing.get('status') or 'queued','job_id':jid},200 if existing.get('status')=='done' else 202
    job={'id':jid,'type':'file_export','created_at':time.time(),'payload':body}
    _file_status_put(jid,status='queued',ok=None)
    try: FILE_Q.put_nowait(job)
    except queue.Full:
        with FILE_JOB_LOCK: FILE_JOB_STATUS.pop(jid,None)
        return {'ok':False,'error':'file worker queue full'},503
    return {'ok':True,'status':'queued','job_id':jid,'queue_size':FILE_Q.qsize()},202


@app.route('/internal/export/file/<job_id>',methods=['GET'])
def internal_export_download_r7(job_id):
    if not authorized(): return {'ok':False},404
    jid=str(job_id or '')[:80]
    with FILE_JOB_LOCK: row=dict(FILE_JOB_STATUS.get(jid) or {})
    if not row or str(row.get('status') or '') not in {'ready','delivering','done','delivered'} or not row.get('ok'): return {'ok':False,'error':'file not ready'},404
    path=Path(str(row.get('path') or ''))
    if not path.is_file(): return {'ok':False,'error':'file expired'},410
    return send_file(path,as_attachment=True,download_name=str(row.get('filename') or path.name),max_age=0)



# ---------------------------------------------------------------------------
# R35 HEAVY-only export/document layer + Redis-optional state durability.
# All expensive selection/serialization/compression/MEGA/Google work happens here.
R35_FRONT_SOURCE = Path(__file__).resolve().parent / '__front_source_not_packaged_r43__.py'
STATE.update({'r33_heavy_exports':0,'r33_heavy_export_failures':0,'r33_direct_mega_events':0,
              'r33_direct_mega_event_bytes':0,'r33_last_event_durability':'','r33_last_export':''})


def _r33_json_load(raw, default=None):
    try: return json.loads(raw) if raw is not None else default
    except Exception: return default


def _r33_cache_ready():
    ok,detail,_meta=_ensure_cache_db_v267()
    if not ok: raise RuntimeError('HEAVY restore DB unavailable: '+str(detail))
    if not CACHE_DB.is_file(): raise RuntimeError('HEAVY restore DB missing')
    return CACHE_DB


def _r33_load_root():
    db=_r33_cache_ready(); con=sqlite3.connect(str(db),timeout=20)
    try:
        row=con.execute("SELECT v FROM kv WHERE k='root'").fetchone()
        return _r33_json_load(row[0],{}) if row else {}
    finally: con.close()


def _r33_load_store(chat_id):
    db=_r33_cache_ready(); cid=str(int(chat_id)); con=sqlite3.connect(str(db),timeout=20)
    try:
        row=con.execute('SELECT v FROM chats WHERE chat_id=?',(cid,)).fetchone()
        store=_r33_json_load(row[0],{}) if row else {}
        if not isinstance(store,dict): store={}
        try:
            for k,v in con.execute('SELECT k,v FROM cold_fields WHERE chat_id=?',(cid,)).fetchall():
                store[str(k)]=_r33_json_load(v,[] if str(k).endswith('records') or str(k)=='records' else {})
        except sqlite3.OperationalError:
            pass
        return store
    finally: con.close()


def _r33_day(rec):
    for key in ('day_key','date_key','date'):
        v=str((rec or {}).get(key) or '')[:10]
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}',v): return v
    ts=str((rec or {}).get('timestamp') or '')
    m=re.search(r'(20\d\d-\d\d-\d\d)',ts)
    return m.group(1) if m else ''


def _r33_sort_key(rec):
    return (_r33_day(rec),str((rec or {}).get('timestamp') or ''),int((rec or {}).get('id') or 0))


def _r33_f(value):
    try: return float(value or 0)
    except Exception: return 0.0


def _r33_ledger_rows(store,currency='ars'):
    settings=store.get('settings') if isinstance(store.get('settings'),dict) else {}
    active=str(settings.get('_active_currency_ledger') or settings.get('currency_mode') or 'ars').lower()
    base=list(store.get('records') or [])
    ars=list(base if active=='ars' else (store.get('ars_records') or []))
    usd=list(base if active=='usd' else (store.get('usd_records') or []))
    if currency=='ars':
        out=[]
        for rec in ars:
            if not isinstance(rec,dict): continue
            amount=_r33_f(rec.get('amount')); usd_amount=_r33_f(rec.get('usd_amount'))
            explicit=str(rec.get('currency') or '').strip().lower()
            pure=bool(rec.get('usd_only')) or explicit in {'usd','$','us$','u$s'} or (abs(amount)<1e-12 and abs(usd_amount)>1e-12)
            if pure: continue
            x=dict(rec); x['_amount']=amount; x['_note']=str(rec.get('note') or ''); out.append(x)
        return sorted(out,key=_r33_sort_key)
    out=[]; seen=set()
    def key(rec,prefix=''):
        op=str(rec.get('operation_key') or '')
        mid=int(rec.get('source_msg_id') or 0)
        if op:return ('op',op)
        if mid:return ('msg',mid)
        return (prefix,int(rec.get('id') or 0),str(rec.get('timestamp') or ''),_r33_day(rec))
    for rec in usd:
        if not isinstance(rec,dict): continue
        x=dict(rec); x['_amount']=_r33_f(rec.get('amount')); x['_note']=str(rec.get('note') or rec.get('usd_note') or '')
        kk=key(x,'usd'); seen.add(kk); out.append(x)
    for rec in ars:
        if not isinstance(rec,dict) or abs(_r33_f(rec.get('usd_amount')))<1e-12: continue
        kk=key(rec,'embedded')
        if kk in seen: continue
        x=dict(rec); x['_amount']=_r33_f(rec.get('usd_amount')); x['_note']=str(rec.get('usd_note') or rec.get('note') or ''); out.append(x); seen.add(kk)
    return sorted(out,key=_r33_sort_key)


def _r33_bounds(mode,day_key,records):
    mode=str(mode or 'all').replace('csv_','').replace('xlsx_','')
    day=str(day_key or '')[:10]
    try: base=datetime.strptime(day,'%Y-%m-%d')
    except Exception: base=datetime.now(timezone.utc)
    if mode=='day': return day,day
    if mode=='week': return (base.replace(tzinfo=None)-__import__('datetime').timedelta(days=6)).strftime('%Y-%m-%d'),day
    if mode=='month': return base.replace(day=1).strftime('%Y-%m-%d'),day
    if mode=='wedthu':
        d=base.replace(tzinfo=None)
        while d.weekday()!=3: d-=__import__('datetime').timedelta(days=1)
        return d.strftime('%Y-%m-%d'),(d+__import__('datetime').timedelta(days=6)).strftime('%Y-%m-%d')
    days=[_r33_day(x) for x in records if _r33_day(x)]
    return (min(days),max(days)) if days else (day,day)


def _r33_fmt_day(day):
    try:return datetime.strptime(str(day)[:10],'%Y-%m-%d').strftime('%d.%m.%y')
    except Exception:return str(day or '')


def _r33_select_finance(body):
    cid=int(body.get('target_chat_id') or body.get('recipient_chat_id') or 0); store=_r33_load_store(cid)
    ftype=str(body.get('source_file_type') or body.get('file_type') or 'csv').lower()
    settings=store.get('settings') if isinstance(store.get('settings'),dict) else {}
    currency='ars' if ftype in {'xlsx','xlsxstat','excel'} else ('usd' if bool(settings.get('usd_transactions_view')) else 'ars')
    all_rows=_r33_ledger_rows(store,currency)
    if str(body.get('operation'))=='exact_export_query':
        start=str(body.get('start_key') or '')[:10]; end=str(body.get('end_key') or '')[:10]
        sr=int(body.get('start_rid') or 0); er=int(body.get('end_rid') or 0)
    else:
        start,end=_r33_bounds(body.get('mode'),body.get('day_key'),all_rows); sr=er=0
    selected=[]
    for rec in all_rows:
        d=_r33_day(rec); rid=int(rec.get('id') or 0)
        if d<start or d>end: continue
        if sr and d==start and rid<sr: continue
        if er and d==end and rid>er: continue
        selected.append(rec)
    opening=sum(_r33_f(x.get('_amount')) for x in all_rows if _r33_day(x)<start or (sr and _r33_day(x)==start and int(x.get('id') or 0)<sr))
    return store,currency,selected,opening,start,end


def _r33_excel_rows(selected,opening):
    rows=[['Дата','Описание','Приход','Расход'],['','Остаток с прошлого раза',opening,''],[]]
    income=expense=0.0; prev=None
    for rec in selected:
        day=_r33_day(rec)
        if prev is not None and day!=prev: rows.append([])
        prev=day; amount=_r33_f(rec.get('_amount')); note=str(rec.get('_note') or '')
        inc=amount if amount>=0 else ''; exp=abs(amount) if amount<0 else ''
        rows.append([_r33_fmt_day(day),note,inc,exp]); income+=max(0,amount); expense+=max(0,-amount)
    startrow=4; endrow=max(startrow,len(rows)); rows.append([])
    ir=len(rows)+1; rows.append(['','Приход за период',{'formula':f'SUM(C{startrow}:C{endrow})','value':income},''])
    er=len(rows)+1; rows.append(['','Расход за период','',{'formula':f'SUM(D{startrow}:D{endrow})','value':expense}])
    rows.append(['','Остаток на руках',{'formula':f'C2+C{ir}-D{er}','value':opening+income-expense},''])
    return rows


def _r33_csv_rows(selected):
    rows=[['date','amount','note']]; prev=None
    for rec in selected:
        day=_r33_day(rec)
        if prev is not None and day!=prev: rows.append(['','',''])
        prev=day; rows.append([_r33_fmt_day(day),_r33_f(rec.get('_amount')),str(rec.get('_note') or '')])
    return rows


def _r33_cat(note):
    text=str(note or '').casefold()
    mapping=[('Продукты',('продукт','еда','хлеб','мол','фрукт','овощ','мясо')),('Хоз общ',('хоз','салф','порош','клей','инструмент','батарей')),('Авто и (бус)',('авто','бенз','соляр','машин','шина','масло')),('орг. техника',('кабель','монитор','заряд','науш','принтер')),('Связь',('тел','связ','сим','интернет')),('переводы',('перевод','western','банков')),('Проживание',('прож','аренд','отель','дом')),('аптечка',('аптек','лекар','стомат'))]
    for name,words in mapping:
        if any(w in text for w in words): return name
    return 'прочие'


def _r33_tabl_rows(body):
    cid=int(body.get('target_chat_id') or body.get('recipient_chat_id') or 0); store=_r33_load_store(cid); recs=_r33_ledger_rows(store,'ars')
    ref=max([_r33_day(x) for x in recs if _r33_day(x)] or [datetime.now().strftime('%Y-%m-%d')]); d=datetime.strptime(ref,'%Y-%m-%d')
    while d.weekday()!=3: d-=__import__('datetime').timedelta(days=1)
    weeks=[]
    for i in range(3,-1,-1):
        st=d-__import__('datetime').timedelta(days=7*i); en=st+__import__('datetime').timedelta(days=6); weeks.append((st.strftime('%Y-%m-%d'),en.strftime('%Y-%m-%d')))
    cats=['Продукты','Хоз общ','Авто и (бус)','прочие','орг. техника','Еда доп и ШБ','Связь','переводы','Проживание','Хоз за ашр','аптечка']
    cols=['Дата','Приход/выдача','Откуда/кому']+cats; rows=[]
    settings=store.get('settings') if isinstance(store.get('settings'),dict) else {}
    reserve=_r33_f(settings.get('cash_reserve') or settings.get('reserve') or store.get('cash_reserve') or 0)
    for start_key,end_key in weeks:
        rows.append(['Неделя',f'{_r33_fmt_day(start_key)} — {_r33_fmt_day(end_key)}']); rows.append(cols)
        opening=sum(_r33_f(r.get('_amount')) for r in recs if _r33_day(r)<start_key)
        rows.append([_r33_fmt_day(start_key),opening,'Остаток с прошлого раза']+['']*len(cats))
        income=expense=0.0; totals={c:0.0 for c in cats}; cur=datetime.strptime(start_key,'%Y-%m-%d')
        for off in range(7):
            dk=(cur+__import__('datetime').timedelta(days=off)).strftime('%Y-%m-%d'); dayrecs=[r for r in recs if _r33_day(r)==dk]
            if not dayrecs: rows.append([_r33_fmt_day(dk)]+['']*(len(cols)-1)); continue
            first=True
            for rec in dayrecs:
                amount=_r33_f(rec.get('_amount')); note=str(rec.get('_note') or ''); row=[_r33_fmt_day(dk) if first else '','','']+['']*len(cats); first=False
                if amount>=0: income+=amount; row[1]=amount; row[2]=note
                else:
                    value=abs(amount); expense+=value; cat=_r35_record_category(rec,store)
                    if cat not in totals: cat='прочие'
                    totals[cat]+=value; row[3+cats.index(cat)]=value
                rows.append(row)
        closing=opening+income-expense; turnover=closing-reserve
        rows.extend([['Итог:',income,'']+[totals.get(c,0.0) or '' for c in cats],['расход:',expense]+['']*(len(cols)-2),['Остаток на руках',closing]+['']*(len(cols)-2),['Гомонковые',reserve]+['']*(len(cols)-2),['Остаток в обороте',turnover]+['']*(len(cols)-2),[],['Расход еды на человека в сутки','']+['']*(len(cols)-2),[]])
    usd=_r33_ledger_rows(store,'usd')
    if usd and weeks:
        st,en=weeks[0][0],weeks[-1][1]; selected=_r35_rows_between(usd,st,en); opening=sum(_r33_f(r.get('_amount')) for r in usd if _r33_day(r)<st)
        rows.extend([[],[]]+_r33_excel_rows(selected,opening))
    return rows

def _r33_db_checksum(path):
    con=sqlite3.connect(str(path)); h=hashlib.sha256()
    try:
        for table in ('kv','chats','meta','cold_fields'):
            cols=[x[1] for x in con.execute(f'PRAGMA table_info({table})').fetchall()]
            if not cols: continue
            order=','.join(cols[:2])
            for row in con.execute(f'SELECT * FROM {table} ORDER BY {order}').fetchall():
                if table=='meta' and len(row)>=2 and str(row[0])=='v153_export' and str(row[1])=='manifest': continue
                h.update(table.encode()); h.update(b'\0')
                for v in row: h.update(str(v).encode('utf-8','replace')); h.update(b'\0')
        return h.hexdigest()
    finally: con.close()


def _r33_sanitize(value):
    if isinstance(value,dict):
        out={}
        for k,v in value.items():
            low=str(k).lower()
            if any(x in low for x in ('password','token','api_key','secret_key','private_key')) and low not in {'secret_messages'}:
                out[str(k)]='***REDACTED***'
            else: out[str(k)]=_r33_sanitize(v)
        return out
    if isinstance(value,list): return [_r33_sanitize(x) for x in value]
    if isinstance(value,tuple): return [_r33_sanitize(x) for x in value]
    return value



def _r35_record_category(rec, store=None):
    rec=rec or {}; store=store or {}
    for key in ('category_override_name','expense_category','category_name','category'):
        val=str(rec.get(key) or '').strip()
        if val: return val
    slug=str(rec.get('category_override_slug') or '').strip()
    if slug:
        cats=(store.get('expense_categories') or store.get('categories') or {}) if isinstance(store,dict) else {}
        if isinstance(cats,dict):
            row=cats.get(slug) or {}
            if isinstance(row,dict) and row.get('name'): return str(row.get('name'))
    return _r33_cat(rec.get('_note') or rec.get('note') or '')


def _r35_category_rows(selected, opening, store, currency='ARS'):
    expenses=[]
    for rec in selected or []:
        if _r33_f((rec or {}).get('_amount'))<0:
            cat=_r35_record_category(rec,store)
            if cat not in expenses: expenses.append(cat)
    canonical=['Продукты','Хоз общ','Авто и (бус)','прочие','орг. техника','Еда доп и ШБ','Связь','переводы','Проживание','Хоз за ашр','аптечка']
    categories=[c for c in canonical if c in expenses]
    categories += [c for c in expenses if c not in categories]
    if not categories: categories=['прочие']
    rows=[[str(currency).upper()]+['']*(2+len(categories)),['Дата','Описание','Приход']+categories,['','Остаток с прошлого раза',opening]+['']*len(categories),[]]
    totals={c:0.0 for c in categories}; income=expense=0.0; prev=None
    for rec in selected or []:
        day=_r33_day(rec)
        if prev is not None and day!=prev: rows.append([])
        prev=day; amount=_r33_f(rec.get('_amount')); note=str(rec.get('_note') or '')
        row=[_r33_fmt_day(day),note,'']+['']*len(categories)
        if amount>=0:
            income+=amount; row[2]=amount
        else:
            value=abs(amount); expense+=value; cat=_r35_record_category(rec,store)
            if cat not in totals:
                cat='прочие' if 'прочие' in totals else categories[-1]
            totals[cat]=totals.get(cat,0.0)+value; row[3+categories.index(cat)]=value
        rows.append(row)
    closing=opening+income-expense
    settings=store.get('settings') if isinstance(store.get('settings'),dict) else {}
    reserve=0.0
    for key in ('cash_reserve','reserve','gomonkovie','гомонковые'):
        if key in settings: reserve=_r33_f(settings.get(key)); break
        if key in store: reserve=_r33_f(store.get(key)); break
    turnover=closing-min(reserve,max(0.0,closing))
    rows.extend([[],['','Сумма по статьям',income]+[totals.get(c,0.0) for c in categories],[],['','Расход',expense]+['']*len(categories),['','Приход',income]+['']*len(categories),['','Остаток на руках',closing]+['']*len(categories),['','Гомонковые',reserve]+['']*len(categories),['','Остаток в обороте',turnover]+['']*len(categories)])
    return rows


def _r35_rows_between(records,start,end,start_rid=0,end_rid=0):
    out=[]
    for rec in records or []:
        d=_r33_day(rec); rid=int((rec or {}).get('id') or 0)
        if d<start or d>end: continue
        if start_rid and d==start and rid<start_rid: continue
        if end_rid and d==end and rid>end_rid: continue
        out.append(rec)
    return out


def _r35_category_export_rows(store, selected, opening, start, end, body):
    ars=_r35_category_rows(selected,opening,store,'ARS')
    usd_all=_r33_ledger_rows(store,'usd')
    sr=int(body.get('start_rid') or 0) if str(body.get('operation'))=='exact_export_query' else 0
    er=int(body.get('end_rid') or 0) if str(body.get('operation'))=='exact_export_query' else 0
    usd=_r35_rows_between(usd_all,start,end,sr,er)
    if not usd: return ars
    usd_open=sum(_r33_f(x.get('_amount')) for x in usd_all if _r33_day(x)<start or (sr and _r33_day(x)==start and int(x.get('id') or 0)<sr))
    return ars+[[],[]]+_r33_excel_rows(usd,usd_open)


def _r35_filter_item_scope(item,tenant_id,chat_ids,key=''):
    if str(key).lstrip('-').isdigit() and int(str(key)) in chat_ids: return True
    if not isinstance(item,dict): return False
    for k in ('chat_id','target_chat_id','owner_chat_id','source_chat_id','recipient_chat_id','parent_first_chat_id'):
        try:
            if int(item.get(k) or 0) in chat_ids: return True
        except Exception: pass
    for k in ('tenant_id','tenant','space_id'):
        if str(item.get(k) or '')==str(tenant_id): return True
    return False


def _r35_filter_root_for_tenant(root,tenant_id,chat_ids):
    root=root if isinstance(root,dict) else {}; out={}
    gs=root.get('_global_settings') if isinstance(root.get('_global_settings'),dict) else {}
    safe_gs={}
    troot=gs.get('tenants_v148') if isinstance(gs.get('tenants_v148'),dict) else {}
    tenants=troot.get('tenants') if isinstance(troot.get('tenants'),dict) else {}
    if tenant_id:
        safe_gs['tenants_v148']={'schema_version':troot.get('schema_version',1),'tenants':{str(tenant_id):_r33_sanitize(tenants.get(str(tenant_id)) or {})},'chat_to_tenant':{str(cid):str(tenant_id) for cid in sorted(chat_ids)},'invite_tokens':{},'legacy_migrated':True}
    for gk,gv in gs.items():
        if gk=='tenants_v148': continue
        if isinstance(gv,list):
            rows=[_r33_sanitize(x) for x in gv if _r35_filter_item_scope(x,tenant_id,chat_ids)]
            if rows: safe_gs[gk]=rows
        elif isinstance(gv,dict):
            filtered={str(k):_r33_sanitize(v) for k,v in gv.items() if _r35_filter_item_scope(v,tenant_id,chat_ids,str(k))}
            if filtered: safe_gs[gk]=filtered
    out['_global_settings']=safe_gs
    for key,value in root.items():
        if key in {'_global_settings','chats','_restore_mode_chat_v150'}: continue
        if isinstance(value,list):
            rows=[_r33_sanitize(x) for x in value if _r35_filter_item_scope(x,tenant_id,chat_ids)]
            if rows: out[key]=rows
        elif isinstance(value,dict):
            filtered={str(k):_r33_sanitize(v) for k,v in value.items() if _r35_filter_item_scope(v,tenant_id,chat_ids,str(k))}
            if filtered: out[key]=filtered
    return out


def _r35_failed_tasks_snapshot(chat_ids=None):
    rows=[]; allowed=set(chat_ids or [])
    try:
        with FILE_JOB_LOCK:
            source=list(FILE_JOB_STATUS.items())
        for jid,row in source:
            if bool((row or {}).get('ok')): continue
            if str((row or {}).get('status') or '') not in {'done','failed','delivery_timeout'}: continue
            cid=int((row or {}).get('recipient_chat_id') or (row or {}).get('target_chat_id') or 0)
            if allowed and cid not in allowed: continue
            rows.append({'job_id':str(jid),'status':str((row or {}).get('status') or ''),'error':str((row or {}).get('error') or '')[:500],'chat_id':cid})
    except Exception as exc:
        rows.append({'load_error':f'{type(exc).__name__}: {str(exc)[:300]}'})
    return rows[:500]

def _r33_full_state(body,jid):
    src=_r33_cache_ready(); folder=FILE_DIR/f'{jid}_full'; folder.mkdir(parents=True,exist_ok=True); raw=folder/'state.sqlite3'
    srccon=sqlite3.connect(str(src),timeout=30); dst=sqlite3.connect(str(raw))
    try: srccon.backup(dst,pages=256,sleep=0.01)
    finally: dst.close(); srccon.close()
    con=sqlite3.connect(str(raw),timeout=30)
    try:
        scope=str(body.get('scope') or 'global'); chat_ids={int(x) for x in (body.get('tenant_chat_ids') or []) if str(x).lstrip('-').isdigit()}
        if scope=='tenant':
            if chat_ids:
                qs=','.join('?' for _ in chat_ids); vals=tuple(str(x) for x in chat_ids)
                con.execute(f'DELETE FROM chats WHERE chat_id NOT IN ({qs})',vals); con.execute(f'DELETE FROM cold_fields WHERE chat_id NOT IN ({qs})',vals)
            else: con.execute('DELETE FROM chats'); con.execute('DELETE FROM cold_fields')
            try:
                rr=con.execute("SELECT v FROM kv WHERE k='root'").fetchone()
                if rr:
                    filtered=_r35_filter_root_for_tenant(_r33_json_load(rr[0],{}),str(body.get('tenant_id') or ''),chat_ids)
                    con.execute("UPDATE kv SET v=? WHERE k='root'",(json.dumps(filtered,ensure_ascii=False,separators=(',',':'),default=str),))
            except Exception as exc:
                raise RuntimeError('tenant kv.root filtering failed: '+str(exc)[:300])
        # sanitize JSON-bearing rows on HEAVY, not FAST
        for table,keycols in (('kv',('k',)),('chats',('chat_id',)),('meta',('kind','k')),('cold_fields',('chat_id','k'))):
            cols=','.join(keycols+('v',))
            try: rows=con.execute(f'SELECT {cols} FROM {table}').fetchall()
            except sqlite3.OperationalError: continue
            for row in rows:
                keys=row[:-1]; val=row[-1]
                try: safe=json.dumps(_r33_sanitize(json.loads(val)),ensure_ascii=False,separators=(',',':'),default=str)
                except Exception: safe=str(val)
                where=' AND '.join(f'{k}=?' for k in keycols); con.execute(f'UPDATE {table} SET v=? WHERE {where}',(safe,*keys))
        con.execute("CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL,k TEXT NOT NULL,v TEXT NOT NULL,PRIMARY KEY(kind,k))")
        count=con.execute('SELECT COUNT(*) FROM chats').fetchone()[0]
        failed=_r35_failed_tasks_snapshot(chat_ids if scope=='tenant' else None)
        manifest={'kind':'telegram_bot_full_state_v153','schema_version':1,'bot_version':'bot_v153_PER_R36_HEAVY','created_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),'scope':scope,'tenant_id':str(body.get('tenant_id') or ''),'chat_ids':sorted(chat_ids) if scope=='tenant' else [],'chat_count':int(count),'failed_tasks':len(failed),'checksum':''}
        con.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','failed_tasks',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v",(json.dumps(failed,ensure_ascii=False,separators=(',',':'),default=str),))
        con.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','manifest',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v",(json.dumps(manifest,ensure_ascii=False,separators=(',',':')),)); con.commit()
    finally: con.close()
    checksum=_r33_db_checksum(raw); con=sqlite3.connect(str(raw));
    try:
        m=json.loads(con.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()[0]); m['checksum']=checksum
        con.execute("UPDATE meta SET v=? WHERE kind='v153_export' AND k='manifest'",(json.dumps(m,ensure_ascii=False,separators=(',',':')),)); con.commit()
    finally: con.close()
    gz=FILE_DIR/f'{jid}.sqlite3.gz'
    with open(raw,'rb') as fin,gzip.open(gz,'wb',compresslevel=4) as fout: shutil.copyfileobj(fin,fout,1024*1024)
    shutil.rmtree(folder,ignore_errors=True)
    return gz,'latest_bot_state.sqlite3.gz'


def _r33_sqlite(body,jid):
    src=_r33_cache_ready(); path=FILE_DIR/f'{jid}.sqlite3'; sc=sqlite3.connect(str(src),timeout=30); dc=sqlite3.connect(str(path))
    try: sc.backup(dc,pages=512,sleep=0.005)
    finally: dc.close(); sc.close()
    return path,'bot_state.sqlite3'


def _r33_chat_json(body,jid):
    cid=int(body.get('target_chat_id') or body.get('recipient_chat_id') or 0); store=_r33_load_store(cid)
    obj={'schema':'per-r35-chat-state','created_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),'chat_id':cid,'store':_r33_sanitize(store)}
    path=FILE_DIR/f'{jid}.json'; path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return path,f'chat_{cid}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'


def _r33_window_doc(body,jid):
    root=_r33_load_root(); gs=root.get('_global_settings') if isinstance(root.get('_global_settings'),dict) else {}
    catalog=gs.get('_window_marker_catalog_v160') if isinstance(gs.get('_window_marker_catalog_v160'),dict) else {}
    tz=gs.get('_window_tz_v160') if isinstance(gs.get('_window_tz_v160'),list) else []
    op=str(body.get('operation') or '')
    lines=[f'Пер-R43 HEAVY export · {op}',f'Создано: {datetime.now(timezone.utc).isoformat(timespec="seconds")}', '']
    if op=='window_markers':
        for marker,row in sorted(catalog.items()):
            rr=row if isinstance(row,dict) else {}; lines.extend([f'{marker} — {rr.get("name") or "без имени"}',f'Последнее изменение: {rr.get("last_named_at") or "—"}','---'])
        name='Маркировки_окон.txt'
    else:
        archive=op=='window_tz_archive'; selected=[]
        for row in tz:
            if not isinstance(row,dict): continue
            status=str(row.get('status') or 'open').lower()
            if (archive and status in {'archived','fixed'}) or ((not archive) and status=='open'): selected.append(row)
        for row in selected:
            src=row.get('source') if isinstance(row.get('source'),dict) else {}; marker=str(row.get('marker') or '')
            lines.extend([f'[{row.get("at")}] {marker} — {row.get("window_name") or (catalog.get(marker) or {}).get("name") or "без имени"}',f'Статус: {row.get("status") or "open"}',f'Источник: chat={src.get("chat_id") or "—"} msg={src.get("message_id") or "—"} callback={src.get("callback") or "—"}',str(row.get('text') or ''),'','---',''])
        name='Архив_ТЗ_окон.txt' if archive else 'ТЗ_окон.txt'
    path=FILE_DIR/f'{jid}.txt'; path.write_text('\n'.join(lines)+'\n',encoding='utf-8'); return path,name


def _r33_mega_find(pattern,limit=400):
    try:
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok:return []
            res=run_cmd(['mega-find',mega_root(),'--pattern='+pattern,'--type=f'],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if res.returncode!=0:return []
            return sorted({x.strip() for x in (res.stdout or '').splitlines() if x.strip()})[-max(1,int(limit)):]
    except Exception:return []


def _r33_journal(body,jid,current=False):
    """Build full or current-deploy journal from durable MEGA chunks.

    R51 current journal is scoped by the actual FAST Render instance (or commit as a
    fallback), never by a hard-coded historical release marker such as R43.
    """
    limit=max(100,min(20000,int(body.get('limit') or 5000)))
    paths=_r33_mega_find('journal_*.json.gz',min(300,limit))
    out=FILE_DIR/f'{jid}.txt'
    snap=body.get('front_runtime_snapshot') if isinstance(body.get('front_runtime_snapshot'),dict) else {}
    current_instance=str(snap.get('render_instance_id') or '')
    current_commit=str(snap.get('render_git_commit') or '')
    current_bot_version=str(snap.get('bot_version') or '')
    lines=[('ЖУРНАЛ ТЕКУЩЕГО ДЕПЛОЯ' if current else 'МАКСИМАЛЬНЫЙ ЖУРНАЛ'),
           f'Создано: {datetime.now(timezone.utc).isoformat(timespec="seconds")}',
           f'MEGA файлов: {len(paths)}','']
    if snap:
        lines.extend(['--- FAST Render #1 snapshot ---',json.dumps(snap,ensure_ascii=False,indent=2,default=str),'--- end FAST snapshot ---',''])
    work=Path(tempfile.mkdtemp(prefix='r51_journal_'))
    matched_chunks=0
    try:
        with MEGA_LOCK:
            for remote in paths:
                if len(lines)>=limit+4: break
                d=work/secrets.token_hex(4); d.mkdir(parents=True,exist_ok=True)
                g=run_cmd(['mega-get',remote,str(d)],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                if g.returncode!=0: continue
                for f in d.rglob('*.gz'):
                    try:
                        doc=json.loads(gzip.decompress(f.read_bytes()).decode('utf-8','replace'))
                        if not isinstance(doc,dict): continue
                        if current:
                            doc_instance=str(doc.get('render_instance_id') or '')
                            doc_commit=str(doc.get('render_git_commit') or '')
                            doc_version=str(doc.get('bot_version') or '')
                            if current_instance:
                                if doc_instance!=current_instance: continue
                            elif current_commit:
                                if doc_commit!=current_commit: continue
                            elif current_bot_version:
                                if doc_version!=current_bot_version: continue
                        rows=doc.get('rows') if isinstance(doc.get('rows'),list) else []
                        if not rows: continue
                        matched_chunks+=1
                        for row in rows:
                            if not isinstance(row,dict): continue
                            lines.append(json.dumps(row,ensure_ascii=False,separators=(',',':'),default=str))
                            if len(lines)>=limit+4: break
                    except Exception:
                        pass
        if current and matched_chunks==0:
            lines.append('В MEGA ещё нет журнальных строк текущего Render-деплоя.')
        out.write_text('\n'.join(lines)+'\n',encoding='utf-8')
        return out,('Журнал_текущего_деплоя.txt' if current else 'Журнал_бота.txt')
    finally:
        shutil.rmtree(work,ignore_errors=True)


def _r33_runtime_zip(body,jid):
    path=FILE_DIR/f'{jid}.zip'; paths=_r33_mega_find('runtime_*.json',180)
    import zipfile
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
        with STATE_LOCK: st=dict(STATE)
        z.writestr('heavy_status.json',json.dumps(st,ensure_ascii=False,indent=2,default=str))
        snap=body.get('front_runtime_snapshot') if isinstance(body.get('front_runtime_snapshot'),dict) else {}
        z.writestr('fast_render1_snapshot.json',json.dumps(snap,ensure_ascii=False,indent=2,default=str))
        z.writestr('r34_manifest.txt',f'Пер-R43 HEAVY runtime export\ncreated={datetime.now(timezone.utc).isoformat(timespec="seconds")}\nindexed={len(paths)}\n')
        z.writestr('mega_runtime_index.txt','\n'.join(paths)+'\n')
        work=Path(tempfile.mkdtemp(prefix='r33_runtime_'))
        try:
            with MEGA_LOCK:
                for idx,remote in enumerate(paths[-120:]):
                    d=work/f'{idx:03d}'; d.mkdir(parents=True,exist_ok=True); g=run_cmd(['mega-get',remote,str(d)],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                    if g.returncode!=0: continue
                    for f in d.rglob('*.json'):
                        try:z.write(f,arcname='runtime/'+f'{idx:03d}_{f.name}')
                        except Exception:pass
        finally: shutil.rmtree(work,ignore_errors=True)
    return path,'Runtime_Watcher_Пер-R43.zip'


def _r33_bot_source(body,jid):
    raise RuntimeError('R43 bot_source is served locally by FAST; no 4.5MB front-source copy is stored on HEAVY')


def _r34_current_applied_revision(refresh=False):
    with STATE_LOCK:
        cached=int(STATE.get('r34_max_applied_revision') or 0)
    if cached and not refresh: return cached
    try:
        _r33_cache_ready()
        con=sqlite3.connect(str(CACHE_DB),timeout=5)
        try:
            row=con.execute('SELECT MAX(revision) FROM r32_state_revisions').fetchone()
            val=int((row or [0])[0] or 0)
        finally: con.close()
        with STATE_LOCK: STATE['r34_max_applied_revision']=max(int(STATE.get('r34_max_applied_revision') or 0),val)
        return val
    except Exception:
        return cached


def _r34_wait_revision(required, timeout=None):
    required=int(required or 0)
    if required<=0: return True,0
    timeout=float(timeout if timeout is not None else env_int('WORKER_R34_EXPORT_REVISION_WAIT_SEC',180,5,900))
    deadline=time.time()+max(1.0,timeout)
    while time.time()<deadline:
        cur=_r34_current_applied_revision(refresh=True)
        if cur>=required: return True,cur
        # Redis replay can close a transient local apply gap without contacting FAST.
        try: _r32_replay_state_events_from_redis(limit=50000)
        except Exception: pass
        time.sleep(0.4)
    return False,_r34_current_applied_revision(refresh=True)


def _r34_state_dependent_operation(op):
    return str(op or '') in {'period_export_query','exact_export_query','tabl_lsx','chat_json','full_state','sqlite','window_markers','window_tz','window_tz_archive','google_period_query','google_exact_query','google_tabl_query'}

def _r33_prepare_file(body,jid):
    op=str(body.get('operation') or '')
    if op in {'period_export_query','exact_export_query'}:
        store,currency,selected,opening,start,end=_r33_select_finance(body); ftype=str(body.get('source_file_type') or body.get('file_type') or 'csv').lower()
        cid=int(body.get('target_chat_id') or body.get('recipient_chat_id') or 0)
        body=dict(body)
        if ftype=='xlsxstat':
            body['rows']=_r35_category_export_rows(store,selected,opening,start,end,body); body['sheet_name']='Статьи'; body['layout']='category'; body['category_layout']=True
        elif ftype in {'xlsx','excel'}:
            body['rows']=_r33_excel_rows(selected,opening); body['sheet_name']='Экспорт'; body['layout']='simple'
        else:
            body['rows']=_r33_csv_rows(selected); body['sheet_name']='Экспорт'; body['layout']='simple'
        body['file_type']='xlsx' if ftype in {'xlsx','xlsxstat','excel'} else 'csv'
        body['filename']=f'chat_{cid}_{start}_{end}.{body["file_type"]}'
        body['caption']=f'📂 {"Excel" if body["file_type"]=="xlsx" else "CSV"} · {currency.upper()} · {start}—{end}'
        return _render_export_file(body,jid),body
    if op=='tabl_lsx':
        b=dict(body); b['rows']=_r33_tabl_rows(body); b['file_type']='xlsx'; b['sheet_name']='4 недели'; b['filename']=f'tabl_lsx_{int(body.get("target_chat_id") or 0)}.xlsx'; b['caption']='📊 Excel /tabl_lsx · 4 недели'; return _render_export_file(b,jid),b
    if op=='chat_json': return _r33_chat_json(body,jid),body
    if op=='full_state': return _r33_full_state(body,jid),body
    if op=='sqlite': return _r33_sqlite(body,jid),body
    if op=='runtime_zip': return _r33_runtime_zip(body,jid),body
    if op=='journal': return _r33_journal(body,jid,False),body
    if op=='journal_current': return _r33_journal(body,jid,True),body
    if op=='bot_source': return _r33_bot_source(body,jid),body
    if op in {'window_markers','window_tz','window_tz_archive'}: return _r33_window_doc(body,jid),body
    return _render_export_file(body,jid),body


def _r33_process_file_job(job):
    jid=str(job.get('id') or ''); body=dict(job.get('payload') or {}); _file_status_put(jid,status='running')
    try:
        required=int(body.get('required_revision') or 0)
        if required and _r34_state_dependent_operation(body.get('operation')):
            ok_rev,cur_rev=_r34_wait_revision(required)
            if not ok_rev: raise RuntimeError(f'HEAVY state revision behind required={required} applied={cur_rev}')
        prepared,body2=_r33_prepare_file(body,jid); path,filename=prepared
        delivery=str(body2.get('delivery') or 'chat'); url=''
        if delivery=='drive': url=_drive_upload_file(path,filename,str(body2.get('drive_folder_id') or ''))
        elif delivery=='google':
            gb=dict(body2); gb['title']=str(body2.get('title') or body2.get('label') or filename)[:95]; gb['spreadsheet_id']=str(body2.get('spreadsheet_id') or '')
            if not gb['spreadsheet_id']: raise RuntimeError('Google spreadsheet_id missing in R34 job')
            url=create_google_sheet(gb)
        with STATE_LOCK:
            STATE['file_jobs']=int(STATE.get('file_jobs') or 0)+1; STATE['file_last_ok']=time.time(); STATE['file_last_error']=''; STATE['r33_heavy_exports']=int(STATE.get('r33_heavy_exports') or 0)+1; STATE['r33_last_export']=str(body2.get('operation') or body2.get('file_type') or '')
        _file_status_put(jid,status='done',ok=True,path=str(path),filename=filename,url=url,delivery=delivery)
        # notify needs body fields; preserve prepared caption/filename.
        job2=dict(job); job2['payload']=body2; job2['payload']['filename']=filename; job2['payload']['caption']=str(body2.get('caption') or '')
        delivered=_notify_front_export_result(job2,True,url=url,filename=filename); _file_status_put(jid,callback_delivered=bool(delivered))
        if delivery in {'drive','google'}: path.unlink(missing_ok=True)
        print(f'[R45 HEAVY EXPORT] {jid} op={body2.get("operation")} ok=True delivery={delivery} callback={delivered}',flush=True)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:700]}'
        with STATE_LOCK:
            STATE['file_failures']=int(STATE.get('file_failures') or 0)+1; STATE['file_last_error']=detail[:240]; STATE['r33_heavy_export_failures']=int(STATE.get('r33_heavy_export_failures') or 0)+1
        _file_status_put(jid,status='done',ok=False,error=detail); _notify_front_export_result(job,False,error=detail)
        print(f'[R45 HEAVY EXPORT] {jid} ok=False {detail}',flush=True)

# file_loop resolves this global at execution time.


def _r33_archive_events_direct(events):
    """Durably archive one immutable state-event batch to MEGA.

    Filename is deterministic, so a retry after a lost HTTP response does not
    create duplicate remote objects. This is the Redis-free durability path.
    """
    if not events:
        return True,'no events',0
    packed=gzip.compress(json.dumps({'schema':32,'created_at':time.time(),'events':events},ensure_ascii=False,separators=(',',':'),default=str).encode('utf-8'),compresslevel=3)
    revisions=[int((x or {}).get('revision') or 0) for x in events]
    min_rev=min(revisions or [0]); max_rev=max(revisions or [0])
    ids='|'.join(str((x or {}).get('event_id') or '') for x in events)
    digest=hashlib.sha256((ids+'|'+str(min_rev)+'|'+str(max_rev)).encode('utf-8')).hexdigest()[:16]
    event_ts=max([float((x or {}).get('created_at') or 0.0) for x in events] or [time.time()]) or time.time()
    day=datetime.fromtimestamp(event_ts,timezone.utc).strftime('%Y%m%d')
    name=f'events_{min_rev:012d}_{max_rev:012d}_{digest}.json.gz'
    work=Path(tempfile.mkdtemp(prefix='r49_event_direct_')); local=work/name; local.write_bytes(packed)
    try:
        with MEGA_LOCK:
            ok,detail=prepare_mega_layout()
            if not ok:return False,detail,0
            root=_r32_events_mega_dir(); remote_day=root.rstrip('/')+'/'+day; remote=remote_day.rstrip('/')+'/'+name
            if not ensure_mega_dir(root) or not ensure_mega_dir(remote_day): return False,'cannot create R49 event MEGA dir',0
            if mega_exists(remote):
                return True,f'MEGA event batch already durable events={len(events)}',len(packed)
            put=run_cmd(['mega-put',str(local),remote_day],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if put.returncode!=0:return False,'mega-put direct events failed: '+(put.stderr or put.stdout or '')[:180],0
            if not mega_exists(remote): return False,'MEGA event batch verify failed',0
        with STATE_LOCK:
            STATE['r33_direct_mega_events']=int(STATE.get('r33_direct_mega_events') or 0)+len(events)
            STATE['r33_direct_mega_event_bytes']=int(STATE.get('r33_direct_mega_event_bytes') or 0)+len(packed)
            STATE['r33_last_event_durability']='mega-direct'
        return True,f'MEGA direct events={len(events)} bytes={len(packed)} rev={min_rev}-{max_rev}',len(packed)
    finally:
        shutil.rmtree(work,ignore_errors=True)


def _r34_process_state_event_wire(wire,max_wire):
    if not wire or len(wire)>int(max_wire): return {'ok':False,'error':f'R34 event batch size invalid bytes={len(wire)} max={int(max_wire)}'},413
    try:
        raw=gzip.decompress(wire) if str(request.headers.get('Content-Encoding') or '').lower()=='gzip' else wire
        body=json.loads(raw.decode('utf-8')); events=body.get('events') if isinstance(body,dict) else None
        if not isinstance(events,list) or not events or len(events)>512: raise ValueError('events invalid')
        events=[x for x in events if _r32_event_valid(x)]
        if not events: raise ValueError('no valid events')
    except Exception as exc: return {'ok':False,'error':f'R34 event decode: {type(exc).__name__}: {str(exc)[:180]}'},400
    # R63 shared-Redis durability. When Redis runtime is ON, every acknowledged
    # logical state batch is stored there first, even in direct R43 mode.  MEGA is
    # then only an archival/fallback backend, so FAST no longer waits on MEGA for
    # normal changes.  When Redis is intentionally OFF, preserve the strict MEGA
    # direct path (or local-only mode if MEGA is also OFF).
    redis_active = _redis_client() is not None
    new_ids=[]
    if redis_active:
        rok,rdetail,new_ids=_r32_redis_store_events(events)
        if not rok:
            with STATE_LOCK: STATE['r32_state_last_error']='Redis durability failed: '+str(rdetail)[:180]
            return {'ok':False,'error':'Redis state durability unavailable: '+str(rdetail)[:180]},503
        durable='redis'
        if mega_enabled() and new_ids:
            _R32_MEGA_WAKE.set()
    elif _R43_DIRECT_HEAVY:
        if mega_enabled():
            mok,mdetail,_bytes=_r33_archive_events_direct(events)
            if not mok:
                with STATE_LOCK: STATE['r32_state_last_error']=str(mdetail)[:220]
                return {'ok':False,'error':'MEGA event durability unavailable: '+str(mdetail)[:180]},503
            durable='mega-direct'
        else:
            durable='local-only-redis-mega-disabled'
    else:
        durable=''; rok,rdetail,new_ids=_r32_redis_store_events(events)
        if rok:
            durable='redis'
            if new_ids: _R32_MEGA_WAKE.set()
        else:
            mok,mdetail,_bytes=_r33_archive_events_direct(events)
            if not mok:
                with STATE_LOCK: STATE['r32_state_last_error']=f'Redis={rdetail}; MEGA={mdetail}'[:220]
                return {'ok':False,'error':'R34 durability unavailable: '+str(rdetail)[:80]+'; '+str(mdetail)[:100]},503
            durable='mega-direct'
    try:
        if _R43_DIRECT_HEAVY:
            with _R43_SNAPSHOT_LOCK:
                ok,detail,applied,stale=_r32_apply_events(events)
        else:
            ok,detail,applied,stale=_r32_apply_events(events)
    except Exception as exc: ok=False; detail=f'{type(exc).__name__}: {str(exc)[:220]}'; applied=stale=0
    max_rev=max([int(x.get('revision') or 0) for x in events] or [0])
    with STATE_LOCK:
        STATE['r32_state_events_received']=int(STATE.get('r32_state_events_received') or 0)+len(events)
        STATE['r32_state_event_bytes']=int(STATE.get('r32_state_event_bytes') or 0)+len(wire)
        STATE['r33_last_event_durability']=durable
        if ok: STATE['r34_max_applied_revision']=max(int(STATE.get('r34_max_applied_revision') or 0),max_rev)
        else: STATE['r32_state_last_error']='durable but apply pending: '+str(detail)[:180]
    try: applied_revision=_r34_current_applied_revision(refresh=True)
    except Exception: applied_revision=(max_rev if ok else int(STATE.get('r34_max_applied_revision') or 0))
    return {'ok':True,'durable':durable,'events':len(events),'new':len(new_ids),'applied':applied,'stale':stale,'apply_ok':bool(ok),'apply_detail':str(detail)[:180],'max_revision':max_rev,'durable_revision':max_rev,'applied_revision':int(applied_revision or 0)},200


def _r33_state_events_view():
    if not authorized(): return {'ok':False},404
    wire=request.get_data(cache=False,as_text=False) or b''
    max_wire=env_int('WORKER_R32_EVENT_MAX_WIRE_KB',8192,32,32768)*1024
    return _r34_process_state_event_wire(wire,max_wire)

@app.route('/internal/state/event-large',methods=['POST'])
def internal_r34_state_event_large():
    if not authorized(): return {'ok':False},404
    wire=request.get_data(cache=False,as_text=False) or b''
    max_wire=env_int('WORKER_R34_EVENT_LARGE_MAX_MB',64,8,128)*1024*1024
    return _r34_process_state_event_wire(wire,max_wire)

# Replace Flask endpoint without registering a duplicate route.
app.view_functions['internal_r32_state_events']=_r33_state_events_view



# ---------------------------------------------------------------------------
# Пер-R43 durable file transport. Redis is the durable job ledger; FILE_Q is only
# an execution cache. A worker restart rehydrates unfinished jobs from Redis.
_R35_JOB_PENDING_KEY='per:r35:heavy:filejobs:pending'
_R35_JOB_PREFIX='per:r35:heavy:filejob:'
_R35_ENQUEUED=set(); _R35_ENQUEUED_LOCK=threading.RLock()
_R35_RESULT_Q=queue.Queue(maxsize=64); _R35_RESULT_ENQUEUED=set(); _R35_RESULT_LOCK=threading.RLock()

# R36 local + MEGA fallback spool. Redis remains preferred, but a missing REDIS_URL
# must never downgrade an accepted HEAVY export to RAM-only durability.
_R36_JOB_DB = CACHE_DIR / 'r36_file_jobs.sqlite3'
_R36_JOB_DB_LOCK = threading.RLock()
_R36_ADMISSION_LOCK = threading.RLock()
_R36_MEGA_JOB_DIR = mega_root().rstrip('/') + '/r36_file_jobs_pending'
_R36_MEGA_LAST_SCAN = {'at':0.0,'error':''}

def _r36_job_db_init():
    with _R36_JOB_DB_LOCK:
        conn=sqlite3.connect(str(_R36_JOB_DB),timeout=5,check_same_thread=False)
        try:
            conn.execute('PRAGMA journal_mode=WAL'); conn.execute('PRAGMA synchronous=FULL'); conn.execute('PRAGMA busy_timeout=5000')
            conn.execute('CREATE TABLE IF NOT EXISTS jobs(job_id TEXT PRIMARY KEY,row_json TEXT NOT NULL,status TEXT NOT NULL,updated_at REAL NOT NULL)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status,updated_at)')
            conn.commit()
        finally: conn.close()

def _r36_local_job_put(jid,row):
    try:
        _r36_job_db_init(); obj=dict(row or {}); obj['job_id']=str(jid); obj['updated_at']=time.time()
        raw=json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str); status=str(obj.get('status') or 'queued')
        with _R36_JOB_DB_LOCK:
            conn=sqlite3.connect(str(_R36_JOB_DB),timeout=5,check_same_thread=False)
            try:
                conn.execute('PRAGMA busy_timeout=5000')
                conn.execute('INSERT INTO jobs(job_id,row_json,status,updated_at) VALUES(?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET row_json=excluded.row_json,status=excluded.status,updated_at=excluded.updated_at',(str(jid),raw,status,time.time()))
                conn.commit(); return True
            finally: conn.close()
    except Exception as exc:
        with STATE_LOCK: STATE['file_last_error']=f'R36 local job spool {type(exc).__name__}: {str(exc)[:160]}'
        return False

def _r36_local_job_get(jid):
    try:
        _r36_job_db_init()
        with _R36_JOB_DB_LOCK:
            conn=sqlite3.connect(str(_R36_JOB_DB),timeout=2,check_same_thread=False)
            try: row=conn.execute('SELECT row_json FROM jobs WHERE job_id=?',(str(jid),)).fetchone()
            finally: conn.close()
        if not row:return {}
        obj=json.loads(row[0]); return obj if isinstance(obj,dict) else {}
    except Exception:return {}

def _r36_local_pending(limit=200):
    out=[]
    try:
        _r36_job_db_init()
        with _R36_JOB_DB_LOCK:
            conn=sqlite3.connect(str(_R36_JOB_DB),timeout=2,check_same_thread=False)
            try: rows=conn.execute("SELECT row_json FROM jobs WHERE status NOT IN ('delivered','closed') ORDER BY updated_at ASC LIMIT ?",(max(1,int(limit)),)).fetchall()
            finally: conn.close()
        for row in rows:
            try:
                obj=json.loads(row[0])
                if isinstance(obj,dict):out.append(obj)
            except Exception:pass
    except Exception:pass
    return out

def _r36_mega_job_path(jid): return _R36_MEGA_JOB_DIR.rstrip('/') + '/job_' + re.sub(r'[^A-Za-z0-9_.-]+','_',str(jid or ''))[:90] + '.json'

def _r36_mega_job_put(jid,row):
    """Synchronous admission witness used only when Redis is unavailable."""
    local=None
    try:
        with MEGA_LOCK:
            ok,detail=mega_login()
            if not ok:return False,detail
            if not ensure_mega_dir(mega_root()) or not ensure_mega_dir(_R36_MEGA_JOB_DIR): return False,'cannot create R36 MEGA job dir'
            work=Path(tempfile.mkdtemp(prefix='r36_job_spool_'))
            local=work/Path(_r36_mega_job_path(jid)).name
            obj=dict(row or {}); obj['job_id']=str(jid); obj['r36_spooled_at']=time.time()
            local.write_text(json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str),encoding='utf-8')
            remote=_r36_mega_job_path(jid)
            try: run_cmd(['mega-rm',remote],timeout=30)
            except Exception: pass
            put=run_cmd(['mega-put',str(local),_R36_MEGA_JOB_DIR],timeout=env_int('R36_MEGA_JOB_TIMEOUT',180,30,600))
            if put.returncode!=0:return False,(put.stderr or put.stdout or 'mega-put failed')[:220]
            if not mega_exists(remote): return False,'MEGA durable job verify failed'
            return True,'MEGA durable job stored'
    except Exception as exc:return False,f'{type(exc).__name__}: {str(exc)[:220]}'
    finally:
        try:
            if local is not None:
                parent=local.parent
                local.unlink(missing_ok=True)
                if parent.name.startswith('r36_job_spool_'): shutil.rmtree(parent,ignore_errors=True)
        except Exception:pass

def _r36_mega_job_delete(jid):
    try:
        with MEGA_LOCK:
            if not mega_login()[0]: return False
            p=run_cmd(['mega-rm',_r36_mega_job_path(jid)],timeout=45)
            return p.returncode==0 or not mega_exists(_r36_mega_job_path(jid))
    except Exception:return False

def _r36_mega_pending_rows(limit=100):
    out=[]; work=None
    try:
        with MEGA_LOCK:
            ok,detail=mega_login()
            if not ok: _R36_MEGA_LAST_SCAN.update(at=time.time(),error=detail); return out
            if not ensure_mega_dir(_R36_MEGA_JOB_DIR): return out
            res=run_cmd(['mega-find',_R36_MEGA_JOB_DIR,'--pattern=job_*.json','--type=f'],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if res.returncode!=0:return out
            paths=[x.strip() for x in (res.stdout or '').splitlines() if x.strip()][:max(1,int(limit))]
            work=Path(tempfile.mkdtemp(prefix='r36_jobs_recover_'))
            for idx,remote in enumerate(paths):
                d=work/f'{idx:03d}'; d.mkdir(parents=True,exist_ok=True)
                g=run_cmd(['mega-get',remote,str(d)],timeout=env_int('R36_MEGA_JOB_TIMEOUT',180,30,600))
                if g.returncode!=0:continue
                files=list(d.glob('*.json'))
                if not files:continue
                try:
                    obj=json.loads(files[0].read_text(encoding='utf-8'))
                    if isinstance(obj,dict) and obj.get('job_id'):out.append(obj)
                except Exception:pass
        _R36_MEGA_LAST_SCAN.update(at=time.time(),error=''); return out
    except Exception as exc:
        _R36_MEGA_LAST_SCAN.update(at=time.time(),error=f'{type(exc).__name__}: {str(exc)[:180]}'); return out
    finally:
        try:
            if work is not None: shutil.rmtree(work,ignore_errors=True)
        except Exception:pass

def _r35_job_key(jid): return _R35_JOB_PREFIX+str(jid or '')[:80]

def _r35_job_get(jid):
    # R36: merge process-local spool and Redis by freshness. Local SQLite survives
    # ordinary process restarts; Redis/MEGA cover container replacement.
    best=_r36_local_job_get(jid)
    c=_redis_client()
    try:
        raw=c.get(_r35_job_key(jid)) if c is not None else None
        if raw:
            obj=json.loads(raw.decode('utf-8') if isinstance(raw,(bytes,bytearray)) else raw)
            if isinstance(obj,dict) and float(obj.get('updated_at') or 0)>=float((best or {}).get('updated_at') or 0): best=obj
    except Exception:pass
    return best if isinstance(best,dict) else {}

def _r35_job_put(jid,row,pending=True):
    row=dict(row or {}); row['job_id']=str(jid); row['updated_at']=time.time()
    _r36_local_job_put(jid,row)
    c=_redis_client()
    if c is None:return False
    try:
        pipe=c.pipeline(transaction=False); pipe.set(_r35_job_key(jid),json.dumps(row,ensure_ascii=False,separators=(',',':'),default=str),ex=259200)
        if pending: pipe.sadd(_R35_JOB_PENDING_KEY,str(jid))
        else: pipe.srem(_R35_JOB_PENDING_KEY,str(jid))
        pipe.execute(); return True
    except Exception:return False

def _file_status_put(job_id,**fields):
    _file_status_put_memory(job_id,**fields)
    try:
        old=_r35_job_get(job_id); old.update(fields); pending=str(fields.get('status') or old.get('status') or '') not in {'delivered','closed'}
        _r35_job_put(job_id,old,pending=pending)
    except Exception: pass

def _r35_enqueue_file(job):
    jid=str((job or {}).get('id') or '')
    if not jid:return False
    with _R35_ENQUEUED_LOCK:
        if jid in _R35_ENQUEUED:return True
        try: FILE_Q.put_nowait(job); _R35_ENQUEUED.add(jid); return True
        except queue.Full:return False

def _r35_enqueue_result(task):
    jid=str((task or {}).get('job',{}).get('id') or '')
    if not jid:return False
    with _R35_RESULT_LOCK:
        if jid in _R35_RESULT_ENQUEUED:return True
        try: _R35_RESULT_Q.put_nowait(task); _R35_RESULT_ENQUEUED.add(jid); return True
        except queue.Full:return False

def _internal_export_file_durable():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    try: cid=int(body.get('recipient_chat_id') or 0)
    except Exception: cid=0
    if not cid:return {'ok':False,'error':'recipient_chat_id required'},400
    rows=body.get('rows') or []
    if not isinstance(rows,list) or len(rows)>100000:return {'ok':False,'error':'invalid/too many rows'},400
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]
    body['job_id']=jid
    with _R36_ADMISSION_LOCK:
        with FILE_JOB_LOCK: mem=dict(FILE_JOB_STATUS.get(jid) or {})
        existing=mem or _r35_job_get(jid)
        if existing:
            status=str(existing.get('status') or 'queued'); ok=existing.get('ok'); backend=str(existing.get('durable_backend') or '')
            if backend and status in {'queued','running','ready','delivering','delivered','done'}:
                job_existing=existing.get('job') if isinstance(existing.get('job'),dict) else None
                if job_existing and status in {'queued','running'}: _r35_enqueue_file(job_existing)
                return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'durable':True,'durable_backend':backend},200 if status in {'delivered','done'} and ok is True else 202
            if ok is False and status in {'failed','closed'} and backend:
                return {'ok':False,'duplicate':True,'status':status,'job_id':jid,'error':str(existing.get('error') or 'HEAVY job failed')[:500],'durable':True,'durable_backend':backend},200
        job={'id':jid,'type':'file_export','created_at':time.time(),'payload':body}
        rec={'status':'admitting','ok':None,'job':job,'recipient_chat_id':cid,'target_chat_id':body.get('target_chat_id'),'durable_backend':''}
        _r36_local_job_put(jid,rec)
        redis_ok=_r35_job_put(jid,rec,pending=True)
        backend='redis' if redis_ok else ''
        mega_detail=''
        if not backend:
            mega_rec=dict(rec); mega_rec.update({'status':'queued','durable_backend':'mega','durable_at':time.time()})
            mega_ok,mega_detail=_r36_mega_job_put(jid,mega_rec)
            if mega_ok: backend='mega'
        if not backend:
            rec.update({'status':'admission_failed','ok':False,'error':'durable spool unavailable: '+str(mega_detail or 'Redis and MEGA unavailable')[:400]})
            _r36_local_job_put(jid,rec)
            with FILE_JOB_LOCK: FILE_JOB_STATUS.pop(jid,None)
            return {'ok':False,'error':rec['error'],'job_id':jid,'durable':False},503
        rec.update({'status':'queued','ok':None,'durable_backend':backend,'durable_at':time.time()})
        _r35_job_put(jid,rec,pending=True)
        _file_status_put(jid,status='queued',ok=None,recipient_chat_id=cid,target_chat_id=body.get('target_chat_id'),durable_backend=backend)
        queued=_r35_enqueue_file(job)
        # A full execution queue is not data loss anymore: the durable recovery loop will
        # pick this job up when capacity returns.
        return {'ok':True,'status':'queued','job_id':jid,'queue_size':FILE_Q.qsize(),'queued_now':bool(queued),'durable':True,'durable_backend':backend},202

try: app.view_functions['internal_export_file_r7']=_internal_export_file_durable
except Exception: pass

def _r35_process_file_job(job):
    jid=str(job.get('id') or ''); body=dict(job.get('payload') or {}); _file_status_put(jid,status='running',ok=None)
    try:
        required=int(body.get('required_revision') or 0)
        if required and _r34_state_dependent_operation(body.get('operation')):
            ok_rev,cur_rev=_r34_wait_revision(required)
            if not ok_rev: raise RuntimeError(f'HEAVY state revision behind required={required} applied={cur_rev}')
        prepared,body2=_r33_prepare_file(body,jid); path,filename=prepared; delivery=str(body2.get('delivery') or 'chat'); url=''
        if delivery=='drive': url=_drive_upload_file(path,filename,str(body2.get('drive_folder_id') or ''))
        elif delivery=='google':
            gb=dict(body2); gb['title']=str(body2.get('title') or body2.get('label') or filename)[:95]; gb['spreadsheet_id']=str(body2.get('spreadsheet_id') or '')
            if not gb['spreadsheet_id']: raise RuntimeError('Google spreadsheet_id missing in R36 job')
            url=create_google_sheet(gb)
        with STATE_LOCK:
            STATE['file_jobs']=int(STATE.get('file_jobs') or 0)+1; STATE['file_last_ok']=time.time(); STATE['file_last_error']=''; STATE['r33_heavy_exports']=int(STATE.get('r33_heavy_exports') or 0)+1; STATE['r33_last_export']=str(body2.get('operation') or body2.get('file_type') or '')
        job2=dict(job); job2['payload']=dict(body2); job2['payload']['filename']=filename; job2['payload']['caption']=str(body2.get('caption') or '')
        rec=_r35_job_get(jid); rec.update({'status':'ready','ok':True,'job':job2,'path':str(path),'filename':filename,'url':url,'delivery':delivery}); _r35_job_put(jid,rec,pending=True)
        _file_status_put(jid,status='ready',ok=True,path=str(path),filename=filename,url=url,delivery=delivery)
        if not _r35_enqueue_result({'job':job2,'ok':True,'extra':{'url':url,'filename':filename}}): raise RuntimeError('R36 result queue full')
        print(f'[R45 HEAVY EXPORT] {jid} op={body2.get("operation")} ready delivery={delivery}',flush=True)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:700]}'
        with STATE_LOCK: STATE['file_failures']=int(STATE.get('file_failures') or 0)+1; STATE['file_last_error']=detail[:240]
        rec=_r35_job_get(jid); rec.update({'status':'failed','ok':False,'job':job,'error':detail}); _r35_job_put(jid,rec,pending=True)
        _file_status_put(jid,status='failed',ok=False,error=detail,recipient_chat_id=body.get('recipient_chat_id'),target_chat_id=body.get('target_chat_id'))
        _r35_enqueue_result({'job':job,'ok':False,'extra':{'error':detail}})
        print(f'[R45 HEAVY EXPORT] {jid} failed {detail}',flush=True)


def file_loop():
    while True:
        job=FILE_Q.get(); jid=str((job or {}).get('id') or '')
        try: process_file_job(job)
        except Exception as exc: print(f'[R36 EXPORT LOOP ERROR] {type(exc).__name__}: {str(exc)[:300]}',flush=True)
        finally:
            with _R35_ENQUEUED_LOCK: _R35_ENQUEUED.discard(jid)
            FILE_Q.task_done()

def _r35_result_loop():
    while True:
        task=_R35_RESULT_Q.get(); job=task.get('job') or {}; jid=str(job.get('id') or '')
        try:
            _file_status_put(jid,status='delivering')
            delivered=bool(_notify_front_export_result(job,bool(task.get('ok')),**dict(task.get('extra') or {})))
            if delivered:
                rec=_r35_job_get(jid); rec.update({'status':'delivered','callback_delivered':True}); _r35_job_put(jid,rec,pending=False)
                _file_status_put(jid,status='delivered',callback_delivered=True)
                if str(rec.get('durable_backend') or '') == 'mega':
                    try: _r36_mega_job_delete(jid)
                    except Exception: pass
                try:
                    path=Path(str(rec.get('path') or ''))
                    if path.is_file(): path.unlink(missing_ok=True)
                except Exception: pass
            else:
                rec=_r35_job_get(jid); rec.update({'status':'ready' if task.get('ok') else 'failed','callback_delivered':False}); _r35_job_put(jid,rec,pending=True)
                _file_status_put(jid,status=rec.get('status'),callback_delivered=False)
        except Exception as exc:
            rec=_r35_job_get(jid); rec.update({'status':'ready' if task.get('ok') else 'failed','callback_delivered':False,'delivery_error':f'{type(exc).__name__}: {str(exc)[:500]}'}); _r35_job_put(jid,rec,pending=True)
        finally:
            with _R35_RESULT_LOCK: _R35_RESULT_ENQUEUED.discard(jid)
            _R35_RESULT_Q.task_done()

def _r42_release_of_record(rec):
    try:
        job=(rec or {}).get('job') if isinstance((rec or {}).get('job'),dict) else {}
        body=(job or {}).get('payload') if isinstance((job or {}).get('payload'),dict) else {}
        return str(body.get('front_release') or '')
    except Exception:
        return ''


def _r42_is_front_owned(rec):
    return bool((rec or {}).get('front_owned') or str((rec or {}).get('durable_backend') or '')=='front-redis')


def _r42_legacy_record(rec):
    rel=_r42_release_of_record(rec)
    if not rel:
        return False
    # Jobs from previous split releases must never be auto-resurrected after an R42
    # deploy. FAST owns the current durable outbox and will re-admit any still-live job.
    return rel != 'Пер-R43'


def _r35_recover_loop():
    """R42 recovery policy.

    FAST Redis outbox is authoritative for front-owned jobs.  HEAVY therefore never
    auto-runs a stale front-owned local record after a process restart; FAST replays the
    same job_id.  Legacy R39-R41 MEGA/local rows are ignored so an old full_state/Excel
    cannot come back to life and trigger an OOM loop after every restart.

    HEAVY-owned R42 jobs (used only when FAST cannot prove durable Redis ownership)
    retain local/Redis/MEGA recovery.
    """
    last_mega_scan=0.0
    # Let FAST reconnect/replay first.  This also prevents a startup stampede.
    time.sleep(max(2.0,min(20.0,float(os.getenv('R42_RECOVERY_START_DELAY_SEC','6') or '6'))))
    while True:
        try:
            records={}
            for rec in _r36_local_pending(250):
                jid=str(rec.get('job_id') or (rec.get('job') or {}).get('id') or '')
                if not jid:
                    continue
                if _r42_legacy_record(rec):
                    # Close only the local witness; never spend startup time deleting MEGA.
                    old=dict(rec); old.update({'status':'closed','ok':False,'error':'R42 superseded stale job from '+(_r42_release_of_record(rec) or 'legacy'),'updated_at':time.time()})
                    _r36_local_job_put(jid,old)
                    continue
                if _r42_is_front_owned(rec):
                    # Current front-owned work is replayed by FAST Redis outbox using the
                    # same job_id.  Do not self-requeue here or it can run twice.
                    continue
                records[jid]=rec
            c=_redis_client(); redis_live=False
            if c is not None:
                try:
                    ids=[x.decode() if isinstance(x,(bytes,bytearray)) else str(x) for x in list(c.smembers(_R35_JOB_PENDING_KEY) or [])[:250]]
                    redis_live=True
                    for jid in ids:
                        rec=_r35_job_get(jid)
                        if not rec or _r42_legacy_record(rec) or _r42_is_front_owned(rec):
                            continue
                        if float(rec.get('updated_at') or 0)>=float((records.get(jid) or {}).get('updated_at') or 0):
                            records[jid]=rec
                except Exception:
                    redis_live=False
            # MEGA recovery remains only for HEAVY-owned jobs created by THIS release.
            # Old r36/r39/r40/r41 remote rows are deliberately ignored.
            if (not redis_live) and time.time()-last_mega_scan>=max(45,env_int('R42_MEGA_RECOVERY_SCAN_SEC',120,45,900)):
                last_mega_scan=time.time()
                for rec in _r36_mega_pending_rows(80):
                    if _r42_release_of_record(rec) != 'Пер-R43':
                        continue
                    if _r42_is_front_owned(rec):
                        continue
                    jid=str(rec.get('job_id') or (rec.get('job') or {}).get('id') or '')
                    if not jid:
                        continue
                    if float(rec.get('updated_at') or 0)>=float((records.get(jid) or {}).get('updated_at') or 0):
                        records[jid]=rec
                    _r36_local_job_put(jid,rec)
            for jid,rec0 in list(records.items()):
                rec=_r35_job_get(jid) or rec0
                status=str(rec.get('status') or 'queued'); backend=str(rec.get('durable_backend') or '')
                job=rec.get('job') if isinstance(rec.get('job'),dict) else None
                if not backend and status in {'admitting','admission_failed'}:
                    continue
                if not job:
                    continue
                if status in {'ready','delivering'} and bool(rec.get('ok')) and Path(str(rec.get('path') or '')).is_file():
                    _r35_enqueue_result({'job':job,'ok':True,'extra':{'url':rec.get('url') or '','filename':rec.get('filename') or ''}})
                elif status=='failed' and rec.get('error'):
                    _r35_enqueue_result({'job':job,'ok':False,'extra':{'error':rec.get('error')}})
                elif status not in {'delivered','closed'}:
                    _r35_enqueue_file(job)
        except Exception as exc:
            with STATE_LOCK:
                STATE['file_last_error']=f'R42 recovery {type(exc).__name__}: {str(exc)[:180]}'
        time.sleep(3.0)

# ---------------------------------------------------------------------------
# R38: production-grade HEAVY admission/recovery for Google jobs and resilient
# Google/Drive HTTP. User jobs survive Render restarts just like file exports.
_R38_GOOGLE_DB = CACHE_DIR / 'r38_google_jobs.sqlite3'
_R38_GOOGLE_DB_LOCK = threading.RLock()
_R38_GOOGLE_ADMISSION_LOCK = threading.RLock()
_R38_GOOGLE_PENDING_KEY = 'per:r38:heavy:google:pending'
_R38_GOOGLE_PREFIX = 'per:r38:heavy:google:'
_R38_GOOGLE_MEGA_DIR = mega_root().rstrip('/') + '/r38_google_jobs_pending'
_R38_GOOGLE_ENQUEUED = set()
_R38_GOOGLE_ENQUEUED_LOCK = threading.RLock()
_R38_GOOGLE_MEGA_SCAN = {'at':0.0,'error':''}


def _r38_google_http(method,url,**kwargs):
    """Retry only transient Google/Drive failures, honoring Retry-After."""
    max_wait=max(30,min(1800,env_int('R38_GOOGLE_RETRY_WINDOW_SEC',600,30,1800)))
    deadline=time.time()+max_wait; attempt=0; delay=1.0; last_exc=None; last_resp=None
    while True:
        attempt+=1
        try:
            # Multipart retries must rewind file objects.
            try:
                for spec in (kwargs.get('files') or {}).values():
                    if isinstance(spec,(tuple,list)) and len(spec)>=2 and hasattr(spec[1],'seek'): spec[1].seek(0)
            except Exception: pass
            resp=requests.request(str(method or 'get').upper(),url,**kwargs); last_resp=resp
            status=int(resp.status_code or 0); text=''
            try:text=str(resp.text or '')[:1800].casefold()
            except Exception:pass
            quota403=(status==403 and any(x in text for x in ('ratelimitexceeded','userratelimitexceeded','resource_exhausted','quota exceeded','rate limit')))
            transient=status in {408,425,429,500,502,503,504} or quota403
            if not transient:return resp
            retry_after=0.0
            try:retry_after=float(str(resp.headers.get('Retry-After','') or '0').strip() or 0)
            except Exception:retry_after=0.0
            wait=max(delay,retry_after,1.0)
            if time.time()+wait>=deadline:return resp
            print(f'[R38 GOOGLE RETRY] {method} status={status} attempt={attempt} wait={wait:.1f}s',flush=True)
            time.sleep(min(120.0,wait)); delay=min(60.0,delay*1.8)
        except Exception as exc:
            last_exc=exc; wait=max(1.0,delay)
            if time.time()+wait>=deadline:raise
            print(f'[R38 GOOGLE RETRY] {method} {type(exc).__name__} attempt={attempt} wait={wait:.1f}s',flush=True)
            time.sleep(min(60.0,wait)); delay=min(60.0,delay*1.8)
    if last_resp is not None:return last_resp
    if last_exc is not None:raise last_exc
    raise RuntimeError('Google request failed without response')


def _r38_google_db_init():
    with _R38_GOOGLE_DB_LOCK:
        con=sqlite3.connect(str(_R38_GOOGLE_DB),timeout=5,check_same_thread=False)
        try:
            con.execute('PRAGMA journal_mode=WAL'); con.execute('PRAGMA synchronous=FULL'); con.execute('PRAGMA busy_timeout=5000')
            con.execute('CREATE TABLE IF NOT EXISTS jobs(job_id TEXT PRIMARY KEY,row_json TEXT NOT NULL,status TEXT NOT NULL,updated_at REAL NOT NULL)')
            con.execute('CREATE INDEX IF NOT EXISTS idx_r38_google_status ON jobs(status,updated_at)'); con.commit()
        finally:con.close()


def _r38_google_local_put(jid,row):
    try:
        _r38_google_db_init(); obj=dict(row or {}); obj['job_id']=str(jid); obj['updated_at']=time.time(); raw=json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str)
        with _R38_GOOGLE_DB_LOCK:
            con=sqlite3.connect(str(_R38_GOOGLE_DB),timeout=5,check_same_thread=False)
            try:
                con.execute('PRAGMA busy_timeout=5000'); con.execute('INSERT INTO jobs(job_id,row_json,status,updated_at) VALUES(?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET row_json=excluded.row_json,status=excluded.status,updated_at=excluded.updated_at',(str(jid),raw,str(obj.get('status') or 'queued'),float(obj['updated_at']))); con.commit()
            finally:con.close()
        return True
    except Exception:return False


def _r38_google_local_get(jid):
    try:
        _r38_google_db_init()
        with _R38_GOOGLE_DB_LOCK:
            con=sqlite3.connect(str(_R38_GOOGLE_DB),timeout=3,check_same_thread=False)
            try:con.execute('PRAGMA busy_timeout=3000'); row=con.execute('SELECT row_json FROM jobs WHERE job_id=?',(str(jid),)).fetchone()
            finally:con.close()
        obj=json.loads(row[0]) if row else {}; return obj if isinstance(obj,dict) else {}
    except Exception:return {}


def _r38_google_local_pending(limit=200):
    out=[]
    try:
        _r38_google_db_init()
        with _R38_GOOGLE_DB_LOCK:
            con=sqlite3.connect(str(_R38_GOOGLE_DB),timeout=3,check_same_thread=False)
            try:con.execute('PRAGMA busy_timeout=3000'); rows=con.execute("SELECT row_json FROM jobs WHERE status NOT IN ('delivered','closed') ORDER BY updated_at ASC LIMIT ?",(max(1,min(500,int(limit))),)).fetchall()
            finally:con.close()
        for row in rows:
            try:
                obj=json.loads(row[0]);
                if isinstance(obj,dict):out.append(obj)
            except Exception:pass
    except Exception:pass
    return out


def _r38_google_key(jid):return _R38_GOOGLE_PREFIX+str(jid or '')[:80]


def _r38_google_get(jid):
    best=_r38_google_local_get(jid); c=_redis_client()
    if c is not None:
        try:
            raw=c.get(_r38_google_key(jid))
            if raw:
                obj=json.loads(raw.decode('utf-8') if isinstance(raw,(bytes,bytearray)) else raw)
                if isinstance(obj,dict) and float(obj.get('updated_at') or 0)>=float(best.get('updated_at') or 0):best=obj
        except Exception:pass
    return best if isinstance(best,dict) else {}


def _r38_google_put(jid,row,pending=True):
    obj=dict(row or {}); obj['job_id']=str(jid); obj['updated_at']=time.time(); local_ok=_r38_google_local_put(jid,obj); c=_redis_client(); redis_ok=False
    if c is not None:
        try:
            pipe=c.pipeline(transaction=False); pipe.set(_r38_google_key(jid),json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str),ex=604800)
            if pending:pipe.sadd(_R38_GOOGLE_PENDING_KEY,str(jid))
            else:pipe.srem(_R38_GOOGLE_PENDING_KEY,str(jid))
            pipe.expire(_R38_GOOGLE_PENDING_KEY,604800); pipe.execute(); redis_ok=True
        except Exception:pass
    try:
        with GOOGLE_JOB_LOCK:
            mem=dict(GOOGLE_JOB_STATUS.get(str(jid)) or {}); mem.update({k:v for k,v in obj.items() if k not in {'job'}}); GOOGLE_JOB_STATUS[str(jid)]=mem
    except Exception:pass
    return bool(redis_ok),bool(local_ok)


def _r38_google_mega_path(jid):return _R38_GOOGLE_MEGA_DIR.rstrip('/')+'/job_'+re.sub(r'[^A-Za-z0-9_.-]+','_',str(jid or ''))[:90]+'.json'


def _r38_google_mega_put(jid,row):
    local=None
    try:
        with MEGA_LOCK:
            ok,detail=mega_login()
            if not ok:return False,detail
            if not ensure_mega_dir(mega_root()) or not ensure_mega_dir(_R38_GOOGLE_MEGA_DIR):return False,'cannot create R38 Google MEGA dir'
            work=Path(tempfile.mkdtemp(prefix='r38_google_spool_')); local=work/Path(_r38_google_mega_path(jid)).name
            obj=dict(row or {}); obj['job_id']=str(jid); obj['r38_spooled_at']=time.time(); local.write_text(json.dumps(obj,ensure_ascii=False,separators=(',',':'),default=str),encoding='utf-8')
            remote=_r38_google_mega_path(jid)
            try:run_cmd(['mega-rm',remote],timeout=30)
            except Exception:pass
            put=run_cmd(['mega-put',str(local),_R38_GOOGLE_MEGA_DIR],timeout=env_int('R38_GOOGLE_MEGA_TIMEOUT',180,30,600))
            if put.returncode!=0:return False,(put.stderr or put.stdout or 'mega-put failed')[:220]
            if not mega_exists(remote):return False,'MEGA Google job verify failed'
            return True,'MEGA durable Google job stored'
    except Exception as exc:return False,f'{type(exc).__name__}: {str(exc)[:220]}'
    finally:
        try:
            if local is not None:shutil.rmtree(local.parent,ignore_errors=True)
        except Exception:pass


def _r38_google_mega_delete(jid):
    try:
        with MEGA_LOCK:
            if not mega_login()[0]:return False
            p=run_cmd(['mega-rm',_r38_google_mega_path(jid)],timeout=45); return p.returncode==0 or not mega_exists(_r38_google_mega_path(jid))
    except Exception:return False


def _r38_google_mega_pending(limit=100):
    out=[]; work=None
    try:
        with MEGA_LOCK:
            ok,detail=mega_login()
            if not ok:_R38_GOOGLE_MEGA_SCAN.update(at=time.time(),error=detail);return out
            if not ensure_mega_dir(_R38_GOOGLE_MEGA_DIR):return out
            res=run_cmd(['mega-find',_R38_GOOGLE_MEGA_DIR,'--pattern=job_*.json','--type=f'],timeout=env_int('MEGA_TIMEOUT',180,30,900))
            if res.returncode!=0:return out
            paths=[x.strip() for x in (res.stdout or '').splitlines() if x.strip()][:max(1,int(limit))]; work=Path(tempfile.mkdtemp(prefix='r38_google_recover_'))
            for i,remote in enumerate(paths):
                d=work/f'{i:03d}'; d.mkdir(parents=True,exist_ok=True); g=run_cmd(['mega-get',remote,str(d)],timeout=env_int('R38_GOOGLE_MEGA_TIMEOUT',180,30,600))
                if g.returncode!=0:continue
                files=list(d.glob('*.json'))
                if files:
                    try:
                        obj=json.loads(files[0].read_text(encoding='utf-8'))
                        if isinstance(obj,dict) and obj.get('job_id'):out.append(obj)
                    except Exception:pass
        _R38_GOOGLE_MEGA_SCAN.update(at=time.time(),error='');return out
    except Exception as exc:_R38_GOOGLE_MEGA_SCAN.update(at=time.time(),error=f'{type(exc).__name__}: {str(exc)[:180]}');return out
    finally:
        try:
            if work is not None:shutil.rmtree(work,ignore_errors=True)
        except Exception:pass


def _r38_google_enqueue(job):
    jid=str((job or {}).get('id') or '')
    if not jid:return False
    with _R38_GOOGLE_ENQUEUED_LOCK:
        if jid in _R38_GOOGLE_ENQUEUED:return True
        try:GOOGLE_Q.put_nowait(job);_R38_GOOGLE_ENQUEUED.add(jid);return True
        except queue.Full:return False


def internal_google_sheet_r38():
    if not authorized():return {'ok':False},404
    body=request.get_json(silent=True) or {}
    try:body['spreadsheet_id']=_sheet_id(body.get('spreadsheet_id'))
    except Exception as exc:return {'ok':False,'error':str(exc)[:600]},400
    try:cid=int(body.get('recipient_chat_id') or 0)
    except Exception:cid=0
    if not cid:return {'ok':False,'error':'recipient_chat_id required'},400
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80];body['job_id']=jid
    with _R38_GOOGLE_ADMISSION_LOCK:
        existing=_r38_google_get(jid)
        if existing and str(existing.get('durable_backend') or ''):
            status=str(existing.get('status') or 'queued'); job=existing.get('job') if isinstance(existing.get('job'),dict) else None
            if job and status in {'queued','running'}:_r38_google_enqueue(job)
            return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'url':str(existing.get('url') or ''),'durable':True,'durable_backend':str(existing.get('durable_backend') or '')},200 if status in {'delivered','closed'} else 202
        job={'id':jid,'type':'google_sheet','created_at':time.time(),'payload':body};rec={'job_id':jid,'status':'admitting','ok':None,'job':job,'recipient_chat_id':cid,'durable_backend':''}
        _r38_google_local_put(jid,rec);redis_ok,_local_ok=_r38_google_put(jid,rec,pending=True);backend='redis' if redis_ok else '';detail=''
        if not backend:
            mrec=dict(rec);mrec.update({'status':'queued','durable_backend':'mega','durable_at':time.time()});mok,detail=_r38_google_mega_put(jid,mrec)
            if mok:backend='mega'
        if not backend:
            rec.update({'status':'admission_failed','ok':False,'error':'durable Google spool unavailable: '+str(detail or 'Redis and MEGA unavailable')[:400]});_r38_google_local_put(jid,rec)
            return {'ok':False,'error':rec['error'],'job_id':jid,'durable':False},503
        rec.update({'status':'queued','ok':None,'durable_backend':backend,'durable_at':time.time()});_r38_google_put(jid,rec,pending=True);queued=_r38_google_enqueue(job)
        return {'ok':True,'status':'queued','job_id':jid,'queue_size':GOOGLE_Q.qsize(),'queued_now':bool(queued),'durable':True,'durable_backend':backend},202

try:app.view_functions['internal_google_sheet']=internal_google_sheet_r38
except Exception:pass


def _notify_front_google_result_r38(job,ok,url='',error=''):
    base,secret=front_base(),peer_secret()
    if not base or not secret:return False
    payload={'job_id':str(job.get('id') or ''),'ok':bool(ok),'url':str(url or ''),'error':str(error or '')[:900],'title':str((job.get('payload') or {}).get('title') or 'Google Excel')[:220],'recipient_chat_id':(job.get('payload') or {}).get('recipient_chat_id'),'target_chat_id':(job.get('payload') or {}).get('target_chat_id'),'tenant_id':(job.get('payload') or {}).get('tenant_id'),'notify_result':bool((job.get('payload') or {}).get('notify_result',True))}
    deadline=time.time()+max(10,min(180,env_int('R38_GOOGLE_CALLBACK_WINDOW_SEC',60,10,180)));delay=1.0
    while time.time()<deadline:
        try:
            r=requests.post(base+'/internal/split/google-result',json=payload,headers={'X-Peer-Secret':secret,'User-Agent':'per-r38-worker-google-result'},timeout=15)
            data={}
            try:data=r.json() if r.content else {}
            except Exception:data={}
            if r.status_code==200 and (bool(data.get('delivered')) or bool(data.get('duplicate'))):return True
            if 400<=r.status_code<500 and r.status_code not in {408,409,425,429}:return False
        except Exception:pass
        time.sleep(delay);delay=min(10.0,delay*1.6)
    return False

_notify_front_google_result=_notify_front_google_result_r38


def _process_google_job_core(job):
    jid=str(job.get('id') or '');body=dict(job.get('payload') or {});rec=_r38_google_get(jid);rec.update({'job':job,'status':'running','ok':None,'started_at':time.time()});_r38_google_put(jid,rec,pending=True)
    try:
        url=create_google_sheet(body);rec=_r38_google_get(jid);rec.update({'job':job,'status':'ready','ok':True,'url':url,'error':'','finished_at':time.time()});_r38_google_put(jid,rec,pending=True)
        with STATE_LOCK:STATE['google_jobs']=int(STATE.get('google_jobs') or 0)+1;STATE['google_last_ok']=time.time();STATE['google_last_error']=''
        delivered=_notify_front_google_result_r38(job,True,url=url)
        if delivered:
            rec.update({'status':'delivered','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
            if str(rec.get('durable_backend') or '')=='mega':_r38_google_mega_delete(jid)
        else:
            rec.update({'status':'ready','callback_delivered':False});_r38_google_put(jid,rec,pending=True)
        print(f'[R45 GOOGLE JOB] {jid} ok=True callback={delivered}',flush=True)
    except Exception as exc:
        detail=f'{type(exc).__name__}: {str(exc)[:600]}';rec=_r38_google_get(jid);rec.update({'job':job,'status':'failed','ok':False,'error':detail,'finished_at':time.time()});_r38_google_put(jid,rec,pending=True)
        with STATE_LOCK:STATE['google_failures']=int(STATE.get('google_failures') or 0)+1;STATE['google_last_error']=detail[:240]
        delivered=_notify_front_google_result_r38(job,False,error=detail)
        if delivered:
            rec.update({'status':'closed','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
            if str(rec.get('durable_backend') or '')=='mega':_r38_google_mega_delete(jid)
        print(f'[R45 GOOGLE JOB] {jid} ok=False callback={delivered} {detail}',flush=True)


def _r38_google_recover_loop():
    """R42 Google recovery follows the same ownership rule as file jobs."""
    last_mega=0.0
    time.sleep(max(2.0,min(20.0,float(os.getenv('R42_RECOVERY_START_DELAY_SEC','6') or '6'))))
    while True:
        try:
            records={}
            for x in _r38_google_local_pending(250):
                jid=str(x.get('job_id') or '')
                if not jid: continue
                if _r42_legacy_record(x):
                    old=dict(x); old.update({'status':'closed','ok':False,'error':'R42 superseded stale Google job from '+(_r42_release_of_record(x) or 'legacy'),'updated_at':time.time()})
                    _r38_google_local_put(jid,old); continue
                if _r42_is_front_owned(x):
                    continue
                records[jid]=x
            c=_redis_client();redis_live=False
            if c is not None:
                try:
                    ids=list(c.smembers(_R38_GOOGLE_PENDING_KEY) or [])[:250];redis_live=True
                    for raw in ids:
                        jid=raw.decode() if isinstance(raw,(bytes,bytearray)) else str(raw);obj=_r38_google_get(jid)
                        if not obj or _r42_legacy_record(obj) or _r42_is_front_owned(obj): continue
                        if float(obj.get('updated_at') or 0)>=float((records.get(jid) or {}).get('updated_at') or 0):records[jid]=obj
                except Exception:redis_live=False
            if (not redis_live) and time.time()-last_mega>=max(45,env_int('R42_GOOGLE_MEGA_SCAN_SEC',120,45,900)):
                last_mega=time.time()
                for obj in _r38_google_mega_pending(80):
                    if _r42_release_of_record(obj) != 'Пер-R43' or _r42_is_front_owned(obj): continue
                    jid=str(obj.get('job_id') or '')
                    if jid and float(obj.get('updated_at') or 0)>=float((records.get(jid) or {}).get('updated_at') or 0):
                        records[jid]=obj;_r38_google_local_put(jid,obj)
            for jid,base_rec in list(records.items()):
                rec=_r38_google_get(jid) or base_rec;status=str(rec.get('status') or 'queued');job=rec.get('job') if isinstance(rec.get('job'),dict) else None
                if not job:continue
                if status=='ready' and rec.get('url'):
                    if _notify_front_google_result_r38(job,True,url=str(rec.get('url') or '')):
                        rec.update({'status':'delivered','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
                        if str(rec.get('durable_backend') or '')=='mega':_r38_google_mega_delete(jid)
                elif status=='failed' and rec.get('error'):
                    if _notify_front_google_result_r38(job,False,error=str(rec.get('error') or '')):
                        rec.update({'status':'closed','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
                        if str(rec.get('durable_backend') or '')=='mega':_r38_google_mega_delete(jid)
                elif status not in {'delivered','closed','admission_failed'}:_r38_google_enqueue(job)
        except Exception as exc:
            with STATE_LOCK:STATE['google_last_error']=f'R42 recovery {type(exc).__name__}: {str(exc)[:180]}'
        time.sleep(3.0)


# ---------------- Пер-R43 stability / speed patch ----------------
# 1) semantic single-flight prevents stale duplicate full-state jobs from running together;
# 2) memory-heavy exports are serialized;
# 3) full-state redaction/checksum streams rows instead of fetchall() into RAM.
_R39_FILE_SIG_LOCK = threading.RLock()
_R39_FILE_SIG = {}
_R39_EXPENSIVE_SEM = threading.Semaphore(max(1, env_int('R39_EXPENSIVE_FILE_CONCURRENCY',1,1,2)))
_R39_EXPENSIVE_OPS = {'full_state','sqlite','runtime_zip','journal','journal_current'}


def _r39_file_signature(body):
    b=body if isinstance(body,dict) else {}
    keys=('operation','recipient_chat_id','target_chat_id','scope','tenant_id','tenant_chat_ids','mode','day_key','start_key','start_rid','end_key','end_rid','source_file_type','file_type','delivery','limit','start_dt','end_dt')
    core={k:b.get(k) for k in keys if k in b}
    raw=json.dumps(core,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str)
    return hashlib.sha256(raw.encode('utf-8','replace')).hexdigest()


def _r39_close_duplicate_job(jid, canonical_jid):
    try:
        rec=_r35_job_get(jid); backend=str(rec.get('durable_backend') or '')
        rec.update({'status':'closed','ok':True,'duplicate_of':str(canonical_jid),'closed_at':time.time()})
        _r35_job_put(jid,rec,pending=False)
        _file_status_put(jid,status='closed',ok=True,duplicate_of=str(canonical_jid))
        if backend=='mega':
            try:_r36_mega_job_delete(jid)
            except Exception:pass
    except Exception:pass


# R42 global compute capacity.  R39 serialized expensive jobs only against each
# other, so full_state could still run beside two XLSX/Google exports and exhaust a
# small Render instance.  R42 allows two ordinary jobs OR one expensive job, never both.
_R42_COMPUTE_SLOTS=max(1,min(3,env_int('R42_HEAVY_COMPUTE_SLOTS',2,1,3)))
_R42_COMPUTE_SEM=threading.BoundedSemaphore(_R42_COMPUTE_SLOTS)

def _r42_acquire_slots(count):
    n=max(1,min(_R42_COMPUTE_SLOTS,int(count or 1)))
    for _ in range(n):
        _R42_COMPUTE_SEM.acquire()
    return n

def _r42_release_slots(count):
    for _ in range(max(0,int(count or 0))):
        try:_R42_COMPUTE_SEM.release()
        except Exception:pass

def _r42_mem_mb():
    try:
        for line in Path('/proc/self/status').read_text(errors='ignore').splitlines():
            if line.startswith('VmRSS:'):
                return round(float(line.split()[1])/1024.0,1)
    except Exception:pass
    return -1.0

def _r39_process_file_job(job):
    jid=str((job or {}).get('id') or ''); body=dict((job or {}).get('payload') or {}); op=str(body.get('operation') or '')
    sig=_r39_file_signature(body); created=float((job or {}).get('created_at') or time.time()); now=time.time()
    # Before any work starts, choose the same deterministic canonical id as FAST from
    # every locally recovered durable record with the same semantic signature.
    try:
        same=[]
        for rec in _r36_local_pending(500):
            if str((rec or {}).get('status') or '') in {'failed','admission_failed'}: continue
            j=(rec or {}).get('job') if isinstance((rec or {}).get('job'),dict) else None
            if not j: continue
            if _r39_file_signature((j or {}).get('payload') or {})==sig:
                cj=str((j or {}).get('id') or (rec or {}).get('job_id') or '')
                if cj: same.append(cj)
        canonical=min(same) if same else jid
    except Exception:
        canonical=jid
    with _R39_FILE_SIG_LOCK:
        old=dict(_R39_FILE_SIG.get(sig) or {})
        old_jid=str(old.get('job_id') or '')
        old_state=str(old.get('state') or '')
        if old_jid and old_state=='running': canonical=old_jid
        if canonical and canonical!=jid:
            _r39_close_duplicate_job(jid,canonical)
            print(f'[R39 FILE DEDUPE] drop={jid} canonical={canonical} op={op}',flush=True)
            return
        _R39_FILE_SIG[sig]={'job_id':jid,'state':'running','started_at':now,'created_at':created}
    started=time.time()
    try:
        if op in _R39_EXPENSIVE_OPS:
            print(f'[R45 FILE START] {jid} op={op} lane=exclusive rss={_r42_mem_mb()}MB',flush=True)
            with _R39_EXPENSIVE_SEM:
                held=_r42_acquire_slots(_R42_COMPUTE_SLOTS)
                try:
                    return _process_file_job_core(job)
                finally:
                    _r42_release_slots(held)
        print(f'[R45 FILE START] {jid} op={op} lane=shared rss={_r42_mem_mb()}MB',flush=True)
        held=_r42_acquire_slots(1)
        try:
            return _process_file_job_core(job)
        finally:
            _r42_release_slots(held)
    finally:
        try:
            rec=_r35_job_get(jid); st=str(rec.get('status') or '')
        except Exception: st=''
        with _R39_FILE_SIG_LOCK:
            cur=_R39_FILE_SIG.get(sig) or {}
            if str(cur.get('job_id') or '')==jid:
                if st in {'failed','admission_failed'}:
                    _R39_FILE_SIG.pop(sig,None)
                else:
                    cur.update({'state':'done','completed_at':time.time(),'status':st}); _R39_FILE_SIG[sig]=cur
        print(f'[R45 FILE END] {jid} op={op} status={st or "unknown"} elapsed={time.time()-started:.2f}s rss={_r42_mem_mb()}MB',flush=True)



def _r39_db_checksum(path):
    con=sqlite3.connect(str(path)); h=hashlib.sha256()
    try:
        for table in ('kv','chats','meta','cold_fields'):
            cols=[x[1] for x in con.execute(f'PRAGMA table_info({table})').fetchall()]
            if not cols: continue
            order=','.join(cols[:2]); cur=con.execute(f'SELECT * FROM {table} ORDER BY {order}')
            while True:
                batch=cur.fetchmany(128)
                if not batch: break
                for row in batch:
                    if table=='meta' and len(row)>=2 and str(row[0])=='v153_export' and str(row[1])=='manifest': continue
                    h.update(table.encode()); h.update(b'\\0')
                    for v in row: h.update(str(v).encode('utf-8','replace')); h.update(b'\\0')
        return h.hexdigest()
    finally: con.close()

_r33_db_checksum=_r39_db_checksum


def _r39_sanitize_table_streaming(con,table,keycols,batch_size=64):
    cols=','.join(tuple(keycols)+('v',)); where=' AND '.join(f'{k}=?' for k in keycols)
    sql=f'UPDATE {table} SET v=? WHERE {where}'; order=','.join(keycols); offset=0
    while True:
        rows=con.execute(f'SELECT {cols} FROM {table} ORDER BY {order} LIMIT ? OFFSET ?',(int(batch_size),int(offset))).fetchall()
        if not rows: break
        updates=[]
        for row in rows:
            keys=row[:-1]; val=row[-1]
            try:safe=json.dumps(_r33_sanitize(json.loads(val)),ensure_ascii=False,separators=(',',':'),default=str)
            except Exception:safe=str(val)
            updates.append((safe,*keys))
        if updates: con.executemany(sql,updates)
        offset += len(rows)
        del rows,updates


def _r39_full_state(body,jid):
    started=time.time(); src=_r33_cache_ready(); folder=FILE_DIR/f'{jid}_full'; folder.mkdir(parents=True,exist_ok=True); raw=folder/'state.sqlite3'
    srccon=sqlite3.connect(str(src),timeout=30); dst=sqlite3.connect(str(raw))
    try:srccon.backup(dst,pages=512,sleep=0.002)
    finally:dst.close();srccon.close()
    con=sqlite3.connect(str(raw),timeout=30)
    try:
        scope=str(body.get('scope') or 'global'); chat_ids={int(x) for x in (body.get('tenant_chat_ids') or []) if str(x).lstrip('-').isdigit()}
        if scope=='tenant':
            if chat_ids:
                qs=','.join('?' for _ in chat_ids); vals=tuple(str(x) for x in chat_ids)
                con.execute(f'DELETE FROM chats WHERE chat_id NOT IN ({qs})',vals);con.execute(f'DELETE FROM cold_fields WHERE chat_id NOT IN ({qs})',vals)
            else:
                con.execute('DELETE FROM chats');con.execute('DELETE FROM cold_fields')
            rr=con.execute("SELECT v FROM kv WHERE k='root'").fetchone()
            if rr:
                filtered=_r35_filter_root_for_tenant(_r33_json_load(rr[0],{}),str(body.get('tenant_id') or ''),chat_ids)
                con.execute("UPDATE kv SET v=? WHERE k='root'",(json.dumps(filtered,ensure_ascii=False,separators=(',',':'),default=str),))
        else:
            # Match v262 semantics, but do not load whole tables into Python RAM.
            for table,keycols in (('kv',('k',)),('chats',('chat_id',)),('meta',('kind','k')),('cold_fields',('chat_id','k'))):
                try:_r39_sanitize_table_streaming(con,table,keycols)
                except sqlite3.OperationalError:pass
            try:chat_ids={int(x[0]) for x in con.execute('SELECT chat_id FROM chats') if str(x[0]).lstrip('-').isdigit()}
            except Exception:chat_ids=set()
        con.execute("CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL,k TEXT NOT NULL,v TEXT NOT NULL,PRIMARY KEY(kind,k))")
        try:count=int(con.execute('SELECT COUNT(*) FROM chats').fetchone()[0] or 0)
        except Exception:count=len(chat_ids)
        failed=_r35_failed_tasks_snapshot(chat_ids if scope=='tenant' else None)
        manifest={'kind':'telegram_bot_full_state_v153','schema_version':1,'bot_version':'bot_v153_PER_R42_HEAVY','created_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),'scope':scope,'tenant_id':str(body.get('tenant_id') or ''),'chat_ids':sorted(chat_ids),'chat_count':count,'failed_tasks':len(failed),'checksum':''}
        con.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','failed_tasks',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v",(json.dumps(failed,ensure_ascii=False,separators=(',',':'),default=str),))
        con.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','manifest',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v",(json.dumps(manifest,ensure_ascii=False,separators=(',',':')),));con.commit()
    finally:con.close()
    checksum=_r39_db_checksum(raw);con=sqlite3.connect(str(raw))
    try:
        m=json.loads(con.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()[0]);m['checksum']=checksum
        con.execute("UPDATE meta SET v=? WHERE kind='v153_export' AND k='manifest'",(json.dumps(m,ensure_ascii=False,separators=(',',':')),));con.commit()
    finally:con.close()
    gz=FILE_DIR/f'{jid}.sqlite3.gz'
    with open(raw,'rb') as fin,gzip.open(gz,'wb',compresslevel=max(1,min(6,env_int('R39_FULL_STATE_GZIP_LEVEL',3,1,6)))) as fout:shutil.copyfileobj(fin,fout,1024*1024)
    shutil.rmtree(folder,ignore_errors=True)
    print(f'[R39 FULL STATE] {jid} scope={scope} chats={count} elapsed={time.time()-started:.2f}s size={gz.stat().st_size if gz.exists() else 0}',flush=True)
    return gz,'latest_bot_state.sqlite3.gz'


_r33_full_state=_r39_full_state

# ---------------- Пер-R43 unified protocol patch ----------------
# Admission-time semantic aliasing makes duplicate_of explicit to FAST.  If a race still
# reaches the execution queue, HEAVY sends a lightweight alias callback instead of
# silently closing a job_id that FAST could be waiting on.


def _r40_find_file_canonical(body,jid):
    try: sig=_r39_file_signature(body or {})
    except Exception: return ''
    candidates=[]
    try:
        for rec in _r36_local_pending(800):
            if not isinstance(rec,dict): continue
            st=str(rec.get('status') or '')
            if st not in {'queued','running','ready','delivering'}: continue
            backend=str(rec.get('durable_backend') or '')
            if not backend: continue
            job=rec.get('job') if isinstance(rec.get('job'),dict) else None
            if not job: continue
            cj=str(job.get('id') or rec.get('job_id') or '')[:80]
            if not cj or cj==str(jid): continue
            if _r39_file_signature(job.get('payload') or {})==sig: candidates.append((cj,rec))
    except Exception: pass
    if not candidates: return ''
    return min(candidates,key=lambda x:x[0])[0]


def internal_export_file_r40():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]; body['job_id']=jid
    # One admission lock covers alias lookup + base admission, so two simultaneous
    # identical POSTs cannot both pass the semantic check on this process.
    with _R36_ADMISSION_LOCK:
        existing=_r35_job_get(jid)
        if existing:
            # Idempotent retry of an alias must stay an alias.  Never fall through
            # to the legacy admission path, which does not know the R40 alias state
            # and could accidentally enqueue a second heavy job for the same request.
            est=str(existing.get('status') or '')
            canonical=str(existing.get('canonical_job_id') or existing.get('duplicate_of') or '')[:80]
            if est == 'alias' and canonical:
                backend=str(existing.get('durable_backend') or 'local')
                return {'ok':True,'duplicate':True,'status':'alias','job_id':jid,'canonical_job_id':canonical,'duplicate_of':canonical,'durable':True,'durable_backend':backend},202
        else:
            canonical=_r40_find_file_canonical(body,jid)
            if canonical:
                crec=_r35_job_get(canonical) or {}; backend=str(crec.get('durable_backend') or 'local')
                rec={'job_id':jid,'status':'alias','ok':True,'duplicate_of':canonical,'canonical_job_id':canonical,'durable_backend':backend,'job':{'id':jid,'type':'file_export','created_at':time.time(),'payload':body},'updated_at':time.time()}
                _r36_local_job_put(jid,rec); _r35_job_put(jid,rec,pending=False)
                _file_status_put(jid,status='alias',ok=True,duplicate_of=canonical,canonical_job_id=canonical,durable_backend=backend)
                print(f'[R40 FILE ALIAS ADMISSION] alias={jid} canonical={canonical} op={body.get("operation")}',flush=True)
                return {'ok':True,'duplicate':True,'status':'alias','job_id':jid,'canonical_job_id':canonical,'duplicate_of':canonical,'durable':True,'durable_backend':backend},202
        return _internal_export_file_durable()

app.view_functions['internal_export_file_r7']=internal_export_file_r40


def _r40_notify_front_alias(jid,canonical,body):
    base,secret=front_base(),peer_secret()
    if not base or not secret:return False
    payload={'job_id':str(jid),'ok':True,'alias_only':True,'canonical_job_id':str(canonical),'duplicate_of':str(canonical),'recipient_chat_id':body.get('recipient_chat_id'),'target_chat_id':body.get('target_chat_id'),'operation':body.get('operation'),'label':body.get('label'),'chat_name':body.get('chat_name')}
    deadline=time.time()+90; delay=1.0
    while time.time()<deadline:
        try:
            r=requests.post(base+'/internal/split/export-result',json=payload,headers={'X-Peer-Secret':secret,'User-Agent':'per-r40-worker-alias'},timeout=12)
            if 200<=r.status_code<300:return True
            if 400<=r.status_code<500 and r.status_code not in {408,409,425,429}:return False
        except Exception:pass
        time.sleep(delay);delay=min(10.0,delay*1.6)
    return False


def _r39_close_duplicate_job(jid, canonical_jid):
    try:
        rec=_r35_job_get(jid); backend=str(rec.get('durable_backend') or ''); job=rec.get('job') if isinstance(rec.get('job'),dict) else {}; body=job.get('payload') if isinstance(job.get('payload'),dict) else {}
        rec.update({'status':'closed','ok':True,'duplicate_of':str(canonical_jid),'canonical_job_id':str(canonical_jid),'closed_at':time.time()})
        _r35_job_put(jid,rec,pending=False); _file_status_put(jid,status='closed',ok=True,duplicate_of=str(canonical_jid),canonical_job_id=str(canonical_jid))
        threading.Thread(target=_r40_notify_front_alias,args=(str(jid),str(canonical_jid),dict(body)),daemon=True,name='per-r40-alias-'+str(jid)[:8]).start()
        if backend=='mega':
            try:_r36_mega_job_delete(jid)
            except Exception:pass
    except Exception:pass


def _r40_category_rows_without_description(rows):
    out=[]; annotations={}
    totals={'остаток с прошлого раза','сумма по статьям','расход','приход','остаток на руках','на руках:','гомонковые','остаток в обороте','расход еды на человека в сутки'}
    for r_idx,raw in enumerate(rows or [],start=1):
        row=list(raw or []); desc=str(row[1] if len(row)>1 else '').strip(); is_header=str(row[0] if row else '').strip().casefold() in {'дата','date'} and desc.casefold() in {'описание','description'}
        if len(row)>1:
            if not is_header and desc and not str(row[0] if row else '').strip() and desc.casefold() in totals: row[0]=desc
            row.pop(1)
        if desc and not is_header and desc.casefold() not in totals:
            for original_c in range(3,len(raw or [])):
                try:
                    v=(raw or [])[original_c]
                    if v not in ('',None,0,0.0): annotations[f'{r_idx},{original_c}']=desc
                except Exception:pass
        out.append(row)
    return out,annotations


def _r40_prepare_google_query(body):
    op=str(body.get('operation') or '')
    if op not in {'google_exact_query','google_period_query','google_tabl_query'}: return body
    required=int(body.get('required_revision') or 0)
    if required:
        ok,cur=_r34_wait_revision(required)
        if not ok: raise RuntimeError(f'HEAVY Google state revision behind required={required} applied={cur}')
    b=dict(body)
    if op=='google_tabl_query':
        rows=_r33_tabl_rows(b)
    else:
        q=dict(b); q['operation']='exact_export_query' if op=='google_exact_query' else 'period_export_query'; q['source_file_type']='xlsxstat'; q['file_type']='xlsx'
        store,currency,selected,opening,start,end=_r33_select_finance(q)
        rows=_r35_category_export_rows(store,selected,opening,start,end,q)
    if str(b.get('layout') or '')=='category_compact':
        rows,notes=_r40_category_rows_without_description(rows); b['annotations']=notes if bool(b.get('include_annotations',True)) else {}
    b['rows']=rows; b['operation']='google_sheet_rows'
    return b


def process_google_job_r40(job):
    body=dict((job or {}).get('payload') or {})
    if str(body.get('operation') or '') in {'google_exact_query','google_period_query','google_tabl_query'}:
        body=_r40_prepare_google_query(body); job=dict(job); job['payload']=body
    return _process_google_job_core(job)

def process_google_job_r42(job):
    jid=str((job or {}).get('id') or '')
    held=_r42_acquire_slots(1)
    try:
        print(f'[R45 GOOGLE START] {jid} rss={_r42_mem_mb()}MB',flush=True)
        return process_google_job_r40(job)
    finally:
        _r42_release_slots(held)
        print(f'[R45 GOOGLE END] {jid} rss={_r42_mem_mb()}MB',flush=True)

# R48: worker threads start only after final R43 owners are defined.



# ---------------------------------------------------------------------------
# Пер-R43: non-blocking two-phase admission when HEAVY has no Redis.
# If FAST proves that its peer outbox is durable in Redis, HEAVY may queue the
# idempotent job immediately and return a provisional ACK.  FAST keeps replaying
# the same job_id until the real callback, so an HEAVY restart cannot lose work.
# When FAST has no durable remote outbox, the old synchronous Redis/MEGA admission
# remains the safety fallback.
_R41_PROVISIONAL_FILE_LOCK = threading.RLock()
_R41_PROVISIONAL_GOOGLE_LOCK = threading.RLock()


def _r41_front_durable(body):
    return bool(isinstance(body,dict) and body.get('front_outbox_durable') and str(body.get('front_outbox_backend') or '')=='redis')


def _r41_heavy_redis_live():
    try:
        c=_redis_client()
        return c is not None and bool(c.ping())
    except Exception:
        return False


def internal_export_file_r41():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    if _r41_front_durable(body) and str(body.get('front_release') or '') != 'Пер-R43':
        return {'ok':False,'error':'R42 release barrier: redeploy FAST R42; stale peer job rejected','job_id':str(body.get('job_id') or '')[:80]},409
    if (not _r41_front_durable(body)) or _r41_heavy_redis_live():
        return internal_export_file_r40()
    try: cid=int(body.get('recipient_chat_id') or 0)
    except Exception: cid=0
    if not cid:return {'ok':False,'error':'recipient_chat_id required'},400
    rows=body.get('rows') or []
    if not isinstance(rows,list) or len(rows)>100000:return {'ok':False,'error':'invalid/too many rows'},400
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]; body['job_id']=jid
    with _R41_PROVISIONAL_FILE_LOCK:
        existing=_r35_job_get(jid) or {}
        canonical=str(existing.get('canonical_job_id') or existing.get('duplicate_of') or '')[:80]
        if canonical:
            return {'ok':True,'status':'alias','job_id':jid,'canonical_job_id':canonical,'duplicate_of':canonical,'provisional':True,'durable':False,'durable_backend':'front-redis'},202
        if existing:
            status=str(existing.get('status') or 'queued'); job=existing.get('job') if isinstance(existing.get('job'),dict) else None
            if job and status in {'queued','running','ready','delivering'}:
                if status in {'queued','running'}:
                    _r35_enqueue_file(job)
                elif bool(existing.get('ok')):
                    p=Path(str(existing.get('path') or ''))
                    if p.is_file():
                        _r35_enqueue_result({'job':job,'ok':True,'extra':{'url':existing.get('url') or '','filename':existing.get('filename') or ''}})
                    else:
                        # Process/container restart lost the generated local file. Rebuild
                        # the SAME job_id from FAST's durable payload.
                        existing.update({'status':'queued','ok':None,'path':'','callback_delivered':False,'updated_at':time.time()})
                        _r36_local_job_put(jid,existing); _file_status_put(jid,status='queued',ok=None,path='',callback_delivered=False)
                        _r35_enqueue_file(job); status='queued'
                return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'provisional':True,'durable':False,'durable_backend':'front-redis'},202
            if status in {'delivered','done'} and existing.get('ok') is True:
                return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'provisional':False,'durable':True,'durable_backend':'completed'},200
            if status in {'failed','closed'} and existing.get('ok') is False:
                return {'ok':False,'duplicate':True,'status':status,'job_id':jid,'error':str(existing.get('error') or 'HEAVY job failed')[:500],'provisional':False,'durable':True,'durable_backend':'completed'},200
        canonical=_r40_find_file_canonical(body,jid)
        if canonical:
            rec={'job_id':jid,'status':'alias','ok':True,'duplicate_of':canonical,'canonical_job_id':canonical,'durable_backend':'front-redis','job':{'id':jid,'type':'file_export','created_at':time.time(),'payload':body},'updated_at':time.time()}
            _r36_local_job_put(jid,rec); _file_status_put(jid,status='alias',ok=True,duplicate_of=canonical,canonical_job_id=canonical,durable_backend='front-redis')
            return {'ok':True,'duplicate':True,'status':'alias','job_id':jid,'canonical_job_id':canonical,'duplicate_of':canonical,'provisional':True,'durable':False,'durable_backend':'front-redis'},202
        job={'id':jid,'type':'file_export','created_at':time.time(),'payload':body}
        rec={'job_id':jid,'status':'queued','ok':None,'job':job,'recipient_chat_id':cid,'target_chat_id':body.get('target_chat_id'),'durable_backend':'front-redis','front_owned':True,'updated_at':time.time()}
        _r36_local_job_put(jid,rec)
        _file_status_put(jid,status='queued',ok=None,recipient_chat_id=cid,target_chat_id=body.get('target_chat_id'),durable_backend='front-redis',front_owned=True)
        queued=_r35_enqueue_file(job)
        print(f'[R43 FILE PROVISIONAL] {jid} op={body.get("operation")} queued={int(bool(queued))} front=redis',flush=True)
        return {'ok':True,'status':'queued','job_id':jid,'queue_size':FILE_Q.qsize(),'queued_now':bool(queued),'provisional':True,'durable':False,'durable_backend':'front-redis'},202


app.view_functions['internal_export_file_r7']=internal_export_file_r41


def _r42_google_redeliver(jid,rec,job):
    try:
        status=str((rec or {}).get('status') or '')
        if status=='ready' and (rec or {}).get('url'):
            if _notify_front_google_result_r38(job,True,url=str((rec or {}).get('url') or '')):
                rec=dict(rec);rec.update({'status':'delivered','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
        elif status=='failed' and (rec or {}).get('error'):
            if _notify_front_google_result_r38(job,False,error=str((rec or {}).get('error') or '')):
                rec=dict(rec);rec.update({'status':'closed','callback_delivered':True,'delivered_at':time.time()});_r38_google_put(jid,rec,pending=False)
    except Exception:
        pass

def internal_google_sheet_r41():
    if not authorized():return {'ok':False},404
    body=request.get_json(silent=True) or {}
    if _r41_front_durable(body) and str(body.get('front_release') or '') != 'Пер-R43':
        return {'ok':False,'error':'R42 release barrier: redeploy FAST R42; stale Google job rejected','job_id':str(body.get('job_id') or '')[:80]},409
    if (not _r41_front_durable(body)) or _r41_heavy_redis_live():
        return internal_google_sheet_r38()
    try:body['spreadsheet_id']=_sheet_id(body.get('spreadsheet_id'))
    except Exception as exc:return {'ok':False,'error':str(exc)[:600]},400
    try:cid=int(body.get('recipient_chat_id') or 0)
    except Exception:cid=0
    if not cid:return {'ok':False,'error':'recipient_chat_id required'},400
    jid=str(body.get('job_id') or secrets.token_hex(12)).strip()[:80]; body['job_id']=jid
    with _R41_PROVISIONAL_GOOGLE_LOCK:
        existing=_r38_google_get(jid) or {}
        if existing:
            status=str(existing.get('status') or 'queued'); job=existing.get('job') if isinstance(existing.get('job'),dict) else None
            if job and status in {'queued','running'}:_r38_google_enqueue(job)
            if job and status in {'ready','failed'}:
                threading.Thread(target=_r42_google_redeliver,args=(jid,dict(existing),job),daemon=True,name='r42-google-redeliver-'+jid[:8]).start()
            if status in {'delivered','closed'}:
                return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'url':str(existing.get('url') or ''),'provisional':False,'durable':True,'durable_backend':'completed'},200
            return {'ok':True,'duplicate':True,'status':status,'job_id':jid,'url':str(existing.get('url') or ''),'provisional':True,'durable':False,'durable_backend':'front-redis'},202
        job={'id':jid,'type':'google_sheet','created_at':time.time(),'payload':body}
        rec={'job_id':jid,'status':'queued','ok':None,'job':job,'recipient_chat_id':cid,'durable_backend':'front-redis','front_owned':True,'updated_at':time.time()}
        _r38_google_local_put(jid,rec); queued=_r38_google_enqueue(job)
        print(f'[R45 GOOGLE PROVISIONAL] {jid} queued={int(bool(queued))} front=redis',flush=True)
        return {'ok':True,'status':'queued','job_id':jid,'queue_size':GOOGLE_Q.qsize(),'queued_now':bool(queued),'provisional':True,'durable':False,'durable_backend':'front-redis'},202


app.view_functions['internal_google_sheet']=internal_google_sheet_r41

try:
    print('[R45 TRANSPORT] two-phase front-owned admission enabled; HEAVY Redis missing no longer blocks on synchronous MEGA for FAST-durable jobs',flush=True)
except Exception:
    pass


try:
    print(f'[R45 RECOVERY] stale R39-R41 auto-replay disabled; front-owned jobs replay only from FAST; compute_slots={_R42_COMPUTE_SLOTS}',flush=True)
except Exception:
    pass

# ---------------------------------------------------------------------------
# R43 direct FAST -> HEAVY contract.
# HEAVY never guesses where the latest business data is. Before every state-dependent
# file/Google job it downloads one transactionally consistent SQLite gzip directly
# from FAST (/internal/split/state). This makes FAST the only data authority and turns
# HEAVY into a deterministic compute service.
def _r43_refresh_front_snapshot(job_id='', required_revision=0):
    """R45 direct snapshot fetch.

    FAST is authoritative.  Reuse the local mirror with HTTP 304 when its state token
    is unchanged.  For a changed snapshot, stream gzip directly into a temp SQLite
    file and perform only a cheap SQLite header/schema probe; the old full
    PRAGMA quick_check on every job was unnecessary and could add tens of seconds.
    """
    base=front_base(); secret=peer_secret()
    if not base or not secret:
        raise RuntimeError('R45 FRONT_SERVICE_URL/PEER_SHARED_SECRET not configured')
    with _R43_SNAPSHOT_LOCK:
        started=time.time()
        prior=str(_R43_SNAPSHOT_STATE.get('last_token') or '')
        force_full=str(job_id or '').startswith(('r44-test-','r45-test-'))
        headers={
            'X-Peer-Secret':secret,
            'X-R43-Job-Snapshot':'1',
            'User-Agent':'per-r45-direct-snapshot/'+str(job_id or '')[:24],
        }
        if prior and CACHE_DB.exists() and not force_full:
            headers['X-R45-If-State-Token']=prior
        try:
            r=requests.get(base+'/internal/split/state',headers=headers,timeout=(4,60),stream=True)
            if r.status_code==304 and prior and CACHE_DB.exists():
                token=str(r.headers.get('X-Split-State-Token','') or prior)
                _R43_SNAPSHOT_STATE.update({'last_ok':time.time(),'last_error':'','last_token':token,'reused':int(_R43_SNAPSHOT_STATE.get('reused') or 0)+1,
                                             'covered_revision':max(int(_R43_SNAPSHOT_STATE.get('covered_revision') or 0),int(required_revision or 0))})
                print(f'[R45 SNAPSHOT REUSE] job={str(job_id)[:24]} elapsed={time.time()-started:.2f}s token={token[:32]}',flush=True)
                return token
            if r.status_code!=200:
                detail=''
                try: detail=str((r.json() if r.content else {}).get('error') or (r.json() if r.content else {}).get('busy') or '')
                except Exception:
                    try:detail=(r.text or '')[:180]
                    except Exception:detail=''
                raise RuntimeError(f'FAST snapshot HTTP {r.status_code}: {detail[:220]}')
            tmp=CACHE_DB.with_name('r45_incoming_'+secrets.token_hex(6)+'.sqlite3')
            written=0
            try:
                r.raw.decode_content=False
                with gzip.GzipFile(fileobj=r.raw,mode='rb') as zin, open(tmp,'wb') as out:
                    while True:
                        chunk=zin.read(1024*1024)
                        if not chunk:break
                        out.write(chunk);written+=len(chunk)
                if written<4096:
                    raise RuntimeError(f'FAST snapshot too small: {written}')
                with open(tmp,'rb') as fh:
                    if fh.read(16)!=b'SQLite format 3\x00':
                        raise RuntimeError('FAST snapshot invalid SQLite header')
                con=None
                try:
                    con=sqlite3.connect(f'file:{tmp}?mode=ro',uri=True,timeout=2)
                    row=con.execute('PRAGMA schema_version').fetchone()
                    if row is None:raise RuntimeError('SQLite schema probe returned no row')
                    con.execute('SELECT name FROM sqlite_master LIMIT 1').fetchone()
                finally:
                    try:
                        if con is not None:con.close()
                    except Exception:pass
                CACHE_DB.parent.mkdir(parents=True,exist_ok=True)
                os.replace(tmp,CACHE_DB)
            finally:
                try:tmp.unlink(missing_ok=True)
                except Exception:pass
            token=str(r.headers.get('X-Split-State-Token','') or '')
            compressed=int(r.headers.get('Content-Length') or 0)
            _R43_SNAPSHOT_STATE.update({'last_ok':time.time(),'last_error':'','last_token':token,'fetches':int(_R43_SNAPSHOT_STATE.get('fetches') or 0)+1,'bytes':int(_R43_SNAPSHOT_STATE.get('bytes') or 0)+max(0,compressed),'sqlite_bytes':written,
                                         'covered_revision':max(int(_R43_SNAPSHOT_STATE.get('covered_revision') or 0),int(required_revision or 0))})
            print(f'[R45 SNAPSHOT] job={str(job_id)[:24]} sqlite={written}B wire={compressed or "?"}B elapsed={time.time()-started:.2f}s token={token[:32]}',flush=True)
            return token
        except Exception as exc:
            _R43_SNAPSHOT_STATE['last_error']=f'{type(exc).__name__}: {str(exc)[:220]}'
            raise
        finally:
            try:r.close()
            except Exception:pass

def _r51_mirror_covers_required_revision(body):
    """Reuse only a revision range proven by a prior exact FAST snapshot.

    Do not infer completeness from MAX(event revision): revisions are time_ns values and
    different shards may arrive independently.  `covered_revision` advances only when
    an exact /internal/split/state image (or its 304-equivalent) has covered that job's
    revision fence.
    """
    try: required=int((body or {}).get('required_revision') or 0)
    except Exception: required=0
    covered=int(_R43_SNAPSHOT_STATE.get('covered_revision') or 0)
    if required<=0 or not CACHE_DB.exists():
        return False,required,covered
    return bool(covered>=required),required,covered


def process_file_job_r43(job):
    jid=str((job or {}).get('id') or '')
    body=dict((job or {}).get('payload') or {})
    op=str(body.get('operation') or '')
    with _R43_JOB_LOCK:
        try:
            if _r34_state_dependent_operation(op):
                covered,required,current=_r51_mirror_covers_required_revision(body)
                if covered:
                    print(f'[R51 MIRROR REUSE] file job={jid[:24]} required={required} applied={current}',flush=True)
                    body['required_revision']=0
                    body['r51_mirror_reused']=True
                else:
                    _r43_refresh_front_snapshot(jid, required_revision=required)
                    body['required_revision']=0
                    body['r43_snapshot_direct']=True
                job=dict(job); job['payload']=body
            return _r39_process_file_job(job)
        finally:
            try:
                import gc as _r45_gc
                body.clear(); _r45_gc.collect()
            except Exception:pass

process_file_job=process_file_job_r43

def process_google_job_r43(job):
    jid=str((job or {}).get('id') or '')
    body=dict((job or {}).get('payload') or {})
    op=str(body.get('operation') or '')
    with _R43_JOB_LOCK:
        try:
            # google_sheet_rows already contains its complete row payload and must not
            # trigger a business-state snapshot at all.
            if op in {'google_exact_query','google_period_query','google_tabl_query'}:
                covered,required,current=_r51_mirror_covers_required_revision(body)
                if covered:
                    print(f'[R51 MIRROR REUSE] google job={jid[:24]} required={required} applied={current}',flush=True)
                    body['required_revision']=0
                    body['r51_mirror_reused']=True
                else:
                    _r43_refresh_front_snapshot(jid, required_revision=required)
                    body['required_revision']=0
                    body['r43_snapshot_direct']=True
                job=dict(job); job['payload']=body
            return process_google_job_r42(job)
        finally:
            try:
                import gc as _r45_gc
                body.clear(); _r45_gc.collect()
            except Exception:pass

process_google_job=process_google_job_r43

def google_loop():
    """Single Google queue consumer; final owner is already bound before startup."""
    while True:
        job=GOOGLE_Q.get(); jid=str((job or {}).get('id') or '')
        try:
            process_google_job(job)
        except Exception as exc:
            print(f'[R48 GOOGLE LOOP ERROR] {type(exc).__name__}: {str(exc)[:300]}',flush=True)
        finally:
            with _R38_GOOGLE_ENQUEUED_LOCK:
                _R38_GOOGLE_ENQUEUED.discard(jid)
            GOOGLE_Q.task_done()

def _r63_seed_shared_redis_from_front():
    """Ensure shared Redis has a full baseline without blocking FAST UI.

    State events alone cannot rebuild an empty database.  On HEAVY startup, if Redis
    is enabled but the full snapshot key is absent, fetch one exact FAST snapshot,
    gzip the local mirror, and store it in Redis.
    """
    client=_redis_client()
    if client is None:
        return
    try:
        if client.exists(_REDIS_SNAPSHOT_KEY):
            return
    except Exception as exc:
        print(f'[R63 REDIS SEED] exists error={type(exc).__name__}: {str(exc)[:180]}',flush=True)
        return
    try:
        _r43_refresh_front_snapshot('r63-redis-seed')
        ok,detail,meta=_gzip_cache_db_v267()
        if not ok:
            raise RuntimeError(detail)
        rok,rdetail=redis_store_snapshot(CACHE_LATEST,meta,clear_deltas=True)
        print(f'[R63 REDIS SEED] ok={rok} {rdetail}',flush=True)
    except Exception as exc:
        print(f'[R63 REDIS SEED] error={type(exc).__name__}: {str(exc)[:240]}',flush=True)

_R48_WORKERS_STARTED=False
_R48_WORKERS_START_LOCK=threading.Lock()

def _start_final_workers():
    """Start queues only after final R43 file/Google owners exist."""
    global _R48_WORKERS_STARTED
    with _R48_WORKERS_START_LOCK:
        if _R48_WORKERS_STARTED:
            return
        _R48_WORKERS_STARTED=True
    if _R43_DIRECT_HEAVY:
        threading.Thread(target=file_loop,name='per-r48-worker-files-1',daemon=True).start()
        threading.Thread(target=_r35_result_loop,name='per-r48-result-1',daemon=True).start()
        threading.Thread(target=_r35_result_loop,name='per-r48-result-2',daemon=True).start()
        threading.Thread(target=google_loop,name='per-r48-worker-google',daemon=True).start()
        threading.Thread(target=peer_loop,name='per-r48-worker-peer',daemon=True).start()
        if _redis_client() is not None:
            try:
                rok,rdetail=redis_load_snapshot_to_cache()
                print(f'[R63 REDIS BOOT] snapshot ok={rok} {rdetail}',flush=True)
                if rok:
                    eok,edetail=_r32_replay_state_events_from_redis(limit=50000)
                    print(f'[R63 REDIS BOOT] events ok={eok} {edetail}',flush=True)
            except Exception as exc:
                print(f'[R63 REDIS BOOT] error={type(exc).__name__}: {str(exc)[:220]}',flush=True)
            threading.Thread(target=_r63_seed_shared_redis_from_front,name='r63-redis-seed',daemon=True).start()
            threading.Thread(target=_checkpoint_loop_v267,name='r63-redis-checkpoint',daemon=True).start()
            if mega_enabled():
                threading.Thread(target=_r32_mega_event_loop,name='r63-redis-mega-archive',daemon=True).start()
        print('[R63 HEAVY] direct FAST compute + shared Redis mirror/recovery',flush=True)
        return

    # Legacy non-direct mode is kept only for explicit emergency configuration.
    threading.Thread(target=file_loop,name='per-r48-worker-files-1',daemon=True).start()
    threading.Thread(target=file_loop,name='per-r48-worker-files-2',daemon=True).start()
    for idx in range(1,5):
        threading.Thread(target=_r35_result_loop,name=f'per-r48-result-{idx}',daemon=True).start()
    threading.Thread(target=_capsule_mega_loop_r20,name='vys262-worker-capsule-mega-r20',daemon=True).start()
    threading.Thread(target=_event_redis_flush_loop_v270,name='vys262-worker-event-redis-r15',daemon=True).start()
    threading.Thread(target=_r32_mega_event_loop,name='per-r32-mega-events',daemon=True).start()
    threading.Thread(target=worker_loop,name='vys262-worker-jobs',daemon=True).start()
    threading.Thread(target=google_loop,name='per-r48-worker-google',daemon=True).start()
    threading.Thread(target=peer_loop,name='per-r48-worker-peer',daemon=True).start()
    try:
        _r6_redis_ok, _r6_redis_detail = redis_load_snapshot_to_cache()
        print(f'[R6 RESTORE CACHE] redis ok={_r6_redis_ok} {_r6_redis_detail}', flush=True)
    except Exception as _r6_exc:
        print(f'[R6 RESTORE CACHE] redis error={type(_r6_exc).__name__}: {str(_r6_exc)[:180]}', flush=True)
    threading.Thread(target=_r35_recover_loop,name='per-r36-job-recovery',daemon=True).start()
    threading.Thread(target=_r38_google_recover_loop,name='per-r38-google-recovery',daemon=True).start()
    threading.Thread(target=_restore_refresh_background,name='vys262-worker-mega-warmup',daemon=True).start()
    threading.Thread(target=_checkpoint_loop_v267,name='vys262-worker-checkpoint-r13',daemon=True).start()
    threading.Thread(target=_event_reconcile_loop_v268,name='vys262-worker-events-r13',daemon=True).start()
    threading.Thread(target=_reconcile_hash_loop_v268,name='vys262-worker-reconcile-r13',daemon=True).start()

# R43 file admission accepts only current FAST jobs for the direct protocol.
def internal_export_file_r43():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    if str(body.get('front_release') or '')!='Пер-R43':
        return {'ok':False,'error':'R43 release barrier: deploy FAST R43','job_id':str(body.get('job_id') or '')[:80]},409
    return internal_export_file_r41()
app.view_functions['internal_export_file_r7']=internal_export_file_r43

def internal_google_sheet_r43():
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}
    if str(body.get('front_release') or '')!='Пер-R43':
        return {'ok':False,'error':'R43 release barrier: deploy FAST R43','job_id':str(body.get('job_id') or '')[:80]},409
    return internal_google_sheet_r41()
app.view_functions['internal_google_sheet']=internal_google_sheet_r43

print('[R45 DIRECT] FAST is sole data authority; HEAVY pulls fresh SQLite before every data job; Redis/MEGA job-state daemons are not required',flush=True)

def _r43_bootstrap_snapshot_loop():
    # During HEAVY-first deploy the previous FAST instance is still live. Seed one
    # local mirror for emergency rolling-deploy restore, then stop. No endless sync.
    time.sleep(2.0)
    delay=1.0
    for _ in range(8):
        try:
            _r43_refresh_front_snapshot('startup')
            print('[R43 BOOTSTRAP] local mirror seeded from FAST',flush=True)
            return
        except Exception as exc:
            _R43_SNAPSHOT_STATE['last_error']=f'bootstrap {type(exc).__name__}: {str(exc)[:180]}'
            time.sleep(delay); delay=min(15.0,delay*1.8)
    print('[R43 BOOTSTRAP] FAST snapshot unavailable; will fetch on first real job',flush=True)

if _R43_DIRECT_HEAVY:
    print('[R45 BOOTSTRAP] eager snapshot skipped; first real data job pulls FAST state',flush=True)

# ---------------------------------------------------------------------------
# R44 DIAGNOSTIC INTEROP API
# Isolated from production job transport.  It lets FAST prove, step by step,
# what HEAVY can see and return: HTTP, reverse HEAVY->FAST, shared Redis,
# fresh FAST snapshot and MEGA folder/file access.
import posixpath as _r44_posixpath
_R44_TEST_KEY_PREFIX='per:r44:test:'
_R44_TEST_REDIS_LOCK=threading.RLock()
_R44_TEST_REDIS_CLIENT=None
_R44_TEST_REDIS_ERROR=''

def _r44_test_redis_client():
    global _R44_TEST_REDIS_CLIENT,_R44_TEST_REDIS_ERROR
    with _R44_TEST_REDIS_LOCK:
        if _R44_TEST_REDIS_CLIENT is not None:
            try:
                if _R44_TEST_REDIS_CLIENT.ping(): return _R44_TEST_REDIS_CLIENT
            except Exception: _R44_TEST_REDIS_CLIENT=None
        if _redis is None:
            _R44_TEST_REDIS_ERROR='redis package unavailable'; return None
        url=str(redis_effective_url() or '').strip()
        if not url:
            _R44_TEST_REDIS_ERROR='REDIS_URL not configured'; return None
        try:
            c=_redis.Redis.from_url(url,decode_responses=False,socket_connect_timeout=2,socket_timeout=3)
            if not c.ping(): raise RuntimeError('PING false')
            _R44_TEST_REDIS_CLIENT=c;_R44_TEST_REDIS_ERROR='';return c
        except Exception as exc:
            _R44_TEST_REDIS_ERROR=f'{type(exc).__name__}: {str(exc)[:180]}';return None

def _r44_test_redis_handshake():
    nonce=str(request.headers.get('X-R44-Redis-Test','') or '').strip()[:80]
    if not nonce:return {'requested':False,'ok':None}
    c=_r44_test_redis_client()
    if c is None:return {'requested':True,'ok':False,'error':_R44_TEST_REDIS_ERROR}
    key=_R44_TEST_KEY_PREFIX+nonce
    try:
        raw=c.get(key)
        if isinstance(raw,bytes):raw=raw.decode('utf-8','replace')
        obj=json.loads(raw or '{}') if raw else {}
        front_seen=str(obj.get('side') or '')=='front'
        ack={'side':'heavy','front_seen':front_seen,'heavy_ts':time.time(),'worker':VERSION}
        c.setex(key,90,json.dumps(ack,separators=(',',':')))
        return {'requested':True,'ok':bool(front_seen),'front_seen':bool(front_seen)}
    except Exception as exc:return {'requested':True,'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:180]}'}

def _r44_test_wrap(payload,status=200):
    obj=dict(payload or {});obj.setdefault('ok',200<=int(status)<300);obj['redis_test']=_r44_test_redis_handshake();obj['r44_diag']=True
    print(f'[R44 TEST] path={request.path} status={status} redis={obj["redis_test"]}',flush=True)
    return obj,status

def _r44_safe_mega_path(raw,allow_file=True):
    root=_r44_posixpath.normpath(mega_root())
    text=str(raw or '').strip() or root
    if not text.startswith('/'): text=root.rstrip('/')+'/'+text.lstrip('/')
    path=_r44_posixpath.normpath(text)
    if path!=root and not path.startswith(root.rstrip('/')+'/'):
        raise ValueError('path outside configured MEGA root')
    if '..' in path.split('/'):
        raise ValueError('invalid MEGA path')
    return path

def _r44_mega_immediate(path):
    path=_r44_safe_mega_path(path)
    ok,detail=mega_login()
    if not ok:raise RuntimeError(detail)
    start=time.monotonic();dirs=[];files=[]
    # mega-find is already a production dependency in this worker.  Filter recursive
    # output to immediate children so the UI behaves like a folder browser.
    with MEGA_LOCK:
        dr=run_cmd(['mega-find',path,'--type=d'],timeout=env_int('R44_MEGA_TEST_TIMEOUT',45,10,180))
        fr=run_cmd(['mega-find',path,'--type=f'],timeout=env_int('R44_MEGA_TEST_TIMEOUT',45,10,180))
    def _rows(proc,kind):
        out=[]
        if proc.returncode!=0:return out
        for line in (proc.stdout or '').splitlines():
            item=str(line or '').strip().rstrip('/')
            if not item or item==path.rstrip('/'):continue
            if not item.startswith('/'):item=path.rstrip('/')+'/'+item.lstrip('/')
            item=_r44_posixpath.normpath(item)
            if _r44_posixpath.dirname(item)!=path.rstrip('/'):continue
            out.append({'type':kind,'name':_r44_posixpath.basename(item),'path':item})
        return out
    if dr.returncode!=0 and fr.returncode!=0:
        raise RuntimeError(((dr.stderr or dr.stdout or '')+' '+(fr.stderr or fr.stdout or '')).strip()[:400] or 'mega-find failed')
    dirs=_rows(dr,'dir');files=_rows(fr,'file')
    # Stable de-duplication in case MEGAcmd returns aliases twice.
    seen=set();entries=[]
    for e in sorted(dirs,key=lambda x:x['name'].casefold())+sorted(files,key=lambda x:x['name'].casefold()):
        key=(e['type'],e['path'])
        if key in seen:continue
        seen.add(key);entries.append(e)
        if len(entries)>=120:break
    parent=_r44_posixpath.dirname(path.rstrip('/')) or '/'
    root=_r44_posixpath.normpath(mega_root())
    if path==root:parent=''
    return {'path':path,'parent':parent,'entries':entries,'elapsed':round(time.monotonic()-start,3),'mega_root':root}

@app.route('/internal/restore/failed-tasks', methods=['POST'])
def internal_restore_failed_tasks():
    """Recreate v153 failed-task objects in MEGA. Runtime MEGA ownership stays on HEAVY."""
    if not authorized():
        return {'ok':False},404
    body = request.get_json(silent=True) or {}
    tasks = body.get('tasks') or []
    if not isinstance(tasks, list) or not tasks or len(tasks) > 50:
        return {'ok':False,'error':'tasks must contain 1..50 rows'},400
    work = Path(tempfile.mkdtemp(prefix='r49_failed_restore_'))
    restored = 0
    try:
        with MEGA_LOCK:
            ok, detail = prepare_mega_layout()
            if not ok:
                return {'ok':False,'error':detail},503
            task_root = mega_root().rstrip('/') + '/' + str(os.getenv('MEGA_TASK_BACKUP_DIR','tasks') or 'tasks').strip('/')
            remote_dir = task_root.rstrip('/') + '/failed'
            if not ensure_mega_dir(task_root) or not ensure_mega_dir(remote_dir):
                return {'ok':False,'error':'cannot prepare MEGA failed-task directory'},503
            for idx, task in enumerate(tasks):
                if not isinstance(task, dict):
                    return {'ok':False,'error':f'task[{idx}] is not an object','restored':restored},400
                key = str(task.get('_restore_key_v153') or task.get('task_id') or task.get('update_id') or task.get('job_id') or '').strip()
                if not key:
                    return {'ok':False,'error':f'task[{idx}] has no key','restored':restored},400
                safe_key = re.sub(r'[^A-Za-z0-9._-]+','_',key).strip('._-')[:96]
                if not safe_key:
                    safe_key = hashlib.sha256(key.encode('utf-8',errors='ignore')).hexdigest()[:24]
                clean = dict(task); clean.pop('_restore_key_v153',None)
                name = f'task_{safe_key}.json'
                local = work / name
                local.write_text(json.dumps(clean,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
                remote = remote_dir.rstrip('/') + '/' + name
                if mega_exists(remote):
                    rm = run_cmd(['mega-rm',remote],timeout=45)
                    if rm.returncode != 0 and mega_exists(remote):
                        return {'ok':False,'error':'cannot replace existing failed-task '+name,'restored':restored},502
                put = run_cmd(['mega-put',str(local),remote_dir],timeout=env_int('MEGA_TIMEOUT',180,30,900))
                if put.returncode != 0 or not mega_exists(remote):
                    return {'ok':False,'error':'mega-put failed-task '+name+': '+(put.stderr or put.stdout or '')[:160],'restored':restored},502
                restored += 1
        return {'ok':True,'restored':restored,'backend':'heavy-mega'},200
    finally:
        shutil.rmtree(work,ignore_errors=True)


def _r59_redis_quick_probe():
    if _redis is None:
        return False, 'redis package unavailable'
    url=str(redis_effective_url() or '').strip()
    if not url:
        return False, 'REDIS_URL empty after runtime enable'
    client=None
    try:
        client=_redis.Redis.from_url(url,socket_connect_timeout=0.8,socket_timeout=1.2,health_check_interval=30)
        ok=bool(client.ping())
        return (ok,'PING=PONG' if ok else 'PING failed')
    except Exception as exc:
        return False,f'{type(exc).__name__}: {str(exc)[:180]}'
    finally:
        try:
            if client is not None: client.close()
        except Exception: pass

@app.route('/internal/runtime/redis', methods=['GET','POST'])
def internal_runtime_redis():
    """R59 owner-controlled runtime Redis switch with a real short PING."""
    if not authorized():
        return {'ok':False},404
    global _REDIS_CLIENT, _R44_TEST_REDIS_CLIENT
    if request.method == 'POST':
        body = request.get_json(silent=True) or {}
        requested = bool(body.get('enabled'))

        # Close cached clients first so every transition is applied to a fresh socket.
        with _REDIS_LOCK:
            old = _REDIS_CLIENT
            _REDIS_CLIENT = None
        try:
            if old is not None: old.close()
        except Exception: pass
        try:
            with _R44_TEST_REDIS_LOCK:
                test_old = _R44_TEST_REDIS_CLIENT
                _R44_TEST_REDIS_CLIENT = None
            if test_old is not None:
                try: test_old.close()
                except Exception: pass
        except Exception: pass

        state = set_redis_runtime_enabled(requested)
        if requested:
            if not bool(state.get('enabled')):
                err=str(state.get('error') or 'HEAVY Redis was not enabled')[:240]
                return {'ok':False,'role':'heavy','redis':state,'error':err},409
            ping_ok,ping_detail=_r59_redis_quick_probe()
            if not ping_ok:
                state=set_redis_runtime_enabled(False)
                state['error']=ping_detail
                with STATE_LOCK:
                    STATE['redis_cache_ok']=False; STATE['redis_last_error']=ping_detail
                return {'ok':False,'role':'heavy','redis':state,'error':ping_detail},503
            state['ping']='PONG'
            with STATE_LOCK:
                STATE['redis_cache_ok']=True; STATE['redis_last_error']=''
        else:
            with STATE_LOCK:
                STATE['redis_cache_ok']=False; STATE['redis_last_error']='runtime Redis disabled by owner'
    else:
        state = redis_runtime_state()
    return {'ok':True,'role':'heavy','redis':state},200


def _r60_redis_decode(value, limit=220):
    if value is None:
        return ''
    if isinstance(value, (bytes, bytearray)):
        value = value.decode('utf-8', errors='replace')
    text = str(value).replace('\x00', '').replace('\r', ' ').replace('\n', ' ')
    # Never expose credentials/tokens in Telegram diagnostics.
    text = re.sub(r'(?i)(redis|rediss)://[^@\s]+@', r'\1://***@', text)
    text = re.sub(r'\b\d{6,12}:[A-Za-z0-9_-]{20,}\b', '<bot-token>', text)
    text = re.sub(r'(?i)(password|passwd|secret|token|authorization|cookie)["\'\s:=]+[^,}\]\s]{4,}', r'\1=<masked>', text)
    return text[:max(20, int(limit))]


def _r60_redis_sensitive_key(key):
    low = str(key or '').casefold()
    return any(x in low for x in ('password','passwd','secret','token','credential','authorization','cookie','session'))


def _r60_redis_key_row(client, key):
    name = key.decode('utf-8', errors='replace') if isinstance(key,(bytes,bytearray)) else str(key)
    try:
        typ = client.type(key)
        if isinstance(typ,(bytes,bytearray)): typ = typ.decode('utf-8',errors='replace')
        typ = str(typ or 'unknown')
    except Exception:
        typ = 'unknown'
    try: ttl_ms = int(client.pttl(key))
    except Exception: ttl_ms = -3
    try:
        mem = client.memory_usage(key)
        mem = int(mem or 0)
    except Exception:
        mem = 0
    preview=''; count=None
    sensitive=_r60_redis_sensitive_key(name)
    try:
        if typ == 'string':
            count=int(client.strlen(key) or 0)
            if sensitive:
                preview='<masked-sensitive-key>'
            elif count <= 8192:
                preview=_r60_redis_decode(client.get(key),180)
            else:
                preview=f'<string {count} bytes>'
        elif typ == 'hash':
            count=int(client.hlen(key) or 0)
            if sensitive:
                preview='<masked-sensitive-key>'
            else:
                cur, vals=client.hscan(key,0,count=3)
                bits=[]
                for k,v in list((vals or {}).items())[:3]:
                    bits.append(_r60_redis_decode(k,50)+'='+_r60_redis_decode(v,80))
                preview='; '.join(bits)
        elif typ == 'list':
            count=int(client.llen(key) or 0)
            if sensitive: preview='<masked-sensitive-key>'
            else: preview=' | '.join(_r60_redis_decode(x,90) for x in (client.lrange(key,0,2) or []))
        elif typ == 'set':
            count=int(client.scard(key) or 0)
            if sensitive: preview='<masked-sensitive-key>'
            else:
                cur, vals=client.sscan(key,0,count=3)
                preview=' | '.join(_r60_redis_decode(x,90) for x in list(vals or [])[:3])
        elif typ == 'zset':
            count=int(client.zcard(key) or 0)
            if sensitive: preview='<masked-sensitive-key>'
            else:
                vals=client.zrange(key,0,2,withscores=True) or []
                preview=' | '.join(_r60_redis_decode(v,80)+f' ({score:g})' for v,score in vals)
        elif typ == 'stream':
            count=int(client.xlen(key) or 0)
            preview='<stream entries>' if sensitive else f'<stream {count} entries>'
    except Exception as exc:
        preview=f'<preview {type(exc).__name__}>'
    return {'key':name[:240],'type':typ,'ttl_ms':ttl_ms,'bytes':mem,'count':count,'preview':preview[:220]}


def _r60_redis_prefix(name):
    parts=str(name or '').split(':')
    if not parts: return '(empty)'
    if parts[0]=='vysbot' and len(parts)>=3: return ':'.join(parts[:3])+':*'
    if parts[0]=='vys262' and len(parts)>=2: return ':'.join(parts[:2])+':*'
    if parts[0]=='per' and len(parts)>=4: return ':'.join(parts[:4])+':*'
    return ':'.join(parts[:min(3,len(parts))])+(':*' if len(parts)>3 else '')


@app.route('/internal/runtime/redis/inspect', methods=['GET'])
def internal_runtime_redis_inspect():
    """R60 owner Redis inspector. Read-only, bounded and secret-masked."""
    if not authorized():
        return {'ok':False},404
    state=redis_runtime_state()
    if not bool(state.get('master_enabled')):
        return {'ok':False,'error':'REDIS_ENABLED=0 in Render','redis':state},409
    if not bool(state.get('enabled')):
        return {'ok':False,'error':'Redis runtime is OFF','redis':state},409
    if _redis is None:
        return {'ok':False,'error':'redis package unavailable','redis':state},503
    url=str(redis_effective_url() or '').strip()
    if not url:
        return {'ok':False,'error':'REDIS_URL empty','redis':state},409
    try:
        page=max(0,min(99,int(request.args.get('page','0') or 0)))
        page_size=max(5,min(20,int(request.args.get('page_size','10') or 10)))
    except Exception:
        page=0; page_size=10
    client=None
    try:
        client=_redis.Redis.from_url(url,socket_connect_timeout=0.9,socket_timeout=1.5,health_check_interval=30)
        pong=bool(client.ping())
        info_mem=client.info('memory') or {}
        info_stats=client.info('stats') or {}
        info_clients=client.info('clients') or {}
        try: dbsize=int(client.dbsize() or 0)
        except Exception: dbsize=0
        keys=[]; cursor=0; truncated=False
        # Bound diagnostics even if Redis grows unexpectedly.
        for raw in client.scan_iter(match='*',count=200):
            keys.append(raw)
            if len(keys)>=500:
                truncated=True; break
        keys.sort(key=lambda x: (x.decode('utf-8',errors='replace') if isinstance(x,(bytes,bytearray)) else str(x)))
        prefix_counts={}
        decoded=[]
        for raw in keys:
            name=raw.decode('utf-8',errors='replace') if isinstance(raw,(bytes,bytearray)) else str(raw)
            decoded.append((name,raw))
            pref=_r60_redis_prefix(name); prefix_counts[pref]=int(prefix_counts.get(pref,0))+1
        total_seen=len(decoded)
        pages=max(1,(total_seen+page_size-1)//page_size)
        page=max(0,min(page,pages-1))
        chosen=decoded[page*page_size:(page+1)*page_size]
        entries=[_r60_redis_key_row(client,raw) for _,raw in chosen]
        top_prefix=sorted(prefix_counts.items(),key=lambda kv:(-kv[1],kv[0]))[:12]
        payload={
            'ok':True,'role':'heavy','redis':state,'ping':'PONG' if pong else 'FAIL',
            'dbsize':dbsize,'scanned':total_seen,'truncated':truncated,
            'page':page,'pages':pages,'page_size':page_size,'entries':entries,
            'prefixes':[{'prefix':k,'count':v} for k,v in top_prefix],
            'memory':{
                'used_memory':int(info_mem.get('used_memory') or 0),
                'used_memory_human':str(info_mem.get('used_memory_human') or ''),
                'maxmemory':int(info_mem.get('maxmemory') or 0),
                'maxmemory_human':str(info_mem.get('maxmemory_human') or ''),
            },
            'stats':{
                'keyspace_hits':int(info_stats.get('keyspace_hits') or 0),
                'keyspace_misses':int(info_stats.get('keyspace_misses') or 0),
                'evicted_keys':int(info_stats.get('evicted_keys') or 0),
                'expired_keys':int(info_stats.get('expired_keys') or 0),
                'connected_clients':int(info_clients.get('connected_clients') or 0),
            },
        }
        return payload,200
    except Exception as exc:
        return {'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:260]}','redis':state},503
    finally:
        try:
            if client is not None: client.close()
        except Exception: pass


@app.route('/internal/r44/test/status',methods=['GET'])
def r44_test_status():
    if not authorized():return {'ok':False},404
    rclient=_r44_test_redis_client();mega_cfg=bool(os.getenv('MEGA_SESSION') or (os.getenv('MEGA_EMAIL') and os.getenv('MEGA_PASSWORD')))
    mega_ok=False;mega_detail='not configured'
    if mega_cfg:
        try:mega_ok,mega_detail=mega_login()
        except Exception as exc:mega_detail=f'{type(exc).__name__}: {str(exc)[:160]}'
    payload={'ok':True,'role':'heavy','version':VERSION,'transport':TRANSPORT_VERSION,'direct_mode':bool(_R43_DIRECT_HEAVY),'front_configured':bool(front_base()),'front_url_host':re.sub(r'^https?://','',front_base()).split('/')[0] if front_base() else '', 'mega_configured':mega_cfg,'mega_ok':bool(mega_ok),'mega_detail':str(mega_detail)[:220],'mega_root':mega_root(),'redis_configured':bool(redis_runtime_state().get('configured')),'redis_runtime_enabled':bool(redis_runtime_state().get('enabled')),'redis_ok':rclient is not None,'redis_error':_R44_TEST_REDIS_ERROR,'file_queue':FILE_Q.qsize(),'google_queue':GOOGLE_Q.qsize(),'snapshot_state':dict(_R43_SNAPSHOT_STATE)}
    return _r44_test_wrap(payload,200)

@app.route('/internal/r44/test/echo',methods=['POST'])
def r44_test_echo():
    if not authorized():return {'ok':False},404
    body=request.get_json(silent=True) or {};nonce=str(body.get('nonce') or '')[:120]
    return _r44_test_wrap({'ok':True,'role':'heavy','nonce':nonce,'received_at':time.time(),'version':VERSION},200)

@app.route('/internal/r44/test/reverse',methods=['POST'])
def r44_test_reverse():
    if not authorized():return {'ok':False},404
    body=request.get_json(silent=True) or {};nonce=str(body.get('nonce') or '')[:120];base=front_base();secret=peer_secret();start=time.monotonic()
    if not base or not secret:return _r44_test_wrap({'ok':False,'error':'FRONT_SERVICE_URL/PEER_SHARED_SECRET missing','front_http_ok':False},503)
    try:
        r=requests.post(base+'/internal/r44/test/reverse',json={'nonce':nonce,'from':'heavy','ts':time.time()},headers={'X-Peer-Secret':secret,'User-Agent':'per-r44-heavy-reverse'},timeout=(4,12))
        try:p=r.json() if r.content else {}
        except Exception:p={'raw':(r.text or '')[:500]}
        ok=200<=r.status_code<300 and isinstance(p,dict) and str(p.get('nonce') or '')==nonce
        return _r44_test_wrap({'ok':ok,'front_http_ok':200<=r.status_code<300,'front_status':r.status_code,'front_reply':p,'elapsed_reverse':round(time.monotonic()-start,3)},200 if ok else 502)
    except Exception as exc:return _r44_test_wrap({'ok':False,'front_http_ok':False,'error':f'{type(exc).__name__}: {str(exc)[:300]}','elapsed_reverse':round(time.monotonic()-start,3)},502)

@app.route('/internal/r44/test/snapshot',methods=['POST'])
def r44_test_snapshot():
    if not authorized():return {'ok':False},404
    start=time.monotonic();nonce=str((request.get_json(silent=True) or {}).get('nonce') or '')[:40];before=int(_R43_SNAPSHOT_STATE.get('bytes') or 0)
    try:
        token=_r43_refresh_front_snapshot('r44-test-'+nonce)
        after=int(_R43_SNAPSHOT_STATE.get('bytes') or 0)
        return _r44_test_wrap({'ok':True,'token':token,'snapshot_bytes':max(0,after-before),'snapshot_state':dict(_R43_SNAPSHOT_STATE),'elapsed_snapshot':round(time.monotonic()-start,3)},200)
    except Exception as exc:return _r44_test_wrap({'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:500]}','elapsed_snapshot':round(time.monotonic()-start,3)},502)

@app.route('/internal/r44/test/mega/list',methods=['GET'])
def r44_test_mega_list():
    if not authorized():return {'ok':False},404
    try:
        obj=_r44_mega_immediate(request.args.get('path',''))
        return _r44_test_wrap({'ok':True,**obj},200)
    except Exception as exc:return _r44_test_wrap({'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:500]}','mega_root':mega_root()},502)

@app.route('/internal/r44/test/mega/file',methods=['GET'])
def r44_test_mega_file():
    if not authorized():return {'ok':False},404
    try:path=_r44_safe_mega_path(request.args.get('path',''))
    except Exception as exc:return {'ok':False,'error':str(exc)[:300]},400
    ok,detail=mega_login()
    if not ok:return {'ok':False,'error':detail[:300]},502
    work=Path(tempfile.mkdtemp(prefix='r44_mega_file_'))
    try:
        with MEGA_LOCK:p=run_cmd(['mega-get',path,str(work)],timeout=env_int('R44_MEGA_FILE_TIMEOUT',120,20,600))
        if p.returncode!=0:
            shutil.rmtree(work,ignore_errors=True);return {'ok':False,'error':(p.stderr or p.stdout or 'mega-get failed')[:500]},502
        candidates=[x for x in work.rglob('*') if x.is_file()]
        if not candidates:
            shutil.rmtree(work,ignore_errors=True);return {'ok':False,'error':'MEGA file not downloaded'},404
        local=candidates[0];size=local.stat().st_size;limit=max(1,min(100,int(os.getenv('R44_MEGA_FILE_MAX_MB','50') or '50')))*1024*1024
        if size>limit:
            shutil.rmtree(work,ignore_errors=True);return {'ok':False,'error':f'file too large for diagnostic transfer: {size} bytes > {limit}'},413
        resp=send_file(str(local),as_attachment=True,download_name=local.name,mimetype='application/octet-stream',conditional=False,max_age=0)
        resp.headers['X-R44-Mega-Path']=path[:500];resp.headers['X-R44-File-Size']=str(size)
        resp.call_on_close(lambda: shutil.rmtree(work,ignore_errors=True))
        _r44_test_redis_handshake()
        print(f'[R44 TEST] mega-file path={path} bytes={size}',flush=True)
        return resp
    except Exception as exc:
        shutil.rmtree(work,ignore_errors=True);return {'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:500]}'},500


# R65 manual recovery browser: owner-triggered read-only access to the whole MEGA
# account through HEAVY.  Normal automatic storage remains strictly locked to
# MEGA_BACKUP_DIR/mega_root(); these endpoints never write outside that root.
def _r65_manual_mega_path(raw):
    text=str(raw or '').strip() or '/'
    if not text.startswith('/'):
        text='/'+text.lstrip('/')
    path=_r44_posixpath.normpath(text)
    if not path.startswith('/'):
        path='/'+path
    if '..' in path.split('/'):
        raise ValueError('invalid MEGA recovery path')
    return path


def _r65_manual_mega_immediate(path):
    """List only the immediate children of an arbitrary MEGA recovery folder."""
    path=_r65_manual_mega_path(path)
    ok,detail=mega_login()
    if not ok:
        raise RuntimeError(detail)
    start=time.monotonic()
    with MEGA_LOCK:
        proc=run_cmd(['mega-ls','-l',path],timeout=env_int('R65_MEGA_BROWSE_TIMEOUT',60,10,240))
    if proc.returncode!=0:
        raise RuntimeError((proc.stderr or proc.stdout or 'mega-ls failed')[:500])
    entries=[]
    for raw in (proc.stdout or '').splitlines():
        line=str(raw or '').strip()
        if not line or line.upper().startswith('FLAGS ') or line.startswith('Versions of '):
            continue
        # MEGAcmd -l format: FLAGS VERS SIZE DATE TIME NAME (NAME may contain spaces).
        parts=line.split(None,5)
        if len(parts)<6:
            continue
        flags,name=parts[0],parts[5].strip()
        if not name or name in {'.','..'}:
            continue
        kind='dir' if flags.startswith('d') else 'file'
        child=(path.rstrip('/')+'/'+name) if path!='/' else '/'+name
        child=_r44_posixpath.normpath(child)
        entries.append({'type':kind,'name':name,'path':child})
    dedup={}
    for row in entries:
        dedup[(row.get('type'),row.get('path'))]=row
    entries=list(dedup.values())
    entries.sort(key=lambda r:(0 if r.get('type')=='dir' else 1,str(r.get('name') or '').casefold()))
    parent='/' if path=='/' else (_r44_posixpath.dirname(path.rstrip('/')) or '/')
    return {'path':path,'parent':parent,'entries':entries,'elapsed_mega':round(time.monotonic()-start,3),'configured_root':mega_root()}


@app.route('/internal/r65/mega/list',methods=['GET'])
def r65_manual_mega_list():
    if not authorized():
        return {'ok':False},404
    try:
        obj=_r65_manual_mega_immediate(request.args.get('path','/'))
        print(f"[R65 MEGA BROWSER] list path={obj.get('path')} entries={len(obj.get('entries') or [])}",flush=True)
        return {'ok':True,**obj},200
    except Exception as exc:
        return {'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:600]}','configured_root':mega_root()},502


@app.route('/internal/r65/mega/file',methods=['GET'])
def r65_manual_mega_file():
    if not authorized():
        return {'ok':False},404
    try:
        path=_r65_manual_mega_path(request.args.get('path',''))
        if path=='/':
            return {'ok':False,'error':'file path required'},400
    except Exception as exc:
        return {'ok':False,'error':str(exc)[:300]},400
    ok,detail=mega_login()
    if not ok:
        return {'ok':False,'error':detail[:400]},502
    work=Path(tempfile.mkdtemp(prefix='r65_mega_recovery_'))
    try:
        with MEGA_LOCK:
            proc=run_cmd(['mega-get',path,str(work)],timeout=env_int('R65_MEGA_FILE_TIMEOUT',240,30,900))
        if proc.returncode!=0:
            shutil.rmtree(work,ignore_errors=True)
            return {'ok':False,'error':(proc.stderr or proc.stdout or 'mega-get failed')[:700]},502
        candidates=[x for x in work.rglob('*') if x.is_file()]
        if not candidates:
            shutil.rmtree(work,ignore_errors=True)
            return {'ok':False,'error':'MEGA file not downloaded'},404
        local=candidates[0]
        size=local.stat().st_size
        limit=max(16,min(512,int(os.getenv('R65_MEGA_FILE_MAX_MB','256') or '256')))*1024*1024
        if size>limit:
            shutil.rmtree(work,ignore_errors=True)
            return {'ok':False,'error':f'file too large for recovery transfer: {size} > {limit}'},413
        resp=send_file(str(local),as_attachment=True,download_name=local.name,mimetype='application/octet-stream',conditional=False,max_age=0)
        resp.headers['X-R65-Mega-Path']=path[:700]
        resp.headers['X-R65-File-Size']=str(size)
        resp.call_on_close(lambda: shutil.rmtree(work,ignore_errors=True))
        print(f'[R65 MEGA BROWSER] file path={path} bytes={size}',flush=True)
        return resp
    except Exception as exc:
        shutil.rmtree(work,ignore_errors=True)
        return {'ok':False,'error':f'{type(exc).__name__}: {str(exc)[:700]}'},500

print('[R45 TEST] diagnostic API ready: status/echo/reverse/snapshot/mega-list/mega-file; Redis optional/test-only; probes do not block FAST callbacks',flush=True)


print('[R65 STABLE] FAST authority; Redis recovery; priority-nav compatible; manual all-MEGA recovery browser ready; automatic MEGA root remains strict',flush=True)

if __name__ == '__main__':
    _start_final_workers()
    port=env_int('PORT',10000,1,65535)
    try:
        from waitress import serve as _r39_waitress_serve
        threads=env_int('HEAVY_HTTP_THREADS',8,4,32)
        print(f'[R45 HTTP] waitress host=0.0.0.0 port={port} threads={threads}',flush=True)
        _r39_waitress_serve(app,host='0.0.0.0',port=port,threads=threads,channel_timeout=120,cleanup_interval=15)
    except Exception as exc:
        print(f'[R45 WAITRESS FALLBACK] {type(exc).__name__}: {str(exc)[:220]}',flush=True)
        try:
            from werkzeug.serving import make_server as _r39_make_server
            print(f'[R45 HTTP] werkzeug-threaded fallback host=0.0.0.0 port={port}',flush=True)
            _r39_make_server('0.0.0.0',port,app,threaded=True).serve_forever()
        except Exception as exc2:
            print(f'[R41 HTTP EMERGENCY] {type(exc2).__name__}: {str(exc2)[:220]}',flush=True)
            app.run(host='0.0.0.0',port=port,threaded=True)
# v262
