# v262
#!/usr/bin/env python3
"""vys-262 Render #2 heavy worker.

Responsibilities:
- mutual peer health ping with Render #1;
- receive coalesced state sync jobs from front;
- fetch a consistent SQLite gzip snapshot and validate PRAGMA quick_check;
- promote/archive the latest durable SQLite snapshot in MEGA;
- serve cached latest snapshot to Render #1 for fast deploy restore;
- execute Google Sheets creation (OAuth + Sheets API) away from Telegram frontend.
"""
from __future__ import annotations
import base64
import gzip
import hashlib
import json
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
from flask import Flask, request, Response

app = Flask(__name__)
VERSION = 'vys-262-worker-r2'
TRANSPORT_VERSION = 'vys-262-worker-r2'


def env_bool(name, default=False):
    return str(os.getenv(name, '1' if default else '0') or '').strip().lower() in {'1','true','yes','on','да'}

def env_int(name, default, lo, hi):
    try: return max(lo, min(hi, int(os.getenv(name, str(default)) or str(default))))
    except Exception: return default

def peer_secret(): return str(os.getenv('PEER_SHARED_SECRET','') or '').strip()
def authorized():
    expected, supplied = peer_secret(), str(request.headers.get('X-Peer-Secret','') or '')
    return bool(expected and secrets.compare_digest(expected, supplied))
def front_base():
    raw = str(os.getenv('FRONT_SERVICE_URL', os.getenv('PEER_SERVICE_URL','')) or '').strip().rstrip('/')
    if raw and not raw.startswith(('http://','https://')): raw = 'https://' + raw
    return raw
def mega_root(): return '/' + str(os.getenv('MEGA_BACKUP_DIR','TelegramBotBackups2-2') or 'TelegramBotBackups2-2').strip('/')
def remote_db_dir(): return mega_root().rstrip('/') + '/database'
def remote_latest(): return remote_db_dir().rstrip('/') + '/latest_bot_state.sqlite3.gz'
def remote_history_dir(): return remote_db_dir().rstrip('/') + '/history'

STATE_LOCK = threading.RLock()
MEGA_LOCK = threading.RLock()
GOOGLE_LOCK = threading.RLock()
STATE = {
    'started_at': time.time(), 'peer_last_attempt':0.0, 'peer_last_ok':0.0, 'peer_last_error':'', 'peer_status':None,
    'job_last_id':'', 'job_last_type':'', 'job_last_reason':'', 'job_last_started':0.0, 'job_last_done':0.0, 'job_last_error':'',
    'sync_count':0, 'sync_failures':0, 'last_snapshot_sha256':'', 'last_snapshot_size':0, 'last_snapshot_at':0.0,
    'last_mega_upload_at':0.0, 'last_restore_download_at':0.0, 'mega_layout_ok':False, 'mega_warmup_ok':False,
    'google_jobs':0, 'google_failures':0, 'google_last_ok':0.0, 'google_last_error':'',
}
JOB_Q = queue.Queue(maxsize=32)
GOOGLE_Q = queue.Queue(maxsize=16)
GOOGLE_JOB_LOCK = threading.RLock()
GOOGLE_JOB_STATUS = {}
SYNC_PENDING_LOCK = threading.RLock(); SYNC_PENDING = False
RESTORE_REFRESH_LOCK = threading.RLock(); RESTORE_REFRESH_RUNNING = False
CACHE_DIR = Path(os.getenv('WORKER_CACHE_DIR','/tmp/vys262_worker') or '/tmp/vys262_worker'); CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_LATEST = CACHE_DIR / 'latest_bot_state.sqlite3.gz'
_GOOGLE_TOKEN = {'token':'','expires_at':0.0}


def run_cmd(args, timeout=120):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, check=False)


def mega_login():
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
    ok, detail = mega_login()
    if not ok: return False, detail
    for p in (mega_root(), remote_db_dir(), remote_history_dir()):
        if not ensure_mega_dir(p): return False, f'cannot create MEGA dir {p}'
    with STATE_LOCK: STATE['mega_layout_ok'] = True
    return True, 'MEGA layout ready'


def mega_legacy_roots():
    raw = str(os.getenv('MEGA_LEGACY_BACKUP_DIRS','/TelegramBotBackups-2T,/TelegramBotBackups') or '')
    out = []
    current = mega_root()
    for item in raw.split(','):
        item = '/' + str(item or '').strip().strip('/')
        if item != '/' and item != current and item not in out:
            out.append(item)
    return out


def mega_restore_candidates():
    rows = [remote_latest()]
    for root in mega_legacy_roots():
        rows.append(root.rstrip('/') + '/database/latest_bot_state.sqlite3.gz')
    return rows


def quick_check_gzip(gz_path: Path):
    work = Path(tempfile.mkdtemp(prefix='vys262_check_')); raw = work / 'state.sqlite3'
    try:
        with gzip.open(gz_path,'rb') as src, open(raw,'wb') as dst: shutil.copyfileobj(src,dst,1024*1024)
        con = sqlite3.connect(str(raw))
        try: row = con.execute('PRAGMA quick_check').fetchone()
        finally: con.close()
        if not row or str(row[0]).lower() != 'ok': return False, f'quick_check={row}', {}
        payload = gz_path.read_bytes(); return True, 'OK', {'sha256_gz':hashlib.sha256(payload).hexdigest(),'size':len(payload)}
    except Exception as exc: return False, f'{type(exc).__name__}: {str(exc)[:180]}', {}
    finally: shutil.rmtree(work, ignore_errors=True)


def fetch_front_snapshot():
    base, secret = front_base(), peer_secret()
    if not base or not secret: return None, 'front URL/secret not configured', {}
    timeout = env_int('WORKER_FRONT_FETCH_TIMEOUT',30,5,180)
    work = Path(tempfile.mkdtemp(prefix='vys262_front_fetch_')); path = work / 'snapshot.sqlite3.gz'
    try:
        r = requests.get(base + '/internal/split/state', headers={'X-Peer-Secret':secret,'User-Agent':'vys-262-worker-fetch'}, timeout=timeout, stream=True)
        if r.status_code != 200: return None, f'front HTTP {r.status_code}: {r.text[:180]}', {}
        with open(path,'wb') as fh:
            for chunk in r.iter_content(1024*1024):
                if chunk: fh.write(chunk)
        ok, detail, meta = quick_check_gzip(path)
        if not ok: return None, detail, meta
        # Caller owns the copied temp path, so move it out of disposable dir.
        fd, durable_name = tempfile.mkstemp(prefix='vys262_snapshot_', suffix='.sqlite3.gz'); os.close(fd)
        durable = Path(durable_name); shutil.copy2(path, durable)
        return durable, 'front snapshot OK', meta
    except Exception as exc: return None, f'front {type(exc).__name__}: {str(exc)[:180]}', {}
    finally: shutil.rmtree(work, ignore_errors=True)


def mega_promote_snapshot(local_gz: Path):
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
    snap, detail, meta = fetch_front_snapshot()
    if not snap: return False, detail
    try:
        ok, detail = mega_promote_snapshot(snap)
        if not ok: return False, detail
        shutil.copy2(snap, CACHE_LATEST)
        with STATE_LOCK:
            STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or ''); STATE['last_snapshot_size'] = int(meta.get('size') or 0)
            STATE['last_snapshot_at'] = time.time(); STATE['last_mega_upload_at'] = time.time(); STATE['sync_count'] += 1
        return True, f"synced {meta.get('size',0)} bytes sha={str(meta.get('sha256_gz') or '')[:12]}"
    finally: snap.unlink(missing_ok=True)


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
                shutil.copy2(rows[0], CACHE_LATEST)
                with STATE_LOCK:
                    STATE['last_restore_download_at'] = time.time()
                    STATE['last_snapshot_sha256'] = str(meta.get('sha256_gz') or '')
                    STATE['last_snapshot_size'] = int(meta.get('size') or 0)
                return CACHE_LATEST, 'MEGA latest OK' if idx == 0 else f'MEGA legacy restore cache OK: {remote}'
            finally:
                shutil.rmtree(work, ignore_errors=True)
        return None, last_detail


def process_job(job):
    global SYNC_PENDING
    jid, kind = str(job.get('id') or ''), str(job.get('type') or '')
    with STATE_LOCK:
        STATE['job_last_id']=jid; STATE['job_last_type']=kind; STATE['job_last_reason']=str(job.get('reason') or '')[:180]; STATE['job_last_started']=time.time(); STATE['job_last_error']=''
    try:
        if kind == 'sync_state': ok, detail = sync_state_job(job)
        else: ok, detail = False, f'unsupported job type: {kind}'
        with STATE_LOCK:
            STATE['job_last_done']=time.time(); STATE['job_last_error']='' if ok else detail[:260]
            if not ok: STATE['sync_failures'] += 1
        print(f'[WORKER JOB] {jid} {kind} ok={ok} {detail}', flush=True)
    finally:
        if kind == 'sync_state':
            with SYNC_PENDING_LOCK: SYNC_PENDING = False


def worker_loop():
    while True:
        job = JOB_Q.get()
        try: process_job(job)
        except Exception as exc:
            with STATE_LOCK: STATE['job_last_error']=f'{type(exc).__name__}: {str(exc)[:240]}'; STATE['sync_failures'] += 1
        finally: JOB_Q.task_done()


def _restore_refresh_background():
    global RESTORE_REFRESH_RUNNING
    try: _download_mega_latest()
    finally:
        with RESTORE_REFRESH_LOCK: RESTORE_REFRESH_RUNNING = False

def _schedule_restore_refresh():
    global RESTORE_REFRESH_RUNNING
    with RESTORE_REFRESH_LOCK:
        if RESTORE_REFRESH_RUNNING: return False
        RESTORE_REFRESH_RUNNING = True
    threading.Thread(target=_restore_refresh_background, name='vys262-worker-restore-refresh', daemon=True).start(); return True


def peer_loop():
    time.sleep(5)
    while True:
        base = front_base()
        with STATE_LOCK: STATE['peer_last_attempt'] = time.time()
        if base:
            try:
                r = requests.get(base + '/peer/health', headers={'User-Agent':'vys-262-worker-peer'}, timeout=12)
                with STATE_LOCK:
                    STATE['peer_status']=int(r.status_code)
                    if 200 <= r.status_code < 300: STATE['peer_last_ok']=time.time(); STATE['peer_last_error']=''
                    else: STATE['peer_last_error']=f'HTTP {r.status_code}'
            except Exception as exc:
                with STATE_LOCK: STATE['peer_status']=None; STATE['peer_last_error']=str(exc)[:220]
        else:
            with STATE_LOCK: STATE['peer_last_error']='FRONT_SERVICE_URL empty'
        time.sleep(600)


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
        r = requests.post(info.get('token_uri') or 'https://oauth2.googleapis.com/token', data={'grant_type':'urn:ietf:params:oauth:grant-type:jwt-bearer','assertion':assertion}, timeout=30)
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
    base = re.sub(r'[\\/?*\[\]:]', ' ', str(title or 'Статьи')); base = re.sub(r'\s+',' ',base).strip(" ' ") or 'Статьи'; suffix=' · '+datetime.now().strftime('%d.%m %H-%M-%S'); return base[:max(1,100-len(suffix))].rstrip()+suffix

def _cell_value(v):
    if isinstance(v,dict) and v.get('formula'): return {'formulaValue':'='+str(v.get('formula') or '').lstrip('=')}
    if isinstance(v,bool): return {'boolValue':v}
    if isinstance(v,(int,float)) and not isinstance(v,bool): return {'numberValue':float(v)}
    return {'stringValue':str(v or '')}

def _cat_fill(idx):
    palette=[(0.78,0.94,0.81),(0.87,0.92,0.97),(0.99,0.89,0.84),(0.89,0.87,0.93),(1.0,0.95,0.8),(0.85,0.92,0.83),(0.81,0.89,0.95),(0.96,0.8,0.8),(0.82,0.88,0.89),(0.92,0.82,0.86),(0.85,0.82,0.91)]
    rgb=palette[max(0,idx)%len(palette)]; return {'red':rgb[0],'green':rgb[1],'blue':rgb[2]}

def create_google_sheet(body):
    rows = body.get('rows') or []; layout = str(body.get('layout') or 'category').lower(); notes_raw = body.get('annotations') or {}; notes={}
    for key,val in notes_raw.items():
        try:
            r,c = key.split(',',1); notes[(int(r),int(c))] = str(val)
        except Exception: pass
    token = _google_token(); spreadsheet_id = _sheet_id(body.get('spreadsheet_id')); headers={'Authorization':f'Bearer {token}','Content-Type':'application/json'}
    info = _google_info(); meta = requests.get(f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}', headers=headers, params={'fields':'spreadsheetId,properties.title'}, timeout=45)
    if meta.status_code >= 300:
        if meta.status_code in (401,403): raise RuntimeError(f'Google Sheets target access {meta.status_code}; share table with {info.get("client_email")}: {meta.text[:400]}')
        raise RuntimeError(f'Google Sheets metadata {meta.status_code}: {meta.text[:400]}')
    max_cols=max((len(r) for r in rows), default=1); row_count=max(100,len(rows)+20); col_count=max(26,max_cols+3); title=_tab_title(body.get('title'))
    add = requests.post(f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate', headers=headers, json={'requests':[{'addSheet':{'properties':{'title':title,'gridProperties':{'rowCount':row_count,'columnCount':col_count,'frozenRowCount':1 if layout in {'compact','category_compact'} else 2}}}}]}, timeout=60)
    if add.status_code >= 300: raise RuntimeError(f'Google Sheets add tab {add.status_code}: {add.text[:500]}')
    try: sheet_id=int(add.json()['replies'][0]['addSheet']['properties']['sheetId'])
    except Exception as exc: raise RuntimeError(f'Google Sheets missing sheetId: {exc}')
    cell_rows=[]
    for r_idx,row in enumerate(rows,start=1):
        values=[]
        for c_idx in range(1,max_cols+1):
            value=row[c_idx-1] if c_idx-1 < len(row) else ''; cell={'userEnteredValue':_cell_value(value)}
            note=str(notes.get((r_idx,c_idx)) or '').strip()
            if note: cell['note']=note
            if r_idx == 1: cell['userEnteredFormat']={'textFormat':{'bold':True}}
            elif layout == 'category' and c_idx >= 4 and value not in ('',None): cell['userEnteredFormat']={'backgroundColor':_cat_fill(c_idx-4)}
            elif layout == 'category_compact' and c_idx >= 3 and value not in ('',None): cell['userEnteredFormat']={'backgroundColor':_cat_fill(c_idx-3)}
            elif layout == 'compact' and c_idx in (2,3) and value not in ('',None): cell['userEnteredFormat']={'backgroundColor':_cat_fill(c_idx-2)}
            values.append(cell)
        cell_rows.append({'values':values})
    reqs=[{'updateCells':{'range':{'sheetId':sheet_id,'startRowIndex':0,'startColumnIndex':0},'rows':cell_rows,'fields':'userEnteredValue,note,userEnteredFormat'}},{'autoResizeDimensions':{'dimensions':{'sheetId':sheet_id,'dimension':'COLUMNS','startIndex':0,'endIndex':max_cols}}}]
    upd = requests.post(f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate', headers=headers, json={'requests':reqs}, timeout=90)
    if upd.status_code >= 300: raise RuntimeError(f'Google Sheets update {upd.status_code}: {upd.text[:500]}')
    # Verify notes when requested, preserving original reliability semantics.
    expected={(r,c):n.strip() for (r,c),n in notes.items() if n.strip()}
    if expected:
        escaped=title.replace("'","''")
        verify=requests.get(f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}', headers=headers, params={'includeGridData':'true','ranges':f"'{escaped}'!A1:ZZ{max(1,len(rows))}",'fields':'sheets(data(rowData(values(note))))'}, timeout=60)
        if verify.status_code >= 300: raise RuntimeError(f'Google Sheets note verify {verify.status_code}: {verify.text[:500]}')
        actual={}
        try:
            row_data=((verify.json().get('sheets') or [{}])[0].get('data') or [{}])[0].get('rowData') or []
            for r0,row_obj in enumerate(row_data,start=1):
                for c0,cell in enumerate(row_obj.get('values') or [],start=1):
                    note=str(cell.get('note') or '').strip()
                    if note: actual[(r0,c0)] = note
        except Exception as exc: raise RuntimeError(f'Google Sheets note verify parse: {exc}')
        missing=[f'{r}:{c}' for (r,c),note in expected.items() if actual.get((r,c)) != note]
        if missing: raise RuntimeError(f'Google Sheets notes not confirmed: {missing[:12]}')
    return f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}'


@app.route('/', methods=['GET','HEAD'])
@app.route('/healthz', methods=['GET','HEAD'])
@app.route('/peer/health', methods=['GET','HEAD'])
def health():
    if request.method == 'HEAD': return '',200
    with STATE_LOCK: state=dict(STATE)
    return {'ok':True,'role':'worker','version':VERSION,'front_configured':bool(front_base()),'mega_configured':bool(os.getenv('MEGA_SESSION') or (os.getenv('MEGA_EMAIL') and os.getenv('MEGA_PASSWORD'))),'google_configured':bool(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')),'queue_size':JOB_Q.qsize(),'google_queue_size':GOOGLE_Q.qsize(),'state':state},200

@app.route('/internal/job', methods=['POST'])
def internal_job():
    global SYNC_PENDING
    if not authorized(): return {'ok':False},404
    body=request.get_json(silent=True) or {}; kind=str(body.get('type') or '').strip()
    if kind != 'sync_state': return {'ok':False,'error':'unsupported job type','supported':['sync_state']},400
    with SYNC_PENDING_LOCK:
        if SYNC_PENDING: return {'ok':True,'status':'coalesced','queue_size':JOB_Q.qsize()},202
        SYNC_PENDING=True
    job={'id':secrets.token_hex(8),'type':kind,'reason':str(body.get('reason') or '')[:180],'created_at':time.time()}
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


def process_google_job(job):
    jid = str(job.get('id') or '')
    body = dict(job.get('payload') or {})
    _google_status_put(jid, status='running')
    try:
        url = create_google_sheet(body)
        with STATE_LOCK:
            STATE['google_jobs'] += 1
            STATE['google_last_ok'] = time.time()
            STATE['google_last_error'] = ''
        _google_status_put(jid, status='done', ok=True, url=url)
        delivered = _notify_front_google_result(job, True, url=url)
        _google_status_put(jid, callback_delivered=bool(delivered))
        print(f'[GOOGLE JOB] {jid} ok=True callback={delivered}', flush=True)
    except Exception as exc:
        detail = f'{type(exc).__name__}: {str(exc)[:600]}'
        with STATE_LOCK:
            STATE['google_failures'] += 1
            STATE['google_last_error'] = detail[:240]
        _google_status_put(jid, status='done', ok=False, error=detail)
        delivered = _notify_front_google_result(job, False, error=detail)
        _google_status_put(jid, callback_delivered=bool(delivered))
        print(f'[GOOGLE JOB] {jid} ok=False callback={delivered} {detail}', flush=True)


def google_loop():
    while True:
        job = GOOGLE_Q.get()
        try:
            process_google_job(job)
        except Exception as exc:
            print(f'[GOOGLE LOOP ERROR] {type(exc).__name__}: {str(exc)[:300]}', flush=True)
        finally:
            GOOGLE_Q.task_done()


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
        meta = requests.get(f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}', headers=headers, params={'fields':'spreadsheetId,properties.title'}, timeout=30)
        if meta.status_code >= 300:
            if meta.status_code in (401,403):
                raise RuntimeError(f'Нет доступа к таблице. Расшарьте её {info.get("client_email")} как Редактору. Google: {meta.text[:300]}')
            raise RuntimeError(f'Google Sheets HTTP {meta.status_code}: {meta.text[:350]}')
        title = str((meta.json().get('properties') or {}).get('title') or '')
        return {'ok':True,'spreadsheet_id':spreadsheet_id,'title':title,'service_email':info.get('client_email')},200
    except Exception as exc:
        return {'ok':False,'error':str(exc)[:700]},502

@app.route('/internal/restore/latest', methods=['GET'])
def internal_restore_latest():
    if not authorized(): return {'ok':False},404
    cache_max_age=env_int('WORKER_RESTORE_CACHE_MAX_AGE_SEC',120,0,3600)
    use_cache=CACHE_LATEST.exists() and cache_max_age>0 and time.time()-CACHE_LATEST.stat().st_mtime<=cache_max_age
    if not use_cache:
        started=_schedule_restore_refresh(); return {'ok':False,'error':'restore cache not ready','refresh_queued':bool(started)},503
    payload=CACHE_LATEST.read_bytes(); resp=Response(payload,status=200,mimetype='application/gzip'); resp.headers['Content-Disposition']='attachment; filename="latest_bot_state.sqlite3.gz"'; resp.headers['X-Worker-Version']=TRANSPORT_VERSION; resp.headers['X-SHA256']=hashlib.sha256(payload).hexdigest(); return resp

@app.route('/internal/status', methods=['GET'])
def internal_status():
    if not authorized(): return {'ok':False},404
    with STATE_LOCK: return {'ok':True,'version':VERSION,'queue_size':JOB_Q.qsize(),'google_queue_size':GOOGLE_Q.qsize(),'state':dict(STATE)},200

threading.Thread(target=worker_loop,name='vys262-worker-jobs',daemon=True).start()
threading.Thread(target=google_loop,name='vys262-worker-google',daemon=True).start()
threading.Thread(target=peer_loop,name='vys262-worker-peer',daemon=True).start()
threading.Thread(target=_restore_refresh_background,name='vys262-worker-mega-warmup',daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=env_int('PORT',10000,1,65535),threaded=True)
# v262
