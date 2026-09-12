#!/usr/bin/env python3
from __future__ import annotations
import ast, hashlib, json, py_compile, re, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent
ROLE='fast' if (ROOT/'bot.py').exists() else 'heavy' if (ROOT/'worker_service.py').exists() else 'unknown'
errors=[]; warnings=[]; checks=[]

def ok(name, cond, detail=''):
    checks.append((name,bool(cond),detail))
    if not cond: errors.append(f'{name}: {detail or "failed"}')

def text(name): return (ROOT/name).read_text(encoding='utf-8',errors='replace')
def count(pattern,s,flags=0): return len(re.findall(pattern,s,flags))

def compile_all():
    bad=[]
    for p in ROOT.rglob('*.py'):
        if '__pycache__' in p.parts: continue
        try: py_compile.compile(str(p),doraise=True)
        except Exception as exc: bad.append(f'{p.relative_to(ROOT)}: {exc}')
    ok('python_compile',not bad,'; '.join(bad[:5]))

compile_all()
for req in ['PROJECT_RULES.md','PATCH_PROTOCOL.md','FINALIZATION_REPORT.md','INFO/BOT_MAP.md','FINALIZATION_GATE.py']:
    ok('required_'+req,(ROOT/req).is_file(),req)

if ROLE=='fast':
    manifest=json.loads(text('modules_manifest.json'))
    files=manifest.get('files') or {}; markers=manifest.get('file_markers') or {}
    hash_bad=[]; marker_bad=[]
    for rel,sha in files.items():
        p=ROOT/rel
        if not p.is_file(): hash_bad.append(rel+':missing'); continue
        got=hashlib.sha256(p.read_bytes()).hexdigest()
        if got!=sha: hash_bad.append(rel)
        lines=p.read_text(encoding='utf-8',errors='replace').splitlines()
        marker=str(markers.get(rel) or '')
        if marker and (not lines or marker not in lines[0] or marker not in lines[-1]): marker_bad.append(rel)
    ok('manifest_sha256',not hash_bad,','.join(hash_bad[:8]))
    ok('module_markers',not marker_bad,','.join(marker_bad[:8]))

    all_py={p.name:p.read_text(encoding='utf-8',errors='replace') for p in ROOT.glob('*.py')}
    hot_methods=('send_message','edit_message_text','edit_message_caption','edit_message_reply_markup','delete_message','send_document','answer_callback_query','process_new_updates')
    binds=[]
    for fn,s in all_py.items():
        for m in hot_methods:
            for mat in re.finditer(rf'(?m)^\s*bot\.{re.escape(m)}\s*=',s): binds.append((fn,m))
    ok('telegram_bindings_only_89',all(fn=='89_callback_final.py' for fn,_ in binds),str(binds))
    ok('telegram_bindings_count',len(binds)==8,f'count={len(binds)} {binds}')

    runtime_py={k:v for k,v in all_py.items() if k!='FINALIZATION_GATE.py'}
    joined='\n'.join(runtime_py.values())
    runtime_ids=set()
    for src in runtime_py.values():
        try:
            tree=ast.parse(src)
            runtime_ids.update(n.id for n in ast.walk(tree) if isinstance(n,ast.Name))
            runtime_ids.update(n.name for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)))
        except Exception:
            pass
    for banned in ['PREV_PROCESS_NEW_UPDATES','ORIGINAL_PROCESS_NEW_UPDATES','_canon_execute_telegram_payload__001','_V263_BASE_EXECUTE_TELEGRAM_PAYLOAD','_canon_mega_run__001','ORIG_MEGA_RUN']:
        ok('absent_'+banned,banned not in runtime_ids,banned)
    ok('one_public_execute',count(r'(?m)^def _execute_telegram_payload\(',joined)==1,'expected one final execute')
    ok('one_execute_core',count(r'(?m)^def _execute_telegram_payload_core\(',joined)==1,'expected one execute core')
    ok('one_mega_public',count(r'(?m)^def _mega_run\(',joined)==1,'expected one _mega_run')
    ok('one_mega_raw',count(r'(?m)^def _mega_exec_raw\(',joined)==1,'expected one _mega_exec_raw')
    ok('one_extension_public',count(r'(?m)^def v149_extension_callback\(',joined)==1,'expected one final extension dispatcher')
    ok('no_extension_rebind',count(r'(?m)^\s*v149_extension_callback\s*=',joined)==0,'extension must be a direct def, not alias')
    ok('message_wrapper_v172_removed','def _v172_install_message_input_wrapper' not in joined,'old on_any_message wrapper installer')
    ok('message_wrapper_v174_removed','def _v174_install_message_wrapper' not in joined,'old on_any_message wrapper installer')
    def assigned_ids(src):
        try:
            tree=ast.parse(src); out=set()
            for n in ast.walk(tree):
                if isinstance(n,(ast.Assign,ast.AnnAssign,ast.NamedExpr)):
                    tgts=n.targets if isinstance(n,ast.Assign) else [n.target]
                    for t in tgts:
                        if isinstance(t,ast.Name): out.add(t.id)
            return out
        except Exception:return set()
    for mod,label in [('100_r33_heavy_offload.py','r100_no_prev_orig_base'),('98_split_front.py','r98_no_prev_orig_base')]:
        bad=sorted(x for x in assigned_ids(all_py.get(mod,'')) if re.search(r'_(?:PREV|ORIG|BASE)_',x))
        ok(label,not bad,','.join(bad[:8]))

    hot_public=['submit_interactive_file_job','v163_webhook_select_lane','contour_callback_guard','build_main_keyboard','v149_extension_callback','_r38_outbox_dispatch_one','_r38_outbox_enqueue','_r38_outbox_pending_rows']
    bad_globals=[]
    for fn,s in all_py.items():
        for name in hot_public:
            if re.search(rf"globals\(\)\[['\"]{re.escape(name)}['\"]\]\s*=",s): bad_globals.append((fn,name))
    ok('no_hot_globals_rebind',not bad_globals,str(bad_globals))

    ns={}; exec(text('runtime_config.py'),ns)
    env=ns.get('FRONT_INTERNAL_ENV') or {}
    limits={'BOT_THREAD_STACK_KB':512,'UI_WORKERS':2,'FAST_UI_WORKERS':2,'WINDOW_RENDER_WORKERS':2,'CALLBACK_ACK_WORKERS':1,'UI_CLEANUP_WORKERS':1,'UI_DELETE_WORKERS':1,'DELTA_WORKERS':1,'BACKGROUND_WORKERS':1,'SCHEDULER_WORKERS':2,'R21_HEAVY_DISPATCH_WORKERS':2,'BOT_JOURNAL_MAX':600,'R32_EVENT_QUEUE_MAX':5000}
    bad=[]
    for k,maxv in limits.items():
        try:
            if int(env.get(k,10**9))>maxv: bad.append(f'{k}={env.get(k)}>{maxv}')
        except Exception: bad.append(f'{k}=invalid')
    ok('fast_memory_budget',not bad,'; '.join(bad))
    ok('state_events_contract','/internal/state/events' in joined,'state events endpoint/transport missing')

elif ROLE=='heavy':
    s=text('worker_service.py')
    tree=ast.parse(s)
    ok('heavy_no_prev_orig_base',not re.search(r'\b[A-Za-z0-9_]*_(?:PREV|ORIG|BASE)_[A-Za-z0-9_]*\b',s),'worker_service predecessor capture remains')
    ok('heavy_no_globals_rebind','globals()[' not in s,'worker_service should not monkey-patch globals')
    ok('heavy_final_file_owner',count(r'(?m)^process_file_job\s*=\s*process_file_job_r43\s*$',s)==1,'final R43 file owner')
    ok('heavy_final_google_owner',count(r'(?m)^process_google_job\s*=\s*process_google_job_r43\s*$',s)==1,'final R43 Google owner')
    ok('heavy_one_file_owner_assignment',count(r'(?m)^process_file_job\s*=',s)==1,'public file owner must be assigned exactly once')
    ok('heavy_one_google_owner_assignment',count(r'(?m)^process_google_job\s*=',s)==1,'public Google owner must be assigned exactly once')
    ok('heavy_one_file_loop',count(r'(?m)^def file_loop\(',s)==1,'expected one file_loop')
    ok('heavy_one_google_loop',count(r'(?m)^def google_loop\(',s)==1,'expected one google_loop')
    ok('heavy_no_r38_google_alias','process_google_job_r38' not in s,'obsolete Google owner alias remains')

    # Catch the exact class of Render crash that compileall cannot detect: a
    # module-level public owner assignment referencing a function defined later.
    seen=set(dir(__builtins__))
    owner_order_bad=[]
    for st in tree.body:
        if isinstance(st,ast.Assign):
            for target in st.targets:
                if isinstance(target,ast.Name) and target.id in {'process_file_job','process_google_job'}:
                    if isinstance(st.value,ast.Name) and st.value.id not in seen:
                        owner_order_bad.append(f'{target.id}={st.value.id}@{st.lineno}')
        if isinstance(st,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
            seen.add(st.name)
        elif isinstance(st,(ast.Import,ast.ImportFrom)):
            for a in st.names: seen.add(a.asname or a.name.split('.')[0])
        elif isinstance(st,ast.Assign):
            for target in st.targets:
                if isinstance(target,ast.Name): seen.add(target.id)
    ok('heavy_owner_definition_order',not owner_order_bad,','.join(owner_order_bad))

    # Worker threads must not start while the module is still defining final
    # owners. They are started only by _start_final_workers() from __main__.
    top_thread_starts=[]
    def has_thread_start(node):
        for n in ast.walk(node):
            if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef,ast.Lambda)) and n is not node:
                continue
            if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='start':
                base=n.func.value
                if isinstance(base,ast.Call) and isinstance(base.func,ast.Attribute) and isinstance(base.func.value,ast.Name) and base.func.value.id=='threading' and base.func.attr=='Thread':
                    return True
        return False
    for st in tree.body:
        if isinstance(st,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)): continue
        if has_thread_start(st): top_thread_starts.append(getattr(st,'lineno',0))
    ok('heavy_no_module_import_worker_start',not top_thread_starts,str(top_thread_starts))
    ok('heavy_final_starter','def _start_final_workers(' in s and "if __name__ == '__main__':\n    _start_final_workers()" in s,'final workers must start from executable entrypoint')
    docker=text('Dockerfile')
    ok('heavy_docker_startup_smoke','R49 HEAVY startup smoke PASS' in docker and 'import worker_service as w' in docker,'Docker build must execute startup import smoke')
    ok('heavy_state_events_route','/internal/state/events' in s,'state events endpoint missing')
    ns={}; exec(text('runtime_config.py'),ns); env=ns.get('WORKER_INTERNAL_ENV') or {}
    try: threads=int(env.get('HEAVY_HTTP_THREADS',999))
    except Exception: threads=999
    ok('heavy_http_threads',threads<=4,f'HEAVY_HTTP_THREADS={threads}')
    try: q=int(env.get('WORKER_EVENT_REDIS_QUEUE_MAX',999999))
    except Exception: q=999999
    ok('heavy_event_queue',q<=1024,f'WORKER_EVENT_REDIS_QUEUE_MAX={q}')
    ok('r61_redis_render_owned_master_and_start',
       '_REDIS_START_ENABLED = _render_flag("REDIS_START_ENABLED", False)' in text('runtime_config.py') and
       '_apply_redis_runtime_state(_REDIS_RENDER_ENABLED and _REDIS_START_ENABLED)' in text('runtime_config.py') and
       'def redis_effective_url' in text('runtime_config.py') and
       'os.environ["REDIS_ENABLED"]' not in text('runtime_config.py') and 'os.environ["REDIS_URL"]' not in text('runtime_config.py') and
       '/internal/runtime/redis' in s,
       'HEAVY Redis master/start/URL must remain Render-owned')
    ok('r60_redis_runtime_ping_and_inspector',
       'def _r59_redis_quick_probe' in s and "state['ping']='PONG'" in s and
       'global _REDIS_CLIENT, _R44_TEST_REDIS_CLIENT' in s and 'set_redis_runtime_enabled(False)' in s and
       "/internal/runtime/redis/inspect" in s and 'def _r60_redis_key_row' in s,
       'HEAVY Redis transition must close clients, PING and rollback on failure')
    ok('r59_render_env_snapshot',
       'def render_env_snapshot' in text('runtime_config.py'),
       'HEAVY must preserve raw Render ENV for diagnostics')
    ok('r56_state_events_respect_mega_master_switch',
       'def _r33_archive_events_direct' in s and "if mega_enabled():" in s and
       "durable='mega-direct'" in s and "durable='local-only-mega-disabled'" in s and
       "MEGA event durability unavailable" in s,
       'HEAVY must use MEGA durability only when MEGA_ENABLED=1 and local mirror when explicitly disabled')
    ok('r49_snapshot_sync_promote',
       "X-Snapshot-Promote-Mode" in s and 'mega_promote_snapshot(incoming)' in s and "'mega_promoted':True" in s,
       'exact snapshot sync promotion contract missing')
    ok('r49_failed_tasks_heavy_mega_owner',
       '/internal/restore/failed-tasks' in s and 'def internal_restore_failed_tasks' in s,
       'failed-task MEGA restoration endpoint missing')
    ok('r49_mega_autocreate_explicit',
       'MEGA_AUTOCREATE_LAYOUT' in s and 'MEGA ROOT RECREATED' in s and 'mega_root_recreated' in s and 'mega_layout_created_dirs' in s,
       'MEGA layout recreation must be explicit/observable')
    ok('r49_mega_autocreate_config',
       str(env.get('MEGA_AUTOCREATE_LAYOUT','')) == '1',
       f"MEGA_AUTOCREATE_LAYOUT={env.get('MEGA_AUTOCREATE_LAYOUT')}")
    ok('r49_worker_redis_snapshot_key',
       'vys262:bot_state:latest_gz' in s and 'redis_load_snapshot_to_cache' in s,
       'HEAVY emergency cache must understand the same Redis snapshot key')
else:
    errors.append('cannot detect role')

print(f'FINALIZATION_GATE role={ROLE}')
for name,passed,detail in checks:
    print(('PASS' if passed else 'FAIL')+f' {name}'+(f' :: {detail}' if detail and not passed else ''))
if errors:
    print(f'FINALIZATION_GATE FAILED ({len(errors)})')
    for e in errors: print(' - '+e)
    sys.exit(1)
print(f'FINALIZATION_GATE PASS ({len(checks)} checks)')
