from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Iterator

from fastapi import Depends, FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from . import db, services
from .analysis import build_inventory_view, load_snapshot, pending_cleanup_map
from .config import Settings
from .errors import Conflict, DrillError
from .lsn import format_lsn
from .schemas import (
    ClusterUpdate,
    PauseSubscriptionRequest,
    PreviewConfirmRequest,
    PreviewRequest,
    SlotPatch,
    SlotUpsert,
    SubscriptionPatch,
    SubscriptionUpsert,
)


def _settings() -> Settings:
    return app.state.settings


def _open_connection(settings: Settings) -> sqlite3.Connection:
    conn = db.connect(settings)
    db.initialize(conn)
    return conn


def get_conn() -> Iterator[sqlite3.Connection]:
    settings: Settings = app.state.settings
    conn = _open_connection(settings)
    try:
        yield conn
    finally:
        db.close(conn)


@asynccontextmanager
async def lifespan(application: FastAPI):
    settings = Settings.from_env()
    application.state.settings = settings
    conn = _open_connection(settings)
    try:
        db.ensure_cluster(conn)
        if settings.seed_demo:
            db.seed(conn, reset=False)
        recovered = services.recover_interrupted(conn)
        application.state.recovered = recovered
    finally:
        db.close(conn)
    yield


app = FastAPI(
    title="PostgreSQL 复制槽清理演练 API",
    version="1.0.0",
    description="值班 DBA 的复制槽清理演练沙盘：录入 WAL 边界/槽/订阅位点，"
    "计算占用与可释放范围，生成清理预览；确认需回传指纹，任何漂移整次拒绝；"
    "故障注入与崩溃恢复均不会留下半完成删除。",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "演练入口", "description": "开始前先看 GET /api/inventory；本页可直接点 Try it out 操作。"},
        {"name": "库存录入", "description": "录入/修正 WAL 边界、复制槽与订阅确认位点。"},
        {"name": "槽清理", "description": "预览 → 确认。确认需回传预览指纹；任何漂移整次拒绝。"},
        {"name": "故障注入", "description": "注入订阅停顿/恢复、清理失败、提交前崩溃，用于演练演示。"},
        {"name": "状态", "description": "查询占用来源、待清理状态、阻断原因与事件流。"},
    ],
)


def _drill_error_handler(request, exc: DrillError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": exc.payload()})


def _validation_error_handler(request, exc) -> JSONResponse:
    """Keep the error envelope consistent with DrillError for 422s too."""
    errors = jsonable_encoder(
        [
            {
                "loc": list(item.get("loc", ())),
                "msg": item.get("msg"),
                "type": item.get("type"),
            }
            for item in exc.errors()
        ]
    )
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_failed",
                "message": "请求参数不合法",
                "detail": {"errors": errors},
            }
        },
    )


app.add_exception_handler(DrillError, _drill_error_handler)
app.add_exception_handler(RequestValidationError, _validation_error_handler)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/api/health", tags=["状态"])
def health(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    revision = conn.execute("SELECT revision FROM cluster WHERE id = 1").fetchone()
    return {
        "ok": True,
        "revision": int(revision["revision"]) if revision else None,
        "ttl_seconds": _settings().preview_ttl_seconds,
        "recovered_at_startup": getattr(app.state, "recovered", None),
    }


# ---------------------------------------------------------------------------
# Inventory: cluster, slots, subscriptions
# ---------------------------------------------------------------------------


@app.get("/api/cluster", tags=["库存录入"])
def read_cluster(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    row = conn.execute("SELECT wal_boundary_lsn, revision, updated_at FROM cluster WHERE id = 1").fetchone()
    return {
        "wal_boundary_lsn": format_lsn(int(row["wal_boundary_lsn"])),
        "revision": int(row["revision"]),
        "updated_at": int(row["updated_at"]),
    }


@app.put("/api/cluster", tags=["库存录入"])
def update_cluster(payload: ClusterUpdate, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.update_cluster_boundary(conn, payload.wal_boundary_lsn)


@app.get("/api/inventory", tags=["演练入口", "状态"])
def inventory(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    snapshot = load_snapshot(conn)
    view = build_inventory_view(snapshot, pending_cleanup_map(conn))
    view["faults"] = services.state_document(conn)["faults"]
    return view


@app.get("/api/slots", tags=["库存录入"])
def list_slots(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict[str, Any]]:
    return inventory(conn)["slots"]


@app.get("/api/slots/{name}", tags=["库存录入"])
def read_slot(name: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.get_slot(conn, name)


@app.post("/api/slots", status_code=201, tags=["库存录入"])
def create_slot(payload: SlotUpsert, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.create_slot(conn, payload.model_dump())


@app.patch("/api/slots/{name}", tags=["库存录入"])
def patch_slot(name: str, payload: SlotPatch, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.update_slot(conn, name, payload.model_dump(exclude_unset=True))


@app.delete("/api/slots/{name}", tags=["库存录入"])
def delete_slot(name: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """Direct delete for inventory correction; same safety gates as cleanup."""
    return services.delete_slot_direct(conn, name)


@app.get("/api/subscriptions", tags=["库存录入"])
def list_subscriptions(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM subscriptions ORDER BY name").fetchall()
    return [services.subscription_view(conn, row) for row in rows]


@app.post("/api/subscriptions", status_code=201, tags=["库存录入"])
def create_subscription(
    payload: SubscriptionUpsert, conn: sqlite3.Connection = Depends(get_conn)
) -> dict[str, Any]:
    return services.create_subscription(conn, payload.model_dump())


@app.get("/api/subscriptions/{name}", tags=["库存录入"])
def read_subscription(name: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.get_subscription(conn, name)


@app.patch("/api/subscriptions/{name}", tags=["库存录入"])
def patch_subscription(
    name: str, payload: SubscriptionPatch, conn: sqlite3.Connection = Depends(get_conn)
) -> dict[str, Any]:
    return services.update_subscription(conn, name, payload.model_dump(exclude_unset=True))


# ---------------------------------------------------------------------------
# Preview / confirm
# ---------------------------------------------------------------------------


@app.post("/api/previews", status_code=201, tags=["槽清理"])
def create_preview(payload: PreviewRequest, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """生成清理预览；被阻断的槽会明确给出阻断原因，但不会被清理。"""
    return services.create_preview(conn, _settings(), payload.slot_names, payload.note)


@app.get("/api/previews", tags=["槽清理"])
def list_previews(
    limit: int = Query(default=50, ge=1, le=200), conn: sqlite3.Connection = Depends(get_conn)
) -> list[dict[str, Any]]:
    return services.list_previews(conn, limit)


@app.get("/api/previews/{preview_id}", tags=["槽清理"])
def read_preview(preview_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.get_preview(conn, preview_id)


@app.post("/api/previews/{preview_id}/confirm", tags=["槽清理"])
def confirm_preview(
    preview_id: str,
    payload: PreviewConfirmRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> dict[str, Any]:
    """确认清理。

    必须回传预览响应中的 ``fingerprint``。若位点、槽状态或 WAL 边界在预览后
    发生变化，整次拒绝：不删除任何槽，返回 409 与逐字段差异。
    """
    return services.confirm_with_faults(conn, preview_id, payload.fingerprint, payload.note)


# ---------------------------------------------------------------------------
# Fault injection / demo
# ---------------------------------------------------------------------------


def _require_demo() -> None:
    if not _settings().demo_enabled:
        raise Conflict("demo_disabled", "演示接口已通过 ENABLE_DEMO=false 关闭")


@app.post("/api/demo/subscriptions/{name}/stall", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_stall(
    name: str,
    payload: PauseSubscriptionRequest | None = None,
    conn: sqlite3.Connection = Depends(get_conn),
) -> dict[str, Any]:
    """注入订阅停顿：槽仍为 active 且被 stalled 订阅引用，因此不可清理。"""
    return services.pause_subscription(conn, name, payload.note if payload else None)


@app.post("/api/demo/subscriptions/{name}/resume", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_resume(name: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    return services.resume_subscription(conn, name)


@app.post("/api/demo/cleanup-failure", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_cleanup_failure(
    slot_names: list[str] = Query(description="命中这些槽时清理事务回滚"),
    conn: sqlite3.Connection = Depends(get_conn),
) -> dict[str, Any]:
    return services.set_cleanup_failure(conn, slot_names)


@app.delete("/api/demo/cleanup-failure", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_cleanup_failure_clear(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    services.clear_cleanup_failure(conn)
    return {"cleanup_failure_slots": []}


@app.post("/api/demo/crash-before-commit", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_crash_arm(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """下一次确认在 COMMIT 前模拟进程死亡；事务回滚，槽与预览都保持原状。"""
    services.arm_crash_before_commit(conn)
    return {"crash_before_commit_armed": True}


@app.delete("/api/demo/crash-before-commit", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_crash_disarm(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    services.clear_crash_arm(conn)
    return {"crash_before_commit_armed": False}


@app.post("/api/demo/previews/{preview_id}/expire", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_expire_preview(preview_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """把预览的过期时间拨到过去，用于演示“过期确认被整次拒绝”。"""
    row = services.get_preview_row(conn, preview_id)
    if row["status"] != "preview":
        raise Conflict("preview_not_pending", f"预览状态为 {row['status']}，不能演示过期")
    now = db.utc_now()
    with db.transaction(conn):
        conn.execute("UPDATE previews SET expires_at = ? WHERE id = ?", (now - 1, preview_id))
        db.record_event(conn, "demo_expire_preview", preview_id=preview_id, at=now)
    return services.get_preview(conn, preview_id)


@app.post("/api/demo/recover", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_recover(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """执行与进程重启时相同的恢复扫描（applying → failed，过期 → expired）。"""
    return services.recover_interrupted(conn)


@app.post("/api/demo/reset", tags=["故障注入"], dependencies=[Depends(_require_demo)])
def demo_reset(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    """重建演示数据（会清空预览与事件）。"""
    return services.reseed(conn)


@app.get("/api/state", tags=["状态"])
def state(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
    doc = services.state_document(conn)
    inventory_view = build_inventory_view(load_snapshot(conn), doc["pending_cleanup"])
    return {
        "cluster": doc["cluster"],
        "faults": doc["faults"],
        "inventory": inventory_view,
        "pending_cleanup": doc["pending_cleanup"],
        "events": doc["events"],
    }


# ---------------------------------------------------------------------------
# Demo console (served outside /docs but links there)
# ---------------------------------------------------------------------------


@app.get("/demo", response_class=HTMLResponse, include_in_schema=False)
def demo_page() -> HTMLResponse:
    return HTMLResponse(DEMO_HTML)


DEMO_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>复制槽清理演练控制台</title>
<style>
  :root { color-scheme: dark; --bg:#0f1419; --panel:#171d24; --line:#2a3441; --fg:#dbe4ee;
          --muted:#8fa1b3; --accent:#4da3ff; --ok:#4ecb8d; --warn:#e8b339; --bad:#f06a6a; }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.55 ui-sans-serif,system-ui,"Noto Sans SC",sans-serif;
         background:var(--bg); color:var(--fg); }
  header { padding:20px 28px; border-bottom:1px solid var(--line); display:flex;
           gap:16px; align-items:baseline; flex-wrap:wrap; }
  h1 { font-size:20px; margin:0; }
  a { color:var(--accent); }
  main { padding:20px 28px; display:grid; gap:18px; grid-template-columns:minmax(0,1.4fr) minmax(0,1fr); }
  section { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:16px; }
  section.wide { grid-column:1 / -1; }
  h2 { font-size:15px; margin:0 0 10px; }
  button { background:#243244; color:var(--fg); border:1px solid var(--line); border-radius:7px;
           padding:7px 11px; cursor:pointer; margin:0 6px 6px 0; font-size:13px; }
  button:hover { border-color:var(--accent); }
  button.primary { background:#1f4f82; border-color:#2e6ca8; }
  button.danger { background:#4a2323; border-color:#6b3232; }
  input, select { background:#0d1218; color:var(--fg); border:1px solid var(--line);
                  border-radius:6px; padding:6px 8px; font:inherit; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }
  th { color:var(--muted); font-weight:600; }
  code, pre { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
  pre { background:#0b0f14; border:1px solid var(--line); border-radius:8px; padding:12px;
        overflow:auto; max-height:340px; white-space:pre-wrap; }
  .pill { display:inline-block; padding:1px 8px; border-radius:999px; font-size:11px; border:1px solid; }
  .ok { color:var(--ok); border-color:var(--ok); } .warn { color:var(--warn); border-color:var(--warn); }
  .bad { color:var(--bad); border-color:var(--bad); } .muted { color:var(--muted); }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:8px; }
  #log { margin:0; }
  @media (max-width: 960px) { main { grid-template-columns:1fr; } }
</style>
</head>
<body>
<header>
  <h1>PostgreSQL 复制槽清理演练控制台</h1>
  <span class="muted">Swagger 操作入口：<a href="/docs">/docs</a></span>
  <span class="muted">|</span>
  <a href="/api/inventory" target="_blank">原始库存 JSON</a>
  <a href="/api/state" target="_blank">状态与事件</a>
</header>
<main>
  <section>
    <h2>1. 当前库存与占用</h2>
    <div class="row">
      <button onclick="loadInventory()">刷新库存</button>
      <span id="summary" class="muted"></span>
    </div>
    <table id="slots"><thead><tr>
      <th>槽</th><th>占用 / 位点</th><th>引用</th><th>清理判断</th><th>待清理</th>
    </tr></thead><tbody></tbody></table>
  </section>

  <section>
    <h2>2. 演练操作</h2>
    <div class="row">
      <label>槽选择</label>
      <select id="slotSelect"></select>
      <button class="primary" onclick="createPreview()">生成预览</button>
    </div>
    <div class="row">
      <button onclick="changeBoundary()">改 WAL 边界（制造漂移）</button>
      <button onclick="expirePreview()">让预览过期</button>
    </div>
    <div class="row">
      <label>槽选择</label>
      <select id="stallSelect"></select>
      <button onclick="stallSub()">注入订阅停顿</button>
      <button onclick="resumeSub()">恢复订阅</button>
    </div>
    <div class="row">
      <label>槽选择</label>
      <select id="failSelect"></select>
      <button class="danger" onclick="injectFailure()">注入清理失败</button>
      <button onclick="clearFailure()">清除失败注入</button>
    </div>
    <div class="row">
      <button class="danger" onclick="armCrash()">注入提交前崩溃</button>
      <button onclick="disarmCrash()">解除崩溃注入</button>
      <button onclick="recover()">执行恢复扫描</button>
      <button class="danger" onclick="resetDemo()">重置演示数据</button>
    </div>
    <p class="muted" style="margin:10px 0 0">
      推荐演示路径：生成预览 → 改 WAL 边界 → 确认（整次拒绝）→ 重新预览 → 让预览过期 →
      确认（整次拒绝）→ 重新预览 → 注入清理失败 → 确认（失败保留，槽仍在）→
      清除注入 → 再确认（成功）。
    </p>
  </section>

  <section class="wide">
    <h2>3. 预览 / 结果</h2>
    <div id="previewBox" class="muted">尚未生成预览。</div>
  </section>

  <section class="wide">
    <h2>4. 请求日志</h2>
    <pre id="log" class="muted">等待操作…</pre>
  </section>
</main>
<script>
const $ = (id) => document.getElementById(id);
let currentPreview = null;

function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function log(title, data) {
  const time = new Date().toLocaleTimeString();
  $('log').textContent = `[${time}] ${title}\n` + JSON.stringify(data, null, 2) +
    "\n\n" + $('log').textContent;
}
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) { opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body); }
  const res = await fetch(path, opts);
  let data; try { data = await res.json(); } catch { data = null; }
  log(`${method} ${path} → ${res.status}`, data);
  return { res, data };
}
const short = (v) => v ? String(v).slice(0, 10) + '…' : '—';

function statusPill(s) {
  const map = { preview:'warn', applying:'warn', completed:'ok', failed:'bad',
                rejected:'bad', expired:'warn', cancelled:'muted' };
  return `<span class="pill ${map[s] || 'muted'}">${esc(s)}</span>`;
}

async function loadInventory() {
  const { data } = await api('GET', '/api/inventory');
  if (!data) return;
  $('summary').textContent =
    `WAL 边界 ${data.wal_boundary_lsn} · 槽 ${data.summary.slot_count} 个 · ` +
    `可清理 ${data.summary.eligible_count} · 被阻断 ${data.summary.blocked_count} · ` +
    `最早保留 ${data.summary.earliest_retained.lsn} (${data.summary.earliest_retained.retained_human})`;
  const tbody = $('slots').querySelector('tbody');
  tbody.innerHTML = data.slots.map(s => {
    const refs = s.subscriptions.length
      ? s.subscriptions.map(x => `${x.name} (${x.status})`).join('<br>')
      : '<span class="muted">—</span>';
    const judge = s.eligible
      ? '<span class="pill ok">可清理</span>'
      : s.blocking_reasons.map(r => `<span class="pill bad" title="${esc(r.source)}">${esc(r.code)}</span>`).join(' ')
        + `<div class="muted">${s.blocking_reasons.map(r => esc(r.message)).join('；')}</div>`;
    const pending = s.pending_cleanup
      ? `<span class="pill warn">${esc(s.pending_cleanup.status)}</span><br><code>${esc(short(s.pending_cleanup.preview_id))}</code>`
      : '<span class="muted">—</span>';
    return `<tr>
      <td><strong>${esc(s.name)}</strong><br><span class="muted">${esc(s.slot_type)} / ${esc(s.wal_status)}</span></td>
      <td><code>${esc(s.restart_lsn)}</code><br><span class="muted">${esc(s.occupancy.retained_human)} · active=${s.active} disabled=${s.disabled}</span></td>
      <td>${refs}</td><td>${judge}</td><td>${pending}</td></tr>`;
  }).join('');
  const opts = data.slots.map(s =>
    `<option value="${esc(s.name)}" ${s.eligible ? 'selected' : ''}>${esc(s.name)}${s.eligible ? '' : '（不可清理）'}</option>`
  ).join('');
  for (const id of ['slotSelect','stallSelect','failSelect']) {
    const old = $(id).value; $(id).innerHTML = opts;
    if (old) $(id).value = old;
  }
}

function renderPreview(p) {
  currentPreview = p;
  const plan = p.plan || {};
  const status = p.status;
  const result = p.result ? `<h3>执行结果</h3><pre>${esc(JSON.stringify(p.result, null, 2))}</pre>` : '';
  const rejection = p.reject_reason
    ? `<h3>拒绝/失败原因</h3><p class="bad">${esc(p.reject_reason)}</p>
       <pre>${esc(JSON.stringify(p.reject_detail, null, 2))}</pre>` : '';
  const detail = plan.slots || [];
  const table = detail.length ? `<table><thead><tr><th>槽</th><th>restart_lsn</th><th>占用</th><th>解绑订阅</th></tr></thead><tbody>` +
    detail.map(s => `<tr><td>${esc(s.name)}</td><td><code>${esc(s.restart_lsn)}</code></td>
      <td>${esc(s.retained_human)}</td>
      <td>${(plan.detach_subscriptions || []).filter(d => d.slot_name === s.name)
             .map(d => esc(d.name)).join(', ') || '<span class="muted">—</span>'}</td></tr>`).join('') +
    `</tbody></table>` : '';
  const canConfirm = ['preview','failed'].includes(status);
  $('previewBox').innerHTML = `
    <div class="row">预览 <code>${esc(p.id)}</code> ${statusPill(status)}
      ${p.expires_at ? `<span class="muted">过期于 ${new Date(p.expires_at*1000).toLocaleTimeString()}</span>` : ''}</div>
    <div class="row">指纹 <code>${esc(p.fingerprint)}</code></div>
    <div class="row">
      <button class="primary" ${canConfirm ? '' : 'disabled'} onclick="confirmPreview()">确认清理（回传指纹）</button>
    </div>
    <h3>清理预览</h3>
    ${table}
    <div class="row" style="margin-top:8px">
      <span>预计可释放：<strong>${esc(plan.reclaimable_range?.human || '—')}</strong>
      （${esc(plan.reclaimable_range?.from_lsn || '—')} → ${esc(plan.reclaimable_range?.to_lsn || '—')}）</span>
      <span class="muted">最早保留点：${esc(plan.earliest_retained?.lsn || '—')} · 清理后：${esc(plan.after_cleanup_earliest?.lsn || '—')}</span>
    </div>
    ${rejection}${result}`;
}

async function createPreview() {
  const name = $('slotSelect').value;
  const { data } = await api('POST', '/api/previews', { slot_names: [name], note: 'demo console' });
  if (data && data.id) renderPreview(data);
  await loadInventory();
}
async function confirmPreview() {
  if (!currentPreview) return;
  const { data } = await api('POST', `/api/previews/${currentPreview.id}/confirm`,
    { fingerprint: currentPreview.fingerprint, note: 'confirmed in demo' });
  if (data && data.preview) renderPreview(data.preview);
  else if (data) await refreshPreview();
  await loadInventory();
}
async function refreshPreview() {
  if (!currentPreview) return;
  const { data } = await api('GET', `/api/previews/${currentPreview.id}`);
  if (data && data.id) renderPreview(data);
}
async function expirePreview() {
  if (!currentPreview) return;
  const { data } = await api('POST', `/api/demo/previews/${currentPreview.id}/expire`);
  if (data && data.id) renderPreview(data);
}
async function changeBoundary() {
  const { data } = await api('GET', '/api/cluster');
  if (!data) return;
  const n = BigInt('0x' + data.wal_boundary_lsn.replace('/', '')) + 1024n * 1024n;
  const lsn = (n >> 32n).toString(16).toUpperCase() + '/' + (n & 0xffffffffn).toString(16).toUpperCase();
  await api('PUT', '/api/cluster', { wal_boundary_lsn: lsn });
  await loadInventory();
}
async function stallSub() {
  const slot = $('stallSelect').value;
  const inv = await (await fetch('/api/inventory')).json();
  const sub = (inv.subscriptions || []).find(s => s.slot_name === slot && s.status !== 'disabled');
  if (!sub) return log('无在线订阅可停顿', { slot });
  await api('POST', `/api/demo/subscriptions/${sub.name}/stall`, { note: 'demo stall' });
  await loadInventory();
}
async function resumeSub() {
  const slot = $('stallSelect').value;
  const inv = await (await fetch('/api/inventory')).json();
  const sub = (inv.subscriptions || []).find(s => s.slot_name === slot && s.status === 'stalled');
  if (!sub) return log('无停顿订阅可恢复', { slot });
  await api('POST', `/api/demo/subscriptions/${sub.name}/resume`);
  await loadInventory();
}
async function injectFailure() {
  await api('POST', `/api/demo/cleanup-failure?slot_names=${encodeURIComponent($('failSelect').value)}`);
}
async function clearFailure() { await api('DELETE', '/api/demo/cleanup-failure'); }
async function armCrash() { await api('POST', '/api/demo/crash-before-commit'); }
async function disarmCrash() { await api('DELETE', '/api/demo/crash-before-commit'); }
async function recover() { await api('POST', '/api/demo/recover'); await loadInventory(); }
async function resetDemo() {
  if (!confirm('重置演示数据？会清空预览与事件。')) return;
  await api('POST', '/api/demo/reset');
  currentPreview = null; $('previewBox').textContent = '演示数据已重置。';
  await loadInventory();
}
loadInventory();
</script>
</body>
</html>
"""
