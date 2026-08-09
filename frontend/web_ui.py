"""Web 渠道的页面。

页面单独成模块,让 `web.py` 只剩传输与路由——HTML/CSS/JS 内联在 Python 常量里时,
混在路由代码中间会让两者都难读。

零依赖是刻意的:不引入构建链(Reference 用 React+Vite+Tailwind),因为本项目的部署形态是
"一个 Python 进程 + 一份 config",加一条 node 构建链会让 `uv run python main.py` 不再是
完整的启动方式。代价是没有组件复用,所以页面刻意保持在"操作台"的复杂度,不做成 SPA。

注意:这些是普通字符串常量,不要对它们做 `%` 或 `.format()`——CSS 与 JS 里的 `{}` 会被吞掉。
"""

from __future__ import annotations

# 两个页面共用的设计令牌与基础排版。仪表盘是被扫读和操作的,不是被逐行阅读的,
# 所以强调:状态用形状(pill/条)编码而不只用数字;数字列一律 tabular-nums 对齐。
_BASE_CSS = """
:root {
  --bg: #F4F6F7; --panel: #FFFFFF; --ink: #17211F; --muted: #5F706C;
  --line: #DCE3E1; --line-soft: #EAEFEE;
  --accent: #0B7A66; --accent-soft: #E2F0EC;
  --ok: #0B7A66; --ok-soft: #E2F0EC;
  --warn: #9A5B12; --warn-soft: #F8EDDF;
  --crit: #A32F31; --crit-soft: #F8E5E5;
  --code: #EEF2F1;
  --shadow: 0 1px 2px rgba(23,33,31,.06);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #101817; --panel: #17211F; --ink: #DCE6E3; --muted: #8CA09B;
    --line: #263230; --line-soft: #1E2A28;
    --accent: #3DB89B; --accent-soft: #16302A;
    --ok: #3DB89B; --ok-soft: #16302A;
    --warn: #D99A4E; --warn-soft: #302517;
    --crit: #E0706F; --crit-soft: #331C1D;
    --code: #1C2725;
    --shadow: 0 1px 2px rgba(0,0,0,.28);
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--bg); color: var(--ink);
  font: 14px/1.6 "PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", -apple-system, "Segoe UI", sans-serif;
}
a { color: var(--accent); }
a:focus-visible, button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible {
  outline: 2px solid var(--accent); outline-offset: 2px;
}
button, input, select, textarea {
  font: inherit; color: var(--ink); background: var(--panel);
  border: 1px solid var(--line); border-radius: 6px; padding: 7px 10px;
}
button { cursor: pointer; }
button:hover { border-color: var(--accent); }
button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
button.danger { color: var(--crit); border-color: var(--crit); background: transparent; }
code, .mono { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .92em; }
.num { font-variant-numeric: tabular-nums; }
.pill {
  display: inline-block; padding: .1em .6em; border-radius: 99px;
  font-size: .78em; font-weight: 600; letter-spacing: .02em; white-space: nowrap;
}
.pill.ok { background: var(--ok-soft); color: var(--ok); }
.pill.warn { background: var(--warn-soft); color: var(--warn); }
.pill.crit { background: var(--crit-soft); color: var(--crit); }
.pill.mut { background: var(--code); color: var(--muted); }
.muted { color: var(--muted); }
"""

CHAT_HTML = (
    """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Kirakira Agent</title>
<style>"""
    + _BASE_CSS
    + """
body { height: 100vh; display: flex; flex-direction: column; }
header {
  display: flex; align-items: center; gap: 12px; padding: 12px 18px;
  border-bottom: 1px solid var(--line); background: var(--panel);
}
header .dot { width: 8px; height: 8px; border-radius: 99px; background: var(--muted); }
header .dot.live { background: var(--ok); }
header h1 { margin: 0; font-size: 15px; font-weight: 650; letter-spacing: .01em; }
header .spacer { flex: 1; }
#log { flex: 1; overflow-y: auto; padding: 20px 18px; }
.wrap { max-width: 860px; margin: 0 auto; display: flex; flex-direction: column; gap: 14px; }
.msg { display: flex; flex-direction: column; gap: 4px; }
.msg .who {
  font-size: .74em; letter-spacing: .12em; text-transform: uppercase; color: var(--muted);
}
.msg .body {
  padding: 10px 14px; border-radius: 8px; white-space: pre-wrap; word-break: break-word;
  border: 1px solid var(--line); background: var(--panel); box-shadow: var(--shadow);
}
.msg.user { align-items: flex-end; }
.msg.user .body { background: var(--accent-soft); border-color: transparent; }
.msg.agent .body { border-left: 3px solid var(--accent); }
.msg.error .body { border-color: var(--crit); color: var(--crit); }
.msg .tools { font-size: .8em; color: var(--muted); }
form { border-top: 1px solid var(--line); background: var(--panel); padding: 12px 18px; }
form .wrap { flex-direction: row; gap: 8px; align-items: flex-end; }
textarea { flex: 1; min-height: 44px; max-height: 200px; resize: vertical; }
.hint { font-size: .78em; color: var(--muted); padding: 0 18px 10px; text-align: center; }
</style>
</head>
<body>
<header>
  <span class="dot" id="dot"></span>
  <h1>Kirakira Agent</h1>
  <span class="pill mut mono" id="sid"></span>
  <span class="spacer"></span>
  <button id="stop">中断本轮</button>
  <a href="/dashboard"><button type="button">仪表盘</button></a>
</header>
<section id="log"><div class="wrap" id="stream"></div></section>
<form id="form">
  <div class="wrap">
    <textarea id="text" placeholder="输入消息，Enter 发送，Shift+Enter 换行"></textarea>
    <button class="primary" id="send">发送</button>
  </div>
</form>
<div class="hint">主动推送与 Drift 的消息会自动出现在这里</div>
<script>
const stream = document.querySelector("#stream");
const log = document.querySelector("#log");
const text = document.querySelector("#text");
const dot = document.querySelector("#dot");
const sessionId = localStorage.kirakiraSessionId || crypto.randomUUID();
localStorage.kirakiraSessionId = sessionId;
document.querySelector("#sid").textContent = sessionId.slice(0, 8);

function add(kind, who, value, meta) {
  const box = document.createElement("div");
  box.className = "msg " + kind;
  const label = document.createElement("div");
  label.className = "who";
  label.textContent = who;
  const body = document.createElement("div");
  body.className = "body";
  body.textContent = value;
  box.append(label, body);
  const tools = (meta && meta.tools_used) || [];
  if (tools.length) {
    const line = document.createElement("div");
    line.className = "tools mono";
    line.textContent = "工具: " + tools.join(", ");
    box.append(line);
  }
  stream.appendChild(box);
  log.scrollTop = log.scrollHeight;
}

async function send() {
  const value = text.value.trim();
  if (!value) return;
  text.value = "";
  add("user", "你", value);
  dot.classList.add("live");
  try {
    const resp = await fetch("/message", {
      method: "POST",
      headers: {"content-type": "application/json"},
      body: JSON.stringify({session_id: sessionId, text: value})
    });
    const data = await resp.json();
    if (data.error) add("error", "错误", data.error);
    else add("agent", "Kirakira", data.content || "(空回复)", data.metadata);
  } catch (err) {
    add("error", "错误", String(err));
  } finally {
    dot.classList.remove("live");
  }
}

document.querySelector("#form").addEventListener("submit", (e) => { e.preventDefault(); send(); });
text.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
});
document.querySelector("#stop").addEventListener("click", async () => {
  await fetch("/interrupt", {
    method: "POST",
    headers: {"content-type": "application/json"},
    body: JSON.stringify({session_id: sessionId})
  });
});

async function pollEvents() {
  while (true) {
    try {
      const resp = await fetch("/events?session_id=" + encodeURIComponent(sessionId));
      if (resp.ok) {
        const data = await resp.json();
        if (data.content) {
          const md = data.metadata || {};
          const who = md.drift ? "Drift" : (md.proactive ? "主动推送" : "Kirakira");
          add("agent", who, data.content, md);
        }
      }
    } catch (_) {}
    await new Promise(r => setTimeout(r, 800));
  }
}
pollEvents();
</script>
</body>
</html>
"""
)


DASHBOARD_HTML = (
    """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Kirakira 仪表盘</title>
<style>"""
    + _BASE_CSS
    + """
body { min-height: 100vh; display: grid; grid-template-columns: 208px 1fr; }
nav {
  border-right: 1px solid var(--line); background: var(--panel);
  padding: 18px 12px; display: flex; flex-direction: column; gap: 4px;
}
nav .brand { font-weight: 650; padding: 0 10px 14px; letter-spacing: .01em; }
nav button {
  border: none; background: transparent; text-align: left; padding: 8px 10px;
  border-radius: 6px; color: var(--muted); width: 100%;
}
nav button:hover { background: var(--code); border-color: transparent; }
nav button.active { background: var(--accent-soft); color: var(--accent); font-weight: 600; }
nav .foot { margin-top: auto; padding: 10px; font-size: .78em; }
main { padding: 22px 26px 60px; overflow-x: hidden; }
h2 { margin: 0 0 4px; font-size: 19px; font-weight: 650; }
.sub { color: var(--muted); margin: 0 0 18px; font-size: .9em; }
.panel {
  background: var(--panel); border: 1px solid var(--line); border-radius: 8px;
  padding: 14px 16px; margin-bottom: 14px; box-shadow: var(--shadow);
}
.panel h3 { margin: 0 0 10px; font-size: 13px; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); font-weight: 600; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 16px; }
.tile { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; box-shadow: var(--shadow); }
.tile .k { font-size: .76em; letter-spacing: .08em; text-transform: uppercase; color: var(--muted); }
.tile .v { font-size: 24px; font-weight: 650; margin-top: 2px; font-variant-numeric: tabular-nums; }
.tile .n { font-size: .82em; color: var(--muted); }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: .9em; min-width: 520px; }
th { text-align: left; font-weight: 600; color: var(--muted); font-size: .82em;
     letter-spacing: .06em; text-transform: uppercase; padding: 6px 10px 6px 0;
     border-bottom: 1px solid var(--line); white-space: nowrap; }
td { padding: 8px 10px 8px 0; border-bottom: 1px solid var(--line-soft); vertical-align: top; }
tr:last-child td { border-bottom: none; }
td.num, th.num { font-variant-numeric: tabular-nums; text-align: right; padding-right: 16px; }
.filters { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }
.filters input, .filters select { min-width: 120px; }
.clip { max-width: 520px; overflow: hidden; text-overflow: ellipsis; }
.rowlink { cursor: pointer; }
.rowlink:hover td { background: var(--code); }
pre { background: var(--code); border-radius: 6px; padding: 12px; overflow-x: auto;
      font-family: ui-monospace, Menlo, monospace; font-size: .84em; margin: 0; }
.empty { color: var(--muted); padding: 18px 0; text-align: center; }
.bar { display: flex; gap: 8px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; }
.bar .spacer { flex: 1; }
dialog { border: 1px solid var(--line); border-radius: 10px; background: var(--panel);
         color: var(--ink); max-width: 720px; width: 92vw; padding: 18px; }
dialog::backdrop { background: rgba(0,0,0,.4); }
@media (max-width: 720px) {
  body { grid-template-columns: 1fr; }
  nav { flex-direction: row; overflow-x: auto; border-right: none; border-bottom: 1px solid var(--line); }
  nav .brand, nav .foot { display: none; }
  main { padding: 16px; }
}
</style>
</head>
<body>
<nav>
  <div class="brand">Kirakira 仪表盘</div>
  <button data-tab="overview" class="active">总览</button>
  <button data-tab="memory">记忆</button>
  <button data-tab="sessions">会话</button>
  <button data-tab="recall">检索回放</button>
  <button data-tab="plugins">插件与代际</button>
  <button data-tab="proactive">主动与 Drift</button>
  <div class="foot muted">
    <a href="/">← 聊天</a><br />
    <span id="ws" class="mono"></span>
  </div>
</nav>
<main id="view"><div class="empty">加载中…</div></main>
<dialog id="modal"><div id="modalBody"></div>
  <div class="bar" style="margin-top:14px"><span class="spacer"></span>
    <button onclick="document.querySelector('#modal').close()">关闭</button></div>
</dialog>
<script>
const view = document.querySelector("#view");
const modal = document.querySelector("#modal");

const esc = (v) => String(v === null || v === undefined ? "" : v)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const get = (url) => fetch(url).then(r => r.json());
const clip = (v, n) => { const s = String(v || ""); return s.length > n ? s.slice(0, n) + "…" : s; };
const when = (v) => { if (!v) return "—"; const d = new Date(v); return isNaN(d) ? String(v) : d.toLocaleString(); };
const pill = (text, kind) => '<span class="pill ' + (kind || "mut") + '">' + esc(text) + "</span>";
const tile = (k, v, n) => '<div class="tile"><div class="k">' + esc(k) + '</div><div class="v">'
  + esc(v) + '</div><div class="n">' + (n || "") + "</div></div>";
const table = (heads, rows) => rows.length
  ? '<div class="scroll"><table><thead><tr>' + heads.map(h =>
      // 注意 h.label 可能是空串(占位列),不能用 || 回退,否则会渲染成 [object Object]
      // h.raw=true 时按 HTML 原样放入(用于全选复选框这类表头控件)
      '<th' + (h.num ? ' class="num"' : "") + ">"
      + (h.raw ? h.label : esc(h.label === undefined ? h : h.label)) + "</th>").join("")
    + "</tr></thead><tbody>" + rows.join("") + "</tbody></table></div>"
  : '<div class="empty">暂无数据</div>';

function openModal(html) { document.querySelector("#modalBody").innerHTML = html; modal.showModal(); }

const tabs = {};

tabs.overview = async () => {
  const d = await get("/api/dashboard/overview");
  document.querySelector("#ws").textContent = clip(d.workspace || "", 26);
  const m = d.memory || {}, p = d.proactive || {}, dr = d.drift || {}, pl = d.plugins || {}, rs = d.restart || {};
  return "<h2>总览</h2><p class=\\"sub\\">一屏看清引擎是否承重、三条链路是否在跑。</p>"
    + '<div class="tiles">'
    + tile("记忆条目", m.total ?? 0, m.load_bearing
        ? pill(m.engine + " · 承重", "ok") : pill("引擎未承重（词法回退）", "warn"))
    + tile("会话", d.sessions ?? 0, "")
    + tile("插件", pl.active ?? 0, (pl.errors ? pill(pl.errors + " 个错误", "crit") : "")
        + " " + (pl.generations ? pill(pl.generations + " 代际在册", "mut") : ""))
    + tile("主动推送", p.enabled ? "运行中" : "未启用",
        p.enabled ? (pill("未读 alert " + (p.unread_alert ?? 0), (p.unread_alert ? "warn" : "mut"))
          + " " + pill("content " + (p.unread_content ?? 0), "mut")) : "")
    + tile("Drift", dr.enabled ? (dr.runs ?? 0) + " 轮" : "未启用",
        dr.enabled ? "最近 " + when(dr.last_drift_at) : "")
    + tile("换代", rs.supervised ? "supervisor 托管" : "未托管",
        rs.supervised ? pill(rs.state || "idle", "ok") : pill("agent_restart 不可用", "mut"))
    + "</div>"
    + '<div class="panel"><h3>主动调度</h3>'
    + (p.enabled
        ? '<div class="scroll"><table><tbody>'
          + "<tr><td>目标</td><td class=\\"mono\\">" + esc(p.target || "—") + "</td></tr>"
          + "<tr><td>下次 tick 间隔</td><td class=\\"num\\">" + esc(p.next_interval_s ?? "—") + " 秒</td></tr>"
          + "<tr><td>推送冷却</td><td>" + (p.in_cooldown ? pill("冷却中", "warn") : pill("空闲", "ok")) + "</td></tr>"
          + "</tbody></table></div>"
        : '<div class="empty">未启用</div>')
    + "</div>";
};

tabs.memory = async () => {
  const q = new URLSearchParams(memState).toString();
  const [d, info] = await Promise.all([
    get("/api/dashboard/memories?" + q), get("/api/dashboard/memory/engine-info")
  ]);
  const rows = (d.memories || []).map(it =>
    '<tr class="rowlink" onclick="showMemory(\\'' + esc(it.id) + '\\')">'
    + '<td><input type="checkbox" class="msel" value="' + esc(it.id)
    + '" onclick="event.stopPropagation();syncSel()" /></td>'
    + "<td>" + pill(it.memory_type || it.kind || "—", "mut") + "</td>"
    + '<td class="clip">' + esc(clip(it.summary || it.content, 110)) + "</td>"
    + "<td>" + (String(it.status) === "active" ? pill("active", "ok") : pill(it.status || "—", "mut")) + "</td>"
    + '<td class="mono muted">' + esc(clip(it.source_ref || "", 18)) + "</td>"
    + '<td class="muted">' + esc(when(it.created_at)) + "</td></tr>");
  const pages = Math.max(1, Math.ceil((d.total || 0) / (d.page_size || 50)));
  return "<h2>记忆</h2><p class=\\"sub\\">经引擎的 admin 协议读取（"
    + esc(info.name || "—") + (info.load_bearing ? " · 承重" : " · 未承重") + "）。</p>"
    + '<div class="panel"><h3>引擎</h3>'
    + "<div>" + (info.capabilities || []).map(c => pill(c, "mut")).join(" ")
    + "</div><div class=\\"muted\\" style=\\"margin-top:8px\\">工具面: "
    + esc((info.tools || []).join(", ") || "—") + "</div></div>"
    + '<div class="panel">'
    + '<div class="filters">'
    + '<input id="mq" placeholder="搜索内容" value="' + esc(memState.q || "") + '" />'
    + '<select id="mtype"><option value="">全部类型</option>'
    + ["event", "profile", "preference", "procedure", "identity"].map(t =>
        '<option value="' + t + '"' + (memState.memory_type === t ? " selected" : "") + ">" + t + "</option>").join("")
    + "</select>"
    + '<select id="mstatus"><option value="">全部状态</option>'
    + ["active", "superseded"].map(s =>
        '<option value="' + s + '"' + (memState.status === s ? " selected" : "") + ">" + s + "</option>").join("")
    + "</select>"
    + '<button class="primary" onclick="applyMem()">筛选</button>'
    + '<span class="spacer"></span>'
    + '<span class="muted num" id="selinfo">未选中</span>'
    + '<button class="danger" id="delbtn" disabled onclick="batchDelete()">物理删除选中</button>'
    + "</div>"
    + table([{label: '<input type="checkbox" onclick="selAll(this)" />', raw: true},
             {label: "类型"}, {label: "内容"}, {label: "状态"}, {label: "来源"}, {label: "创建"}], rows)
    + '<div class="bar" style="margin-top:12px"><span class="muted num">共 ' + (d.total || 0)
    + " 条 · 第 " + (d.page || 1) + " / " + pages + " 页</span><span class=\\"spacer\\"></span>"
    + '<button onclick="memPage(-1)">上一页</button><button onclick="memPage(1)">下一页</button></div>'
    + "</div>";
};

tabs.recall = async () => {
  const q = new URLSearchParams({q: recallState.q, page: recallState.page}).toString();
  const [d, info] = await Promise.all([
    get("/api/dashboard/recall?" + q), get("/api/dashboard/overview")
  ]);
  const ov = (info || {}).recall || {};
  if (!d.available) {
    return "<h2>检索回放</h2>" + '<div class="panel"><div class="empty">'
      + "检索回放未启用（需要承重的记忆引擎）。</div></div>";
  }
  const rows = (d.turns || []).map(t =>
    '<tr class="rowlink" onclick="showRecall(\\'' + esc(t.turn_id) + '\\')">'
    + '<td class="clip">' + esc(clip(t.user_text, 70)) + "</td>"
    + '<td class="num">' + (t.context_prepare_count ?? 0) + "</td>"
    + "<td>" + (t.injected ? pill("已注入", "ok") : pill("未注入", "warn")) + "</td>"
    + '<td class="num">' + (t.recall_call_count ?? 0) + "</td>"
    + '<td class="mono muted">' + esc(clip(t.session_key, 22)) + "</td>"
    + '<td class="muted">' + esc(when(t.timestamp)) + "</td></tr>");
  return "<h2>检索回放</h2><p class=\\"sub\\">每一轮召回了什么、有没有注入、模型又主动查了什么"
    + "——检索质量出问题时不必靠猜。</p>"
    + '<div class="tiles">'
    + tile("已记录轮次", ov.total ?? 0, "")
    + tile("最近一轮", ov.latest_at ? when(ov.latest_at) : "—", "")
    + "</div>"
    + '<div class="panel"><div class="filters">'
    + '<input id="rq" placeholder="按用户提问搜索" value="' + esc(recallState.q || "") + '" />'
    + '<button class="primary" onclick="applyRecall()">筛选</button></div>'
    + table([{label: "用户提问"}, {label: "自动召回", num: true}, {label: "注入"},
             {label: "主动查询", num: true}, {label: "会话"}, {label: "时间"}], rows)
    + '<div class="bar" style="margin-top:12px"><span class="muted num">共 ' + (d.total || 0)
    + ' 轮</span><span class="spacer"></span>'
    + '<button onclick="recallPage(-1)">上一页</button><button onclick="recallPage(1)">下一页</button>'
    + "</div></div>"
    + '<div class="panel"><h3>消息检索</h3>'
    + '<div class="filters"><input id="mq2" placeholder="跨会话搜索历史消息" />'
    + '<button class="primary" onclick="searchMessages()">搜索</button></div>'
    + '<div id="msgres" class="empty">输入关键词开始搜索</div></div>';
};

tabs.sessions = async () => {
  const list = await get("/api/dashboard/sessions");
  const rows = (list.sessions || []).map(s =>
    '<tr class="rowlink" onclick="showSession(\\'' + esc(s.key) + '\\')">'
    + '<td class="mono">' + esc(s.key) + "</td>"
    + '<td class="num">' + (s.message_count ?? 0) + "</td>"
    + '<td class="muted">' + esc(when(s.updated_at)) + "</td>"
    + '<td><button class="danger" onclick="event.stopPropagation();delSession(\\''
    + esc(s.key) + '\\')">删除</button></td></tr>');
  return "<h2>会话</h2><p class=\\"sub\\">点一行看最近消息。删除会同时移除持久化历史。</p>"
    + '<div class="panel">'
    + table([{label: "Session Key"}, {label: "消息数", num: true}, {label: "最近更新"}, {label: ""}], rows)
    + "</div>";
};

tabs.plugins = async () => {
  const d = await get("/api/dashboard/plugins");
  const active = (d.active || []).map(p =>
    "<tr><td class=\\"mono\\">" + esc(p.id) + "</td><td>" + esc(p.version || "—") + "</td>"
    + '<td class="clip">' + esc(clip(p.desc, 70)) + "</td>"
    + "<td>" + (p.lifecycle ? pill("已装载", "ok") : pill("仅声明", "mut")) + "</td></tr>");
  const gens = (d.generations || []).map(g =>
    "<tr><td class=\\"mono\\">" + esc(g.plugin_id) + "</td>"
    + '<td class="mono muted">' + esc(g.generation_id) + "</td>"
    + "<td>" + pill(g.state, g.state === "active" ? "ok" : "mut") + "</td>"
    + '<td class="num">' + (g.lease_count ?? 0) + "</td></tr>");
  const retired = (d.retired || []).map(g =>
    "<tr><td class=\\"mono\\">" + esc(g.plugin_id) + "</td>"
    + '<td class="mono muted">' + esc(g.generation_id) + "</td>"
    + '<td class="num">' + (g.lease_count ?? 0) + "</td>"
    + "<td>" + (g.can_quiesce ? pill("可销毁", "ok") : pill("仍有在途租约", "warn")) + "</td></tr>");
  const errs = Object.entries(d.errors || {}).map(([k, v]) =>
    "<tr><td class=\\"mono\\">" + esc(k) + "</td><td>" + esc(v) + "</td></tr>");
  return "<h2>插件与代际</h2><p class=\\"sub\\">lease_count 非零 = 仍有在途 turn 持着该代际，"
    + "这正是「换代不抽走在途能力」的运行时证据。</p>"
    + '<div class="panel"><h3>已装载</h3>'
    + table([{label: "插件"}, {label: "版本"}, {label: "说明"}, {label: "状态"}], active) + "</div>"
    + '<div class="panel"><h3>当前代际</h3>'
    + table([{label: "插件"}, {label: "代际"}, {label: "状态"}, {label: "租约", num: true}], gens) + "</div>"
    + (retired.length ? '<div class="panel"><h3>待排空</h3>'
        + table([{label: "插件"}, {label: "代际"}, {label: "租约", num: true}, {label: ""}], retired) + "</div>" : "")
    + (errs.length ? '<div class="panel"><h3>装载错误</h3>'
        + table([{label: "插件"}, {label: "错误"}], errs) + "</div>" : "");
};

tabs.proactive = async () => {
  const [p, d] = await Promise.all([get("/api/dashboard/proactive"), get("/api/dashboard/drift")]);
  let html = "<h2>主动与 Drift</h2><p class=\\"sub\\">后台时钟的决策轨迹与空闲期的自主活动。</p>";
  if (!p.enabled) html += '<div class="panel"><h3>主动推送</h3><div class="empty">未启用</div></div>';
  else {
    const decisions = (p.recent_decisions || []).map(x =>
      "<tr><td>" + pill(x.action || "—",
          (x.action === "alert_pushed" || x.action === "content_pushed") ? "ok"
            : (String(x.action).indexOf("fail") >= 0 ? "crit" : "mut")) + "</td>"
      + '<td class="clip">' + esc(clip(x.detail, 90)) + "</td>"
      + '<td class="muted">' + esc(when(x.decided_at)) + "</td></tr>");
    html += '<div class="panel"><h3>主动推送</h3>'
      + '<div class="tiles" style="margin-bottom:12px">'
      + tile("未读 alert", p.unread_alert ?? 0, "")
      + tile("未读 content", p.unread_content ?? 0, "")
      + tile("电量", (p.energy ?? 0).toFixed ? (p.energy).toFixed(2) : p.energy, "base " + (p.base_score ?? "—"))
      + tile("下次 tick", (p.estimated_next_interval_s ?? "—") + "s",
          p.in_cooldown ? pill("冷却中", "warn") : pill("空闲", "ok"))
      + "</div>"
      + '<div class="muted" style="margin-bottom:8px">流水线: '
      + (p.modules || []).map(m => '<span class="pill mut mono">' + esc(m) + "</span>").join(" ") + "</div>"
      + table([{label: "动作"}, {label: "详情"}, {label: "时间"}], decisions) + "</div>";
  }
  if (!d.enabled) html += '<div class="panel"><h3>Drift</h3><div class="empty">未启用</div></div>';
  else {
    const runs = (d.recent_runs || []).map(r =>
      "<tr><td class=\\"mono\\">" + esc(r.skill) + "</td>"
      + "<td>" + pill(r.status || "—", r.status === "completed" ? "ok" : "mut") + "</td>"
      + "<td>" + pill(r.message_result || "—", r.message_result === "sent" ? "ok" : "mut") + "</td>"
      + '<td class="clip">' + esc(clip(r.briefing, 70)) + "</td>"
      + '<td class="muted">' + esc(when(r.run_at)) + "</td></tr>");
    const skills = (d.skills || []).map(s =>
      "<tr><td class=\\"mono\\">" + esc(s.name) + "</td>"
      + '<td class="muted">' + esc(when(s.last_run_at)) + "</td>"
      + '<td class="clip">' + esc(clip(s.scratchpad, 80)) + "</td>"
      + '<td class="clip">' + esc(clip(s.next_tendency, 60)) + "</td></tr>");
    const obs = (d.self_observations || []).map(o =>
      "<tr><td class=\\"mono\\">" + esc(o.skill) + "</td>"
      + '<td class="clip">' + esc(clip((o.payload || {}).note, 120)) + "</td></tr>");
    html += '<div class="panel"><h3>Drift 运行</h3>'
      + table([{label: "技能"}, {label: "状态"}, {label: "投递"}, {label: "简报"}, {label: "时间"}], runs) + "</div>"
      + '<div class="panel"><h3>技能连续性</h3>'
      + table([{label: "技能"}, {label: "上次"}, {label: "scratchpad"}, {label: "下轮倾向"}], skills) + "</div>"
      + (obs.length ? '<div class="panel"><h3>自我观察</h3>'
          + table([{label: "技能"}, {label: "记录"}], obs) + "</div>" : "");
  }
  return html;
};

let memState = {page: 1, page_size: 50, q: "", memory_type: "", status: ""};
function applyMem() {
  memState.q = document.querySelector("#mq").value.trim();
  memState.memory_type = document.querySelector("#mtype").value;
  memState.status = document.querySelector("#mstatus").value;
  memState.page = 1;
  render("memory");
}
function memPage(delta) { memState.page = Math.max(1, memState.page + delta); render("memory"); }

function selectedIds() {
  return [...document.querySelectorAll(".msel")].filter(c => c.checked).map(c => c.value);
}
function syncSel() {
  const n = selectedIds().length;
  const info = document.querySelector("#selinfo");
  const btn = document.querySelector("#delbtn");
  if (info) info.textContent = n ? ("已选中 " + n + " 条") : "未选中";
  if (btn) btn.disabled = n === 0;
}
function selAll(box) {
  document.querySelectorAll(".msel").forEach(c => { c.checked = box.checked; });
  syncSel();
}
async function batchDelete() {
  const ids = selectedIds();
  if (!ids.length) return;
  // 物理删除绕过了记忆系统"逻辑退休"的默认保护,所以要二次确认 + 服务端 confirm 令牌
  if (!confirm("物理删除 " + ids.length + " 条记忆？\\n这会连同向量一起移除，不可恢复。\\n"
      + "（只想标记失效请用详情里的「标记失效」）")) return;
  const resp = await fetch("/api/dashboard/memories/batch-delete", {
    method: "POST", headers: {"content-type": "application/json"},
    body: JSON.stringify({ids: ids, confirm: "HARD_DELETE"})
  });
  const data = await resp.json();
  if (data.error) alert("删除失败: " + data.error);
  render("memory");
}

let recallState = {page: 1, q: ""};
function applyRecall() {
  recallState.q = document.querySelector("#rq").value.trim();
  recallState.page = 1;
  render("recall");
}
function recallPage(delta) { recallState.page = Math.max(1, recallState.page + delta); render("recall"); }

async function showRecall(turnId) {
  const d = await get("/api/dashboard/recall/turn?id=" + encodeURIComponent(turnId));
  const t = d.turn || {};
  const prep = t.context_prepare || {};
  const hits = (prep.items || []).map(it =>
    "<tr><td>" + pill(it.memory_type || "—", "mut") + "</td>"
    + '<td class="clip">' + esc(clip(it.summary, 110)) + "</td>"
    + '<td class="num">' + (typeof it.score === "number" ? it.score.toFixed(3) : "—") + "</td>"
    + "<td>" + (it.injected ? pill("注入", "ok") : pill("未注入", "mut")) + "</td></tr>");
  const calls = (t.recall_memory_calls || []).map(c =>
    '<div style="margin-top:10px"><div class="muted mono">'
    + esc(JSON.stringify(c.arguments || {})) + "</div>"
    + table([{label: "类型"}, {label: "内容"}, {label: "分数", num: true}],
        (c.items || []).map(it =>
          "<tr><td>" + pill(it.memory_type || "—", "mut") + "</td>"
          + '<td class="clip">' + esc(clip(it.summary, 110)) + "</td>"
          + '<td class="num">'
          + (typeof it.score === "number" ? it.score.toFixed(3) : "—") + "</td></tr>"))
    + "</div>").join("");
  openModal('<h3 style="margin-top:0">检索回放</h3>'
    + '<div class="muted mono">' + esc(t.session_key || "") + " · " + esc(when(t.timestamp)) + "</div>"
    + '<pre style="margin:10px 0">' + esc(t.user_text || "") + "</pre>"
    + "<h3>自动召回（context）</h3>"
    + '<div class="muted" style="margin-bottom:6px">'
    + (prep.injected ? pill("已注入 " + (prep.injected_chars ?? 0) + " 字符", "ok")
        : pill("未注入上下文", "warn"))
    + " " + pill("引擎 " + esc((prep.trace || {}).engine || "—"), "mut") + "</div>"
    + table([{label: "类型"}, {label: "内容"}, {label: "分数", num: true}, {label: "注入"}], hits)
    + (calls ? "<h3>模型主动查询</h3>" + calls : ""));
}

async function searchMessages() {
  const box = document.querySelector("#msgres");
  const q = document.querySelector("#mq2").value.trim();
  if (!q) { box.className = "empty"; box.textContent = "输入关键词开始搜索"; return; }
  const d = await get("/api/dashboard/messages?q=" + encodeURIComponent(q) + "&limit=50");
  const rows = (d.messages || []).map(m =>
    "<tr><td>" + pill(m.role || "—", m.role === "user" ? "mut" : "ok") + "</td>"
    + '<td class="clip">' + esc(clip(m.content, 140)) + "</td>"
    + '<td class="mono muted">' + esc(clip(m.session_key || m.source_ref, 24)) + "</td>"
    + '<td class="muted">' + esc(when(m.timestamp)) + "</td></tr>");
  box.className = "";
  box.innerHTML = table([{label: "角色"}, {label: "内容"}, {label: "会话"}, {label: "时间"}], rows)
    + (d.deletable === false
        ? '<div class="muted" style="margin-top:8px">只读：' + esc(d.deletable_reason || "") + "</div>"
        : "");
}

async function showMemory(id) {
  const [d, sim] = await Promise.all([
    get("/api/dashboard/memory?id=" + encodeURIComponent(id)),
    get("/api/dashboard/memory/similar?id=" + encodeURIComponent(id) + "&limit=6")
  ]);
  const m = d.memory || {};
  const rows = (sim.items || []).map(s =>
    "<tr><td class=\\"num\\">" + (typeof s.score === "number" ? s.score.toFixed(3) : "—") + "</td>"
    + '<td class="clip">' + esc(clip(s.summary || s.content, 90)) + "</td></tr>");
  openModal("<h3 style=\\"margin-top:0\\">记忆详情</h3>"
    + "<div>" + pill(m.memory_type || m.kind || "—", "mut") + " "
    + (String(m.status) === "active" ? pill("active", "ok") : pill(m.status || "—", "mut")) + "</div>"
    + "<pre style=\\"margin:10px 0\\">" + esc(m.summary || m.content || "") + "</pre>"
    + '<div class="muted mono">id: ' + esc(m.id) + " · source: " + esc(m.source_ref || "—") + "</div>"
    + (rows.length ? "<h3>相似记忆</h3>" + table([{label: "分数", num: true}, {label: "内容"}], rows) : "")
    + '<div class="bar" style="margin-top:12px"><button class="danger" onclick="forgetMemory(\\''
    + esc(id) + '\\')">标记失效</button></div>');
}

async function forgetMemory(id) {
  if (!confirm("把这条记忆标记为失效？（逻辑退休，可在 superseded 里找到）")) return;
  await fetch("/api/dashboard/memory?id=" + encodeURIComponent(id), {method: "DELETE"});
  modal.close();
  render("memory");
}

async function showSession(key) {
  const d = await get("/api/dashboard/session?key=" + encodeURIComponent(key));
  const s = d.session || {};
  const rows = (s.messages || []).map(m =>
    "<tr><td>" + pill(m.role, m.role === "user" ? "mut" : (m.drift ? "warn" : "ok"))
    + (m.proactive ? " " + pill("主动", "warn") : "") + "</td>"
    + '<td class="clip">' + esc(clip(m.content, 160)) + "</td>"
    + '<td class="muted">' + esc(when(m.timestamp)) + "</td></tr>");
  openModal("<h3 style=\\"margin-top:0\\" class=\\"mono\\">" + esc(key) + "</h3>"
    + '<div class="muted">共 ' + (s.total_messages ?? 0) + " 条 · 已归档至第 "
    + (s.last_consolidated ?? 0) + " 条</div><div style=\\"margin-top:10px\\">"
    + table([{label: "角色"}, {label: "内容"}, {label: "时间"}], rows) + "</div>");
}

async function delSession(key) {
  if (!confirm("删除会话 " + key + " ？历史将不可恢复。")) return;
  await fetch("/api/dashboard/session?key=" + encodeURIComponent(key), {method: "DELETE"});
  render("sessions");
}

async function render(name) {
  document.querySelectorAll("nav button").forEach(b =>
    b.classList.toggle("active", b.dataset.tab === name));
  try {
    view.innerHTML = await tabs[name]();
  } catch (err) {
    view.innerHTML = '<div class="panel"><h3>加载失败</h3><pre>' + esc(String(err)) + "</pre></div>";
  }
  location.hash = name;
}

document.querySelectorAll("nav button").forEach(b =>
  b.addEventListener("click", () => render(b.dataset.tab)));
render(location.hash.replace("#", "") in tabs ? location.hash.replace("#", "") : "overview");
</script>
</body>
</html>
"""
)
