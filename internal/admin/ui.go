package admin

// uiHTML is the embedded read-only UI (§3.4: a single static page reading
// /admin/status.json, rendering the DAG as mermaid and the job history).
// It is deliberately dependency-free apart from the mermaid CDN script.
const uiHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Eventboat — read-only console</title>
<style>
  body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 0; background: #0f1420; color: #e6e9f0; }
  header { padding: 14px 20px; background: #151d30; border-bottom: 1px solid #26304a; display: flex; gap: 16px; align-items: baseline; }
  header h1 { font-size: 16px; margin: 0; }
  header span { color: #8b96ad; font-size: 12px; }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 16px 20px; }
  .card { background: #151d30; border: 1px solid #26304a; border-radius: 8px; padding: 12px; min-height: 120px; }
  .card h2 { font-size: 13px; margin: 0 0 8px; color: #9fb0cd; text-transform: uppercase; letter-spacing: .04em; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #26304a; }
  .pill { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; background: #223154; }
  .pill.ok { background: #1d4030; color: #7be3a8; }
  .pill.err { background: #4a1f28; color: #ff9aa8; }
  .pill.paused { background: #443a1f; color: #ffd479; }
  .muted { color: #8b96ad; }
  #dagger { overflow-x: auto; }
  footer { padding: 10px 20px; color: #586180; font-size: 11px; }
</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
</head>
<body>
<header>
  <h1>Eventboat</h1><span>read-only console — configuration changes go through verify (CLI / MCP)</span>
  <span id="conn" class="muted"></span>
</header>
<main>
  <div class="card"><h2>Pipelines</h2><div id="pipelines"></div></div>
  <div class="card"><h2>Topology</h2><div id="dagger"><div class="muted">deploy a pipeline to see its DAG</div></div></div>
  <div class="card"><h2>Job runs</h2><div id="jobs"><div class="muted">no runs yet</div></div></div>
  <div class="card"><h2>Event log (SSE)</h2><div id="events" class="muted">connecting…</div></div>
</main>
<footer>status source: /admin/status.json · events: /admin/sse · metrics: /metrics</footer>
<script>
let mermaidReady = false;
if (window.mermaid) { mermaid.initialize({ startOnLoad: false, theme: 'dark' }); mermaidReady = true; }

// When the server runs with a token (internal/admin/security.go), the
// sign-in page forwards it here as ?token=; it is kept in sessionStorage and
// stripped from the URL. Data fetches then use the Authorization header; the
// SSE stream uses ?token= because EventSource cannot set headers (the
// leakage caveat is documented in security.go).
const qpToken = new URLSearchParams(location.search).get('token');
if (qpToken) { sessionStorage.setItem('eb_admin_token', qpToken); history.replaceState(null, '', '/admin/'); }
const TOKEN = sessionStorage.getItem('eb_admin_token');
const AUTH = TOKEN ? {headers: {Authorization: 'Bearer ' + TOKEN}} : {};

async function refresh() {
  try {
    const res = await fetch('/admin/status.json', AUTH);
    if (res.status === 401) { document.getElementById('conn').textContent = 'unauthorized (token required)'; return; }
    const pipelines = await res.json();
    render(pipelines);
    document.getElementById('conn').textContent = 'live';
  } catch (e) { document.getElementById('conn').textContent = 'status fetch failed'; }
}

function render(pipelines) {
  const pl = document.getElementById('pipelines');
  if (!pipelines.length) { pl.innerHTML = '<div class="muted">nothing deployed</div>'; return; }
  let html = '<table><tr><th>pipeline</th><th>mode</th><th>status</th><th>in-flight</th><th>msg/s</th><th>committed</th><th>dead</th></tr>';
  for (const p of pipelines) {
    const cls = p.status === 'running' ? 'ok' : (p.status === 'paused' || p.status === 'drained' ? 'paused' : 'err');
    html += '<tr><td>' + esc(p.pipeline) + '</td><td>' + esc(p.mode) + '</td>' +
      '<td><span class="pill ' + cls + '">' + esc(p.status) + '</span></td>' +
      '<td>' + p.in_flight + '</td><td>' + p.messages_per_sec.toFixed(1) + '</td>' +
      '<td>' + p.committed + '</td><td>' + p.dead_lettered + '</td></tr>';
  }
  html += '</table>';
  pl.innerHTML = html;

  // DAG from node sections (a faithful client-side rendering of the topology;
  // edges flow sources → transforms → sinks in declared order).
  const p0 = pipelines[0];
  if (mermaidReady && p0.nodes && p0.nodes.length) {
    let g = 'flowchart LR\n';
    for (const n of p0.nodes) {
      const shape = n.section === 'sources' ? '([%s])' : (n.section === 'sinks' ? '[[%s]]' : '[%s]');
      g += '  ' + id(n.node) + sprintf(shape, [n.node + (n.plugin ? '<br/>' + n.plugin : '')]) + '\n';
    }
    const order = p0.nodes.map(n => n.node);
    for (let i = 1; i < order.length; i++) g += '  ' + id(order[i-1]) + ' --> ' + id(order[i]) + '\n';
    mermaid.render('dag-svg', g).then(({svg}) => { document.getElementById('dagger').innerHTML = svg; });
  }

  const jobs = document.getElementById('jobs');
  const runs = (pipelines.flatMap(p => p.recent_runs || []));
  if (runs.length) {
    let jh = '<table><tr><th>run</th><th>pipeline</th><th>status</th><th>trigger</th><th>rows</th><th>delivered</th><th>dead</th></tr>';
    for (const r of runs.slice(0, 12)) {
      const cls = r.status === 'success' ? 'ok' : (r.status === 'running' || r.status === 'pending' ? 'paused' : 'err');
      jh += '<tr><td>' + esc(r.run_id.slice(0,8)) + '</td><td>' + esc(r.pipeline) + '</td>' +
        '<td><span class="pill ' + cls + '">' + esc(r.status) + '</span></td><td>' + esc(r.trigger) + '</td>' +
        '<td>' + r.rows_read + '</td><td>' + r.delivered + '</td><td>' + r.dead_lettered + '</td></tr>';
    }
    jobs.innerHTML = jh + '</table>';
  }
}

function esc(s) { const d = document.createElement('div'); d.textContent = s ?? ''; return d.innerHTML; }
function id(s) { return 'n' + s.replace(/[^A-Za-z0-9_]/g, '_'); }
function sprintf(fmt, args) { return fmt.replace('%s', () => args[0]); }

refresh();
setInterval(refresh, 2000);
const ev = new EventSource('/admin/sse' + (TOKEN ? '?token=' + encodeURIComponent(TOKEN) : ''));
ev.addEventListener('status', e => render(JSON.parse(e.data)));
ev.onerror = () => { document.getElementById('events').textContent = 'event stream disconnected; retrying…'; };
ev.addEventListener('deploy', e => logEvent('deploy', e.data));
ev.addEventListener('job', e => logEvent('job', e.data));
function logEvent(kind, data) {
  const el = document.getElementById('events');
  el.classList.remove('muted');
  const line = document.createElement('div');
  line.textContent = new Date().toLocaleTimeString() + '  ' + kind + '  ' + data;
  el.prepend(line);
  while (el.children.length > 20) el.removeChild(el.lastChild);
}
</script>
</body>
</html>
`
