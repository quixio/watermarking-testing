import os
import json
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

load_dotenv()
from fastapi.responses import HTMLResponse, JSONResponse
from quixlake import QuixLakeClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

QUIXLAKE_URL = os.environ.get(
    "QUIXLAKE_URL",
    "https://quixlake-quixers-testrigdemodatawarehouse-prod.az-france-0.app.quix.io",
)

DEFAULT_WM_TABLE   = "carcoloursv3"
DEFAULT_NOWM_TABLE = "carcoloursnomwv3"
DEFAULT_LIMIT      = 10


def build_query(wm_table: str, nowm_table: str, limit: int) -> str:
    return f"""
SELECT
  watermarking.run_id,
  count(watermarking.count) as "watermarking_count",
  count(nowatermarking.count) as "nowatermarking_count",
  abs(max(watermarking.count) - min(watermarking.count)) as "watermarking_diff",
  abs(max(nowatermarking.count) - min(nowatermarking.count)) as "nowatermarking_diff"
FROM {nowm_table} as nowatermarking
LEFT OUTER JOIN {wm_table} as watermarking ON watermarking.run_id == nowatermarking.run_id
GROUP BY watermarking.run_id
ORDER BY run_id DESC
LIMIT {limit}
""".strip()


def get_client() -> QuixLakeClient:
    token = os.environ.get("QUIX_LAKE_TOKEN")
    if not token:
        raise RuntimeError("QUIX_LAKE_TOKEN environment variable is not set")
    return QuixLakeClient(base_url=QUIXLAKE_URL, token=token)


@app.get("/api/data")
async def get_data(
    wm_table: str = DEFAULT_WM_TABLE,
    nowm_table: str = DEFAULT_NOWM_TABLE,
    limit: int = DEFAULT_LIMIT,
):
    try:
        client = get_client()
        df = client.query(build_query(wm_table, nowm_table, limit))
        records = df.to_dict(orient="records")
        return JSONResponse(content={"data": records, "count": len(records)})
    except Exception as e:
        logger.error("Query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Watermarking Experiment Dashboard</title>
  <style>
    :root {
      --bg: #0f1117;
      --surface: #1a1d27;
      --border: #2d3047;
      --text: #e2e8f0;
      --text-muted: #8892a4;
      --accent: #4f8ef7;
      --green: #34d399;
      --orange: #f59e0b;
      --red: #f87171;
      --purple: #a78bfa;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
      min-height: 100vh;
      padding: 2rem;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 2rem;
      flex-wrap: wrap;
      gap: 1rem;
    }
    h1 {
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--text);
    }
    h1 span { color: var(--accent); }
    .meta {
      display: flex;
      align-items: center;
      gap: 1.25rem;
      font-size: 0.8rem;
      color: var(--text-muted);
    }
    .pulse {
      display: inline-block;
      width: 8px; height: 8px;
      border-radius: 50%;
      background: var(--green);
      animation: pulse 2s infinite;
    }
    .pulse.error { background: var(--red); animation: none; }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.3; }
    }
    .controls {
      display: flex;
      align-items: flex-end;
      gap: 1rem;
      flex-wrap: wrap;
      margin-bottom: 1.5rem;
      padding: 1rem 1.25rem;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
    }
    .control-group {
      display: flex;
      flex-direction: column;
      gap: 0.3rem;
    }
    .control-group label {
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: var(--text-muted);
    }
    .control-group input {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 0.35rem 0.7rem;
      color: var(--text);
      font-size: 0.85rem;
      width: 100%;
      outline: none;
      transition: border-color 0.15s;
    }
    .control-group input:focus { border-color: var(--accent); }
    .control-group input[type=number] { width: 80px; }
    .btn-refresh {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 0.25rem 0.9rem;
      color: var(--text);
      font-size: 0.8rem;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s;
    }
    .btn-refresh:hover { background: #2d3047; border-color: var(--accent); }
    .btn-refresh:active { opacity: 0.7; }
    .legend {
      display: flex;
      gap: 1.5rem;
      flex-wrap: wrap;
      margin-bottom: 1.5rem;
      font-size: 0.78rem;
      color: var(--text-muted);
    }
    .legend-item { display: flex; align-items: center; gap: 0.4rem; }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
    }
    .dot-blue  { background: var(--accent); }
    .dot-orange{ background: var(--orange); }
    .dot-green { background: var(--green); }
    .dot-purple{ background: var(--purple); }

    /* Summary cards */
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
      margin-bottom: 2rem;
    }
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 1.1rem 1.25rem;
    }
    .card-label { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.4rem; }
    .card-value { font-size: 1.7rem; font-weight: 700; }
    .card-value.blue   { color: var(--accent); }
    .card-value.orange { color: var(--orange); }
    .card-value.green  { color: var(--green); }
    .card-value.purple { color: var(--purple); }

    /* Table */
    .table-wrap {
      overflow-x: auto;
      border-radius: 10px;
      border: 1px solid var(--border);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.88rem;
    }
    thead tr {
      background: #141720;
    }
    thead th {
      padding: 0.85rem 1.1rem;
      text-align: left;
      font-weight: 600;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
    }
    tbody tr {
      border-bottom: 1px solid var(--border);
      transition: background 0.15s;
    }
    tbody tr:last-child { border-bottom: none; }
    tbody tr:hover { background: #1f2337; }
    tbody tr.new-row {
      animation: fadeIn 0.6s ease;
    }
    @keyframes fadeIn {
      from { background: #1e2a4a; }
      to   { background: transparent; }
    }
    td {
      padding: 0.85rem 1.1rem;
      vertical-align: middle;
    }
    .run-id {
      font-family: monospace;
      font-weight: 600;
      color: var(--accent);
      font-size: 0.85rem;
    }
    .badge-rank {
      display: inline-block;
      background: #2d3047;
      color: var(--text-muted);
      border-radius: 4px;
      padding: 0.1rem 0.45rem;
      font-size: 0.7rem;
      margin-right: 0.5rem;
    }
    .pill {
      display: inline-block;
      padding: 0.2rem 0.65rem;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 600;
    }
    .pill-blue   { background: rgba(79,142,247,0.15); color: var(--accent); }
    .pill-orange { background: rgba(245,158,11,0.15); color: var(--orange); }
    .pill-green  { background: rgba(52,211,153,0.15); color: var(--green); }
    .pill-purple { background: rgba(167,139,250,0.15); color: var(--purple); }

    .bar-wrap {
      display: flex;
      align-items: center;
      gap: 0.6rem;
    }
    .bar-bg {
      flex: 1;
      background: var(--border);
      border-radius: 999px;
      height: 6px;
      min-width: 60px;
      max-width: 120px;
    }
    .bar-fill {
      height: 100%;
      border-radius: 999px;
      transition: width 0.4s ease;
    }
    .bar-fill.blue   { background: var(--accent); }
    .bar-fill.orange { background: var(--orange); }

    .error-box {
      background: rgba(248,113,113,0.1);
      border: 1px solid rgba(248,113,113,0.3);
      border-radius: 10px;
      padding: 1.5rem;
      color: var(--red);
      margin-top: 1rem;
      font-size: 0.88rem;
    }
    .loading {
      color: var(--text-muted);
      font-size: 0.9rem;
      padding: 3rem 0;
      text-align: center;
    }
    footer {
      margin-top: 2.5rem;
      font-size: 0.72rem;
      color: var(--text-muted);
      text-align: center;
    }
  </style>
</head>
<body>
  <header>
    <h1>Watermarking <span>Experiment Dashboard</span></h1>
    <div class="meta">
      <span id="status-dot" class="pulse"></span>
      <span id="status-text">Live</span>
      <button class="btn-refresh" id="btn-refresh" onclick="fetchData()">↻ Refresh</button>
      <span id="last-updated"></span>
    </div>
  </header>

  <div class="controls">
    <div class="control-group">
      <label>Watermarking table</label>
      <input type="text" id="wm-table" value="carcoloursv3" />
    </div>
    <div class="control-group">
      <label>No-watermarking table</label>
      <input type="text" id="nowm-table" value="carcoloursnomwv3" />
    </div>
    <div class="control-group">
      <label>Runs to display</label>
      <input type="number" id="run-limit" value="10" min="1" max="100" />
    </div>
  </div>

  <div class="legend">
    <div class="legend-item"><div class="dot dot-blue"></div> Watermarking count</div>
    <div class="legend-item"><div class="dot dot-orange"></div> No-watermarking count</div>
    <div class="legend-item"><div class="dot dot-green"></div> Watermarking diff (spread)</div>
    <div class="legend-item"><div class="dot dot-purple"></div> No-watermarking diff (spread)</div>
  </div>

  <div class="cards" id="cards">
    <div class="card"><div class="card-label">Runs shown</div><div class="card-value blue" id="stat-runs">—</div></div>
    <div class="card"><div class="card-label">Avg WM count</div><div class="card-value green" id="stat-wm-avg">—</div></div>
    <div class="card"><div class="card-label">Avg NoWM count</div><div class="card-value orange" id="stat-nowm-avg">—</div></div>
    <div class="card"><div class="card-label">Avg WM diff</div><div class="card-value green" id="stat-wm-diff">—</div></div>
    <div class="card"><div class="card-label">Avg NoWM diff</div><div class="card-value purple" id="stat-nowm-diff">—</div></div>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Run ID</th>
          <th>WM Count</th>
          <th>NoWM Count</th>
          <th>WM Diff</th>
          <th>NoWM Diff</th>
        </tr>
      </thead>
      <tbody id="table-body">
        <tr><td colspan="6" class="loading">Loading data…</td></tr>
      </tbody>
    </table>
  </div>

  <div id="error-box" style="display:none"></div>

  <footer>Queries <code>carcoloursv3</code> ⋈ <code>carcoloursnomwv3</code> · last 10 runs</footer>

  <script>
    let prevRunIds = [];
    let maxDiff = 1;

    function avg(arr) {
      if (!arr.length) return 0;
      return arr.reduce((a, b) => a + b, 0) / arr.length;
    }
    function fmt(n) {
      if (n === null || n === undefined) return '—';
      return Number(n).toLocaleString();
    }
    function fmtAvg(n) {
      return n.toFixed(1);
    }

    async function fetchData() {
      const errorBox = document.getElementById('error-box');
      const dot = document.getElementById('status-dot');
      const statusText = document.getElementById('status-text');
      const btn = document.getElementById('btn-refresh');
      btn.disabled = true;
      btn.textContent = '↻ Loading…';
      try {
        const wmTable   = document.getElementById('wm-table').value.trim()   || 'carcoloursv3';
        const nowmTable = document.getElementById('nowm-table').value.trim() || 'carcoloursnomwv3';
        const limit     = parseInt(document.getElementById('run-limit').value) || 10;
        const params    = new URLSearchParams({ wm_table: wmTable, nowm_table: nowmTable, limit });
        const res = await fetch('/api/data?' + params);
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        const rows = json.data;

        dot.className = 'pulse';
        statusText.textContent = 'Live';
        errorBox.style.display = 'none';

        // Update stats
        document.getElementById('stat-runs').textContent = rows.length;
        const wmCounts   = rows.map(r => r.watermarking_count  ?? 0);
        const nowmCounts = rows.map(r => r.nowatermarking_count ?? 0);
        const wmDiffs    = rows.map(r => r.watermarking_diff   ?? 0);
        const nowmDiffs  = rows.map(r => r.nowatermarking_diff  ?? 0);
        document.getElementById('stat-wm-avg').textContent   = fmtAvg(avg(wmCounts));
        document.getElementById('stat-nowm-avg').textContent = fmtAvg(avg(nowmCounts));
        document.getElementById('stat-wm-diff').textContent  = fmtAvg(avg(wmDiffs));
        document.getElementById('stat-nowm-diff').textContent= fmtAvg(avg(nowmDiffs));

        maxDiff = Math.max(1, ...wmDiffs, ...nowmDiffs);
        const newRunIds = rows.map(r => r.run_id);

        const tbody = document.getElementById('table-body');
        tbody.innerHTML = '';
        rows.forEach((row, i) => {
          const isNew = !prevRunIds.includes(row.run_id);
          const tr = document.createElement('tr');
          if (isNew && prevRunIds.length) tr.classList.add('new-row');

          const wmPct   = Math.round(((row.watermarking_diff   ?? 0) / maxDiff) * 100);
          const nowmPct = Math.round(((row.nowatermarking_diff  ?? 0) / maxDiff) * 100);

          tr.innerHTML = `
            <td><span class="badge-rank">${i + 1}</span></td>
            <td><span class="run-id">${row.run_id ?? '—'}</span></td>
            <td><span class="pill pill-blue">${fmt(row.watermarking_count)}</span></td>
            <td><span class="pill pill-orange">${fmt(row.nowatermarking_count)}</span></td>
            <td>
              <div class="bar-wrap">
                <span class="pill pill-green">${fmt(row.watermarking_diff)}</span>
                <div class="bar-bg"><div class="bar-fill blue" style="width:${wmPct}%"></div></div>
              </div>
            </td>
            <td>
              <div class="bar-wrap">
                <span class="pill pill-purple">${fmt(row.nowatermarking_diff)}</span>
                <div class="bar-bg"><div class="bar-fill orange" style="width:${nowmPct}%"></div></div>
              </div>
            </td>
          `;
          tbody.appendChild(tr);
        });

        prevRunIds = newRunIds;
        const now = new Date();
        const hh = String(now.getHours()).padStart(2, '0');
        const mm = String(now.getMinutes()).padStart(2, '0');
        document.getElementById('last-updated').textContent =
          'Updated ' + hh + ':' + mm;
      } catch (err) {
        dot.className = 'pulse error';
        statusText.textContent = 'Error';
        errorBox.style.display = 'block';
        errorBox.className = 'error-box';
        errorBox.textContent = 'Failed to load data: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.textContent = '↻ Refresh';
      }
    }

    // Don't auto-fetch on load — wait for user to click Refresh
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
