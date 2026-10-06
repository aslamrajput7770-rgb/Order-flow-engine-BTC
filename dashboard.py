"""Live dashboard for orderflow_engine.

Serves a single page that polls the engine snapshot and renders:
  * price line for the closed execution-timeframe bars
  * per-bar cumulative delta with the dynamic +/-2-sigma trigger bands
  * the live footprint matrix (bid/ask volume per price)
  * the rolling sigma window state and recent signals

Enabled with ``--dashboard-port <port>``. Kept out of the engine file so the
trading logic stays readable and independently testable.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

try:
    from aiohttp import web
except ImportError:  # pragma: no cover
    web = None

if TYPE_CHECKING:
    from orderflow_engine import Engine

log = logging.getLogger("orderflow.dashboard")

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Order Flow Engine - BTCUSD</title>
<style>
  :root{--bg:#0b0e14;--panel:#121722;--line:#1f2633;--txt:#d7dee8;--dim:#7d8899;
        --up:#1fc98a;--down:#ff5b6e;--acc:#4aa8ff;--warn:#ffb648;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
       font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
  header{display:flex;gap:18px;align-items:center;padding:12px 18px;border-bottom:1px solid var(--line);
         background:var(--panel);position:sticky;top:0;z-index:5;flex-wrap:wrap}
  header h1{font-size:14px;margin:0;letter-spacing:.5px}
  .pill{padding:2px 9px;border-radius:10px;border:1px solid var(--line);font-size:11px}
  .pill.live{color:var(--up);border-color:#17402f}
  .pill.paper{color:var(--warn);border-color:#4a3a12}
  .kpis{display:flex;gap:14px;flex-wrap:wrap;padding:14px 18px}
  .kpi{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:9px 13px;min-width:120px}
  .kpi .k{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.6px}
  .kpi .v{font-size:18px;margin-top:3px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:0 18px 18px}
  @media(max-width:980px){.grid{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px}
  .card h2{margin:0 0 8px;font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.7px}
  canvas{width:100%;height:220px;display:block}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th,td{text-align:right;padding:4px 6px;border-bottom:1px solid var(--line)}
  th:first-child,td:first-child{text-align:left}
  th{color:var(--dim);font-weight:500;font-size:10px;text-transform:uppercase}
  .bid{color:var(--down)} .ask{color:var(--up)}
  .foot{padding:0 18px 24px;color:var(--dim);font-size:11px}
  .scroll{max-height:260px;overflow:auto}
</style>
</head>
<body>
<header>
  <h1>ORDER FLOW ENGINE</h1>
  <span id="mode" class="pill">-</span>
  <select id="symsel" class="pill" style="background:var(--panel);color:var(--txt)"></select>
  <span id="tf" class="pill">-</span>
  <span id="clock" class="pill">-</span>
</header>

<div class="kpis">
  <div class="kpi"><div class="k">mark</div><div class="v" id="mark">-</div></div>
  <div class="kpi"><div class="k">bar delta</div><div class="v" id="delta">-</div></div>
  <div class="kpi"><div class="k">2&sigma; trigger</div><div class="v" id="thr">-</div></div>
  <div class="kpi"><div class="k">mean |&delta;|</div><div class="v" id="mean">-</div></div>
  <div class="kpi"><div class="k">sigma</div><div class="v" id="sigma">-</div></div>
  <div class="kpi"><div class="k">window</div><div class="v" id="samples">-</div></div>
  <div class="kpi"><div class="k">gate</div><div class="v" id="armed">-</div></div>
  <div class="kpi"><div class="k">open</div><div class="v" id="open">-</div></div>
</div>

<div class="grid">
  <div class="card"><h2>Price</h2><canvas id="px"></canvas></div>
  <div class="card"><h2>Cumulative delta vs dynamic 2&sigma; band</h2><canvas id="dl"></canvas></div>
  <div class="card"><h2>Live footprint (bid / ask volume by price)</h2>
    <div class="scroll"><table id="fp"><thead><tr><th>price</th><th>bid</th><th>ask</th><th>delta</th></tr></thead><tbody></tbody></table></div>
  </div>
  <div class="card"><h2>Rolling window samples (|&delta;|)</h2><canvas id="win"></canvas></div>
  <div class="card"><h2>Signals</h2>
    <div class="scroll"><table id="sig"><thead><tr><th>time</th><th>sym</th><th>side</th><th>delta</th><th>trigger</th><th>price</th></tr></thead><tbody></tbody></table></div>
  </div>
  <div class="card"><h2>Open positions</h2>
    <div class="scroll"><table id="pos"><thead><tr><th>symbol</th><th>qty</th><th>entry</th><th>stop</th><th>thr</th></tr></thead><tbody></tbody></table></div>
  </div>
</div>
<div class="foot" id="foot">waiting for data...</div>

<script>
const $ = id => document.getElementById(id);
const fmt = (n,d=2) => (n===null||n===undefined||isNaN(n)) ? "-" : Number(n).toLocaleString(undefined,{maximumFractionDigits:d});
const hhmm = ms => new Date(ms).toLocaleTimeString();

function drawLine(cv, pts, color, extra){
  const c = cv.getContext("2d"), W = cv.width = cv.clientWidth*devicePixelRatio,
        H = cv.height = 220*devicePixelRatio; c.scale(1,1);
  c.clearRect(0,0,W,H);
  if(!pts.length){ return; }
  const xs = pts.map(p=>p.x), ys = pts.map(p=>p.y);
  let lo = Math.min(...ys), hi = Math.max(...ys);
  if(extra){ lo = Math.min(lo, extra.lo); hi = Math.max(hi, extra.hi); }
  if(hi===lo){ hi+=1; lo-=1; }
  const pad=8*devicePixelRatio, X=v=>pad+(W-2*pad)*(v-Math.min(...xs))/((Math.max(...xs)-Math.min(...xs))||1),
        Y=v=>H-pad-(H-2*pad)*(v-lo)/(hi-lo);
  // bands
  if(extra && extra.bands){ extra.bands.forEach(b=>{
    c.strokeStyle=b.color; c.setLineDash(b.dash||[6,6]); c.lineWidth=1*devicePixelRatio;
    c.beginPath(); c.moveTo(pad,Y(b.v)); c.lineTo(W-pad,Y(b.v)); c.stroke(); c.setLineDash([]);
    c.fillStyle=b.color; c.font=(10*devicePixelRatio)+"px monospace";
    c.fillText(b.label, pad+4, Y(b.v)-3*devicePixelRatio);
  });}
  // zero line
  if(extra && extra.zero!==undefined && lo<extra.zero && hi>extra.zero){
    c.strokeStyle="#2a3341"; c.beginPath(); c.moveTo(pad,Y(extra.zero)); c.lineTo(W-pad,Y(extra.zero)); c.stroke();
  }
  c.strokeStyle=color; c.lineWidth=1.6*devicePixelRatio; c.beginPath();
  pts.forEach((p,i)=>{ const x=X(p.x), y=Y(p.y); i?c.lineTo(x,y):c.moveTo(x,y); });
  c.stroke();
}

function drawBars(cv, bars, thr){
  const c = cv.getContext("2d"), W = cv.width = cv.clientWidth*devicePixelRatio,
        H = cv.height = 220*devicePixelRatio; c.clearRect(0,0,W,H);
  if(!bars.length){ return; }
  const vals = bars.map(b=>b.delta).concat([thr, -thr, 0]);
  let lo = Math.min(...vals), hi = Math.max(...vals); if(hi===lo){hi+=1;lo-=1;}
  const pad=8*devicePixelRatio, Y=v=>H-pad-(H-2*pad)*(v-lo)/(hi-lo),
        bw=(W-2*pad)/bars.length;
  c.strokeStyle="#3a2a3a"; c.setLineDash([6,6]); c.lineWidth=1*devicePixelRatio;
  [thr,-thr].forEach(v=>{ c.beginPath(); c.moveTo(pad,Y(v)); c.lineTo(W-pad,Y(v)); c.stroke(); });
  c.setLineDash([]);
  c.fillStyle="#6b7686"; c.font=(10*devicePixelRatio)+"px monospace";
  c.fillText("+"+fmt(thr,0)+" (2\u03c3)", pad+4, Y(thr)-3*devicePixelRatio);
  c.fillText("-"+fmt(thr,0), pad+4, Y(-thr)+11*devicePixelRatio);
  c.strokeStyle="#2a3341"; c.beginPath(); c.moveTo(pad,Y(0)); c.lineTo(W-pad,Y(0)); c.stroke();
  bars.forEach((b,i)=>{ const y0=Y(0), y1=Y(b.delta);
    c.fillStyle = b.delta>=0 ? "#1fc98a" : "#ff5b6e";
    c.fillRect(pad+i*bw+1*devicePixelRatio, Math.min(y0,y1), Math.max(1,bw-2*devicePixelRatio), Math.abs(y1-y0)||1);
  });
}

async function tick(){
  try{
    const r = await fetch("api/state",{cache:"no-store"});
    const s = await r.json();
    $("mode").textContent = s.mode; $("mode").className = "pill "+(s.mode==="LIVE"?"live":"paper");

    // populate the symbol selector once, preserving the user's choice
    const sel = $("symsel");
    if(sel.options.length !== (s.symbols||[]).length){
      const keep = sel.value;
      sel.innerHTML = "";
      (s.symbols||[]).forEach(sym=>{ const o=document.createElement("option"); o.value=sym; o.textContent=sym; sel.appendChild(o); });
      sel.value = keep || s.symbol;
    }
    const chosen = sel.value || s.symbol;
    // per-symbol window when available; flat fields as the fallback (older shape)
    const view = (s.per_symbol && s.per_symbol[chosen]) ? s.per_symbol[chosen] : s;
    $("tf").textContent = s.execution_tf+" / win "+s.rolling_tf;
    $("clock").textContent = hhmm(s.now_ms);
    $("mark").textContent = fmt(view.mark_price);
    const b = (view.bars||{})[s.execution_tf] || {};
    $("delta").textContent = fmt(b.delta);
    $("delta").style.color = (b.delta||0)>=0 ? "var(--up)" : "var(--down)";
    $("thr").textContent = fmt(view.threshold,0);
    $("mean").textContent = fmt(view.mean,0);
    $("sigma").textContent = fmt(view.sigma,0);
    $("samples").textContent = view.samples + "/" + (view.delta_filter||{}).window;
    $("armed").textContent = view.armed ? "ARMED" : "warming";
    $("armed").style.color = view.armed ? "var(--up)" : "var(--warn)";
    $("open").textContent = (s.open_positions||[]).length;

    const exec = ((view.history||[]).find(h=>h.tf===s.execution_tf)||{}).bars || [];
    drawLine($("px"), exec.map(x=>({x:x.start_ms,y:x.close})), "#4aa8ff", {});
    drawBars($("dl"), exec.slice(-80), view.threshold);
    drawLine($("win"), ((view.delta_filter||{}).recent||[]).map((v,i)=>({x:i,y:v})), "#ffb648",
             {bands:[{v:view.threshold,color:"#7d8899",label:"2\u03c3 "+fmt(view.threshold,0)}], zero:0});

    const tb = $("fp").querySelector("tbody"); tb.innerHTML = "";
    (b.matrix||[]).slice().reverse().forEach(row=>{
      const tr=document.createElement("tr");
      tr.innerHTML = `<td>${fmt(row.price,1)}</td><td class="bid">${fmt(row.bid,0)}</td>`+
                     `<td class="ask">${fmt(row.ask,0)}</td><td style="color:${row.delta>=0?'var(--up)':'var(--down)'}">${fmt(row.delta,0)}</td>`;
      tb.appendChild(tr);
    });

    const sb = $("sig").querySelector("tbody"); sb.innerHTML="";
    (s.signals||[]).slice().reverse().forEach(g=>{
      const tr=document.createElement("tr");
      tr.innerHTML=`<td>${hhmm(g.ts)}</td><td>${g.symbol||s.symbol}</td>`+
                   `<td style="color:${g.direction==='bullish'?'var(--up)':'var(--down)'}">${g.direction}</td>`+
                   `<td>${fmt(g.delta,0)}</td><td>${fmt(g.threshold,0)}</td><td>${fmt(g.price,1)}</td>`;
      sb.appendChild(tr);
    });

    const pb = $("pos").querySelector("tbody"); pb.innerHTML="";
    (s.open_positions||[]).forEach(p=>{
      const tr=document.createElement("tr");
      tr.innerHTML=`<td>${p.symbol}</td><td>${p.qty}</td><td>${fmt(p.entry_premium,3)}</td>`+
                   `<td>${fmt(p.stop_price,3)}</td><td>${fmt(p.entry_threshold,0)}</td>`;
      pb.appendChild(tr);
    });

    $("foot").textContent = `engine ${s.version} | view ${chosen} | window n=${view.samples} mean=${fmt(view.mean,1)} sigma=${fmt(view.sigma,1)} trigger=${fmt(view.threshold,1)} | updated ${hhmm(s.now_ms)}`;
  }catch(e){ $("foot").textContent = "no data: "+e; }
}
tick(); setInterval(tick, 1000);
</script>
</body>
</html>
"""


async def _index(_request):
    return web.Response(text=PAGE, content_type="text/html")


def _state_factory(engine: "Engine"):
    async def _state(_request):
        return web.json_response(engine.snapshot())
    return _state


async def serve(engine: "Engine", port: int):
    """Start the dashboard and return the AppRunner (call .cleanup() to stop)."""
    if web is None:
        raise RuntimeError("aiohttp is required for the dashboard")
    app = web.Application()
    app.router.add_get("/", _index)
    app.router.add_get("/api/state", _state_factory(engine))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    return runner
