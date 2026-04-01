from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .crypto_insights import (
    build_live_signal_rows,
    count_strategy_signals,
    fetch_latest_basis,
    hourly_regime_rows,
    load_spread_positions,
    pre_funding_state,
    simulate_strategy_history,
    spike_forensics_rows,
)
from .crypto_pnl import cost_assumptions
from .crypto_settings import FEE_PRESETS, _resolve_fee_settings
from .crypto_storage import CryptoSQLiteStorage
from .crypto_strategy_lab import build_strategy_lab


def serve_crypto_dashboard(
    database_path: str | Path,
    settings,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> None:
    db_path = str(database_path)

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._write_html(_dashboard_html())
                return
            if parsed.path == "/api/summary":
                query = parse_qs(parsed.query)
                effective_settings = _scenario_settings(settings, query)
                self._write_json(_dashboard_payload(db_path, effective_settings))
                return
            self.send_error(404, "Not Found")

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _write_html(self, body: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _write_json(self, payload: dict) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Crypto dashboard: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _dashboard_payload(database_path: str, settings) -> dict:
    storage = CryptoSQLiteStorage(database_path)
    try:
        latest_basis = fetch_latest_basis(storage)
        positions = load_spread_positions(storage, latest_basis, settings)
        open_positions = [row for row in positions if row["status"] == "OPEN"]
        closed_positions = [row for row in positions if row["status"] == "CLOSED"]
        live_signals = build_live_signal_rows(storage, settings, latest_basis, open_positions)
        what_if = simulate_strategy_history(storage, settings, lookback_days=7)
        history_start = datetime.now(timezone.utc) - timedelta(days=7)
        regime_history = hourly_regime_rows(storage, settings, None, history_start)
        regime_history = [
            {
                **row,
                "long_hit_60": (row["max_basis_bps"] or -99999.0) >= 60.0,
                "reverse_hit_60": (row["min_basis_bps"] or 99999.0) <= -60.0,
            }
            for row in regime_history
        ]
        counts = {
            "quotes": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_quotes")[0]["n"]),
            "trades": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_trades")[0]["n"]),
            "funding": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_funding")[0]["n"]),
            "open_interest": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_open_interest")[0]["n"]),
            "basis": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_basis")[0]["n"]),
            "signals": count_strategy_signals(storage),
            "spreads": int(storage.fetch_all("SELECT COUNT(*) AS n FROM crypto_spread_positions")[0]["n"]),
            "open_spreads": len(open_positions),
            "closed_spreads": len(closed_positions),
        }
        recent_signals = [
            dict(row)
            for row in storage.fetch_all(
                """
                SELECT s.timestamp, s.pair, s.side, s.spread_bps, s.threshold,
                       s.signal_source, p.signal_quality_score, p.signal_quality_band, p.status
                FROM crypto_signals s
                LEFT JOIN crypto_spread_positions p ON p.signal_id = s.id
                WHERE s.signal_source LIKE 'basis_tier%'
                ORDER BY s.timestamp DESC
                LIMIT 30
                """
            )
        ]
        reference_time = (
            max(datetime.fromisoformat(str(row["timestamp"])) for row in latest_basis)
            if latest_basis
            else datetime.now(timezone.utc)
        )
        funding_clock = pre_funding_state(reference_time, settings)
        spike_forensics = spike_forensics_rows(storage, settings, lookback_days=7, threshold_bps=60.0)[:50]
        strategy_lab = build_strategy_lab(
            storage,
            settings,
            latest_basis=latest_basis,
            lookback_days=settings.strategy_lookback_days,
        )
    finally:
        storage.close()

    assumptions = cost_assumptions(settings)
    return {
        "counts": counts,
        "scenario_options": {
            "fee_presets": [{"value": key, "label": value["label"]} for key, value in FEE_PRESETS.items()],
            "exit_modes": [
                {"value": "mid", "label": "Mid Exit"},
                {"value": "maker", "label": "Maker Exit"},
                {"value": "taker", "label": "Taker Exit"},
            ],
        },
        "cost_assumptions": assumptions,
        "funding_clock": funding_clock,
        "latest_basis": latest_basis,
        "live_signals": live_signals,
        "open_summary": _build_open_summary(open_positions),
        "open_positions": open_positions,
        "closed_summary": _build_closed_summary(closed_positions),
        "recent_signals": recent_signals,
        "recent_positions": positions[:50],
        "regime_history": regime_history,
        "spike_forensics": spike_forensics,
        "strategy_lab": strategy_lab,
        "what_if_summary": what_if["summary_rows"],
        "what_if_trades": what_if["trade_rows"][:50],
        "what_if_equity": {
            "lookback_days": what_if["lookback_days"],
            "points": what_if["equity_points"],
            "trades": len(what_if["trade_rows"]),
            "cumulative_net_without_borrow_quote": (
                0.0
                if not what_if["equity_points"]
                else float(what_if["equity_points"][-1]["cumulative_net_without_borrow_quote"])
            ),
            "cumulative_net_with_borrow_quote": (
                0.0
                if not what_if["equity_points"]
                else float(what_if["equity_points"][-1]["cumulative_net_with_borrow_quote"])
            ),
        },
    }


def _build_open_summary(open_positions: list[dict]) -> dict:
    gross = sum(float(row["live_gross_pnl_quote"] or 0.0) for row in open_positions)
    net_without = sum(float(row["live_net_without_borrow_quote"] or 0.0) for row in open_positions)
    net_with = sum(float(row["live_net_with_borrow_quote"] or 0.0) for row in open_positions)
    borrow = sum(float(row["borrow_cost_quote"] or 0.0) for row in open_positions)
    count = len(open_positions)
    return {
        "active_positions": count,
        "gross_live_pnl_quote": gross,
        "net_live_without_borrow_quote": net_without,
        "net_live_with_borrow_quote": net_with,
        "borrow_cost_quote": borrow,
        "avg_net_with_borrow_quote": (net_with / count) if count else 0.0,
    }


def _build_closed_summary(closed_positions: list[dict]) -> list[dict]:
    grouped: dict[str, dict[str, float]] = {}
    for row in closed_positions:
        pair = str(row["pair"])
        bucket = grouped.setdefault(
            pair,
            {
                "pair": pair,
                "trades": 0.0,
                "gross_pnl_quote": 0.0,
                "net_without_borrow_quote": 0.0,
                "net_with_borrow_quote": 0.0,
                "borrow_cost_quote": 0.0,
                "positive_count": 0.0,
            },
        )
        bucket["trades"] += 1
        bucket["gross_pnl_quote"] += float(row["live_gross_pnl_quote"] or 0.0)
        bucket["net_without_borrow_quote"] += float(row["live_net_without_borrow_quote"] or 0.0)
        bucket["net_with_borrow_quote"] += float(row["live_net_with_borrow_quote"] or 0.0)
        bucket["borrow_cost_quote"] += float(row["borrow_cost_quote"] or 0.0)
        if float(row["live_net_with_borrow_quote"] or 0.0) > 0:
            bucket["positive_count"] += 1
    result = []
    for pair in sorted(grouped):
        bucket = grouped[pair]
        trades = int(bucket["trades"])
        result.append(
            {
                "pair": pair,
                "trades": trades,
                "gross_pnl_quote": bucket["gross_pnl_quote"],
                "net_without_borrow_quote": bucket["net_without_borrow_quote"],
                "net_with_borrow_quote": bucket["net_with_borrow_quote"],
                "borrow_cost_quote": bucket["borrow_cost_quote"],
                "win_rate": (bucket["positive_count"] / trades) if trades else None,
            }
        )
    return result


def _scenario_settings(settings, query: dict[str, list[str]]):
    fee_preset = _query_value(query, "fee_preset", settings.fee_preset)
    exit_mode = _query_value(query, "exit_mode", settings.exit_mode)
    if fee_preset not in FEE_PRESETS:
        fee_preset = settings.fee_preset
    if exit_mode not in {"mid", "maker", "taker"}:
        exit_mode = settings.exit_mode

    maker_override = _query_float(query, "maker_entry_fee_bps")
    exit_fee_override = _query_float(query, "exit_fee_bps")
    exit_slippage_override = _query_float(query, "exit_slippage_bps")
    maker_entry_fee_bps, exit_fee_bps, exit_slippage_bps = _resolve_fee_settings(
        fee_preset=fee_preset,
        exit_mode=exit_mode,
        maker_entry_fee_bps=maker_override,
        exit_fee_bps=exit_fee_override,
        exit_slippage_bps=exit_slippage_override,
        defaults=settings,
    )
    return replace(
        settings,
        fee_preset=fee_preset,
        exit_mode=exit_mode,
        maker_entry_fee_bps=maker_entry_fee_bps,
        exit_fee_bps=exit_fee_bps,
        exit_slippage_bps=exit_slippage_bps,
    )


def _query_value(query: dict[str, list[str]], key: str, default: str) -> str:
    values = query.get(key)
    if not values or values[0] == "":
        return default
    return values[0]


def _query_float(query: dict[str, list[str]], key: str) -> float | None:
    values = query.get(key)
    if not values or values[0] == "":
        return None
    try:
        return float(values[0])
    except ValueError:
        return None


def _dashboard_html() -> str:
    return _HTML


_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Crypto Regime Dashboard</title><meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--bg:#0b1020;--panel:rgba(15,23,42,.92);--text:#e5e7eb;--muted:#94a3b8;--line:rgba(148,163,184,.16);--good:#22c55e;--bad:#ef4444;--warn:#f59e0b}
body{margin:0;font-family:"Segoe UI",system-ui,sans-serif;background:radial-gradient(circle at top,#1e293b,var(--bg));color:var(--text)}
.wrap{max-width:1380px;margin:0 auto;padding:24px}.grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
.card{background:var(--panel);border:1px solid rgba(148,163,184,.16);border-radius:14px;padding:16px;box-shadow:0 16px 40px rgba(0,0,0,.25)}
.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.value{font-size:28px;font-weight:700;margin-top:6px}
.section{margin-top:20px}table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:600;position:sticky;top:0;background:rgba(15,23,42,.98)}.pos{color:var(--good)}.neg{color:var(--bad)}.warn{color:var(--warn)}
.table-wrap{overflow-x:auto}.stamp,.legend,p{color:var(--muted)}
select{width:100%;padding:10px 12px;border-radius:10px;border:1px solid rgba(148,163,184,.2);background:#0b1220;color:var(--text)}
.chart{width:100%;height:260px;border:1px solid var(--line);border-radius:12px;background:linear-gradient(180deg,rgba(56,189,248,.08),rgba(15,23,42,.08))}
</style></head><body><div class="wrap">
<h1>Crypto Regime Dashboard</h1><p>Live Binance spot plus Bybit perp data. Entries are regime-aware, reverse mode is paper-only, and reverse PnL is shown with and without borrow cost.</p>
<div class="stamp" id="stamp">Loading...</div><div class="grid" id="counts"></div>
<div class="section card"><h2>Scenario Switcher</h2><div class="grid"><div><div class="label">Fee Preset</div><select id="scenario-fee-preset"></select></div><div><div class="label">Exit Mode</div><select id="scenario-exit-mode"></select></div></div></div>
<div class="section card"><h2>Aggregate Cost Assumptions</h2><div class="grid" id="costs"></div></div>
<div class="section card"><h2>Strategy Leadership</h2><div class="grid" id="strategy-primary"></div><div class="table-wrap" id="strategy-summary"></div></div>
<div class="section card"><h2>Strategy Live Board</h2><div class="table-wrap" id="strategy-live"></div></div>
<div class="section card"><h2>Pre-Funding Alert Mode</h2><div class="grid" id="funding-clock"></div></div>
<div class="section card"><h2>Live Regime And Signal Board</h2><div class="table-wrap" id="live-signals"></div></div>
<div class="section card"><h2>Latest Basis Snapshot</h2><div class="table-wrap" id="basis"></div></div>
<div class="section card"><h2>Spike Forensics</h2><div class="legend">All episodes with absolute basis at or above 60 bps over the last 7 days.</div><div class="table-wrap" id="spike-forensics"></div></div>
<div class="section card"><h2>Open Spread PnL</h2><div class="grid" id="open-summary"></div><div class="table-wrap" id="open-positions"></div></div>
<div class="section card"><h2>7-Day What-If Summary</h2><div class="table-wrap" id="what-if-summary"></div></div>
<div class="section card"><h2>7-Day What-If Equity</h2><div class="grid" id="what-if-cards"></div><div class="legend">Historical paper replay using regime-matched entries from stored basis data. Reverse mode includes a 10% APY spot borrow assumption.</div><div id="what-if-equity"></div></div>
<div class="section card"><h2>7-Day Regime Timeline</h2><div id="regime-history"></div></div>
<div class="section card"><h2>Recent Signals</h2><div class="table-wrap" id="recent-signals"></div></div>
<div class="section card"><h2>Per-Trade Spread Blotter</h2><div class="table-wrap" id="recent-positions"></div></div>
<div class="section card"><h2>Historical What-If Trades</h2><div class="table-wrap" id="what-if-trades"></div></div>
<div class="section card"><h2>Recent Strategy Trades</h2><div class="table-wrap" id="strategy-trades"></div></div>
</div>
<script>
const fmt=(v,d=2)=>v===null||v===undefined?'n/a':Number(v).toFixed(d),pct=v=>v===null||v===undefined?'n/a':(Number(v)*100).toFixed(2)+'%',score=v=>v===null||v===undefined?'n/a':Number(v).toFixed(2),cls=v=>(v??0)>=0?'pos':'neg',stateCls=v=>v==='CONTANGO'?'pos':(v==='BACKWARDATION'?'neg':'warn'),mins=v=>v===null||v===undefined?'n/a':fmt(v/60000,1),secs=v=>v===null||v===undefined?'n/a':fmt(v/1000,0),yesno=v=>v?'ON':'OFF';
let scenarioLoaded=false;const scenario={fee_preset:'',exit_mode:''};
function table(rows,cols){if(!rows||rows.length===0)return'<p>No data yet.</p>';const h='<thead><tr>'+cols.map(c=>`<th>${c.label}</th>`).join('')+'</tr></thead>';const b=rows.map(r=>'<tr>'+cols.map(c=>{const v=c.render?c.render(r[c.key],r):r[c.key];const k=c.className?c.className(r[c.key],r):'';return `<td class="${k}">${v??''}</td>`}).join('')+'</tr>').join('');return `<table>${h}<tbody>${b}</tbody></table>`}
function dualCurve(p){const pts=p.points||[];if(!pts.length)return'<p>No historical what-if exits yet.</p>';const w=1100,h=260,px=24,py=20;const vals=[];pts.forEach(x=>{vals.push(Number(x.cumulative_net_without_borrow_quote));vals.push(Number(x.cumulative_net_with_borrow_quote))});let mn=Math.min(...vals),mx=Math.max(...vals);if(mn===mx){mn-=1;mx+=1}const x=i=>px+(i*(w-px*2)/Math.max(pts.length-1,1)),y=v=>h-py-((v-mn)/(mx-mn))*(h-py*2),line=k=>pts.map((q,i)=>`${x(i)},${y(Number(q[k]))}`).join(' '),zero=(mn<0&&mx>0)?`<line x1="${px}" y1="${y(0)}" x2="${w-px}" y2="${y(0)}" stroke="rgba(148,163,184,.35)" stroke-dasharray="4 4"></line>`:'';return `<svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${zero}<polyline fill="none" stroke="#38bdf8" stroke-width="3" points="${line('cumulative_net_without_borrow_quote')}"></polyline><polyline fill="none" stroke="#ef4444" stroke-width="3" points="${line('cumulative_net_with_borrow_quote')}"></polyline></svg>`}
function regimeCharts(rows){if(!rows||rows.length===0)return'<p>No history yet.</p>';const byPair={};rows.forEach(r=>{(byPair[r.pair]=byPair[r.pair]||[]).push(r)});return Object.entries(byPair).map(([pair,pts])=>{const w=1100,h=120,pad=16,vals=pts.map(p=>Number(p.avg_basis_bps??0));let mn=Math.min(...vals),mx=Math.max(...vals);if(mn===mx){mn-=1;mx+=1}const bw=(w-pad*2)/Math.max(pts.length,1);const y=v=>h-pad-((v-mn)/(mx-mn))*(h-pad*2);const bars=pts.map((p,i)=>{const color=p.regime==='CONTANGO'?'rgba(34,197,94,.35)':(p.regime==='BACKWARDATION'?'rgba(239,68,68,.35)':'rgba(148,163,184,.25)');const x=pad+i*bw;const hit=p.long_hit_60?`<circle cx="${x+bw/2}" cy="${pad+8}" r="3" fill="#22c55e"></circle>`:(p.reverse_hit_60?`<circle cx="${x+bw/2}" cy="${h-pad-8}" r="3" fill="#ef4444"></circle>`:'');return `<rect x="${x}" y="${pad}" width="${Math.max(bw-1,1)}" height="${h-pad*2}" fill="${color}"></rect>${hit}`}).join('');const poly=pts.map((p,i)=>`${pad+i*bw+bw/2},${y(Number(p.avg_basis_bps??0))}`).join(' ');return `<div class="card section"><h3>${pair}</h3><svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${bars}<polyline fill="none" stroke="#f8fafc" stroke-width="2" points="${poly}"></polyline></svg><div class="legend">Hourly average basis line. Green dot = max basis >= 60 bps. Red dot = min basis <= -60 bps.</div></div>`}).join('')}
function query(){const q=new URLSearchParams();if(scenario.fee_preset)q.set('fee_preset',scenario.fee_preset);if(scenario.exit_mode)q.set('exit_mode',scenario.exit_mode);return q.toString()?`/api/summary?${q.toString()}`:'/api/summary'}
function bindScenario(p){if(scenarioLoaded)return;const fee=document.getElementById('scenario-fee-preset');const exit=document.getElementById('scenario-exit-mode');fee.innerHTML=p.scenario_options.fee_presets.map(o=>`<option value="${o.value}">${o.label}</option>`).join('');exit.innerHTML=p.scenario_options.exit_modes.map(o=>`<option value="${o.value}">${o.label}</option>`).join('');scenario.fee_preset=p.cost_assumptions.fee_preset;scenario.exit_mode=p.cost_assumptions.exit_mode;fee.value=scenario.fee_preset;exit.value=scenario.exit_mode;fee.addEventListener('change',()=>{scenario.fee_preset=fee.value;refresh()});exit.addEventListener('change',()=>{scenario.exit_mode=exit.value;refresh()});scenarioLoaded=true}
async function refresh(){const p=await fetch(query()).then(r=>r.json());bindScenario(p);document.getElementById('stamp').textContent='Updated '+new Date().toLocaleTimeString();
document.getElementById('counts').innerHTML=Object.entries(p.counts).map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join('');
document.getElementById('costs').innerHTML=[['Fee Preset',p.cost_assumptions.fee_preset],['Exit Mode',p.cost_assumptions.exit_mode],['Entry Cost (bps)',fmt(p.cost_assumptions.maker_entry_fee_bps)],['Exit Fee (bps)',fmt(p.cost_assumptions.exit_fee_bps)],['Exit Slippage (bps)',fmt(p.cost_assumptions.exit_slippage_bps)],['Borrow APY',pct(p.cost_assumptions.reverse_spot_borrow_apy)],['Funding Divergence Entry',fmt(p.cost_assumptions.funding_divergence_entry_rate_bps)],['Funding Flip Hold (hr)',fmt(p.cost_assumptions.funding_flip_hold_ms/3600000,2)],['OI Drop Trigger',pct(p.cost_assumptions.liquidation_oi_drop_pct)],['Funding In PnL',p.cost_assumptions.include_funding_in_pnl?'Yes':'No']].map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join('');
const primary=p.strategy_lab.primary_strategy;
document.getElementById('strategy-primary').innerHTML=primary?[['Primary This Week',primary.label],['Category',primary.category],['Status',primary.status],['Trades',primary.trades],['Win Rate',primary.win_rate===null?'n/a':pct(primary.win_rate)],['EV / Trade',fmt(primary.ev_per_trade_quote,4)]].map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join(''):'<p>No strategy leader yet.</p>';
document.getElementById('strategy-summary').innerHTML=table(p.strategy_lab.summary_rows,[{key:'label',label:'Strategy'},{key:'category',label:'Category'},{key:'status',label:'Status',className:v=>stateCls(v==='ACTIVE'?'CONTANGO':(v==='TRACKING'?'NEUTRAL':'BACKWARDATION'))},{key:'trades',label:'Trades'},{key:'wins',label:'Wins'},{key:'win_rate',label:'Win Rate',render:v=>v===null?'n/a':pct(v)},{key:'gross_pnl_quote',label:'Gross',render:v=>fmt(v,4),className:v=>cls(v)},{key:'net_pnl_quote',label:'Net',render:v=>fmt(v,4),className:v=>cls(v)},{key:'ev_per_trade_quote',label:'EV / Trade',render:v=>fmt(v,4),className:v=>cls(v)},{key:'live_candidates',label:'Live Candidates'},{key:'dominant_regime',label:'Dominant Regime',className:v=>stateCls(v)},{key:'is_primary',label:'Primary',render:v=>yesno(v)}]);
document.getElementById('strategy-live').innerHTML=table(p.strategy_lab.live_rows,[{key:'strategy_label',label:'Strategy'},{key:'pair',label:'Pair'},{key:'status',label:'Status',className:v=>stateCls(String(v).startsWith('READY')?'CONTANGO':(v==='MONITOR'?'NEUTRAL':'BACKWARDATION'))},{key:'regime',label:'Regime',className:v=>stateCls(v)},{key:'signal_value',label:'Signal',render:v=>fmt(v,2),className:v=>cls(v)},{key:'edge_value',label:'Edge/Score',render:v=>fmt(v,4),className:v=>cls(v)},{key:'notes',label:'Notes'}]);
document.getElementById('funding-clock').innerHTML=[['Next Funding UTC',p.funding_clock.next_funding_time],['Countdown (min)',mins(p.funding_clock.countdown_ms)],['Pre-Funding Alert',yesno(p.funding_clock.alert_active)],['Alert Window (min)',mins(p.cost_assumptions.pre_funding_window_ms)],['Tier2 Pre-Funding Threshold',fmt(p.cost_assumptions.pre_funding_basis_threshold_bps)],['Duration Filter Samples',p.cost_assumptions.basis_consecutive_samples_required]].map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join('');
document.getElementById('live-signals').innerHTML=table(p.live_signals,[{key:'pair',label:'Pair'},{key:'regime',label:'Regime',className:v=>stateCls(v)},{key:'mode',label:'Mode',className:v=>stateCls(v==='REVERSE_PAPER'?'BACKWARDATION':(v==='ORIGINAL'?'CONTANGO':'NEUTRAL'))},{key:'status',label:'Status',className:v=>stateCls(v)},{key:'premium_bps',label:'Basis (bps)',render:v=>fmt(v),className:v=>cls(v)},{key:'regime_avg_basis_bps',label:'1h Avg Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'basis_trend_10m_bps',label:'10m Trend',render:v=>fmt(v),className:v=>cls(v)},{key:'momentum_bps',label:'3m Momentum',render:v=>fmt(v),className:v=>cls(v)},{key:'current_funding_rate',label:'Funding (bps)',render:v=>v===null||v===undefined?'n/a':fmt(v*10000),className:v=>cls(v)},{key:'active_threshold_bps',label:'Tier1 Threshold',render:v=>fmt(v)},{key:'basis_only_threshold_bps',label:'Tier2 Threshold',render:v=>fmt(v)},{key:'tier1_duration_count',label:'Tier1 Run'},{key:'tier2_duration_count',label:'Tier2 Run'},{key:'pre_funding_alert_active',label:'Pre-Funding',render:v=>yesno(v)},{key:'time_to_next_funding_ms',label:'To Funding (min)',render:v=>mins(v)},{key:'regime_changes_today',label:'Regime Changes Today'},{key:'regime_duration_ms',label:'Regime Age (hr)',render:v=>v===null||v===undefined?'n/a':fmt(v/3600000,2)},{key:'signal_quality_score',label:'Quality Score',render:v=>score(v),className:v=>cls(v)},{key:'signal_quality_band',label:'Band',className:v=>stateCls(v)}]);
document.getElementById('basis').innerHTML=table(p.latest_basis,[{key:'pair',label:'Pair'},{key:'premium_bps',label:'Premium (bps)',render:v=>fmt(v),className:v=>cls(v)},{key:'spot_mid',label:'Spot Mid',render:v=>fmt(v,4)},{key:'perp_mid',label:'Perp Mid',render:v=>fmt(v,4)},{key:'current_funding_rate',label:'Funding (bps)',render:v=>v===null||v===undefined?'n/a':fmt(v*10000)},{key:'timestamp',label:'Time'}]);
document.getElementById('spike-forensics').innerHTML=table(p.spike_forensics,[{key:'pair',label:'Pair'},{key:'peak_timestamp',label:'Peak Time'},{key:'direction',label:'Dir',className:v=>stateCls(v)},{key:'peak_basis_bps',label:'Peak Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'duration_ms',label:'Duration (s)',render:v=>secs(v)},{key:'samples',label:'Samples'},{key:'funding_peak_bps',label:'Funding @ Peak',render:v=>fmt(v),className:v=>cls(v)},{key:'basis_5m_bps',label:'Basis +5m',render:v=>fmt(v),className:v=>cls(v)},{key:'basis_15m_bps',label:'Basis +15m',render:v=>fmt(v),className:v=>cls(v)},{key:'basis_30m_bps',label:'Basis +30m',render:v=>fmt(v),className:v=>cls(v)},{key:'basis_5m_abs_change_bps',label:'Abs Change +5m',render:v=>fmt(v),className:v=>cls(v)},{key:'pre_funding_alert_active',label:'Pre-Funding',render:v=>yesno(v)}]);
document.getElementById('open-summary').innerHTML=[['Active Positions',p.open_summary.active_positions],['Gross Live PnL',fmt(p.open_summary.gross_live_pnl_quote,4)],['Net No Borrow',fmt(p.open_summary.net_live_without_borrow_quote,4)],['Net With Borrow',fmt(p.open_summary.net_live_with_borrow_quote,4)],['Borrow Cost',fmt(p.open_summary.borrow_cost_quote,4)],['Avg Net With Borrow',fmt(p.open_summary.avg_net_with_borrow_quote,4)]].map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join('');
document.getElementById('open-positions').innerHTML=table(p.open_positions,[{key:'timestamp',label:'Entered'},{key:'pair',label:'Pair'},{key:'side',label:'Side'},{key:'entry_mode',label:'Mode'},{key:'entry_basis_bps',label:'Entry Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'live_basis_bps',label:'Live Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'live_gross_pnl_quote',label:'Gross',render:v=>fmt(v,4),className:v=>cls(v)},{key:'live_net_without_borrow_quote',label:'Net No Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'live_net_with_borrow_quote',label:'Net With Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'borrow_cost_quote',label:'Borrow Cost',render:v=>fmt(v,4),className:v=>cls(-Math.abs(v||0))},{key:'age_minutes',label:'Age (min)',render:v=>fmt(v,2)}]);
document.getElementById('what-if-summary').innerHTML=table(p.what_if_summary,[{key:'pair',label:'Pair'},{key:'long_hits_60bps',label:'Long Hits >= 60bps'},{key:'reverse_hits_60bps',label:'Reverse Hits <= -60bps'},{key:'what_if_trades',label:'What-If Trades'},{key:'what_if_gross_pnl_quote',label:'Gross',render:v=>fmt(v,4),className:v=>cls(v)},{key:'what_if_net_without_borrow_quote',label:'Net No Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'what_if_net_with_borrow_quote',label:'Net With Borrow',render:v=>fmt(v,4),className:v=>cls(v)}]);
document.getElementById('what-if-cards').innerHTML=[['Lookback Days',p.what_if_equity.lookback_days],['Historical Trades',p.what_if_equity.trades],['Net No Borrow',fmt(p.what_if_equity.cumulative_net_without_borrow_quote,4)],['Net With Borrow',fmt(p.what_if_equity.cumulative_net_with_borrow_quote,4)]].map(([k,v])=>`<div class="card"><div class="label">${k}</div><div class="value">${v}</div></div>`).join('');
document.getElementById('what-if-equity').innerHTML=dualCurve(p.what_if_equity);
document.getElementById('regime-history').innerHTML=regimeCharts(p.regime_history);
document.getElementById('recent-signals').innerHTML=table(p.recent_signals,[{key:'timestamp',label:'Time'},{key:'pair',label:'Pair'},{key:'side',label:'Side'},{key:'signal_source',label:'Source'},{key:'spread_bps',label:'Basis (bps)',render:v=>fmt(v),className:v=>cls(v)},{key:'threshold',label:'Threshold',render:v=>fmt(v)},{key:'signal_quality_score',label:'Score',render:v=>score(v),className:v=>cls(v)},{key:'signal_quality_band',label:'Band',className:v=>stateCls(v)},{key:'status',label:'Position',className:v=>stateCls(v)}]);
document.getElementById('recent-positions').innerHTML=table(p.recent_positions,[{key:'timestamp',label:'Entered'},{key:'exit_timestamp',label:'Exited'},{key:'pair',label:'Pair'},{key:'side',label:'Side'},{key:'entry_mode',label:'Mode'},{key:'status',label:'Status',className:v=>stateCls(v)},{key:'entry_basis_bps',label:'Entry Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'live_basis_bps',label:'Current/Exit Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'exit_reason',label:'Exit Reason'},{key:'live_net_without_borrow_quote',label:'Net No Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'live_net_with_borrow_quote',label:'Net With Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'borrow_cost_quote',label:'Borrow',render:v=>fmt(v,4),className:v=>cls(-Math.abs(v||0))}]);
document.getElementById('what-if-trades').innerHTML=table(p.what_if_trades,[{key:'exit_timestamp',label:'Exit Time'},{key:'pair',label:'Pair'},{key:'side',label:'Side'},{key:'entry_basis_bps',label:'Entry Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'exit_basis_bps',label:'Exit Basis',render:v=>fmt(v),className:v=>cls(v)},{key:'gross_pnl_quote',label:'Gross',render:v=>fmt(v,4),className:v=>cls(v)},{key:'net_without_borrow_quote',label:'Net No Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'net_with_borrow_quote',label:'Net With Borrow',render:v=>fmt(v,4),className:v=>cls(v)},{key:'borrow_cost_quote',label:'Borrow',render:v=>fmt(v,4),className:v=>cls(-Math.abs(v||0))},{key:'exit_reason',label:'Exit Reason'}]);
document.getElementById('strategy-trades').innerHTML=table(p.strategy_lab.trade_rows,[{key:'strategy_label',label:'Strategy'},{key:'pair',label:'Pair'},{key:'entry_timestamp',label:'Entry'},{key:'exit_timestamp',label:'Exit'},{key:'side',label:'Side'},{key:'regime',label:'Regime',className:v=>stateCls(v)},{key:'gross_pnl_quote',label:'Gross',render:v=>fmt(v,4),className:v=>cls(v)},{key:'net_pnl_quote',label:'Net',render:v=>fmt(v,4),className:v=>cls(v)},{key:'exit_reason',label:'Exit Reason'}]);
}
refresh();setInterval(refresh,2000);
</script></body></html>"""
