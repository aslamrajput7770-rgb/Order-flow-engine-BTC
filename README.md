# Institutional Order Flow & Footprint Delta Engine (BTCUSD)

A single-file, production-oriented trading engine for **Delta Exchange India**
that reads the raw trade tape, builds a live footprint matrix, computes
cumulative delta, and sells OTM options when a dynamic 2-sigma order-flow
imbalance clears the rolling gate.

```
trades (WS)  ->  footprint matrix (1m/3m/5m)  ->  diagonal imbalance scan
             ->  dynamic 2-sigma delta gate (rolling |delta|, no static number)
             ->  OTM strike selection (cushion beyond high-volume node)
             ->  SELL CALL / SELL PUT  ->  SQLite + JSON ledger  ->  1.5x stop
```

Everything lives in [`orderflow_engine.py`](orderflow_engine.py).

## Multi-asset (BTCUSD + XAUTUSD)

The same engine runs several symbols on **one websocket, one process**, with
fully isolated state. `--symbols` (default `BTCUSD,XAUTUSD`) is the watchlist.

```
trades(WS, sy=BTCUSD)  -> Strategy[BTCUSD]  -> DeltaFilter[BTCUSD]  -> delta_samples
trades(WS, sy=XAUTUSD) -> Strategy[XAUTUSD] -> DeltaFilter[XAUTUSD] -> xaut_delta_samples
```

* Each symbol has its **own footprint bars, own `_fired` latch and own 40-bar
  rolling mean/σ/trigger** - gold's price/volume scale never moves BTC's gate.
* Per-asset persistence: BTC writes `delta_samples`, XAUT writes
  `xaut_delta_samples`.
* Execution routes by symbol (`XAUTUSD -> XAUT` chain, `BTCUSD -> BTC` chain).
* The dashboard has a symbol selector to switch the live view per asset.

## Dynamic volume tuning (2-sigma rolling filter)

The trigger is **not** a hardcoded block size. `DeltaFilter` keeps a rolling
window of the last **40 closed bars** (default 15m) and, in real time, computes
the **mean and standard deviation (σ) of the absolute cumulative delta**. A bar
only triggers when its |delta| breaks past **2.0σ** of that window:

| rolling average | σ | trigger (2σ) |
| --- | --- | --- |
| ~300 | 150 | 300 |
| 1500 | 1500 | 3000 |

So a quiet Asian session automatically lowers the bar, and US-session momentum
raises it - the filter breathes with the market instead of breaking on a fixed
number.

* The gate stays **closed during warmup** (fewer than `--rolling-min-bars`
  closed bars) so a thin history cannot fabricate a trigger.
* Every closed rolling bar is written to the `delta_samples` table, so the
  window is **restored on restart** instead of re-warming for hours.
* The live threshold, mean and σ are mirrored into `state.json`
  (`delta_filter`) and stamped onto every position
  (`entry_threshold` / `entry_mean` / `entry_sigma`).
* Tune with `--delta-sigma-mult`, `--rolling-window`, `--rolling-tf`,
  `--rolling-min-bars`. Pass `--static-delta` to fall back to the legacy fixed
  thresholds.

## Live dashboard

The engine is headless by default. Add `--dashboard-port 8080` (or set
`DASHBOARD_PORT`) to serve a live chart page at `http://<host>:8080/`:

* price line for the closed execution-timeframe bars
* per-bar cumulative delta drawn against the dynamic **+/-2 sigma** trigger
  bands (you can literally watch the band widen in the US session)
* the live **footprint matrix** - bid / ask volume at every price
* the rolling sigma window samples, recent signals, and open positions

It reads from the same engine object (`Engine.snapshot()`), so what you see is
the exact data driving the orders - not a replay. The page polls
`/api/state` once per second.

```bash
python3 orderflow_engine.py --dashboard-port 8080
# then open http://localhost:8080/
```

## Safety model (read this first)

This engine is capital-bearing. It is deliberately built so it **cannot arm
itself by accident**:

* Real orders are sent only when **both** `--live` is passed **and**
  `DELTA_API_KEY` / `DELTA_API_SECRET` are present.
* Without those, the engine runs the *entire* strategy and execution path and
  records synthetic ("paper") fills. This is the default.
* Every position is written to SQLite and `state.json` **before** the order is
  transmitted, so a crash mid-send can never orphan a position silently.
* A `pending` row that cannot be confirmed on the exchange is marked
  `orphaned`, never silently re-opened.

Do not run with `--live` until you have validated the strategy on testnet and
understand that it will place real sell-side option orders.

## Requirements

```bash
pip install -r requirements.txt   # aiohttp, websockets
```

Python 3.9+ (developed and tested on 3.13).

## Run

```bash
# Paper mode (default) - live market data, synthetic fills
python3 orderflow_engine.py

# With the in-file supervisor (restarts within 10s, forced ledger recovery)
python3 orderflow_engine.py --watchdog

# Real trading (requires credentials in the environment)
export DELTA_API_KEY=...
export DELTA_API_SECRET=...
python3 orderflow_engine.py --live
```

Useful flags: `--execution-tf 1m|3m|5m`, `--imbalance-multiple 2.5`,
`--delta-block-threshold 1000`, `--delta-sweep-threshold 800`,
`--stacked-imbalances 2`, `--premium-stop-multiple 1.5`, `--option-qty 1`,
`--max-open-positions 1`, `--stats-interval 30`. Run `--help` for the full set.

## Supervision

Two options, both restart the engine within 10s and force a ledger recovery
read on boot:

* `python3 orderflow_engine.py --watchdog` - self-contained supervisor.
* [`orderflow-engine.service`](orderflow-engine.service) - systemd unit.
  Install to `/etc/systemd/system/`, edit paths, put credentials in an
  `EnvironmentFile` with mode `0600`, then
  `systemctl enable --now orderflow-engine`.
* [`run_engine.sh`](run_engine.sh) - plain shell supervisor for non-systemd hosts.

## Functional mapping

| Spec | Where |
| --- | --- |
| Live async tick buffer, bid/ask classification | `MarketFeed`, `FootprintBar.add_trade` |
| Footprint matrix per 1m/3m/5m bar | `Strategy`, `FootprintBar.matrix` |
| Diagonal imbalance scan (P vs P-1 / P+1, >=2.5x) | `FootprintBar._scan_diagonal` |
| Dynamic 2-sigma delta gate (rolling mean/σ) | `DeltaFilter`, `Strategy.delta_gate` |
| Asymmetric SELL CALL / SELL PUT | `Strategy.evaluate`, `Engine._execute` |
| OTM strike outside high-volume node + cushion | `select_contract` |
| 1.5x premium stop, market buy-back | `Engine._monitor_once` |
| Atomic SQLite + JSON mirror before network | `Ledger`, `atomic_write_json` |
| Cold-start recovery of open contracts | `Engine._recover`, `_reconcile_pending` |
| Cold-start recovery of the sigma window | `Ledger.recent_delta_samples`, `DeltaFilter.load` |
| Live dashboard (footprint + delta + 2σ band) | `Engine.snapshot`, `dashboard.py` |
| 24/7 supervisor watchdog | `run_watchdog`, `run_engine.sh`, systemd unit |

## Data sources (verified against live Delta India)

* REST: `https://api.india.delta.exchange`
  (`/v2/products`, `/v2/tickers`, `/v2/orders`, `/v2/positions`).
  Auth headers: `api-key`, `timestamp`, `signature` where
  `signature = HMAC_SHA256(secret, METHOD + timestamp + path + query + body)`.
* WebSocket: `wss://public-socket.india.delta.exchange`
  channels `trades` and `mark_price` (`MARK:BTCUSD`). The legacy private pod
  rejects `trades` with *"subscription forbidden ... use appropriate pod"*.
* Delta trade `t` timestamps are in **microseconds**; the engine normalises all
  timestamps to milliseconds (`to_ms`).

## Operational notes

* **Option chain freshness.** Products carry no volume/OI, so the chain is
  enriched from `/v2/tickers?contract_types=...` and cached for 30s.
* **Nearest expiry.** The raw chain spans every listed expiry, so strikes
  repeat. `select_contract` restricts to the nearest expiry to get a unique
  strike ladder before applying the OTM cushion and HVN boundary.
* **One entry per cleared bar.** Without a latch the tape would re-fire on
  every tick after the gate opens; `Strategy._fired` permits one execution per
  bar per timeframe.
* **Stop-loss semantics.** `stop_price = entry_premium * 1.5`; the position is
  closed at market when the live mark price reaches it. Because a short option
  can lose more than its premium, the fill is not guaranteed to be exactly the
  stop - the order is a market buy-back.

## Ledger schema

`orderflow_state.db` (WAL, `synchronous=FULL`):

* `positions` - uid, symbol, product_id, side, qty, entry/exit premium,
  stop price, entry delta, timestamps, state (`pending`/`open`/`closed`/`orphaned`).
* `executions` - append-only audit of every intent and fill.
* `engine_events` - recovery, shutdown, errors.

`state.json` is an atomic mirror of currently open/pending positions.
