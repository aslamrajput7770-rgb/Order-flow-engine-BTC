# AGENTS.md

## What this repo is
Single-file institutional order-flow / footprint / cumulative-delta engine for
BTCUSD on Delta Exchange India (`orderflow_engine.py`), plus an optional live
dashboard (`dashboard.py`), tests (`test_orderflow_engine.py`) and deploy assets.

## Local commands
- Tests: `python3 -m unittest test_orderflow_engine` (36 tests)
- Lint: `python3 -m pyflakes orderflow_engine.py dashboard.py`
- Run (paper): `python3 orderflow_engine.py --watchdog --dashboard-port 8080`
- Run multi-asset: `python3 orderflow_engine.py --symbols BTCUSD,XAUTUSD --watchdog`

## Multi-asset (BTCUSD + XAUTUSD)
The engine now runs several symbols on ONE websocket and ONE process, with full
state isolation:
- `--symbols` (default `BTCUSD,XAUTUSD`) is the watchlist. `--symbol` is only the
  primary/legacy alias (flat dashboard fields + the `self.strategy` alias).
- Each symbol gets its **own `Strategy`** (own footprint bars, own `_fired`
  latch) and its **own `DeltaFilter`** (own 40-bar rolling mean/σ/trigger), so
  gold's price/volume scale can never move BTC's gate (or vice versa).
- Samples are persisted per asset: BTC → `delta_samples`, XAUT →
  **`xaut_delta_samples`**. `Ledger._sample_table()` routes by symbol; the
  legacy 2-arg `record_delta_sample()` / `recent_delta_samples()` still default
  to BTCUSD for backward compatibility.
- The compact feed carries the symbol in the `sy` field of each trade
  (`{"type":"trades","sy":"XAUTUSD","p":...,"s":...,"r":"m","t":...}`).
  `MarketFeed._handle_trade` reads `sy` and calls
  `Engine.on_trade(..., symbol)`; `mark_price` carries `MARK:<SYM>`.
- Execution routing: `Engine._asset_for(symbol)` maps `XAUTUSD→XAUT`,
  `BTCUSD→BTC`, and the chain cache is keyed `(symbol, contract_type)` so a XAUT
  signal builds the XAUT chain, never BTC's.
- `snapshot()` keeps the flat (BTC) fields for the old dashboard shape AND adds
  `symbols` + `per_symbol`; `dashboard.py` has a symbol selector to switch views.

## Strategy knobs (execution / exit)
- `--min-premium` / `--max-premium` (default 200 / 400): pick the OTM strike whose
  live premium sits in this band (closest to midpoint), scanning every live
  expiry and preferring the nearest one that can supply the band — so an
  expiry-day chain that has collapsed to a few points cannot force a deep-OTM
  "chillar" strike. If no expiry reaches the band it takes the OTM strike
  closest to it. OTM-only is enforced — the engine never sells an ITM option.
  0/0 disables the band and restores the old cushion/HVN rule.
- `--premium-take-profit-pct` (default 0.5): book profit when the premium falls to
  `entry * (1 - pct)`; closes with reason `take_profit` and frees the slot.
  With a 200-400 band, 50% decay books ~100-200 points per trade.
- `--premium-stop-multiple` (default 1.5): loss ceiling, `entry * multiple`.
- `--max-open-positions` (default 5): multi-entry cap. Every fresh signal opens a
  new short until this many are open; the expiry backstop still runs.

## Deploy target (production)
- Host: AWS EC2 `ec2-user@3.108.53.100` (Amazon Linux 2023, Python 3.9.25).
- SSH key is provided per-session; do not commit it.
- Deploy: `sudo OLD_SERVICE="<svc> ..." bash deploy_aws.sh` from the repo copy.
  - `deploy_aws.sh` auto-detects `dnf` (Amazon Linux) vs `apt-get`.
  - `GO_LIVE=1` arms real orders; without keys the engine runs in PAPER mode.
- Install dir `/opt/orderflow`; credentials in `/etc/orderflow/orderflow.env` (0600).
- Services: `orderflow-engine.service` (engine, port 8080 dashboard) and
  `orderflow-tunnel.service` (cloudflared, exposes dashboard publicly).
- The host security group does NOT open 8080, so the dashboard is reached via the
  cloudflared quick tunnel. The URL changes on every tunnel restart:
  `sudo journalctl -u orderflow-tunnel | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -1`
- The service unit intentionally has NO `StandardOutput=append:` — the engine
  writes its own log file; adding it duplicates every line.

## Gotchas learned
- Python 3.9: asyncio Event/Lock must be created on the running loop. `Engine`
  exposes them as lazy properties; do not move them back into `__init__`.
- `EnvironmentFile` must be optional (`EnvironmentFile=-...`) or systemd fails
  the unit when the env file is empty/absent.
- The engine must be started with the venv interpreter (`/opt/orderflow/venv/bin/python`);
  the raw unit template points at system `python3` which lacks `aiohttp`.
- `Strategy.history` MUST be keyed by `cfg.timeframes | {rolling_tf, execution_tf}`.
  The rolling-window tf (15m) is materialised even though it is not in
  `timeframes`; if its history deque is missing, closing a 15m bar raises
  `KeyError: '15m'`, which propagates out of the feed handler, tears down the
  websocket, and reconnects every ~30s — so `delta_samples` never fills and the
  dynamic 2-sigma gate never arms. Covered by
  `test_rolling_tf_bar_close_persists_sample`.
- Dashboard signals are persisted as `engine_events(kind='signal')` and restored on
  cold start. Never keep them in the in-memory deque only: a restart (e.g. a
  deploy) would blank the dashboard's signal table. If the DB predates the
  persistence, `_backfill_signals_from_positions()` rebuilds rows from position
  entries so the table is never empty.
