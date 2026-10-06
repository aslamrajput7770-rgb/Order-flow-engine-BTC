# Order Flow Engine — BTCUSD (Delta Exchange India)

Institutional-grade order flow, execution, and cumulative-delta trading engine
for **BTCUSD options only**. Single-file Python engine, 24/7 automated.

## What it does
- Streams live BTCUSD trades from `wss://public-socket.india.delta.exchange`.
- Builds **footprint bars** (1m / 3m / 5m) and measures **cumulative delta** and
  stacked imbalances — no SMC/supply-demand, no indicator forecast.
- Dynamic **2-sigma** gate on a rolling 15m absolute-delta window: a bar whose
  delta exceeds `2 * sigma` of that window is an institutional order-flow event.
- On a qualifying signal it sells an OTM option (bearish delta -> call, bullish
  delta -> put) and manages the position to a fast profit.

## Strategy (Option C — locked)
- **Premium band 200-400**: the OTM strike whose live premium sits in the band is
  chosen, scanning every live expiry and preferring the nearest one that can
  supply the band. OTM-only; the engine never sells an ITM option.
- **Fast take-profit 50%**: closes when the premium falls to `entry * 0.5`,
  booking roughly **100-200 points per trade**, and frees the slot for the next
  signal.
- **Multi-entry up to 5**: every fresh signal opens a new short until 5 are open.
- **Stop loss 1.5x** entry premium as an absolute loss ceiling.
- **Expiry backstop**: any remaining position is closed before settlement.
- **Volume-driven, not timer-driven**: quiet hours simply produce no signals; the
  moment delta crosses 2-sigma the entry fires.

## Files
| File | Purpose |
|---|---|
| `orderflow_engine.py` | the engine (single file) |
| `dashboard.py` | live dashboard on `:8080` (footprint, delta, signals, positions) |
| `requirements.txt` | dependencies |
| `orderflow-engine.service` | systemd unit |
| `orderflow-tunnel.service` | optional Cloudflare tunnel for the dashboard |
| `deploy_aws.sh` | one-shot deploy to the AWS host |
| `run_engine.sh` | supervisor/run helper |
| `test_orderflow_engine.py` | unit tests (28) |

## Run locally (paper)
```bash
pip install -r requirements.txt
python3 orderflow_engine.py --watchdog --dashboard-port 8080 \
  --min-premium 200 --max-premium 400 \
  --premium-take-profit-pct 0.5 --max-open-positions 5
```
Then open `http://localhost:8080`.

## Tests
```bash
python3 -m unittest test_orderflow_engine
```

## Deploy (AWS)
```bash
# on the host, with the repo staged in ~/orderflow-src
sudo bash deploy_aws.sh
sudo systemctl status orderflow-engine
```
Credentials are read from `/etc/orderflow/orderflow.env`
(`DELTA_API_KEY` / `DELTA_API_SECRET`, mode 0600). Without them the engine runs
in **PAPER** mode.

## Notes
- Runtime state (`orderflow_state.db`, `state.json`, `*.log`, heartbeat) is
  gitignored — never commit live state or credentials.
- Signals are persisted to the ledger and restored on cold start, so a restart
  cannot blank the dashboard.
