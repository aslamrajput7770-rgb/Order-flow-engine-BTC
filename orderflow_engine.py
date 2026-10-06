#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Institutional Order Flow & Footprint Delta Engine
=================================================
BTCUSD  |  Delta Exchange India  |  Single-file production engine

Pipeline
--------
  1. Live tick tape (WebSocket ``trades``) -> aggressor-classified footprint
     matrix per candle (1m / 3m / 5m) with a strict diagonal (P vs P-1 / P+1)
     imbalance scan at a configurable multiple (default 2.5x).
  2. Cumulative-delta consensus gate. No execution layer is unlocked until the
     active bar's absolute delta clears a hard institutional block threshold
     (>= 1000 blocks) or the sweep threshold (>= 800 blocks).
  3. Asymmetric OTM option selling. Bearish delta + stacked bearish imbalances
     -> SELL CALL. Bullish delta + stacked bullish imbalances -> SELL PUT.
     Strike selection walks the live option chain outward past the high-volume
     node boundary with an explicit safety cushion.
  4. Durable ledger. Every position is persisted atomically to SQLite
     (``orderflow_state.db``) and mirrored to ``state.json`` BEFORE the order
     touches the network. Cold start re-adopts open contracts from the ledger.
  5. Supervisor layer. ``--watchdog`` runs a low-overhead process supervisor
     that restarts the engine within 10s and forces a ledger recovery read.

Safety
------
Real orders are only ever transmitted when BOTH ``--live`` is passed AND API
credentials are present. Without them the engine runs the complete strategy and
execution path against synthetic fills (paper mode). This is deliberate: the
engine is capital-bearing and must never arm itself implicitly.

Author: OpenHands agent.  Version: 1.0.0
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - dependency guard
    aiohttp = None  # type: ignore

try:
    from websockets.asyncio.client import connect as ws_connect
except Exception:  # pragma: no cover - websockets < 12 fallback
    try:
        from websockets import connect as ws_connect  # type: ignore
    except ImportError:
        ws_connect = None  # type: ignore

VERSION = "1.0.0"

DEFAULT_REST_BASE = os.environ.get("DELTA_REST_BASE", "https://api.india.delta.exchange")
# The compact ``trades`` / ``mark_price`` channels are served from the new
# public market-data pod. The legacy private pod (socket.india.delta.exchange)
# rejects ``trades`` with "subscription forbidden ... use appropriate pod".
DEFAULT_WS_URL = os.environ.get("DELTA_WS_URL", "wss://public-socket.india.delta.exchange")

TF_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000}

log = logging.getLogger("orderflow")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Config:
    # market
    symbol: str = "BTCUSD"
    asset: str = "BTC"
    timeframes: tuple = ("1m", "3m", "5m")
    execution_tf: str = "1m"

    # footprint / imbalance
    imbalance_multiple: float = 2.5
    stacked_imbalances_required: int = 1
    imbalance_stack_ttl_s: float = 180.0

    # cumulative delta gate
    delta_block_threshold: float = 1000.0   # static fallback only (dynamic mode ignores)
    delta_sweep_threshold: float = 800.0    # static fallback only (dynamic mode ignores)
    delta_print_grid: float = 0.0           # >0 enables near-print confirmation
    delta_print_tolerance: float = 8.0

    # dynamic rolling-average delta filter
    dynamic_delta: bool = True       # 2-sigma rolling filter is the default trigger
    delta_sigma_mult: float = 2.0    # trigger at 2.0 x sigma of |delta|
    rolling_window: int = 40         # last N closed bars
    rolling_tf: str = "15m"          # interval of the rolling window
    rolling_min_bars: int = 10       # min closed bars before the dynamic gate arms
    rolling_fallback_sigma: float = 400.0  # sigma seed during warmup

    # option strike selection
    otm_cushion_pct: float = 0.010   # strike must sit >=1% beyond spot
    safety_cushion_pct: float = 0.015
    min_otm_distance: float = 100.0
    chain_scan_strikes: int = 30
    # premium-band selection: pick the OTM strike whose premium sits in this band.
    # 0 disables the band and falls back to the cushion/HVN rule.
    min_premium: float = 0.0
    max_premium: float = 0.0

    # risk
    premium_stop_multiple: float = 1.5       # stop at Entry Premium x 1.5
    premium_take_profit_pct: float = 0.5     # book at Entry Premium x (1 - pct); 0 disables
    option_qty: int = 1
    max_open_positions: int = 5              # cap concurrent positions (multi-entry)

    # execution
    order_type: str = "market_order"     # market_order | limit_order
    limit_offset_pct: float = 0.002

    # infrastructure
    poll_interval: float = 5.0
    stats_interval: float = 30.0
    ws_heartbeat: float = 20.0
    rest_timeout: float = 15.0
    rest_base: str = DEFAULT_REST_BASE
    ws_url: str = DEFAULT_WS_URL
    state_db: str = "orderflow_state.db"
    state_json: str = "state.json"
    heartbeat_file: str = "orderflow_engine.heartbeat"
    log_file: str = "orderflow_engine.log"
    dashboard_port: int = 0            # 0 = disabled; >0 serves the live dashboard

    # mode
    live: bool = False


def now_ms() -> int:
    return int(time.time() * 1000)


def to_ms(ts: Any) -> int:
    """Normalize an exchange timestamp to milliseconds.

    Delta's compact ``trades`` payload reports ``t`` in microseconds
    (e.g. ``1791221892178446``), while REST/ISO timestamps are in ms/s.
    """
    try:
        v = int(ts)
    except (TypeError, ValueError):
        return now_ms()
    if v > 10 ** 14:        # microseconds
        v //= 1000
    elif v < 10 ** 11:      # seconds
        v *= 1000
    return v


def parse_iso_ts(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Imbalance:
    ts: int
    kind: str            # "bullish" | "bearish"
    price: float
    dominant_vol: float
    diagonal_vol: float
    ratio: float


@dataclass
class FootprintBar:
    tf: str
    start_ms: int
    end_ms: int
    tick_size: float
    multiple: float
    imbalance_ttl_s: float
    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    delta: float = 0.0
    trades: int = 0
    # tick-index -> volume. ask_vol = buyer-aggressor (lifts ask),
    # bid_vol = seller-aggressor (hits bid).
    ask_vol: dict = field(default_factory=dict)
    bid_vol: dict = field(default_factory=dict)
    imbalances: deque = field(default_factory=lambda: deque(maxlen=256))
    bull_stack: int = 0
    bear_stack: int = 0
    _last_imbalance_ms: int = 0

    # -- ingestion ---------------------------------------------------------
    def add_trade(self, price: float, size: float, aggressor: str, ts: int) -> None:
        if size <= 0 or price <= 0:
            return
        k = int(round(price / self.tick_size))
        if aggressor == "buy":
            self.ask_vol[k] = self.ask_vol.get(k, 0.0) + size
            self.delta += size
        elif aggressor == "sell":
            self.bid_vol[k] = self.bid_vol.get(k, 0.0) + size
            self.delta -= size
        else:
            return
        self.trades += 1
        if self.open == 0.0:
            self.open = price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self._scan_diagonal(k, ts)

    def _scan_diagonal(self, k: int, ts: int) -> None:
        ask_here = self.ask_vol.get(k, 0.0)
        bid_here = self.bid_vol.get(k, 0.0)
        bid_below = self.bid_vol.get(k - 1, 0.0)   # Bid at P-1 tick
        ask_above = self.ask_vol.get(k + 1, 0.0)   # Ask at P+1 tick

        if bid_below > 0 and ask_here >= self.multiple * bid_below:
            self._register("bullish", k, ask_here, bid_below, ts)
        if ask_above > 0 and bid_here >= self.multiple * ask_above:
            self._register("bearish", k, bid_here, ask_above, ts)

    def _register(self, kind: str, k: int, dom: float, diag: float, ts: int) -> None:
        self._decay_stacks(ts)
        self.imbalances.append(
            Imbalance(ts, kind, k * self.tick_size, dom, diag, dom / diag if diag else float("inf"))
        )
        if kind == "bullish":
            self.bull_stack += 1
            self.bear_stack = 0
        else:
            self.bear_stack += 1
            self.bull_stack = 0
        self._last_imbalance_ms = ts

    def _decay_stacks(self, ts: int) -> None:
        if self._last_imbalance_ms and (ts - self._last_imbalance_ms) > self.imbalance_ttl_s * 1000:
            self.bull_stack = 0
            self.bear_stack = 0

    # -- serialization -----------------------------------------------------
    def matrix(self, levels: int = 12) -> list:
        """Return the current footprint ladder (top ``levels`` price levels)."""
        keys = set(self.ask_vol) | set(self.bid_vol)
        ordered = sorted(keys, reverse=True)[:levels]
        out = []
        for k in ordered:
            out.append(
                {
                    "price": round(k * self.tick_size, 8),
                    "bid": round(self.bid_vol.get(k, 0.0), 6),
                    "ask": round(self.ask_vol.get(k, 0.0), 6),
                    "delta": round(self.ask_vol.get(k, 0.0) - self.bid_vol.get(k, 0.0), 6),
                }
            )
        return out


@dataclass
class Signal:
    direction: str        # "bearish" -> SELL CALL ; "bullish" -> SELL PUT
    delta: float
    price: float
    ts: int
    bull_stack: int
    bear_stack: int
    tf: str
    note: str = ""
    threshold: float = 0.0   # dynamic trigger level actually used
    mean: float = 0.0        # rolling mean of |delta|
    sigma: float = 0.0       # rolling standard deviation of |delta|
    samples: int = 0         # closed bars behind the dynamic threshold


# ---------------------------------------------------------------------------
# Dynamic rolling-average delta filter (2-sigma)
# ---------------------------------------------------------------------------
class DeltaFilter:
    """Rolling mean/sigma of absolute cumulative delta.

    Tracks the last ``window`` closed bars at ``tf`` and derives the trade
    trigger as ``sigma_mult * sigma``. The threshold therefore scales with the
    market: quiet hours shrink it, US-session momentum expands it. There is no
    hardcoded block size in the decision path.
    """

    def __init__(self, window: int = 40, sigma_mult: float = 2.0,
                 min_bars: int = 10, fallback_sigma: float = 400.0):
        self.window = window
        self.sigma_mult = sigma_mult
        self.min_bars = min_bars
        self.fallback_sigma = fallback_sigma
        self.samples: deque = deque(maxlen=window)

    def add_bar(self, abs_delta: float) -> None:
        self.samples.append(float(abs_delta))

    def stats(self) -> tuple:
        """Return (mean, sigma, samples)."""
        n = len(self.samples)
        if n == 0:
            return 0.0, self.fallback_sigma, 0
        mean = sum(self.samples) / n
        if n < 2:
            return mean, self.fallback_sigma, n
        var = sum((x - mean) ** 2 for x in self.samples) / (n - 1)
        return mean, var ** 0.5, n

    @property
    def armed(self) -> bool:
        return len(self.samples) >= self.min_bars

    def threshold(self) -> float:
        _, sigma, _ = self.stats()
        return max(self.sigma_mult * sigma, 1.0)

    def snapshot(self) -> dict:
        mean, sigma, n = self.stats()
        return {
            "window": self.window,
            "sigma_mult": self.sigma_mult,
            "min_bars": self.min_bars,
            "samples": n,
            "armed": self.armed,
            "mean": round(mean, 4),
            "sigma": round(sigma, 4),
            "threshold": round(self.threshold(), 4),
            "recent": [round(x, 4) for x in list(self.samples)[-self.window:]],
        }

    def load(self, recent: Iterable) -> None:
        for x in recent:
            self.add_bar(x)


# ---------------------------------------------------------------------------
# Durable ledger (SQLite + atomic JSON mirror)
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_uid    TEXT UNIQUE NOT NULL,
    symbol          TEXT NOT NULL,
    product_id      INTEGER,
    contract_type   TEXT,
    side            TEXT NOT NULL,
    qty             REAL NOT NULL,
    entry_premium   REAL NOT NULL,
    entry_underlying REAL,
    stop_price      REAL NOT NULL,
    entry_delta     REAL,
    entry_threshold REAL,
    entry_mean      REAL,
    entry_sigma     REAL,
    entry_ts        INTEGER NOT NULL,
    expiry_ts       INTEGER,
    state           TEXT NOT NULL DEFAULT 'pending',
    exit_premium    REAL,
    exit_ts         INTEGER,
    exit_reason     TEXT,
    order_id        TEXT,
    raw_entry       TEXT,
    raw_exit        TEXT
);
CREATE TABLE IF NOT EXISTS executions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER NOT NULL,
    position_uid TEXT,
    action       TEXT NOT NULL,
    symbol       TEXT,
    product_id   INTEGER,
    side         TEXT,
    qty          REAL,
    price        REAL,
    order_id     TEXT,
    status       TEXT,
    detail       TEXT
);
CREATE TABLE IF NOT EXISTS engine_events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     INTEGER NOT NULL,
    kind   TEXT NOT NULL,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS delta_samples (
    bar_start INTEGER PRIMARY KEY,
    tf        TEXT NOT NULL,
    abs_delta REAL NOT NULL,
    delta     REAL NOT NULL,
    mean      REAL,
    sigma     REAL,
    threshold REAL,
    ts        INTEGER NOT NULL
);
"""


def atomic_write_json(path: str, obj: Any) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp_state_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(obj, fh, indent=2, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


class Ledger:
    def __init__(self, db_path: str, json_path: str):
        self.db_path = db_path
        self.json_path = json_path
        self.conn = sqlite3.connect(db_path, timeout=10.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()
        self.mirror()

    def _migrate(self) -> None:
        """Add columns introduced after a database may already exist."""
        cur = self.conn.execute("PRAGMA table_info(positions)")
        cols = {r["name"] for r in cur.fetchall()}
        for col in ("entry_threshold", "entry_mean", "entry_sigma"):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE positions ADD COLUMN {col} REAL")

    # -- low level ---------------------------------------------------------
    def _exec(self, sql: str, params: Iterable = ()) -> sqlite3.Cursor:
        cur = self.conn.execute(sql, tuple(params))
        self.conn.commit()
        return cur

    def event(self, kind: str, detail: Any = None) -> None:
        self._exec(
            "INSERT INTO engine_events(ts,kind,detail) VALUES(?,?,?)",
            (now_ms(), kind, json.dumps(detail, default=str)),
        )

    def recent_events(self, kind: str, limit: int) -> list:
        """Return up to `limit` most recent events of `kind`, oldest first."""
        cur = self.conn.execute(
            "SELECT detail FROM engine_events WHERE kind=? ORDER BY id DESC LIMIT ?",
            (kind, int(limit)),
        )
        out = []
        for r in cur.fetchall():
            try:
                out.append(json.loads(r["detail"]))
            except (TypeError, ValueError):
                continue
        out.reverse()
        return out

    # -- positions ---------------------------------------------------------
    def open_position(self, pos: dict) -> None:
        """Persist a position as 'pending' BEFORE it is sent to the network."""
        self._exec(
            """INSERT INTO positions
               (position_uid, symbol, product_id, contract_type, side, qty,
                entry_premium, entry_underlying, stop_price, entry_delta,
                entry_threshold, entry_mean, entry_sigma,
                entry_ts, expiry_ts, state, order_id, raw_entry)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                pos["position_uid"], pos["symbol"], pos.get("product_id"),
                pos.get("contract_type"), pos["side"], pos["qty"],
                pos["entry_premium"], pos.get("entry_underlying"),
                pos["stop_price"], pos.get("entry_delta"),
                pos.get("entry_threshold"), pos.get("entry_mean"), pos.get("entry_sigma"),
                pos["entry_ts"], pos.get("expiry_ts"), pos.get("state", "pending"),
                pos.get("order_id"), json.dumps(pos.get("raw_entry"), default=str),
            ),
        )
        self.mirror()

    def mark_open(self, uid: str, order_id: Optional[str], raw: Any = None) -> None:
        self._exec(
            "UPDATE positions SET state='open', order_id=?, raw_entry=? WHERE position_uid=?",
            (order_id, json.dumps(raw, default=str), uid),
        )
        self.mirror()

    def close_position(self, uid: str, exit_premium: float, reason: str, raw: Any = None) -> None:
        self._exec(
            """UPDATE positions SET state='closed', exit_premium=?, exit_ts=?,
               exit_reason=?, raw_exit=? WHERE position_uid=?""",
            (exit_premium, now_ms(), reason, json.dumps(raw, default=str), uid),
        )
        self.mirror()

    def set_state(self, uid: str, state: str) -> None:
        self._exec("UPDATE positions SET state=? WHERE position_uid=?", (state, uid))
        self.mirror()

    def open_positions(self) -> list:
        cur = self.conn.execute(
            "SELECT * FROM positions WHERE state IN ('pending','open') ORDER BY entry_ts ASC"
        )
        return [dict(r) for r in cur.fetchall()]

    def closed_positions(self) -> list:
        cur = self.conn.execute(
            "SELECT * FROM positions WHERE state='closed' ORDER BY entry_ts ASC"
        )
        return [dict(r) for r in cur.fetchall()]

    def position(self, uid: str) -> Optional[dict]:
        cur = self.conn.execute("SELECT * FROM positions WHERE position_uid=?", (uid,))
        row = cur.fetchone()
        return dict(row) if row else None

    def record_execution(self, **kw) -> None:
        self._exec(
            """INSERT INTO executions
               (ts, position_uid, action, symbol, product_id, side, qty,
                price, order_id, status, detail)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                kw.get("ts", now_ms()), kw.get("position_uid"), kw.get("action"),
                kw.get("symbol"), kw.get("product_id"), kw.get("side"),
                kw.get("qty"), kw.get("price"), kw.get("order_id"),
                kw.get("status"), json.dumps(kw.get("detail"), default=str),
            ),
        )

    # -- rolling delta samples --------------------------------------------
    def record_delta_sample(self, bar_start: int, tf: str, delta: float,
                            mean: float, sigma: float, threshold: float) -> None:
        self._exec(
            """INSERT OR REPLACE INTO delta_samples
               (bar_start, tf, abs_delta, delta, mean, sigma, threshold, ts)
               VALUES (?,?,?,?,?,?,?,?)""",
            (bar_start, tf, abs(delta), delta, mean, sigma, threshold, now_ms()),
        )

    def recent_delta_samples(self, tf: str, limit: int) -> list:
        cur = self.conn.execute(
            "SELECT abs_delta FROM delta_samples WHERE tf=? ORDER BY bar_start DESC LIMIT ?",
            (tf, limit),
        )
        rows = [r["abs_delta"] for r in cur.fetchall()]
        rows.reverse()   # oldest -> newest
        return rows

    # -- mirror ------------------------------------------------------------
    def mirror(self, delta_filter: Optional[dict] = None) -> None:
        cur = self.conn.execute(
            "SELECT * FROM positions WHERE state IN ('pending','open') ORDER BY entry_ts ASC"
        )
        snapshot = {
            "engine": "orderflow_engine",
            "version": VERSION,
            "updated_ts": now_ms(),
            "open_positions": [dict(r) for r in cur.fetchall()],
        }
        if delta_filter is not None:
            snapshot["delta_filter"] = delta_filter
        atomic_write_json(self.json_path, snapshot)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Delta Exchange India REST client (HMAC-SHA256 signed)
# ---------------------------------------------------------------------------
class DeltaREST:
    def __init__(self, cfg: Config, session: "aiohttp.ClientSession", api_key: str = "", api_secret: str = ""):
        self.cfg = cfg
        self.session = session
        self.api_key = api_key
        self.api_secret = api_secret

    @property
    def armed(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _signature(self, method: str, path: str, query: str, body: str) -> tuple:
        ts = str(int(time.time()))
        payload = method + ts + path + (query or "") + (body or "")
        sig = hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return sig, ts

    async def request(self, method: str, path: str, params: Optional[dict] = None,
                      body: Optional[dict] = None, signed: bool = False) -> Any:
        query = urllib.parse.urlencode(sorted(params.items())) if params else ""
        raw_body = ""
        if body is not None:
            raw_body = json.dumps(body, separators=(",", ":"))
        url = self.cfg.rest_base + path + (f"?{query}" if query else "")
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if signed:
            if not self.armed:
                raise RuntimeError("signed request requires API credentials")
            sig, ts = self._signature(method, path, query, raw_body)
            headers.update({"api-key": self.api_key, "signature": sig, "timestamp": ts})
        async with self.session.request(
            method, url, headers=headers, data=raw_body or None,
            timeout=aiohttp.ClientTimeout(total=self.cfg.rest_timeout),
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status} {method} {path}: {text[:300]}")
            return json.loads(text) if text else {}

    # -- public market data ------------------------------------------------
    async def products(self, contract_types: Optional[str] = None) -> list:
        params = {"page_size": 1000}
        if contract_types:
            params["contract_types"] = contract_types
        data = await self.request("GET", "/v2/products", params=params)
        return data.get("result", []) or []

    async def product(self, symbol: str) -> dict:
        data = await self.request("GET", f"/v2/products/{symbol}")
        return data.get("result", {}) or {}

    async def ticker(self, symbol: str) -> dict:
        data = await self.request("GET", f"/v2/tickers/{symbol}")
        return data.get("result", {}) or {}

    async def bulk_tickers(self, contract_types: str) -> dict:
        """All tickers for a contract type, keyed by product_id.

        The products endpoint omits traded volume / open interest, so the option
        chain must be enriched from here for high-volume-node detection.
        """
        data = await self.request("GET", "/v2/tickers", params={"contract_types": contract_types})
        out = {}
        for t in data.get("result", []) or []:
            pid = t.get("product_id")
            if pid is not None:
                out[int(pid)] = t
        return out

    async def option_chain(self, asset: str, contract_type: str) -> list:
        products = await self.products(contract_types=contract_type)
        try:
            tickers = await self.bulk_tickers(contract_type)
        except Exception:  # noqa: BLE001
            tickers = {}
        prefix = "C" if contract_type == "call_options" else "P"
        out = []
        for p in products:
            sym = p.get("symbol", "")
            if not sym.startswith(f"{prefix}-{asset}-") or p.get("state") != "live":
                continue
            t = tickers.get(int(p.get("id", -1)), {})
            merged = dict(p)
            merged["volume"] = t.get("volume", p.get("volume", 0))
            merged["oi_contracts"] = t.get("oi_contracts", p.get("oi_contracts", 0))
            if t.get("mark_price") is not None:
                merged["mark_price"] = t.get("mark_price")
            out.append(merged)
        return out

    # -- trading -----------------------------------------------------------
    async def place_order(self, product_id: int, side: str, size: float,
                          order_type: str, limit_price: Optional[float] = None) -> dict:
        body = {
            "product_id": int(product_id),
            "size": size,
            "side": side,
            "order_type": order_type,
        }
        if order_type == "limit_order" and limit_price is not None:
            body["limit_price"] = str(limit_price)
        data = await self.request("POST", "/v2/orders", body=body, signed=True)
        return data.get("result", data) or {}

    async def positions(self) -> list:
        data = await self.request("GET", "/v2/positions", params={"size": 100}, signed=True)
        return data.get("result", []) or []


# ---------------------------------------------------------------------------
# Broker wrapper: real orders when armed, synthetic fills otherwise
# ---------------------------------------------------------------------------
class Broker:
    def __init__(self, cfg: Config, rest: DeltaREST):
        self.cfg = cfg
        self.rest = rest

    @property
    def live(self) -> bool:
        return self.cfg.live and self.rest.armed

    async def sell_option(self, product: dict, qty: int, limit_price: Optional[float]) -> dict:
        if not self.live:
            return {"id": f"paper-{uuid.uuid4().hex[:12]}", "state": "paper", "average_fill_price": limit_price}
        order_type = self.cfg.order_type
        price = None
        if order_type == "limit_order":
            price = limit_price
        return await self.rest.place_order(int(product["id"]), "sell", qty, order_type, price)

    async def buy_back(self, product_id: int, qty: int, limit_price: Optional[float] = None) -> dict:
        if not self.live:
            return {"id": f"paper-{uuid.uuid4().hex[:12]}", "state": "paper", "average_fill_price": limit_price}
        return await self.rest.place_order(int(product_id), "buy", qty, "market_order", None)


# ---------------------------------------------------------------------------
# Strategy: footprint -> delta gate -> signal
# ---------------------------------------------------------------------------
class Strategy:
    def __init__(self, cfg: Config, on_bar_close=None):
        self.cfg = cfg
        self.bars: dict = {}
        # History must cover every timeframe the engine materialises, including
        # the rolling-window tf and the execution tf; otherwise closing a bar on
        # a tf that is not in cfg.timeframes (e.g. 15m) raises KeyError and tears
        # down the websocket before the sample is ever persisted.
        self.history: dict = {
            tf: deque(maxlen=256)
            for tf in (set(cfg.timeframes) | {cfg.rolling_tf, cfg.execution_tf})
        }
        self.session_cum_delta = 0.0
        self._fired: dict = {}   # tf -> bar_start_ms already emitted
        self._on_bar_close = on_bar_close
        self.filter = DeltaFilter(
            window=cfg.rolling_window,
            sigma_mult=cfg.delta_sigma_mult,
            min_bars=cfg.rolling_min_bars,
            fallback_sigma=cfg.rolling_fallback_sigma,
        )

    def _bar_for(self, tf: str, ts: int, tick_size: float) -> FootprintBar:
        span = TF_MS[tf]
        start = ts - (ts % span)
        bar = self.bars.get(tf)
        if bar is None or bar.start_ms != start:
            if bar is not None:
                self.session_cum_delta += bar.delta
                self.history[tf].append(bar)
                if tf == self.cfg.rolling_tf:
                    self.filter.add_bar(abs(bar.delta))
                    if self._on_bar_close:
                        self._on_bar_close(bar, self.filter)
            bar = FootprintBar(
                tf=tf, start_ms=start, end_ms=start + span, tick_size=tick_size,
                multiple=self.cfg.imbalance_multiple,
                imbalance_ttl_s=self.cfg.imbalance_stack_ttl_s,
            )
            self.bars[tf] = bar
        return bar

    def on_trade(self, price: float, size: float, aggressor: str, ts: int, tick_size: float) -> Optional[Signal]:
        # the rolling window bar must be materialised even if it is not the
        # execution timeframe, otherwise it never accumulates a delta.
        tfs = set(self.cfg.timeframes) | {self.cfg.rolling_tf, self.cfg.execution_tf}
        for tf in tfs:
            self._bar_for(tf, ts, tick_size).add_trade(price, size, aggressor, ts)
        if self.cfg.execution_tf not in self.bars:
            return None
        return self.evaluate(self.bars[self.cfg.execution_tf])

    def delta_gate(self, bar: FootprintBar) -> Optional[tuple]:
        """Return (delta, threshold, mean, sigma, samples) when the gate opens.

        In dynamic mode the threshold is ``sigma_mult * sigma`` of the rolling
        absolute-delta window - no static block size is consulted. During warmup
        (fewer than ``rolling_min_bars`` closed bars) the gate stays closed so a
        thin history cannot fabricate a trigger.
        """
        d = bar.delta
        ad = abs(d)
        cfg = self.cfg
        if cfg.dynamic_delta:
            mean, sigma, n = self.filter.stats()
            if not self.filter.armed:
                return None
            threshold = self.filter.threshold()
            if ad < threshold:
                return None
            return d, threshold, mean, sigma, n
        if ad < cfg.delta_sweep_threshold:
            return None
        if cfg.delta_print_grid > 0 and ad >= cfg.delta_block_threshold:
            nearest = round(ad / cfg.delta_print_grid) * cfg.delta_print_grid
            if abs(ad - nearest) > cfg.delta_print_tolerance:
                return None
        return d, cfg.delta_sweep_threshold, 0.0, 0.0, 0

    def evaluate(self, bar: FootprintBar) -> Optional[Signal]:
        gate = self.delta_gate(bar)
        if gate is None:
            return None
        d, threshold, mean, sigma, n = gate
        required = max(1, self.cfg.stacked_imbalances_required)
        direction = None
        if d < 0 and bar.bear_stack >= required:
            direction = "bearish"
        elif d > 0 and bar.bull_stack >= required:
            direction = "bullish"
        if direction is None:
            return None
        # One execution per cleared bar per timeframe: the tape keeps printing
        # after the gate opens, so without this latch a single bar would spam
        # entries on every tick.
        if self._fired.get(bar.tf) == bar.start_ms:
            return None
        self._fired[bar.tf] = bar.start_ms
        if self.cfg.dynamic_delta:
            note = (f"{direction} | |delta| {abs(d):.0f} >= {self.cfg.delta_sigma_mult:.1f}sigma "
                    f"({threshold:.0f}) | mean {mean:.0f} sigma {sigma:.0f} n={n}")
        else:
            note = (f"{direction} | |delta| {abs(d):.0f} >= static {threshold:.0f}")
        return Signal(direction, d, bar.close, bar.start_ms + 1, bar.bull_stack,
                      bar.bear_stack, bar.tf, note, threshold, mean, sigma, n)


# ---------------------------------------------------------------------------
# Strike selection
# ---------------------------------------------------------------------------
def _select_by_premium_band(live: list, underlying: float, contract_type: str, cfg: Config) -> Optional[dict]:
    """Pick the OTM strike whose premium sits in [min,max], scanning every expiry.

    We prefer the nearest expiry that can actually reach the band, so an
    expiry-day chain whose premiums have collapsed to a few points does not force
    a deep-OTM "chillar" strike. If no expiry reaches the band, the OTM strike
    closest to it wins (ties break toward the nearer expiry).
    """
    mid = (cfg.min_premium + cfg.max_premium) / 2.0
    by_exp: dict = {}
    for s, p in live:
        is_otm = s > underlying if contract_type == "call_options" else s < underlying
        if not is_otm:
            continue
        prem = fnum(p.get("mark_price"))
        if prem <= 0:
            continue
        by_exp.setdefault(p.get("settlement_time") or "", []).append((s, p, prem))
    if not by_exp:
        return None
    for exp in sorted(by_exp):
        band = [t for t in by_exp[exp] if cfg.min_premium <= t[2] <= cfg.max_premium]
        if band:
            band.sort(key=lambda t: abs(t[2] - mid))
            return band[0][1]
    best = None
    for exp in sorted(by_exp):
        for s, p, prem in by_exp[exp]:
            distance = max(cfg.min_premium - prem, prem - cfg.max_premium, 0.0)
            cand = (distance, exp, s, p)
            if best is None or cand < best:
                best = cand
    return best[3] if best else None


def select_contract(products: list, underlying: float, contract_type: str, cfg: Config) -> Optional[dict]:
    """Nearest liquid strike sitting outside the high-volume node with a cushion.

    The raw chain spans every listed expiry, so strikes repeat. When a premium
    band is configured we first scan every live expiry for a strike that can
    supply the band (preferring the nearest such expiry). Otherwise we restrict
    to the nearest expiry (unique strikes) and apply the OTM cushion and the
    high-volume-node boundary.
    """
    live = []
    for p in products:
        if p.get("state") != "live":
            continue
        strike = fnum(p.get("strike_price"))
        if strike <= 0:
            continue
        live.append((strike, p))
    if not live:
        return None

    if cfg.min_premium > 0 and cfg.max_premium >= cfg.min_premium:
        band_sel = _select_by_premium_band(live, underlying, contract_type, cfg)
        if band_sel is not None:
            return band_sel

    expiries = sorted({p.get("settlement_time") for _, p in live if p.get("settlement_time")})
    if expiries:
        nearest = expiries[0]
        live = [(s, p) for s, p in live if p.get("settlement_time") == nearest]
    if not live:
        return None

    by_strike: dict = {}
    for s, p in live:
        by_strike.setdefault(s, p)
    items = sorted(by_strike.items())          # ascending strike, unique
    strikes = [s for s, _ in items]
    steps = [b - a for a, b in zip(strikes, strikes[1:]) if b > a]
    step = min(steps) if steps else max(1.0, underlying * 0.001)

    def liquidity(p: dict) -> float:
        return fnum(p.get("volume")) + fnum(p.get("oi_contracts"))

    total_liq = sum(liquidity(p) for _, p in items)
    hvn = max(items, key=lambda sp: liquidity(sp[1]))[0] if total_liq > 0 else underlying

    if contract_type == "call_options":
        floor = max(underlying * (1 + cfg.otm_cushion_pct), underlying + cfg.min_otm_distance)
        boundary = max(floor, hvn + step)
        candidates = [(s, p) for s, p in items if s >= boundary]
        if not candidates:
            candidates = [(s, p) for s, p in items if s >= floor]
    else:
        ceil = min(underlying * (1 - cfg.otm_cushion_pct), underlying - cfg.min_otm_distance)
        boundary = min(ceil, hvn - step)
        candidates = [(s, p) for s, p in items if s <= boundary]
        if not candidates:
            candidates = [(s, p) for s, p in items if s <= ceil]
        candidates.reverse()                   # nearest strike first

    if not candidates:
        return None
    safe = []
    for s, p in candidates:
        distance = (s - underlying) if contract_type == "call_options" else (underlying - s)
        if distance >= cfg.safety_cushion_pct * underlying:
            safe.append((s, p))
    chosen = safe[0] if safe else candidates[0]
    return chosen[1]


# ---------------------------------------------------------------------------
# Market data feed (WebSocket)
# ---------------------------------------------------------------------------
class MarketFeed:
    def __init__(self, cfg: Config, engine: "Engine"):
        self.cfg = cfg
        self.engine = engine
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._session()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("feed disconnected: %s (retry in %.1fs)", exc, backoff)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    async def _session(self) -> None:
        if ws_connect is None:
            raise RuntimeError("websockets library unavailable")
        sub = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "trades", "symbols": [self.cfg.symbol]},
                    {"name": "mark_price", "symbols": [f"MARK:{self.cfg.symbol}"]},
                ]
            },
        }
        async with ws_connect(self.cfg.ws_url, ping_interval=self.cfg.ws_heartbeat,
                              ping_timeout=self.cfg.ws_heartbeat, max_size=2 ** 22) as ws:
            await ws.send(json.dumps(sub))
            log.info("feed connected: %s", self.cfg.ws_url)
            self.engine.on_feed_up()
            async for raw in ws:
                if self._stop.is_set():
                    break
                self._handle(raw)

    def _handle(self, raw: Any) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        if isinstance(msg, list):
            for item in msg:
                self._handle_item(item)
            return
        self._handle_item(msg)

    def _handle_item(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return
        mtype = msg.get("type")
        if mtype == "trades":
            trades = msg.get("trades") if isinstance(msg.get("trades"), list) else [msg]
            for t in trades:
                self._handle_trade(t)
        elif mtype in ("v2/ticker", "mark_price", "ticker"):
            self.engine.on_ticker(msg)

    def _handle_trade(self, t: dict) -> None:
        price = fnum(t.get("p", t.get("price")))
        size = fnum(t.get("s", t.get("size")))
        ts = to_ms(t.get("t", t.get("timestamp")))
        role = t.get("r", t.get("buyer_role"))
        side = t.get("side")
        if role == "t":
            aggressor = "buy"
        elif role == "m":
            aggressor = "sell"
        elif side in ("buy", "sell"):
            aggressor = side
        else:
            aggressor = None
        if not price or not size or aggressor is None:
            return
        self.engine.on_trade(price, size, aggressor, ts)


# ---------------------------------------------------------------------------
# Engine orchestrator
# ---------------------------------------------------------------------------
class Engine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.ledger = Ledger(cfg.state_db, cfg.state_json)
        self.strategy = Strategy(cfg, on_bar_close=self._on_rolling_bar)
        self.session: Optional["aiohttp.ClientSession"] = None
        self.rest: Optional[DeltaREST] = None
        self.broker: Optional[Broker] = None
        self.feed: Optional[MarketFeed] = None
        # asyncio primitives are created lazily on first use so they bind to
        # the running loop. Creating them here would bind them to whatever
        # loop is current at construction time (the default loop on py3.9),
        # which then raises "attached to a different loop" once the engine
        # runs under asyncio.run().
        self._stop_event: Optional[asyncio.Event] = None
        self._exec_lock_obj: Optional[asyncio.Lock] = None
        self._tick_size = 0.5
        self._mark_price: float = 0.0
        self._open: dict = {}          # uid -> position dict
        self._chain_cache: dict = {}   # contract_type -> (ts, [products])
        self._signals: deque = deque(maxlen=100)
        self._dash_runner = None

    @property
    def _stop(self) -> asyncio.Event:
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        return self._stop_event

    @property
    def _exec_lock(self) -> asyncio.Lock:
        if self._exec_lock_obj is None:
            self._exec_lock_obj = asyncio.Lock()
        return self._exec_lock_obj

    def _on_rolling_bar(self, bar, delta_filter) -> None:
        """Persist every closed rolling bar so the sigma window survives restarts."""
        mean, sigma, _ = delta_filter.stats()
        self.ledger.record_delta_sample(bar.start_ms, bar.tf, bar.delta, mean, sigma,
                                        delta_filter.threshold())
        self.ledger.mirror(delta_filter.snapshot())

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        if aiohttp is None:
            raise RuntimeError("aiohttp is required")
        api_key = os.environ.get("DELTA_API_KEY", "")
        api_secret = os.environ.get("DELTA_API_SECRET", "")
        self.session = aiohttp.ClientSession()
        self.rest = DeltaREST(self.cfg, self.session, api_key, api_secret)
        self.broker = Broker(self.cfg, self.rest)
        self.feed = MarketFeed(self.cfg, self)

        log.info("engine %s starting | mode=%s | symbol=%s",
                 VERSION, "LIVE" if self.broker.live else "PAPER", self.cfg.symbol)

        await self._load_market_meta()
        await self._recover()

        tasks = [
            asyncio.create_task(self.feed.run(), name="feed"),
            asyncio.create_task(self._monitor_loop(), name="monitor"),
            asyncio.create_task(self._stats_loop(), name="stats"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
        ]
        if self.cfg.dashboard_port:
            from dashboard import serve as serve_dashboard
            self._dash_runner = await serve_dashboard(self, self.cfg.dashboard_port)
            log.info("live dashboard on http://0.0.0.0:%d", self.cfg.dashboard_port)
        try:
            await self._stop.wait()
        finally:
            self.feed.stop()
            if self._dash_runner is not None:
                await self._dash_runner.cleanup()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.ledger.event("shutdown", {"open": list(self._open)})
            self.ledger.close()
            if self.session:
                await self.session.close()
            log.info("engine stopped")

    def request_stop(self) -> None:
        self._stop.set()

    async def _load_market_meta(self) -> None:
        try:
            prod = await self.rest.product(self.cfg.symbol)
            self._tick_size = fnum(prod.get("tick_size"), 0.5) or 0.5
        except Exception as exc:  # noqa: BLE001
            log.warning("could not load %s metadata (%s); defaulting tick=0.5", self.cfg.symbol, exc)
        log.info("tick size for %s = %s", self.cfg.symbol, self._tick_size)

    # -- cold start recovery ----------------------------------------------
    async def _recover(self) -> None:
        # restore the rolling sigma window first so the dynamic gate is armed
        # immediately after a restart instead of waiting hours to warm up.
        samples = self.ledger.recent_delta_samples(self.cfg.rolling_tf, self.cfg.rolling_window)
        if samples:
            self.strategy.filter.load(samples)
            mean, sigma, n = self.strategy.filter.stats()
            log.info("cold start: restored %d rolling %s delta samples | mean=%.1f sigma=%.1f "
                     "threshold=%.1f armed=%s", n, self.cfg.rolling_tf, mean, sigma,
                     self.strategy.filter.threshold(), self.strategy.filter.armed)
        else:
            log.info("cold start: no rolling delta history; dynamic gate warms up over "
                     "%d closed %s bars", self.cfg.rolling_min_bars, self.cfg.rolling_tf)

        restored = self.ledger.recent_events("signal", 100)
        for rec in restored:
            self._signals.append(rec)
        if restored:
            log.info("cold start: restored %d signal(s) to the dashboard", len(restored))
        elif self._backfill_signals_from_positions():
            log.info("cold start: backfilled %d signal(s) from trade history",
                     len(self._signals))

        rows = self.ledger.open_positions()
        if not rows:
            log.info("cold start: no open positions in ledger")
            return
        log.info("cold start: re-adopting %d position(s) from ledger", len(rows))
        for row in rows:
            uid = row["position_uid"]
            if row["state"] == "pending":
                row = await self._reconcile_pending(row)
                if row is None:
                    continue
            self._open[uid] = row
            self.ledger.event("recover", {"uid": uid, "symbol": row["symbol"], "state": row["state"]})

    def _backfill_signals_from_positions(self) -> bool:
        """Rebuild dashboard signal rows from trade history.

        Older builds kept signals in memory only, so a restart wiped them off the
        dashboard even though the trades were still in the ledger. Rebuild them
        from each position's entry so the table is not empty after a deploy.
        """
        rows = self.ledger.closed_positions() + self.ledger.open_positions()
        rows = [r for r in rows if r.get("entry_ts") is not None
                and r.get("entry_premium") is not None]
        if not rows:
            return False
        for r in sorted(rows, key=lambda x: x["entry_ts"])[-100:]:
            bullish = r.get("contract_type") == "put_options"
            d = abs(r.get("entry_delta") or 0.0)
            self._signals.append({
                "ts": r["entry_ts"],
                "direction": "bullish" if bullish else "bearish",
                "delta": d if bullish else -d,
                "price": r.get("entry_underlying") or 0.0,
                "threshold": r.get("entry_threshold") or 0.0,
                "mean": r.get("entry_mean") or 0.0,
                "sigma": r.get("entry_sigma") or 0.0,
                "samples": 0, "tf": self.cfg.execution_tf,
                "note": "backfilled from trade history",
            })
        return True

    async def _reconcile_pending(self, row: dict) -> Optional[dict]:
        """A pending row may or may not have filled before a crash."""
        if not self.broker.live:
            log.warning("pending position %s kept as open (paper mode, cannot reconcile)", row["position_uid"])
            self.ledger.mark_open(row["position_uid"], row.get("order_id"))
            row["state"] = "open"
            return row
        try:
            live = await self.rest.positions()
        except Exception as exc:  # noqa: BLE001
            log.error("reconcile failed for %s: %s", row["position_uid"], exc)
            return row
        for p in live:
            if int(p.get("product_id", -1)) == int(row.get("product_id") or -1) and fnum(p.get("size")) != 0:
                log.info("pending %s confirmed open on exchange", row["position_uid"])
                self.ledger.mark_open(row["position_uid"], row.get("order_id"), p)
                row["state"] = "open"
                return row
        log.warning("pending %s not found on exchange; marking orphaned", row["position_uid"])
        self.ledger.set_state(row["position_uid"], "orphaned")
        return None

    # -- feed callbacks ----------------------------------------------------
    def on_feed_up(self) -> None:
        self._write_heartbeat()

    def on_ticker(self, msg: dict) -> None:
        mark = fnum(msg.get("mark_price", msg.get("p")))
        if mark:
            self._mark_price = mark
        self._write_heartbeat()

    def on_trade(self, price: float, size: float, aggressor: str, ts: int) -> None:
        self._write_heartbeat()
        signal = self.strategy.on_trade(price, size, aggressor, ts, self._tick_size)
        if signal is None:
            return
        record = {
            "ts": now_ms(), "direction": signal.direction, "delta": signal.delta,
            "price": signal.price, "threshold": signal.threshold, "mean": signal.mean,
            "sigma": signal.sigma, "samples": signal.samples, "tf": signal.tf,
            "note": signal.note,
        }
        self._signals.append(record)
        # persist so a restart cannot wipe the signal history off the dashboard
        self.ledger.event("signal", record)
        asyncio.create_task(self._handle_signal(signal))

    # -- execution ---------------------------------------------------------
    async def _handle_signal(self, signal: Signal) -> None:
        async with self._exec_lock:
            if len(self._open) >= self.cfg.max_open_positions:
                return
            try:
                await self._execute(signal)
            except Exception as exc:  # noqa: BLE001
                log.exception("execution failed: %s", exc)
                self.ledger.event("execution_error", {"error": str(exc), "signal": asdict(signal)})

    async def _execute(self, signal: Signal) -> None:
        cfg = self.cfg
        contract_type = "call_options" if signal.direction == "bearish" else "put_options"
        chain = await self._get_chain(contract_type)
        product = select_contract(chain, signal.price, contract_type, cfg)
        if product is None:
            log.warning("no eligible %s strike for signal %s", contract_type, signal.direction)
            return
        ticker = await self.rest.ticker(product["symbol"])
        premium = fnum(ticker.get("mark_price"))
        if premium <= 0:
            log.warning("no premium for %s; aborting", product["symbol"])
            return
        stop_price = premium * cfg.premium_stop_multiple
        uid = f"pos-{uuid.uuid4().hex[:16]}"
        expiry_ts = parse_iso_ts(product.get("settlement_time"))
        pos = {
            "position_uid": uid,
            "symbol": product["symbol"],
            "product_id": product["id"],
            "contract_type": contract_type,
            "side": "sell",
            "qty": cfg.option_qty,
            "entry_premium": premium,
            "entry_underlying": signal.price,
            "stop_price": stop_price,
            "entry_delta": signal.delta,
            "entry_threshold": signal.threshold,
            "entry_mean": signal.mean,
            "entry_sigma": signal.sigma,
            "entry_ts": now_ms(),
            "expiry_ts": expiry_ts,
            "state": "pending",
        }
        # Persist to SQLite + JSON mirror BEFORE the network call.
        self.ledger.open_position(pos)
        self.ledger.record_execution(
            position_uid=uid, action="entry_intent", symbol=pos["symbol"],
            product_id=pos["product_id"], side="sell", qty=pos["qty"],
            price=premium, status="pending", detail=asdict(signal),
        )
        log.info("SELL %s x%d @ %.4f (stop %.4f) | %s",
                 pos["symbol"], cfg.option_qty, premium, stop_price, signal.note)

        order = await self.broker.sell_option(product, cfg.option_qty, premium)
        fill = fnum(order.get("average_fill_price"), premium) or premium
        pos["entry_premium"] = fill
        pos["stop_price"] = fill * cfg.premium_stop_multiple
        self.ledger.mark_open(uid, order.get("id"), order)
        self.ledger.record_execution(
            position_uid=uid, action="entry", symbol=pos["symbol"],
            product_id=pos["product_id"], side="sell", qty=pos["qty"], price=fill,
            order_id=order.get("id"), status=order.get("state", "open"), detail=order,
        )
        self._open[uid] = pos
        log.info("position open %s | fill=%.4f | stop=%.4f", uid, fill, pos["stop_price"])

    async def _get_chain(self, contract_type: str) -> list:
        cached = self._chain_cache.get(contract_type)
        if cached and (time.time() - cached[0]) < 30:
            return cached[1]
        chain = await self.rest.option_chain(self.cfg.asset, contract_type)
        self._chain_cache[contract_type] = (time.time(), chain)
        return chain

    # -- risk monitor ------------------------------------------------------
    async def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._monitor_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("monitor error: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _monitor_once(self) -> None:
        for uid, pos in list(self._open.items()):
            try:
                ticker = await self.rest.ticker(pos["symbol"])
            except Exception as exc:  # noqa: BLE001
                log.warning("ticker failed for %s: %s", pos["symbol"], exc)
                continue
            mark = fnum(ticker.get("mark_price"))
            if mark <= 0:
                continue
            expiry = pos.get("expiry_ts")
            if expiry and now_ms() >= expiry:
                self.ledger.close_position(uid, mark, "expired")
                self._open.pop(uid, None)
                log.info("position %s expired", uid)
                continue
            # Fast profit booking: short premium decays toward zero, so a fall to
            # Entry Premium x (1 - pct) is the win. Book it immediately and free
            # the slot; the expiry backstop above still catches the rest.
            tp_pct = self.cfg.premium_take_profit_pct
            if tp_pct > 0:
                tp_price = pos["entry_premium"] * (1.0 - tp_pct)
                if mark <= tp_price:
                    log.info("TAKE PROFIT %s mark=%.4f <= target=%.4f (entry=%.4f, -%.0f%%) -> market buy-back",
                             uid, mark, tp_price, pos["entry_premium"], tp_pct * 100)
                    order = await self.broker.buy_back(int(pos["product_id"]), int(pos["qty"]), mark)
                    fill = fnum(order.get("average_fill_price"), mark) or mark
                    self.ledger.close_position(uid, fill, "take_profit", order)
                    self.ledger.record_execution(
                        position_uid=uid, action="tp_buyback", symbol=pos["symbol"],
                        product_id=pos["product_id"], side="buy", qty=pos["qty"], price=fill,
                        order_id=order.get("id"), status="closed", detail=order,
                    )
                    self._open.pop(uid, None)
                    continue
            # Absolute loss ceiling: Entry Premium x 1.5. Bypass everything else.
            if mark >= pos["stop_price"]:
                log.warning("STOP HIT %s mark=%.4f >= stop=%.4f -> market buy-back",
                            uid, mark, pos["stop_price"])
                order = await self.broker.buy_back(int(pos["product_id"]), int(pos["qty"]), mark)
                fill = fnum(order.get("average_fill_price"), mark) or mark
                self.ledger.close_position(uid, fill, "stop_loss_1.5x", order)
                self.ledger.record_execution(
                    position_uid=uid, action="stop_buyback", symbol=pos["symbol"],
                    product_id=pos["product_id"], side="buy", qty=pos["qty"], price=fill,
                    order_id=order.get("id"), status="closed", detail=order,
                )
                self._open.pop(uid, None)

    # -- dashboard snapshot ------------------------------------------------
    def snapshot(self) -> dict:
        """Full live view of the tape, footprint, delta window and positions."""
        cfg = self.cfg
        flt = self.strategy.filter
        mean, sigma, n = flt.stats()
        bars = {}
        for tf, bar in self.strategy.bars.items():
            bars[tf] = {
                "tf": tf, "start_ms": bar.start_ms, "open": bar.open,
                "high": bar.high, "low": bar.low, "close": bar.close,
                "delta": round(bar.delta, 2), "trades": bar.trades,
                "bull_stack": bar.bull_stack, "bear_stack": bar.bear_stack,
                "matrix": bar.matrix(),
            }
        closed = []
        for tf, hist in self.strategy.history.items():
            closed.append({
                "tf": tf,
                "bars": [{"start_ms": b.start_ms, "close": b.close,
                          "delta": round(b.delta, 2)} for b in list(hist)[-120:]],
            })
        return {
            "engine": "orderflow_engine",
            "version": VERSION,
            "now_ms": now_ms(),
            "mode": "LIVE" if (self.broker and self.broker.live) else "PAPER",
            "symbol": cfg.symbol,
            "execution_tf": cfg.execution_tf,
            "rolling_tf": cfg.rolling_tf,
            "mark_price": self._mark_price,
            "tick_size": self._tick_size,
            "delta_filter": flt.snapshot(),
            "threshold": flt.threshold(),
            "armed": flt.armed,
            "mean": mean,
            "sigma": sigma,
            "samples": n,
            "bars": bars,
            "history": closed,
            "signals": list(self._signals)[-50:],
            "open_positions": list(self._open.values()),
        }

    # -- observability -----------------------------------------------------
    async def _stats_loop(self) -> None:
        while not self._stop.is_set():
            try:
                bar = self.strategy.bars.get(self.cfg.execution_tf)
                if bar:
                    flt = self.strategy.filter
                    mean, sigma, n = flt.stats()
                    log.info("bar %s [%s] trades=%d delta=%.1f bull_stack=%d bear_stack=%d "
                             "mark=%.2f open=%d | sigma-window n=%d mean=%.0f sigma=%.0f "
                             "trigger=%.0f%s",
                             bar.tf, datetime.fromtimestamp(bar.start_ms / 1000, timezone.utc)
                             .strftime("%H:%M"), bar.trades, bar.delta, bar.bull_stack,
                             bar.bear_stack, self._mark_price, len(self._open),
                             n, mean, sigma, flt.threshold(),
                             "" if flt.armed else " (warming up)")
            except Exception as exc:  # noqa: BLE001
                log.debug("stats error: %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.stats_interval)
            except asyncio.TimeoutError:
                pass


    # -- heartbeat ---------------------------------------------------------
    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            self._write_heartbeat()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    def _write_heartbeat(self) -> None:
        try:
            with open(self.cfg.heartbeat_file, "w") as fh:
                fh.write(str(now_ms()))
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Watchdog supervisor (in-file)
# ---------------------------------------------------------------------------
def _engine_argv(args: argparse.Namespace) -> list:
    argv = [sys.executable, os.path.abspath(__file__)]
    if args.live:
        argv.append("--live")
    argv += [
        "--timeframes", args.timeframes,
        "--execution-tf", args.execution_tf,
        "--imbalance-multiple", str(args.imbalance_multiple),
        "--delta-sigma-mult", str(args.delta_sigma_mult),
        "--rolling-window", str(args.rolling_window),
        "--rolling-tf", args.rolling_tf,
        "--rolling-min-bars", str(args.rolling_min_bars),
        "--rolling-fallback-sigma", str(args.rolling_fallback_sigma),
        "--stacked-imbalances", str(args.stacked_imbalances),
        "--min-premium", str(args.min_premium),
        "--max-premium", str(args.max_premium),
        "--premium-stop-multiple", str(args.premium_stop_multiple),
        "--premium-take-profit-pct", str(args.premium_take_profit_pct),
        "--option-qty", str(args.option_qty),
        "--max-open-positions", str(args.max_open_positions),
        "--state-db", args.state_db,
        "--state-json", args.state_json,
        "--heartbeat-file", args.heartbeat_file,
    ]
    if getattr(args, "dashboard_port", 0):
        argv += ["--dashboard-port", str(args.dashboard_port)]
    return argv


def run_watchdog(args: argparse.Namespace) -> int:
    """Low-overhead supervisor: restart the engine within 10s of any crash.

    On every restart the engine performs a forced ledger recovery read, so an
    unexpected API timeout, network drop, or container reset can never leave a
    position unmanaged.
    """
    restart_delay = 5.0
    max_restarts = args.max_restarts
    argv = _engine_argv(args)
    log.info("watchdog supervising: %s", " ".join(argv))
    restarts = 0
    while True:
        started = time.time()
        try:
            proc = subprocess.Popen(argv)
            rc = proc.wait()
        except KeyboardInterrupt:
            return 0
        except Exception as exc:  # noqa: BLE001
            log.error("watchdog spawn failed: %s", exc)
            rc = -1
        uptime = time.time() - started
        if args.max_restarts and restarts >= max_restarts:
            log.error("watchdog reached max restarts (%d); exiting", max_restarts)
            return rc
        if uptime > 60:
            restarts = 0  # healthy run resets the crash budget
        restarts += 1
        log.warning("engine exited rc=%s after %.1fs; restarting in %.0fs "
                    "(forced ledger recovery on boot)", rc, uptime, restart_delay)
        try:
            time.sleep(restart_delay)
        except KeyboardInterrupt:
            return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Institutional Order Flow & Footprint Delta Engine")
    p.add_argument("--live", action="store_true", help="arm real order transmission (requires API keys)")
    p.add_argument("--watchdog", action="store_true", help="run as supervising parent process")
    p.add_argument("--max-restarts", type=int, default=0, help="watchdog restart budget (0 = unlimited)")
    p.add_argument("--symbol", default="BTCUSD")
    p.add_argument("--asset", default="BTC")
    p.add_argument("--timeframes", default="1m,3m,5m")
    p.add_argument("--execution-tf", default="1m", choices=list(TF_MS))
    p.add_argument("--imbalance-multiple", type=float, default=2.5)
    p.add_argument("--stacked-imbalances", type=int, default=1)
    p.add_argument("--delta-block-threshold", type=float, default=1000.0,
                   help="static fallback only; ignored while dynamic filter is on")
    p.add_argument("--delta-sweep-threshold", type=float, default=800.0,
                   help="static fallback only; ignored while dynamic filter is on")
    p.add_argument("--delta-print-grid", type=float, default=0.0)
    p.add_argument("--delta-print-tolerance", type=float, default=8.0)
    p.add_argument("--static-delta", action="store_true",
                   help="disable the 2-sigma dynamic filter and use static thresholds")
    p.add_argument("--delta-sigma-mult", type=float, default=2.0,
                   help="trigger multiple of rolling sigma (default 2.0)")
    p.add_argument("--rolling-window", type=int, default=40,
                   help="number of closed bars in the rolling sigma window")
    p.add_argument("--rolling-tf", default="15m", choices=list(TF_MS),
                   help="interval of the rolling sigma window")
    p.add_argument("--rolling-min-bars", type=int, default=10,
                   help="closed bars required before the dynamic gate arms")
    p.add_argument("--rolling-fallback-sigma", type=float, default=400.0,
                   help="sigma seed used during warmup")
    p.add_argument("--otm-cushion-pct", type=float, default=0.010)
    p.add_argument("--safety-cushion-pct", type=float, default=0.015)
    p.add_argument("--min-otm-distance", type=float, default=100.0)
    p.add_argument("--min-premium", type=float, default=0.0,
                   help="premium-band strike selection: min OTM premium (0 = disabled)")
    p.add_argument("--max-premium", type=float, default=0.0,
                   help="premium-band strike selection: max OTM premium (0 = disabled)")
    p.add_argument("--premium-stop-multiple", type=float, default=1.5)
    p.add_argument("--premium-take-profit-pct", type=float, default=0.5,
                   help="book profit when premium falls to entry x (1 - pct); 0 disables")
    p.add_argument("--option-qty", type=int, default=1)
    p.add_argument("--max-open-positions", type=int, default=5)
    p.add_argument("--order-type", default="market_order", choices=["market_order", "limit_order"])
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--stats-interval", type=float, default=30.0)
    p.add_argument("--rest-base", default=DEFAULT_REST_BASE)
    p.add_argument("--ws-url", default=DEFAULT_WS_URL)
    p.add_argument("--state-db", default="orderflow_state.db")
    p.add_argument("--state-json", default="state.json")
    p.add_argument("--heartbeat-file", default="orderflow_engine.heartbeat")
    p.add_argument("--log-file", default="orderflow_engine.log")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--dashboard-port", type=int,
                   default=int(os.environ.get("DASHBOARD_PORT", "0") or 0),
                   help="serve the live footprint/delta dashboard on this port (0 = off)")
    p.add_argument("--version", action="version", version=f"orderflow_engine {VERSION}")
    return p


def config_from_args(args: argparse.Namespace) -> Config:
    tfs = tuple(t.strip() for t in (args.timeframes or "1m").split(",") if t.strip())
    for tf in tfs:
        if tf not in TF_MS:
            raise SystemExit(f"unsupported timeframe: {tf}")
    if args.execution_tf not in tfs:
        tfs = tuple(dict.fromkeys((*tfs, args.execution_tf)))
    return Config(
        symbol=args.symbol, asset=args.asset, timeframes=tfs, execution_tf=args.execution_tf,
        imbalance_multiple=args.imbalance_multiple,
        stacked_imbalances_required=args.stacked_imbalances,
        delta_block_threshold=args.delta_block_threshold,
        delta_sweep_threshold=args.delta_sweep_threshold,
        delta_print_grid=args.delta_print_grid,
        delta_print_tolerance=args.delta_print_tolerance,
        dynamic_delta=not args.static_delta,
        delta_sigma_mult=args.delta_sigma_mult,
        rolling_window=args.rolling_window,
        rolling_tf=args.rolling_tf,
        rolling_min_bars=args.rolling_min_bars,
        rolling_fallback_sigma=args.rolling_fallback_sigma,
        otm_cushion_pct=args.otm_cushion_pct,
        safety_cushion_pct=args.safety_cushion_pct,
        min_otm_distance=args.min_otm_distance,
        min_premium=args.min_premium,
        max_premium=args.max_premium,
        premium_stop_multiple=args.premium_stop_multiple,
        premium_take_profit_pct=args.premium_take_profit_pct,
        option_qty=args.option_qty,
        max_open_positions=args.max_open_positions,
        order_type=args.order_type,
        poll_interval=args.poll_interval,
        stats_interval=args.stats_interval,
        rest_base=args.rest_base,
        ws_url=args.ws_url,
        state_db=args.state_db,
        state_json=args.state_json,
        heartbeat_file=args.heartbeat_file,
        log_file=args.log_file,
        dashboard_port=args.dashboard_port,
        live=args.live,
    )


def setup_logging(cfg: Config, level: str) -> None:
    handlers = [logging.StreamHandler(sys.stdout)]
    if cfg.log_file:
        handlers.append(logging.FileHandler(cfg.log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = config_from_args(args)
    setup_logging(cfg, args.log_level)

    if args.watchdog:
        return run_watchdog(args)

    engine = Engine(cfg)

    async def _runner() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, engine.request_stop)
            except NotImplementedError:  # pragma: no cover - Windows
                pass
        await engine.start()

    try:
        asyncio.run(_runner())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())