"""Self-contained tests for orderflow_engine. Run: python3 -m unittest -v."""

import asyncio
import json
import os
import tempfile
import unittest

from orderflow_engine import (Config, DeltaFilter, Engine, Ledger, Strategy,
                              select_contract, to_ms)


class TestDeltaFilter(unittest.TestCase):
    def test_threshold_scales_with_sigma(self):
        f = DeltaFilter(window=40, sigma_mult=2.0, min_bars=3, fallback_sigma=1.0)
        for x in (280, 300, 320):        # mean 300, sigma 20 -> 2sigma = 40
            f.add_bar(x)
        mean, sigma, n = f.stats()
        self.assertEqual(n, 3)
        self.assertAlmostEqual(mean, 300.0)
        self.assertAlmostEqual(sigma, 20.0, places=6)
        self.assertAlmostEqual(f.threshold(), 40.0)

    def test_quiet_market_lowers_trigger(self):
        quiet = DeltaFilter(min_bars=3, fallback_sigma=1.0)
        for x in (100, 100, 100):
            quiet.add_bar(x)
        hot = DeltaFilter(min_bars=3, fallback_sigma=1.0)
        for x in (1400, 1500, 1600):
            hot.add_bar(x)
        self.assertLess(quiet.threshold(), hot.threshold())
        # the scaling examples from the spec: avg 300 -> ~600, avg 1500 -> ~3000
        self.assertLessEqual(quiet.threshold(), 100.0)
        self.assertGreaterEqual(hot.threshold(), 200.0)

    def test_gate_arms_only_after_min_bars(self):
        cfg = Config(dynamic_delta=True, rolling_min_bars=3, rolling_window=40,
                     delta_sigma_mult=2.0, rolling_fallback_sigma=1.0)
        s = Strategy(cfg)
        self.assertFalse(s.filter.armed)
        self.assertIsNone(s.delta_gate(s._bar_for("1m", 1_700_000_000_000, 0.5)))

    def test_dynamic_gate_triggers_on_outlier(self):
        cfg = Config(dynamic_delta=True, rolling_min_bars=3, rolling_window=40,
                     delta_sigma_mult=2.0, rolling_tf="1m", execution_tf="1m",
                     timeframes=("1m",), stacked_imbalances_required=1)
        s = Strategy(cfg)
        for x in (280, 300, 320):
            s.filter.add_bar(x)          # sigma=20 -> trigger 40
        bar = s._bar_for("1m", 1_700_000_000_000, 0.5)
        bar.add_trade(100.5, 1, "buy", 1_700_000_000_000)     # ask above -> bearish stack
        bar.add_trade(100.0, 50, "sell", 1_700_000_000_000)   # delta -49 clears 40
        self.assertGreaterEqual(bar.bear_stack, 1)
        sig = s.evaluate(bar)
        self.assertIsNotNone(sig)
        self.assertAlmostEqual(sig.threshold, 40.0, places=6)
        self.assertGreaterEqual(abs(sig.delta), sig.threshold)


class TestStrategy(unittest.TestCase):
    def setUp(self):
        # static path: exercises the legacy gate independent of the sigma filter
        self.cfg = Config(timeframes=("1m",), execution_tf="1m",
                          imbalance_multiple=2.5, delta_sweep_threshold=800,
                          delta_block_threshold=1000, dynamic_delta=False)
        self.ts = 1_700_000_000_000

    def test_bearish_imbalance_and_delta_gate(self):
        s = Strategy(self.cfg)
        bar = s._bar_for("1m", self.ts, 0.5)
        bar.add_trade(100.5, 5, "buy", self.ts)    # ask at P+1
        bar.add_trade(100.0, 20, "sell", self.ts)  # bid at P, 20 >= 2.5*5
        self.assertGreaterEqual(bar.bear_stack, 1)
        self.assertEqual(bar.delta, -15)
        self.assertIsNone(s.evaluate(bar), "delta -15 must not clear the gate")
        for _ in range(60):
            bar.add_trade(100.0, 20, "sell", self.ts)
        sig = s.evaluate(bar)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, "bearish")

    def test_bullish_imbalance(self):
        s = Strategy(self.cfg)
        bar = s._bar_for("1m", self.ts, 0.5)
        bar.add_trade(100.0, 4, "sell", self.ts)
        bar.add_trade(100.5, 30, "buy", self.ts)   # ask at P+1 >= 2.5 * bid at P
        for _ in range(60):
            bar.add_trade(100.5, 30, "buy", self.ts)
        sig = s.evaluate(bar)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, "bullish")

    def test_one_signal_per_bar(self):
        s = Strategy(self.cfg)
        bar = s._bar_for("1m", self.ts, 0.5)
        for _ in range(60):
            bar.add_trade(100.0, 20, "sell", self.ts)
            bar.add_trade(100.5, 1, "buy", self.ts)
        self.assertIsNotNone(s.evaluate(bar))
        self.assertIsNone(s.evaluate(bar), "must not re-fire on the same bar")

    def test_footprint_matrix_shape(self):
        s = Strategy(self.cfg)
        bar = s._bar_for("1m", self.ts, 0.5)
        bar.add_trade(100.0, 3, "sell", self.ts)
        rows = bar.matrix()
        self.assertTrue(rows)
        self.assertEqual(set(rows[0]), {"price", "bid", "ask", "delta"})

    def test_rolling_tf_bar_close_persists_sample(self):
        # Regression: the rolling window tf (15m) is materialised even though it
        # is not in cfg.timeframes. Closing it must not raise KeyError and must
        # hand the closed bar to the on_bar_close callback so the sigma sample
        # is recorded.
        cfg = Config(timeframes=("1m", "3m", "5m"), execution_tf="1m",
                     rolling_tf="15m", rolling_window=40, rolling_min_bars=10)
        fired = []
        s = Strategy(cfg, on_bar_close=lambda bar, flt: fired.append(bar))
        base = 900_000  # aligned to a 15m boundary
        for ts in (base + 1000, base + 2000):
            s.on_trade(85000.0, 1.0, "buy", ts, 0.5)
        self.assertIn("15m", s.history, "rolling tf must have a history deque")
        # cross into the next 15m bar -> previous one closes
        s.on_trade(85001.0, 1.0, "sell", base + 900_100, 0.5)
        self.assertEqual([b.tf for b in fired], ["15m"])
        self.assertEqual(len(s.filter.samples), 1)
        self.assertAlmostEqual(s.filter.samples[0], 2.0)


class TestStrikeSelection(unittest.TestCase):
    def setUp(self):
        self.cfg = Config()

    @staticmethod
    def _prod(prefix, strike, vol, oi):
        return {"symbol": f"{prefix}-BTC-{strike}-081026", "strike_price": str(strike),
                "volume": vol, "oi_contracts": oi, "state": "live", "id": int(strike),
                "settlement_time": "2026-10-08T12:00:00Z"}

    def test_call_outside_hvn_with_cushion(self):
        chain = [self._prod("C", k, 100 if k == 86000 else 10, 50)
                 for k in range(84000, 90001, 500)]
        sel = select_contract(chain, 85500.0, "call_options", self.cfg)
        self.assertIsNotNone(sel)
        self.assertGreaterEqual(float(sel["strike_price"]), 85500 * 1.015)

    def test_put_outside_hvn_with_cushion(self):
        chain = [self._prod("P", k, 100 if k == 84000 else 10, 50)
                 for k in range(80000, 87001, 500)]
        sel = select_contract(chain, 85500.0, "put_options", self.cfg)
        self.assertIsNotNone(sel)
        self.assertLessEqual(float(sel["strike_price"]), 85500 * 0.985)

    def test_empty_liquidity_does_not_collapse_to_edge(self):
        chain = [self._prod("P", k, 0, 0) for k in range(82000, 89001, 400)]
        sel = select_contract(chain, 85000.0, "put_options", self.cfg)
        self.assertIsNotNone(sel)
        self.assertLessEqual(float(sel["strike_price"]), 85000 * 0.985)

    @staticmethod
    def _prod_prem(prefix, strike, prem, exp="2026-10-08T12:00:00Z"):
        return {"symbol": f"{prefix}-BTC-{strike}-081026", "strike_price": str(strike),
                "mark_price": str(prem), "volume": 10, "oi_contracts": 10,
                "state": "live", "id": int(strike),
                "settlement_time": exp}

    def test_premium_band_skips_collapsed_expiry(self):
        # nearest expiry has collapsed to ~2 points (expiry day); the next expiry
        # carries the band. The band must reach past the collapsed expiry.
        cfg = Config(min_premium=200.0, max_premium=400.0)
        near = [self._prod_prem("C", k, 2.0, "2026-10-06T12:00:00Z")
                for k in range(86000, 88001, 200)]
        far = [self._prod_prem("C", k, 400.0 - (k - 86600) / 10.0, "2026-10-07T12:00:00Z")
               for k in range(86000, 88001, 200)]
        sel = select_contract(near + far, 86000.0, "call_options", cfg)
        self.assertIsNotNone(sel)
        self.assertEqual(sel["settlement_time"], "2026-10-07T12:00:00Z")
        self.assertGreaterEqual(float(sel["mark_price"]), 200.0)
        self.assertLessEqual(float(sel["mark_price"]), 400.0)

    def test_premium_band_picks_otm_strike_in_band(self):
        cfg = Config(min_premium=150.0, max_premium=250.0)
        # premiums decay as the strike moves away from spot (85,500)
        chain = [self._prod_prem("C", k, max(1.0, (88000 - k) / 10.0))
                 for k in range(84000, 90001, 200)]
        sel = select_contract(chain, 85500.0, "call_options", cfg)
        self.assertIsNotNone(sel)
        prem = float(sel["mark_price"])
        self.assertGreaterEqual(prem, 150.0)
        self.assertLessEqual(prem, 250.0)
        self.assertGreater(float(sel["strike_price"]), 85500.0)   # OTM only

    def test_premium_band_never_sells_itm(self):
        cfg = Config(min_premium=150.0, max_premium=250.0)
        # only ITM calls have premiums in band; engine must not sell them
        chain = [self._prod_prem("C", k, 200.0) for k in range(80000, 85501, 200)]
        sel = select_contract(chain, 85500.0, "call_options", cfg)
        if sel is not None:
            self.assertGreater(float(sel["strike_price"]), 85500.0)

    def test_premium_band_nearest_when_none_in_band(self):
        cfg = Config(min_premium=150.0, max_premium=250.0)
        # no strike reaches the band; premiums decay as strikes move OTM, so the
        # 86,000 strike has the highest premium -> closest to the band.
        chain = [self._prod_prem("C", k, 5.0 + (90000 - k) / 1000.0)
                 for k in range(86000, 90001, 500)]
        sel = select_contract(chain, 85500.0, "call_options", cfg)
        self.assertIsNotNone(sel)
        self.assertGreater(float(sel["strike_price"]), 85500.0)
        self.assertEqual(float(sel["strike_price"]), 86000.0)


class TestLedger(unittest.TestCase):
    def test_persist_mirror_and_recover(self):
        with tempfile.TemporaryDirectory() as d:
            led = Ledger(os.path.join(d, "t.db"), os.path.join(d, "t.json"))
            pos = dict(position_uid="u1", symbol="C-BTC-90000-081026", product_id=1,
                       contract_type="call_options", side="sell", qty=1,
                       entry_premium=100.0, entry_underlying=85000.0, stop_price=150.0,
                       entry_delta=-1058.0, entry_ts=1, expiry_ts=9999999999999)
            led.open_position(pos)
            led.mark_open("u1", "ord1", {"state": "open"})
            rows = led.open_positions()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["stop_price"], 150.0)
            with open(os.path.join(d, "t.json")) as fh:
                snap = json.load(fh)
            self.assertEqual(snap["open_positions"][0]["position_uid"], "u1")
            led.close_position("u1", 120.0, "stop_loss_1.5x")
            self.assertEqual(led.open_positions(), [])
            led.close()

    def test_signals_persist_and_restore(self):
        with tempfile.TemporaryDirectory() as d:
            led = Ledger(os.path.join(d, "t.db"), os.path.join(d, "t.json"))
            for i in range(3):
                led.event("signal", {"ts": i, "direction": "bullish", "delta": 1000 + i,
                                     "price": 85000.0, "threshold": 900.0})
            led.event("other", {"ignore": True})
            restored = led.recent_events("signal", 100)
            self.assertEqual([r["delta"] for r in restored], [1000, 1001, 1002])
            self.assertEqual(len(led.recent_events("signal", 2)), 2)
            led.close()


class _FakeREST:
    def __init__(self, mark):
        self.mark = mark

    async def ticker(self, symbol):
        return {"mark_price": str(self.mark)}


class _FakeBroker:
    def __init__(self):
        self.buys = []

    @property
    def live(self):
        return False

    async def buy_back(self, product_id, qty, limit_price=None):
        self.buys.append((product_id, qty, limit_price))
        return {"id": "x", "average_fill_price": limit_price, "state": "filled"}


class TestRiskMonitor(unittest.TestCase):
    def _run(self, mark, stop, expiry, tag):
        async def go():
            with tempfile.TemporaryDirectory() as d:
                cfg = Config(state_db=os.path.join(d, "s.db"),
                             state_json=os.path.join(d, "s.json"))
                eng = Engine(cfg)
                eng.rest = _FakeREST(mark)
                eng.broker = _FakeBroker()
                uid = f"u-{tag}"
                pos = dict(position_uid=uid, symbol="C-BTC-88400-061026", product_id=1,
                           contract_type="call_options", side="sell", qty=1,
                           entry_premium=stop / 1.5, entry_underlying=85000.0,
                           stop_price=stop, entry_delta=-1000.0, entry_ts=1,
                           expiry_ts=expiry, state="open")
                eng.ledger.open_position(pos)
                eng.ledger.mark_open(uid, "o1")
                eng._open[uid] = pos
                await eng._monitor_once()
                row = eng.ledger.position(uid)
                eng.ledger.close()
                return row
        return asyncio.run(go())

    def test_stop_loss_at_1_5x(self):
        row = self._run(6.0, 5.0, 9999999999999, "stop")
        self.assertEqual(row["state"], "closed")
        self.assertEqual(row["exit_reason"], "stop_loss_1.5x")

    def test_take_profit_books_at_half_premium(self):
        # entry 5.0 (stop 7.5); mark 2.4 <= 5.0 * 0.5 = 2.5 -> take profit
        row = self._run(2.4, 7.5, 9999999999999, "tp")
        self.assertEqual(row["state"], "closed")
        self.assertEqual(row["exit_reason"], "take_profit")

    def test_no_take_profit_above_target(self):
        # mark 3.0 > 2.5 target -> stays open (below stop, before expiry)
        row = self._run(3.0, 7.5, 9999999999999, "tp_hold")
        self.assertEqual(row["state"], "open")

    def test_expiry_close(self):
        row = self._run(4.0, 5.0, 1, "exp")
        self.assertEqual(row["exit_reason"], "expired")

    def test_holds_below_stop(self):
        row = self._run(4.9, 5.0, 9999999999999, "hold")
        self.assertEqual(row["state"], "open")


class TestSignalRecovery(unittest.TestCase):
    def _engine(self, d):
        cfg = Config(state_db=os.path.join(d, "s.db"), state_json=os.path.join(d, "s.json"))
        eng = Engine(cfg)
        eng.rest = _FakeREST(85000.0)
        eng.broker = _FakeBroker()
        return eng

    def test_backfill_from_trade_history(self):
        with tempfile.TemporaryDirectory() as d:
            eng = self._engine(d)
            uid = "u-bf"
            eng.ledger.open_position(dict(
                position_uid=uid, symbol="C-BTC-88400-061026", product_id=1,
                contract_type="call_options", side="sell", qty=1,
                entry_premium=100.0, entry_underlying=85000.0, stop_price=150.0,
                entry_delta=-1200.0, entry_threshold=900.0, entry_mean=200.0,
                entry_sigma=450.0, entry_ts=1234, expiry_ts=9999999999999, state="open"))
            eng.ledger.mark_open(uid, "o1")
            eng.ledger.close_position(uid, 50.0, "take_profit")
            self.assertTrue(eng._backfill_signals_from_positions())
            self.assertEqual(len(eng._signals), 1)
            rec = eng._signals[0]
            self.assertEqual(rec["direction"], "bearish")
            self.assertEqual(rec["delta"], -1200.0)
            self.assertEqual(rec["threshold"], 900.0)
            eng.ledger.close()

    def test_signal_roundtrip_through_ledger(self):
        with tempfile.TemporaryDirectory() as d:
            eng = self._engine(d)
            eng.ledger.event("signal", {"ts": 1, "direction": "bullish", "delta": 1500.0})
            restored = eng.ledger.recent_events("signal", 100)
            self.assertEqual(restored[0]["delta"], 1500.0)
            eng.ledger.close()


class TestTimestampNormalization(unittest.TestCase):
    def test_microseconds(self):
        self.assertEqual(to_ms(1791221892178446), 1791221892178)

    def test_seconds(self):
        self.assertEqual(to_ms(1700000000), 1700000000000)

    def test_milliseconds_passthrough(self):
        self.assertEqual(to_ms(1700000000000), 1700000000000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
