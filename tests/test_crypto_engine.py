from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from fx_edge_lab.crypto_analysis import summarize_crypto_database
from fx_edge_lab.crypto_engine import CryptoResearchEngine
from fx_edge_lab.crypto_models import (
    CryptoPairSettings,
    CryptoResearchSettings,
    FundingSnapshot,
    OrderBookSnapshot,
    SignalEvent,
)
from fx_edge_lab.crypto_storage import CryptoSQLiteStorage


class CryptoEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = f"{self.temp_dir.name}/crypto.sqlite"

    def test_basis_funding_momentum_signal_and_spread_exit_are_recorded(self) -> None:
        pair = CryptoPairSettings(
            name="BTCUSDT",
            binance_spot_symbol="btcusdt",
            bybit_linear_symbol="BTCUSDT",
            imbalance_threshold=0.0,
            signal_cooldown_ms=10_000,
            order_size=1.0,
            basis_entry_threshold_bps=10.0,
            basis_entry_threshold_low_bps=10.0,
            basis_entry_threshold_high_bps=10.0,
            basis_exit_threshold_bps=5.0,
            funding_entry_min_rate=0.0005,
            funding_exit_rate=0.0,
            basis_momentum_window_ms=100,
            max_hold_ms=1_000,
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            quote_throttle_ms=0,
            imbalance_levels=2,
            basis_sample_interval_ms=0,
            basis_consecutive_samples_required=1,
            analysis_horizons_ms=(100, 200),
            maker_entry_fee_bps=1.0,
            exit_fee_bps=2.0,
            regime_window_ms=100,
            regime_contango_bps=10.0,
            regime_backwardation_bps=-10.0,
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        base_time = datetime.now(tz=timezone.utc)
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="binance",
                market_type="spot",
                symbol="btcusdt",
                timestamp=base_time,
                bids=((100.0, 4.0), (99.9, 4.0)),
                asks=((100.1, 1.0), (100.2, 1.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                market_type="linear",
                symbol="BTCUSDT",
                timestamp=base_time,
                bids=((100.09, 3.0), (100.08, 3.0)),
                asks=((100.11, 1.0), (100.12, 1.0)),
            )
        )
        engine.on_funding(
            FundingSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(milliseconds=10),
                current_funding_rate=0.0006,
                average_funding_rate=0.0005,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                market_type="linear",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(milliseconds=150),
                bids=((100.24, 3.0), (100.23, 3.0)),
                asks=((100.26, 1.0), (100.27, 1.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                market_type="linear",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(milliseconds=250),
                bids=((100.07, 3.0), (100.06, 3.0)),
                asks=((100.09, 1.0), (100.10, 1.0)),
            )
        )

        summary_text = summarize_crypto_database(self.db_path, settings)
        self.assertIn("signals=1", summary_text)
        self.assertIn("spreads=1", summary_text)
        self.assertIn("closed_spreads=1", summary_text)
        self.assertIn("BTCUSDT trades=1", summary_text)

        spread_rows = storage.fetch_all(
            """
            SELECT status, exit_reason, signal_quality_score, signal_quality_band, gross_pnl_quote, net_pnl_quote
            FROM crypto_spread_positions
            """
        )
        self.assertEqual(1, len(spread_rows))
        self.assertEqual("CLOSED", spread_rows[0]["status"])
        self.assertEqual("BASIS_CONVERGED", spread_rows[0]["exit_reason"])
        self.assertGreater(float(spread_rows[0]["signal_quality_score"]), 0.0)
        self.assertEqual("SCALE_UP", spread_rows[0]["signal_quality_band"])
        self.assertGreater(float(spread_rows[0]["gross_pnl_quote"]), 0.0)
        self.assertGreater(float(spread_rows[0]["net_pnl_quote"]), 0.0)

    def test_basis_and_funding_are_logged(self) -> None:
        pair = CryptoPairSettings(
            name="BTCUSDT",
            binance_spot_symbol="btcusdt",
            bybit_linear_symbol="BTCUSDT",
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            quote_throttle_ms=0,
            imbalance_levels=2,
            basis_sample_interval_ms=0,
            analysis_horizons_ms=(100,),
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        base_time = datetime.now(tz=timezone.utc)
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="binance",
                market_type="spot",
                symbol="btcusdt",
                timestamp=base_time,
                bids=((100.0, 3.0), (99.9, 3.0)),
                asks=((100.1, 1.0), (100.2, 1.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                market_type="linear",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(milliseconds=10),
                bids=((100.4, 3.0), (100.3, 3.0)),
                asks=((100.5, 1.0), (100.6, 1.0)),
            )
        )
        engine.on_funding(
            FundingSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(milliseconds=20),
                current_funding_rate=0.0001,
                average_funding_rate=0.0002,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )

        summary_text = summarize_crypto_database(self.db_path, settings)
        self.assertIn("funding=1", summary_text)
        self.assertIn("Latest Basis", summary_text)
        self.assertIn("funding=1.00bps", summary_text)

    def test_adaptive_basis_threshold_changes_with_signal_count(self) -> None:
        pair = CryptoPairSettings(
            name="BTCUSDT",
            binance_spot_symbol="btcusdt",
            bybit_linear_symbol="BTCUSDT",
            basis_entry_threshold_bps=80.0,
            basis_entry_threshold_low_bps=60.0,
            basis_entry_threshold_high_bps=120.0,
        )
        settings = CryptoResearchSettings(database_path=self.db_path, pairs=(pair,))
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        timestamp = datetime.now(tz=timezone.utc)
        self.assertEqual(60.0, engine._adaptive_basis_threshold_bps(pair, timestamp))

        for idx in range(2):
            storage.insert_signal(
                SignalEvent(
                    pair="BTCUSDT",
                    venue="cross",
                    market_type="spread",
                    symbol="BTCUSDT",
                    timestamp=timestamp + timedelta(seconds=idx),
                    side="LONG_SPOT_SHORT_PERP",
                    signal_source="basis_tier1_confirmed",
                    entry_bid=100.0,
                    entry_ask=100.8,
                    entry_mid=80.0,
                    spread_bps=80.0,
                    imbalance=0.0,
                    threshold=60.0,
                )
            )
        self.assertEqual(80.0, engine._adaptive_basis_threshold_bps(pair, timestamp))

        for idx in range(2, 11):
            storage.insert_signal(
                SignalEvent(
                    pair="BTCUSDT",
                    venue="cross",
                    market_type="spread",
                    symbol="BTCUSDT",
                    timestamp=timestamp + timedelta(seconds=idx),
                    side="LONG_SPOT_SHORT_PERP",
                    signal_source="basis_tier1_confirmed",
                    entry_bid=100.0,
                    entry_ask=100.8,
                    entry_mid=80.0,
                    spread_bps=80.0,
                    imbalance=0.0,
                    threshold=80.0,
                )
            )
        self.assertEqual(120.0, engine._adaptive_basis_threshold_bps(pair, timestamp))

    def test_backwardation_reverse_mode_records_positive_spread_exit(self) -> None:
        pair = CryptoPairSettings(
            name="DOGEUSDT",
            binance_spot_symbol="dogeusdt",
            bybit_linear_symbol="DOGEUSDT",
            imbalance_threshold=0.0,
            signal_cooldown_ms=10_000,
            order_size=100.0,
            basis_entry_threshold_bps=10.0,
            basis_entry_threshold_low_bps=10.0,
            basis_entry_threshold_high_bps=10.0,
            basis_exit_threshold_bps=5.0,
            funding_entry_min_rate=0.0005,
            funding_exit_rate=0.0,
            basis_momentum_window_ms=100,
            max_hold_ms=1_000,
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            quote_throttle_ms=0,
            imbalance_levels=2,
            basis_sample_interval_ms=0,
            basis_consecutive_samples_required=1,
            maker_entry_fee_bps=1.0,
            exit_fee_bps=2.0,
            reverse_spot_borrow_apy=0.10,
            regime_window_ms=100,
            regime_contango_bps=10.0,
            regime_backwardation_bps=-10.0,
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        base_time = datetime.now(tz=timezone.utc)
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="DOGEUSDT",
                venue="binance",
                market_type="spot",
                symbol="dogeusdt",
                timestamp=base_time,
                bids=((1.0000, 1000.0), (0.9999, 1000.0)),
                asks=((1.0001, 1000.0), (1.0002, 1000.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="DOGEUSDT",
                venue="bybit",
                market_type="linear",
                symbol="DOGEUSDT",
                timestamp=base_time,
                bids=((0.9988, 1000.0), (0.9987, 1000.0)),
                asks=((0.9990, 1000.0), (0.9991, 1000.0)),
            )
        )
        engine.on_funding(
            FundingSnapshot(
                pair="DOGEUSDT",
                venue="bybit",
                symbol="DOGEUSDT",
                timestamp=base_time + timedelta(milliseconds=10),
                current_funding_rate=-0.0006,
                average_funding_rate=-0.0005,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="DOGEUSDT",
                venue="bybit",
                market_type="linear",
                symbol="DOGEUSDT",
                timestamp=base_time + timedelta(milliseconds=150),
                bids=((0.9983, 1000.0), (0.9982, 1000.0)),
                asks=((0.9985, 1000.0), (0.9986, 1000.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="DOGEUSDT",
                venue="bybit",
                market_type="linear",
                symbol="DOGEUSDT",
                timestamp=base_time + timedelta(milliseconds=250),
                bids=((0.9996, 1000.0), (0.9995, 1000.0)),
                asks=((0.9998, 1000.0), (0.9999, 1000.0)),
            )
        )

        rows = storage.fetch_all(
            """
            SELECT p.status, p.exit_reason, p.gross_pnl_quote, p.net_pnl_quote, s.side
            FROM crypto_spread_positions p
            JOIN crypto_signals s ON s.id = p.signal_id
            """
        )
        self.assertEqual(1, len(rows))
        self.assertEqual("SHORT_SPOT_LONG_PERP", rows[0]["side"])
        self.assertEqual("CLOSED", rows[0]["status"])
        self.assertEqual("BASIS_CONVERGED", rows[0]["exit_reason"])
        self.assertGreater(float(rows[0]["gross_pnl_quote"]), 0.0)

    def test_single_sample_spike_does_not_fire_with_duration_filter(self) -> None:
        pair = CryptoPairSettings(
            name="AVAXUSDT",
            binance_spot_symbol="avaxusdt",
            bybit_linear_symbol="AVAXUSDT",
            imbalance_threshold=0.0,
            signal_cooldown_ms=10_000,
            order_size=2.0,
            extreme_basis_size_fraction=0.5,
            basis_entry_threshold_bps=80.0,
            basis_entry_threshold_low_bps=60.0,
            basis_entry_threshold_high_bps=120.0,
            basis_exit_threshold_bps=30.0,
            funding_entry_min_rate=0.0005,
            basis_momentum_window_ms=180000,
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            quote_throttle_ms=0,
            imbalance_levels=2,
            basis_sample_interval_ms=1000,
            regime_window_ms=3600000,
            basis_consecutive_samples_required=3,
            pre_funding_window_ms=3600000,
            pre_funding_basis_threshold_bps=50.0,
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        base_time = datetime(2026, 1, 1, 15, 10, 0, tzinfo=timezone.utc)
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="AVAXUSDT",
                venue="binance",
                market_type="spot",
                symbol="avaxusdt",
                timestamp=base_time,
                bids=((100.0, 5.0), (99.9, 5.0)),
                asks=((100.1, 5.0), (100.2, 5.0)),
            )
        )
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="AVAXUSDT",
                venue="bybit",
                market_type="linear",
                symbol="AVAXUSDT",
                timestamp=base_time,
                bids=((99.15, 5.0), (99.14, 5.0)),
                asks=((99.25, 5.0), (99.26, 5.0)),
            )
        )

        rows = storage.fetch_all(
            """
            SELECT p.quantity, s.signal_source, s.side
            FROM crypto_spread_positions p
            JOIN crypto_signals s ON s.id = p.signal_id
            """
        )
        self.assertEqual(0, len(rows))

    def test_prefunding_extreme_basis_signal_uses_half_size_after_three_samples(self) -> None:
        pair = CryptoPairSettings(
            name="AVAXUSDT",
            binance_spot_symbol="avaxusdt",
            bybit_linear_symbol="AVAXUSDT",
            imbalance_threshold=0.0,
            signal_cooldown_ms=10_000,
            order_size=2.0,
            extreme_basis_size_fraction=0.5,
            basis_entry_threshold_bps=80.0,
            basis_entry_threshold_low_bps=60.0,
            basis_entry_threshold_high_bps=120.0,
            basis_exit_threshold_bps=30.0,
            funding_entry_min_rate=0.0005,
            basis_momentum_window_ms=180000,
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            quote_throttle_ms=0,
            imbalance_levels=2,
            basis_sample_interval_ms=1000,
            regime_window_ms=3600000,
            basis_consecutive_samples_required=3,
            pre_funding_window_ms=3600000,
            pre_funding_basis_threshold_bps=50.0,
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)
        engine = CryptoResearchEngine(settings, storage)

        base_time = datetime(2026, 1, 1, 15, 10, 0, tzinfo=timezone.utc)
        engine.on_orderbook(
            OrderBookSnapshot(
                pair="AVAXUSDT",
                venue="binance",
                market_type="spot",
                symbol="avaxusdt",
                timestamp=base_time,
                bids=((100.0, 5.0), (99.9, 5.0)),
                asks=((100.1, 5.0), (100.2, 5.0)),
            )
        )
        for idx in range(3):
            ts = base_time + timedelta(seconds=idx)
            engine.on_orderbook(
                OrderBookSnapshot(
                    pair="AVAXUSDT",
                    venue="bybit",
                    market_type="linear",
                    symbol="AVAXUSDT",
                    timestamp=ts,
                    bids=((99.45, 5.0), (99.44, 5.0)),
                    asks=((99.55, 5.0), (99.56, 5.0)),
                )
            )

        rows = storage.fetch_all(
            """
            SELECT p.quantity, s.signal_source, s.side
            FROM crypto_spread_positions p
            JOIN crypto_signals s ON s.id = p.signal_id
            """
        )
        self.assertEqual(1, len(rows))
        self.assertEqual("basis_tier2_extreme", rows[0]["signal_source"])
        self.assertEqual("SHORT_SPOT_LONG_PERP", rows[0]["side"])
        self.assertAlmostEqual(1.0, float(rows[0]["quantity"]))


if __name__ == "__main__":
    unittest.main()
