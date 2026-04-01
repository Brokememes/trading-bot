from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from fx_edge_lab.crypto_models import (
    BasisObservation,
    CryptoPairSettings,
    CryptoResearchSettings,
    FundingSnapshot,
    OpenInterestSnapshot,
)
from fx_edge_lab.crypto_storage import CryptoSQLiteStorage
from fx_edge_lab.crypto_strategy_lab import (
    STRATEGY_FUNDING_FLIP,
    STRATEGY_LIQUIDATION,
    build_strategy_lab,
)


class CryptoStrategyLabTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = f"{self.temp_dir.name}/strategy-lab.sqlite"

    def test_strategy_lab_reports_funding_flip_and_liquidation_trades(self) -> None:
        pair = CryptoPairSettings(
            name="BTCUSDT",
            binance_spot_symbol="btcusdt",
            bybit_linear_symbol="BTCUSDT",
            order_size=1.0,
            max_hold_ms=60_000,
        )
        settings = CryptoResearchSettings(
            database_path=self.db_path,
            maker_entry_fee_bps=0.0,
            exit_fee_bps=0.0,
            exit_slippage_bps=0.0,
            funding_flip_hold_ms=60_000,
            liquidation_oi_drop_pct=0.02,
            liquidation_price_move_pct_min=0.001,
            liquidation_snapback_hold_ms=60_000,
            strategy_lookback_days=7,
            pairs=(pair,),
        )
        storage = CryptoSQLiteStorage(settings.database_path)
        self.addCleanup(storage.close)

        base_time = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=5)

        for minutes, spot_mid, perp_mid, premium_bps in (
            (0, 100.0, 100.10, 10.0),
            (1, 98.0, 101.00, 306.12),
            (2, 99.5, 99.0, -50.0),
        ):
            timestamp = base_time + timedelta(minutes=minutes)
            storage.log_basis(
                BasisObservation(
                    pair="BTCUSDT",
                    timestamp=timestamp,
                    spot_venue="binance",
                    spot_symbol="btcusdt",
                    perp_venue="bybit",
                    perp_symbol="BTCUSDT",
                    spot_mid=spot_mid,
                    perp_mid=perp_mid,
                    premium_bps=premium_bps,
                    spot_imbalance=0.0,
                    perp_imbalance=0.0,
                    current_funding_rate=0.0,
                    average_funding_rate=0.0,
                    next_funding_time=timestamp + timedelta(hours=8),
                    basis_rate=None,
                    basis_value=None,
                )
            )

        storage.log_funding(
            FundingSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                symbol="BTCUSDT",
                timestamp=base_time,
                current_funding_rate=0.0006,
                average_funding_rate=0.0005,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )
        storage.log_funding(
            FundingSnapshot(
                pair="BTCUSDT",
                venue="bybit",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(minutes=1),
                current_funding_rate=-0.0002,
                average_funding_rate=0.0001,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )
        storage.log_funding(
            FundingSnapshot(
                pair="BTCUSDT",
                venue="binance",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(minutes=1),
                current_funding_rate=0.0001,
                average_funding_rate=0.0001,
                next_funding_time=base_time + timedelta(hours=8),
                basis_rate=None,
                basis_value=None,
            )
        )

        storage.log_open_interest(
            OpenInterestSnapshot(
                pair="BTCUSDT",
                venue="binance",
                symbol="BTCUSDT",
                timestamp=base_time,
                interval="5m",
                open_interest=1000.0,
                open_interest_value=100000.0,
            )
        )
        storage.log_open_interest(
            OpenInterestSnapshot(
                pair="BTCUSDT",
                venue="binance",
                symbol="BTCUSDT",
                timestamp=base_time + timedelta(minutes=1),
                interval="5m",
                open_interest=970.0,
                open_interest_value=95060.0,
            )
        )

        strategy_lab = build_strategy_lab(storage, settings, lookback_days=7)
        summaries = {row["strategy_id"]: row for row in strategy_lab["summary_rows"]}

        self.assertIn(STRATEGY_FUNDING_FLIP, summaries)
        self.assertIn(STRATEGY_LIQUIDATION, summaries)
        self.assertEqual(1, summaries[STRATEGY_FUNDING_FLIP]["trades"])
        self.assertEqual(1, summaries[STRATEGY_LIQUIDATION]["trades"])
        self.assertGreater(summaries[STRATEGY_FUNDING_FLIP]["net_pnl_quote"], 0.0)
        self.assertGreater(summaries[STRATEGY_LIQUIDATION]["net_pnl_quote"], 0.0)
        self.assertIsNotNone(strategy_lab["primary_strategy"])
