from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from fx_edge_lab.engine import BasisArbitrageEngine
from fx_edge_lab.models import AppSettings, MarketQuote, PairSettings
from fx_edge_lab.notifiers import CompositeNotifier
from fx_edge_lab.storage import SQLiteStorage


class _CaptureNotifier:
    def __init__(self) -> None:
        self.alerts = []

    def send(self, alert) -> None:
        self.alerts.append(alert)


class EngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = f"{self.temp_dir.name}/test.sqlite"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_buy_alert_when_futures_are_rich(self) -> None:
        pair = PairSettings(
            name="EURUSD",
            spot_symbol="EURUSD",
            futures_symbol="6EM26",
            pip_size=0.0001,
            threshold_pips=1.5,
        )
        settings = AppSettings(database_path=self.db_path, pairs=(pair,), stale_after_ms=60_000)
        storage = SQLiteStorage(settings.database_path)
        notifier = _CaptureNotifier()
        engine = BasisArbitrageEngine(settings, storage, CompositeNotifier(notifier))

        now = datetime.now(tz=timezone.utc)
        engine.on_futures_quote(MarketQuote("EURUSD", "6EM26", 1.1010, 1.1012, now, "test-futures"))
        engine.on_spot_quote(MarketQuote("EURUSD", "EURUSD", 1.1006, 1.1008, now, "test-spot"))

        self.assertEqual(len(notifier.alerts), 1)
        self.assertEqual(notifier.alerts[0].direction, "BUY")
        self.assertGreater(notifier.alerts[0].gap_pips, 1.5)
        storage.close()

    def test_inverted_usdjpy_future_normalizes_before_signal(self) -> None:
        pair = PairSettings(
            name="USDJPY",
            spot_symbol="USDJPY",
            futures_symbol="6JM26",
            pip_size=0.01,
            threshold_pips=1.5,
            normalization="invert",
        )
        settings = AppSettings(database_path=self.db_path, pairs=(pair,), stale_after_ms=60_000)
        storage = SQLiteStorage(settings.database_path)
        notifier = _CaptureNotifier()
        engine = BasisArbitrageEngine(settings, storage, CompositeNotifier(notifier))

        now = datetime.now(tz=timezone.utc)
        engine.on_futures_quote(MarketQuote("USDJPY", "6JM26", 0.00674, 0.00675, now, "test-futures"))
        engine.on_spot_quote(MarketQuote("USDJPY", "USDJPY", 147.95, 147.97, now, "test-spot"))

        self.assertEqual(len(notifier.alerts), 1)
        self.assertEqual(notifier.alerts[0].direction, "BUY")
        storage.close()

    def test_gap_closure_updates_alert_status(self) -> None:
        pair = PairSettings(
            name="EURUSD",
            spot_symbol="EURUSD",
            futures_symbol="6EM26",
            pip_size=0.0001,
            threshold_pips=1.5,
            close_tolerance_pips=0.2,
        )
        settings = AppSettings(database_path=self.db_path, pairs=(pair,), stale_after_ms=60_000)
        storage = SQLiteStorage(settings.database_path)
        notifier = _CaptureNotifier()
        engine = BasisArbitrageEngine(settings, storage, CompositeNotifier(notifier))

        now = datetime.now(tz=timezone.utc)
        engine.on_futures_quote(MarketQuote("EURUSD", "6EM26", 1.1010, 1.1012, now, "test-futures"))
        engine.on_spot_quote(MarketQuote("EURUSD", "EURUSD", 1.1006, 1.1008, now, "test-spot"))

        later = now + timedelta(seconds=5)
        engine.on_futures_quote(MarketQuote("EURUSD", "6EM26", 1.1007, 1.1009, later, "test-futures"))
        engine.on_spot_quote(MarketQuote("EURUSD", "EURUSD", 1.1007, 1.1009, later, "test-spot"))

        summary = storage.summary()
        self.assertEqual(summary["alerts"], 1)
        self.assertEqual(summary["closed_alerts"], 1)
        storage.close()


if __name__ == "__main__":
    unittest.main()
