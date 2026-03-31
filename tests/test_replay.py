from __future__ import annotations

import csv
import tempfile
import unittest

from fx_edge_lab.engine import BasisArbitrageEngine
from fx_edge_lab.models import AppSettings, PairSettings
from fx_edge_lab.notifiers import CompositeNotifier
from fx_edge_lab.replay import load_replay_rows, replay_rows
from fx_edge_lab.storage import SQLiteStorage


class _NullNotifier:
    def send(self, alert) -> None:
        return


class ReplayTests(unittest.TestCase):
    def test_replay_csv_drives_engine_and_logs_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = f"{temp_dir}/sample.csv"
            db_path = f"{temp_dir}/sample.sqlite"

            with open(csv_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(["timestamp", "pair", "spot_bid", "spot_ask", "futures_bid", "futures_ask"])
                writer.writerow(["2026-03-30T09:00:00+00:00", "EURUSD", 1.1000, 1.1002, 1.1004, 1.1006])
                writer.writerow(["2026-03-30T09:00:05+00:00", "EURUSD", 1.1004, 1.1006, 1.1004, 1.1006])

            rows = load_replay_rows(csv_path)
            settings = AppSettings(
                database_path=db_path,
                stale_after_ms=10_000_000,
                pairs=(
                    PairSettings(
                        name="EURUSD",
                        spot_symbol="EURUSD",
                        futures_symbol="6EM26",
                        pip_size=0.0001,
                        threshold_pips=1.5,
                    ),
                ),
            )
            storage = SQLiteStorage(settings.database_path)
            try:
                engine = BasisArbitrageEngine(settings, storage, CompositeNotifier(_NullNotifier()))
                replay_rows(rows, engine)
                summary = storage.summary()
                self.assertEqual(summary["alerts"], 1)
                self.assertEqual(summary["closed_alerts"], 1)
            finally:
                storage.close()


if __name__ == "__main__":
    unittest.main()
