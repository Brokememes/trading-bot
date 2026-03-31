from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fx_edge_lab.crypto_settings import load_crypto_settings


class CryptoSettingsTests(unittest.TestCase):
    def test_binance_spot_regular_taker_preset_resolves_expected_fees(self) -> None:
        settings = self._load(
            {
                "fee_preset": "binance_spot_regular",
                "exit_mode": "taker",
            }
        )
        self.assertEqual(settings.maker_entry_fee_bps, 10.0)
        self.assertEqual(settings.exit_fee_bps, 10.0)
        self.assertEqual(settings.exit_slippage_bps, 0.0)

    def test_bybit_linear_maker_exit_uses_maker_fee(self) -> None:
        settings = self._load(
            {
                "fee_preset": "bybit_linear_vip0",
                "exit_mode": "maker",
            }
        )
        self.assertEqual(settings.maker_entry_fee_bps, 2.0)
        self.assertEqual(settings.exit_fee_bps, 2.0)
        self.assertEqual(settings.exit_slippage_bps, 0.0)

    def test_mid_exit_zeroes_exit_costs(self) -> None:
        settings = self._load(
            {
                "fee_preset": "binance_spot_regular_bnb",
                "exit_mode": "mid",
                "exit_slippage_bps": 3.0,
            }
        )
        self.assertEqual(settings.maker_entry_fee_bps, 7.5)
        self.assertEqual(settings.exit_fee_bps, 0.0)
        self.assertEqual(settings.exit_slippage_bps, 0.0)

    def _load(self, payload: dict) -> object:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "crypto.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_crypto_settings(path)


if __name__ == "__main__":
    unittest.main()
