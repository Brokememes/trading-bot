from math import isclose
import unittest

from fx_edge_lab.dxy import dxy_from_components, implied_eurusd, residual_pct, rolling_zscores


class DxyTests(unittest.TestCase):
    def test_implied_eurusd_round_trips_through_dxy_formula(self) -> None:
        eurusd = 1.0875
        dxy = dxy_from_components(
            eurusd=eurusd,
            usdjpy=148.2,
            gbpusd=1.2960,
            usdcad=1.3810,
            usdsek=10.5800,
            usdchf=0.8920,
        )

        implied = implied_eurusd(
            dxy=dxy,
            usdjpy=148.2,
            gbpusd=1.2960,
            usdcad=1.3810,
            usdsek=10.5800,
            usdchf=0.8920,
        )

        self.assertTrue(isclose(implied, eurusd, rel_tol=1e-12))

    def test_residual_pct_is_zero_when_prices_match(self) -> None:
        self.assertEqual(residual_pct(1.10, 1.10), 0.0)

    def test_rolling_zscores_use_prior_window_only(self) -> None:
        values = [1.0, 1.1, 0.9, 1.2, 1.3, 2.0]
        zscores = rolling_zscores(values, window=3)

        self.assertEqual(zscores[:3], [None, None, None])
        self.assertIsNotNone(zscores[3])
        self.assertIsNotNone(zscores[-1])
        self.assertGreater(zscores[-1], 1.0)


if __name__ == "__main__":
    unittest.main()
