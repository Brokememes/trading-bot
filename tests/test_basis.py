from math import exp, isclose
import unittest

from fx_edge_lab.basis import basis_metrics, fair_future_price
from fx_edge_lab.signals import combine_views


class BasisTests(unittest.TestCase):
    def test_fair_future_uses_quote_minus_base_rates(self) -> None:
        spot = 1.10
        usd_rate = 4.5
        eur_rate = 2.0
        days_to_expiry = 30

        fair = fair_future_price(
            spot=spot,
            quote_rate_pct=usd_rate,
            base_rate_pct=eur_rate,
            days_to_expiry=days_to_expiry,
        )

        expected = spot * exp(((usd_rate - eur_rate) / 100.0) * (days_to_expiry / 365.0))
        self.assertTrue(isclose(fair, expected, rel_tol=1e-12))

    def test_basis_metrics_gap_matches_future_minus_fair(self) -> None:
        metrics = basis_metrics(
            spot=1.09,
            future=1.0915,
            quote_rate_pct=4.4,
            base_rate_pct=2.4,
            days_to_expiry=20,
        )

        self.assertTrue(isclose(metrics.gap, 1.0915 - metrics.fair_future, rel_tol=1e-12))
        self.assertTrue(
            isclose(metrics.gap_pct, (1.0915 / metrics.fair_future) - 1.0, rel_tol=1e-12)
        )

    def test_aligned_short_signal_requires_both_models(self) -> None:
        views = combine_views(
            residual_pct_value=0.0025,
            gap_pct_value=0.0011,
            residual_threshold_bp=15.0,
            gap_threshold_bp=8.0,
        )

        self.assertEqual(views.residual_view, "EUR_RICH")
        self.assertEqual(views.basis_view, "FUTURES_RICH")
        self.assertEqual(views.aligned_trade, "SHORT_EUR")


if __name__ == "__main__":
    unittest.main()
