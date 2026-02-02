import unittest

import pandas as pd

from src.enrichment.core.pipelines import options_analyzer


class TestOptionsLogic(unittest.TestCase):
    def test_expected_move_pct(self):
        # Test default haircut (0.75)
        # IV = 0.5 (50%), DTE = 30
        iv = 0.5
        dte = 30
        expected_haircut = 0.75
        expected_val = iv * (dte / 365.0) ** 0.5 * expected_haircut * 100.0

        val = options_analyzer._expected_move_pct(iv, dte)
        self.assertAlmostEqual(val, expected_val, places=4)
        print(f"Expected Move (IV=50%, DTE=30, H=0.75): {val:.2f}%")

    def test_process_contract_ml_pick_strictness(self):
        # Test that ML pick does NOT get free pass on spread/vol/be
        row = pd.Series(
            {
                "ticker": "AAPL",
                "contract_symbol": "AAPL260101C00200000",
                "bid": 1.0,
                "ask": 2.0,  # Spread = 1.0 / 1.5 = 66% (BAD)
                "last_price": 1.5,
                "expiration_date": "2026-02-01",
                "fetch_date": "2026-01-01",  # DTE ~30
                "option_type": "Call",
                "underlying_price": 100,
                "strike": 110,
                "implied_volatility": 0.5,
                "hv_30": 0.2,  # IV/HV = 2.5 (Expensive)
                "is_ml_pick": True,
                "score_percentile": 0.5,  # Neutral score
                "news_score": 0.5,
                "outlook_signal": "Strongly Bullish",  # Aligned
            }
        )

        result = options_analyzer._process_contract(row)
        # Expect Weak or Fair, NOT Strong, because of Spread and Expensive IV
        # Red flags: Spread (bad), Vol (expensive), BE (check)

        # Spread: 66% > 15% -> Flag
        # Vol: 0.5 / 0.2 = 2.5 > 1.5 -> Expensive -> Flag
        # BE: Strike 110 + 1.5 = 111.5. Spot 100. Dist = 11.5%.
        # Exp Move: 0.5 * sqrt(31/365) * 0.75 = 0.5 * 0.29 * 0.75 = 0.109 = 10.9%.
        # BE (11.5%) > Exp Move (10.9%) -> Flag (barely)

        # So 3 Red Flags. Should be "Weak".
        print(f"Result Quality: {result['setup_quality_signal']}")
        print(f"Result Summary: {result['summary']}")

        self.assertEqual(result["setup_quality_signal"], "Weak")
        self.assertIn(
            "ML SNIPER ALERT", result["summary"]
        )  # Should still identify as ML Sniper

    def test_process_contract_rip_hunter_spread_check(self):
        # Test Rip Hunter with bad spread AND expensive Vol
        row = pd.Series(
            {
                "ticker": "TSLA",
                "contract_symbol": "TSLA...",
                "bid": 1.0,
                "ask": 2.0,  # Spread 66% (BAD)
                "last_price": 1.5,
                "expiration_date": "2026-02-01",
                "fetch_date": "2026-01-01",
                "option_type": "Call",
                "underlying_price": 100,
                "strike": 105,
                "implied_volatility": 0.5,
                "hv_30": 0.2,  # 0.5/0.2 = 2.5 -> Expensive (BAD)
                "is_ml_pick": False,
                "score_percentile": 0.95,  # Rip Hunter
                "news_score": 0.0,
                "outlook_signal": "Strongly Bullish",
            }
        )

        result = options_analyzer._process_contract(row)
        # Rip Hunter Forgiveness Logic:
        # Vol: Check -> Expensive -> False (Red Flag 1)
        # Spread: Check -> Bad -> False (Red Flag 2)
        # BE: OK.

        # Result: 2 Red Flags -> Weak.

        print(f"Rip Hunter Result Quality: {result['setup_quality_signal']}")
        self.assertEqual(result["setup_quality_signal"], "Weak")
        self.assertIn("Multiple risks", result["summary"])
