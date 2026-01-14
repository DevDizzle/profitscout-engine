
import unittest
from unittest.mock import MagicMock, patch, ANY
import pandas as pd
from datetime import date, timedelta
from src.ingestion.core.pipelines import refresh_stock_metadata
from src.ingestion.core import config

class TestRefreshStockMetadata(unittest.TestCase):

    def setUp(self):
        self.mock_bq_client = MagicMock()
        self.mock_fmp_client = MagicMock()
        self.mock_storage_client = MagicMock()
        self.mock_publisher_client = MagicMock()

    def test_get_existing_metadata_status(self):
        # Mock BQ response
        mock_df = pd.DataFrame([
            {"ticker": "AAPL", "last_call_date": date(2025, 10, 15)},
            {"ticker": "GOOGL", "last_call_date": date(2025, 7, 20)},
        ])
        self.mock_bq_client.query.return_value.to_dataframe.return_value = mock_df
        
        tickers = ["AAPL", "GOOGL", "MSFT"]
        status = refresh_stock_metadata._get_existing_metadata_status(self.mock_bq_client, tickers)
        
        self.assertEqual(status["AAPL"], date(2025, 10, 15))
        self.assertEqual(status["GOOGL"], date(2025, 7, 20))
        self.assertIsNone(status.get("MSFT")) # Should be None (initialized in function but not in DF)
        # Note: In the function, status is initialized with None for all tickers.
        # MSFT is in 'tickers' but not in DF, so it remains None.
        self.assertIsNone(status["MSFT"])

    def test_fetch_latest_transcripts_bulk(self):
        # Mock FMP response
        # AAPL: Valid transcript
        # MSFT: No transcript
        
        def side_effect(ticker):
            if ticker == "AAPL":
                return {
                    "symbol": "AAPL",
                    "date": "2025-10-30 17:00:00",
                    "fillingDate": "2025-10-30 18:00:00",
                    "year": 2025,
                    "quarter": 4
                }
            return None

        self.mock_fmp_client.get_latest_transcript.side_effect = side_effect
        
        tickers = ["AAPL", "MSFT"]
        # Use simple executor or mock it? 
        # The function uses ThreadPoolExecutor. We can just run it.
        # We need to make sure config.MAX_WORKERS_TIERING is accessible.
        
        results = refresh_stock_metadata._fetch_latest_transcripts_bulk(tickers, self.mock_fmp_client)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["ticker"], "AAPL")
        self.assertEqual(results[0]["earnings_call_date"], "2025-10-30 17:00:00")

    @patch("src.ingestion.core.pipelines.refresh_stock_metadata.get_tickers")
    @patch("src.ingestion.core.pipelines.refresh_stock_metadata._fetch_profiles_bulk")
    @patch("src.ingestion.core.pipelines.refresh_stock_metadata._fetch_latest_transcripts_bulk")
    @patch("src.ingestion.core.pipelines.refresh_stock_metadata._get_existing_metadata_status")
    def test_run_pipeline_logic(self, mock_get_status, mock_fetch_transcripts, mock_fetch_profiles, mock_get_tickers):
        # Setup
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        mock_get_tickers.return_value = tickers
        
        today = date.today()
        stale_date = today - timedelta(days=100)
        fresh_date = today - timedelta(days=10)
        
        # Status:
        # AAPL: Fresh -> Should be ignored
        # GOOGL: Stale -> Should be processed
        # MSFT: Missing -> Should be processed (Priority 1)
        # AMZN: Stale -> Should be processed
        
        mock_get_status.return_value = {
            "AAPL": fresh_date,
            "GOOGL": stale_date,
            "MSFT": None,
            "AMZN": stale_date
        }
        
        # Mock fetching data
        # We expect calls for MSFT, GOOGL, AMZN (sorted: MSFT first, then stale ones)
        
        mock_fetch_transcripts.return_value = [
            {"ticker": "MSFT", "earnings_call_date": today, "quarter_end_date": today, "earnings_year": 2026, "earnings_quarter": 1},
            {"ticker": "GOOGL", "earnings_call_date": today, "quarter_end_date": today, "earnings_year": 2026, "earnings_quarter": 1}
        ]
        mock_fetch_profiles.return_value = pd.DataFrame([
            {"ticker": "MSFT", "company_name": "Microsoft", "industry": "Tech", "sector": "Tech"},
            {"ticker": "GOOGL", "company_name": "Google", "industry": "Tech", "sector": "Tech"}
        ])
        
        refresh_stock_metadata.run_pipeline(
            self.mock_fmp_client, 
            self.mock_bq_client, 
            self.mock_storage_client, 
            self.mock_publisher_client
        )
        
        # Verify batch processed
        # Expected work items: MSFT (None), GOOGL (Stale), AMZN (Stale).
        # Sorted: MSFT (date.min/None), then GOOGL/AMZN.
        # Note: In implementation, None maps to date.min.
        
        # Verify _fetch_latest_transcripts_bulk called with subset of tickers
        args, _ = mock_fetch_transcripts.call_args
        processed_tickers = args[0]
        self.assertIn("MSFT", processed_tickers)
        self.assertIn("GOOGL", processed_tickers)
        self.assertIn("AMZN", processed_tickers)
        self.assertNotIn("AAPL", processed_tickers)
        
        # Verify BQ Load
        self.mock_bq_client.load_table_from_dataframe.assert_called()
        
        # Verify MERGE query
        # self.mock_bq_client.query.assert_called() # Called multiple times (merge + cleanup)
        
        # Verify Cleanup Query
        cleanup_query_substring = "DELETE FROM"
        found_cleanup = False
        for call in self.mock_bq_client.query.call_args_list:
            if cleanup_query_substring in call[0][0]:
                found_cleanup = True
        self.assertTrue(found_cleanup, "Cleanup query (DELETE) was not called")

if __name__ == "__main__":
    unittest.main()
