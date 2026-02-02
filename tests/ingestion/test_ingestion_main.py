# tests/ingestion/test_ingestion_main.py
"""
Unit tests for the ingestion Cloud Function entry points.

This test suite mocks external dependencies to test the HTTP-triggered
functions in `src/ingestion/main.py` directly.
"""

import importlib
import unittest
from unittest.mock import MagicMock, patch


class TestIngestionMain(unittest.TestCase):
    """Test suite for ingestion function entry points."""

    def setUp(self):
        """
        Set up mocks by starting patchers for all external clients.
        This prevents real clients from being initialized when `main` is imported.
        """
        self.patchers = [
            patch("google.cloud.storage.Client"),
            patch("google.cloud.bigquery.Client"),
            patch("google.cloud.pubsub_v1.PublisherClient"),
            patch("google.cloud.firestore.Client"),
            patch("src.ingestion.core.clients.fmp_client.FMPClient"),
            patch("src.ingestion.core.clients.sec_api_client.SecApiClient"),
        ]
        # Start all patchers
        self.mock_storage_class = self.patchers[0].start()
        self.patchers[1].start()  # bq
        self.patchers[2].start()  # pubsub
        self.mock_firestore_class = self.patchers[3].start()
        self.mock_fmp_class = self.patchers[4].start()
        self.patchers[5].start()  # sec_api

        # Get instances of the mocks, which is what the code uses
        self.mock_storage_client = self.mock_storage_class.return_value
        self.mock_firestore_client = self.mock_firestore_class.return_value
        self.mock_fmp_client = self.mock_fmp_class.return_value

        # Now it's safe to import main, as all client classes are mocked.
        from src.ingestion import main

        # Reload the module to ensure it uses our patched clients
        importlib.reload(main)
        self.main = main

    def tearDown(self):
        """Stop all patches to clean up the test environment."""
        for patcher in self.patchers:
            patcher.stop()

    @patch("src.ingestion.core.pipelines.fundamentals.run_pipeline")
    def test_refresh_fundamentals_success(self, mock_run_pipeline):
        """Test a successful call to refresh_fundamentals."""
        # Ensure the global clients in the reloaded module are our mock instances.
        self.main.storage_client = self.mock_storage_client
        self.main.fmp_client = self.mock_fmp_client

        mock_request = MagicMock()
        response, status_code = self.main.refresh_fundamentals(mock_request)

        self.assertEqual(status_code, 202)
        self.assertEqual(response, "Fundamentals refresh pipeline started.")
        mock_run_pipeline.assert_called_once_with(
            fmp_client=self.mock_fmp_client, storage_client=self.mock_storage_client
        )

    @patch("src.ingestion.core.pipelines.fundamentals.run_pipeline")
    def test_refresh_fundamentals_client_error(self, mock_run_pipeline):
        """Test refresh_fundamentals with a missing client."""
        # Simulate a missing client by setting it to None.
        self.main.fmp_client = None
        self.main.storage_client = self.mock_storage_client

        mock_request = MagicMock()
        response, status_code = self.main.refresh_fundamentals(mock_request)

        self.assertEqual(status_code, 500)
        self.assertIn("Server config error", response)
        mock_run_pipeline.assert_not_called()

        @patch("src.ingestion.core.pipelines.spy_price_sync.run_pipeline")
        def test_sync_spy_price_history_success(self, mock_run_pipeline):
            """Test running the SPY price sync when clients are configured."""
            self.main.bq_client = MagicMock()
            self.main.firestore_client = self.mock_firestore_client
            self.main.fmp_client = self.mock_fmp_client

            mock_request = MagicMock()
            response, status_code = self.main.sync_spy_price_history(mock_request)

            self.assertEqual(status_code, 202)
            self.assertEqual(response, "SPY price sync pipeline started.")
            mock_run_pipeline.assert_called_once_with(
                bq_client=self.main.bq_client,
                fmp_client=self.main.fmp_client,
            )


if __name__ == "__main__":
    unittest.main()
