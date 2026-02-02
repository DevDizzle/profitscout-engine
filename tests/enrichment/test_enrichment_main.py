# tests/enrichment/test_enrichment_main.py
"""
Unit tests for the enrichment Cloud Function entry points.

This test suite mocks the enrichment pipelines to ensure that the
HTTP-triggered functions in `src/enrichment/main.py` can be invoked
and return the expected responses.
"""

import unittest
from unittest.mock import MagicMock, patch


class TestEnrichmentMain(unittest.TestCase):
    """Test suite for enrichment function entry points."""

    @patch("src.enrichment.core.pipelines.mda_analyzer.run_pipeline")
    def test_run_mda_analyzer_success(self, mock_run_pipeline):
        """
        Test the run_mda_analyzer endpoint for a successful invocation.
        """
        from src.enrichment import main

        mock_request = MagicMock()

        response, status_code = main.run_mda_analyzer(mock_request)

        self.assertEqual(status_code, 200)
        self.assertEqual(response, "MD&A analyzer pipeline finished.")
        mock_run_pipeline.assert_called_once()


if __name__ == "__main__":
    unittest.main()
