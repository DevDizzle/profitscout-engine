import unittest
from unittest.mock import MagicMock, patch

from src.enrichment import main


class TestNewsAnalyzerMain(unittest.TestCase):
    @patch("src.enrichment.core.pipelines.news_analyzer.run_pipeline")
    def test_run_news_analyzer_success(self, mock_run_pipeline):
        """
        Test the run_news_analyzer endpoint for a successful invocation.
        """
        mock_request = MagicMock()
        response, status_code = main.run_news_analyzer(mock_request)

        self.assertEqual(status_code, 200)
        self.assertEqual(response, "News analyzer pipeline finished.")
        mock_run_pipeline.assert_called_once()


if __name__ == "__main__":
    unittest.main()
