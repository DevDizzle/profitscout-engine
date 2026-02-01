import unittest
from unittest.mock import MagicMock, patch

from src.serving.core.pipelines import social_media_poster


class TestSocialMediaPoster(unittest.TestCase):

    @patch("src.serving.core.pipelines.social_media_poster.bigquery.Client")
    @patch("src.serving.core.pipelines.social_media_poster.firestore.Client")
    @patch("src.serving.core.pipelines.social_media_poster.XClient")
    @patch("src.serving.core.pipelines.social_media_poster.vertex_ai.generate")
    @patch("src.serving.core.pipelines.social_media_poster.read_blob")
    def test_run_pipeline(
        self,
        mock_read_blob,
        mock_generate,
        mock_x_client_cls,
        mock_firestore_cls,
        mock_bq_cls,
    ):
        # Setup mocks
        mock_bq_client = mock_bq_cls.return_value
        mock_db = mock_firestore_cls.return_value
        mock_x_client = mock_x_client_cls.return_value
        mock_x_client.client = True  # Simulate initialized client

        # Mock BQ winners response
        mock_bq_client.query.return_value = [
            {"ticker": "TSLA", "weighted_score": 90, "setup_quality_signal": "High"},
            {"ticker": "NVDA", "weighted_score": 85, "setup_quality_signal": "High"},
        ]

        # Mock Firestore (first ticker not posted, second posted)
        mock_collection = mock_db.collection.return_value

        # Document for TSLA (does not exist)
        mock_doc_tsla = MagicMock()
        mock_doc_tsla.get.return_value.exists = False

        # Document for NVDA (exists)
        mock_doc_nvda = MagicMock()
        mock_doc_nvda.get.return_value.exists = True

        def doc_side_effect(doc_id):
            if "TSLA" in doc_id:
                return mock_doc_tsla
            if "NVDA" in doc_id:
                return mock_doc_nvda
            return MagicMock()

        mock_collection.document.side_effect = doc_side_effect

        # Mock Read Blob (Page JSON)
        mock_read_blob.return_value = '{"seo": {"title": "TSLA Analysis"}, "analystBrief": "Bullish", "tradeSetup": "Call Wall at 300"}'

        # Mock Vertex AI
        mock_generate.return_value = (
            '"$TSLA is looking good! 🚀 https://gammarips.com/TSLA"'
        )

        # Mock X Post
        mock_x_client.post_tweet.return_value = "1234567890"

        # Run Pipeline
        social_media_poster.run_pipeline()

        # Assertions

        # Should verify TSLA was processed
        mock_read_blob.assert_called()
        mock_generate.assert_called()
        mock_x_client.post_tweet.assert_called_with(
            "$TSLA is looking good! 🚀 https://gammarips.com/TSLA"
        )
        mock_doc_tsla.set.assert_called()

        # Should verify NVDA was skipped (because it exists)
        # We can check that post_tweet was called only once (for TSLA)
        self.assertEqual(mock_x_client.post_tweet.call_count, 1)


if __name__ == "__main__":
    unittest.main()
