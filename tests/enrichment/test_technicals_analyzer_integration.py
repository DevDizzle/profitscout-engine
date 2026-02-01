import json
import unittest
from unittest.mock import MagicMock, patch

from src.enrichment.core import config
from src.enrichment.core.pipelines import technicals_analyzer


class TestTechnicalsAnalyzerIntegration(unittest.TestCase):
    def setUp(self):
        # Create >10 days of data to satisfy the new validation check
        self.sample_technicals = {"technicals": []}
        self.sample_prices = {"prices": []}

        for i in range(15):
            date_str = f"2023-10-{10 + i}"
            self.sample_technicals["technicals"].append(
                {"date": date_str, "RSI_14": 55.0 + i, "SMA_50": 150.0 + i}
            )
            self.sample_prices["prices"].append({"date": date_str, "close": 152.0 + i})

    @patch("src.enrichment.core.clients.vertex_ai.genai.Client")
    @patch("src.enrichment.core.gcs.read_blob")
    @patch("src.enrichment.core.gcs.write_text")
    def test_process_blob_client_initialization_and_execution(
        self, mock_write, mock_read, mock_genai_client
    ):
        """
        Verifies that:
        1. Vertex AI client is initialized with location='global'.
        2. Technicals analyzer pipeline calls generate with response_mime_type="application/json".
        """

        # --- Setup Mocks ---
        # 1. Mock GCS Data
        def side_effect_read(bucket, blob_name):
            if "technicals.json" in blob_name:
                return json.dumps(self.sample_technicals)
            if "prices.json" in blob_name:
                return json.dumps(self.sample_prices)
            return None

        mock_read.side_effect = side_effect_read

        # 2. Mock Vertex AI Client & Response
        mock_client_instance = MagicMock()
        mock_genai_client.return_value = mock_client_instance

        # Mock the generate_content_stream to return a dummy JSON string
        mock_chunk = MagicMock()
        mock_chunk.text = (
            '{"score": 0.85, "strategy_bias": "Bullish", "analysis": "Looks good."}'
        )
        mock_client_instance.models.generate_content_stream.return_value = [mock_chunk]

        # --- Execute Pipeline Logic (Single Blob) ---
        # We call process_blob directly to avoid threading/listing complexity
        blob_name = "technicals/AAPL_technicals.json"
        technicals_analyzer.process_blob(blob_name)

        # --- Assertions ---

        # 1. Verify Client Initialization (The Critical Fix)
        # We check the arguments passed to the genai.Client constructor
        # Note: Depending on how the client is lazy-loaded, it might have been initialized
        # in a previous test or import. To be safe, we might need to force re-init or check calls.
        # Since we patched the class 'src.enrichment.core.clients.vertex_ai.genai.Client',
        # any NEW instantiation should be captured.

        # FORCE re-initialization for this test to ensure we capture the call
        from src.enrichment.core.clients import vertex_ai

        vertex_ai._client = None

        # Re-run to trigger init
        technicals_analyzer.process_blob(blob_name)

        # Check call args
        call_args = mock_genai_client.call_args
        self.assertIsNotNone(call_args, "genai.Client should have been instantiated")
        _, kwargs = call_args
        self.assertEqual(
            kwargs.get("location"),
            "global",
            "Client must be initialized with location='global'",
        )
        self.assertEqual(kwargs.get("project"), config.PROJECT_ID)

        # 2. Verify Generation Call
        # We need to verify that generate_content_stream was called correctly
        # AND that the configuration passed to it contained the mime_type.

        generate_call = mock_client_instance.models.generate_content_stream.call_args
        self.assertIsNotNone(generate_call)
        _, gen_kwargs = generate_call

        # Check model name
        self.assertEqual(gen_kwargs.get("model"), config.MODEL_NAME)

        # Check Config (for mime_type)
        gen_config = gen_kwargs.get("config")
        self.assertIsNotNone(gen_config)
        self.assertEqual(
            gen_config.response_mime_type,
            "application/json",
            "Must request JSON output",
        )

        # 3. Verify Output Write
        # We called process_blob twice, so we expect two writes
        self.assertEqual(mock_write.call_count, 2)
        args, _ = mock_write.call_args  # Checks the *last* call
        self.assertIn("AAPL_technicals.json", args[1])


if __name__ == "__main__":
    unittest.main()
