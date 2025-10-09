# tests/utils/test_fetch_images.py
"""
Unit tests for the fetch_images utility script.

This test suite mocks the Google Cloud Storage client to test the
`get_tickers_from_gcs` function in `src/utils/fetch_images.py` without
making actual cloud calls.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.utils import fetch_images


@patch("src.utils.fetch_images.storage.Client")
class TestFetchImages(unittest.TestCase):
    """Test suite for the fetch_images utility."""

    def test_get_tickers_from_gcs_success(self, mock_storage_client):
        """
        Test that tickers are loaded correctly when the GCS file exists.
        """
        # Arrange: Configure the mock GCS client to simulate a file.
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_text.return_value = "AAPL\nGOOG\nMSFT"

        # Configure the mock instance that will be returned by the mocked Client class.
        mock_client_instance = mock_storage_client.return_value
        mock_client_instance.bucket.return_value.blob.return_value = mock_blob

        # Act: Call the function under test.
        tickers = fetch_images.get_tickers_from_gcs("fake-bucket", "fake-path.txt")

        # Assert: Verify the function's behavior.
        self.assertEqual(tickers, ["AAPL", "GOOG", "MSFT"])
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("fake-bucket")
        mock_client_instance.bucket.return_value.blob.assert_called_once_with(
            "fake-path.txt"
        )
        mock_blob.download_as_text.assert_called_once()

    def test_get_tickers_from_gcs_file_not_found(self, mock_storage_client):
        """
        Test that an empty list is returned when the GCS file does not exist.
        """
        # Arrange: Configure the mock to simulate a non-existent file.
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False

        mock_client_instance = mock_storage_client.return_value
        mock_client_instance.bucket.return_value.blob.return_value = mock_blob

        # Act: Call the function under test.
        tickers = fetch_images.get_tickers_from_gcs("fake-bucket", "fake-path.txt")

        # Assert: Verify the function's behavior.
        self.assertEqual(tickers, [])
        mock_blob.download_as_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()