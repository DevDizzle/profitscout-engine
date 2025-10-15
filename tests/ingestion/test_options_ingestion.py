# tests/ingestion/test_options_ingestion.py
import pytest
from unittest.mock import patch, MagicMock

from src.ingestion import main


@patch("src.ingestion.main.os.environ")
@patch("src.ingestion.main.bigquery.Client")
@patch("src.ingestion.main.PolygonClient")
@patch("src.ingestion.main.options_chain_fetcher")
def test_fetch_options_chain_success(
    mock_options_chain_fetcher, mock_polygon_client, mock_bigquery_client, mock_environ
):
    """
    Tests the successful execution of the fetch_options_chain function.
    """
    mock_environ.get.return_value = "test_api_key"
    mock_request = MagicMock()

    result, status_code = main.fetch_options_chain(mock_request)

    assert status_code == 202
    assert result == "Options chain fetch started."
    mock_options_chain_fetcher.run_pipeline.assert_called_once()


@patch("src.ingestion.main.bigquery.Client")
@patch("src.ingestion.main._get_secret_or_env")
def test_fetch_options_chain_no_api_key(mock_get_secret, mock_bigquery_client):
    """
    Tests that the function returns a 500 error if the POLYGON_API_KEY is not set.
    """
    mock_get_secret.return_value = None
    mock_request = MagicMock()

    result, status_code = main.fetch_options_chain(mock_request)

    assert status_code == 500
    assert result == "Server config error: failed to initialize PolygonClient."
