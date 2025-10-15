# tests/enrichment/test_options_enrichment.py
import pytest
from unittest.mock import patch, MagicMock

from src.enrichment import main


@patch("src.enrichment.main.options_candidate_selector")
def test_run_options_candidate_selector(mock_pipeline):
    """
    Tests the successful execution of the run_options_candidate_selector function.
    """
    mock_request = MagicMock()
    result, status_code = main.run_options_candidate_selector(mock_request)

    assert status_code == 200
    assert result == "Options candidate selector pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.enrichment.main.options_analyzer")
def test_run_options_analyzer(mock_pipeline):
    """
    Tests the successful execution of the run_options_analyzer function.
    """
    mock_request = MagicMock()
    result, status_code = main.run_options_analyzer(mock_request)

    assert status_code == 200
    assert result == "Options analyzer pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.enrichment.main.options_feature_engineering")
def test_run_options_feature_engineering(mock_pipeline):
    """
    Tests the successful execution of the run_options_feature_engineering function.
    """
    mock_request = MagicMock()
    result, status_code = main.run_options_feature_engineering(mock_request)

    assert status_code == 200
    assert result == "Options feature engineering pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()
