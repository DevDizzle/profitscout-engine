# tests/options/serving/test_main.py
import pytest
from unittest.mock import patch, MagicMock

from src.options.serving import main


@patch("src.options.serving.main.performance_tracker_updater")
def test_run_performance_tracker_updater(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_performance_tracker_updater(mock_request)
    assert status_code == 200
    assert result == "Performance tracker update pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.winners_dashboard_generator")
def test_run_winners_dashboard_generator(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_winners_dashboard_generator(mock_request)
    assert status_code == 200
    assert result == "Winners dashboard generator pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.recommendations_generator")
def test_run_recommendations_generator(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_recommendations_generator(mock_request)
    assert status_code == 200
    assert result == "Recommendations generator pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.sync_calendar_to_firestore")
def test_run_sync_calendar_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_sync_calendar_to_firestore(mock_request)
    assert status_code == 200
    assert result == "Sync calendar events to Firestore pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.sync_options_to_firestore")
def test_run_sync_options_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    mock_request.is_json = False
    result, status_code = main.run_sync_options_to_firestore(mock_request)
    assert status_code == 200
    assert "Full reset: False" in result
    mock_pipeline.run_pipeline.assert_called_once_with(full_reset=False)


@patch("src.options.serving.main.sync_winners_to_firestore")
def test_run_sync_winners_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_sync_winners_to_firestore(mock_request)
    assert status_code == 200
    assert result == "Sync winners to Firestore pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.dashboard_generator")
def test_run_dashboard_generator(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_dashboard_generator(mock_request)
    assert status_code == 200
    assert result == "Dashboard generator pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.data_cruncher")
def test_run_data_cruncher(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_data_cruncher(mock_request)
    assert status_code == 200
    assert result == "Data cruncher pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.page_generator")
def test_run_page_generator(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_page_generator(mock_request)
    assert status_code == 200
    assert result == "Page generator pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.price_chart_generator")
def test_run_price_chart_generator(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_price_chart_generator(mock_request)
    assert status_code == 200
    assert result == "Price chart generator pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.data_bundler")
def test_run_data_bundler(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_data_bundler(mock_request)
    assert status_code == 200
    assert result == "Data bundler pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.sync_to_firestore")
def test_run_sync_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    mock_request.is_json = False
    result, status_code = main.run_sync_to_firestore(mock_request)
    assert status_code == 200
    assert "Full reset: False" in result
    mock_pipeline.run_pipeline.assert_called_once_with(full_reset=False)


@patch("src.options.serving.main.sync_options_candidates_to_firestore")
def test_run_sync_options_candidates_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_sync_options_candidates_to_firestore(mock_request)
    assert status_code == 200
    assert result == "Sync options candidates to Firestore pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()


@patch("src.options.serving.main.sync_performance_tracker_to_firestore")
def test_run_sync_performance_tracker_to_firestore(mock_pipeline):
    mock_request = MagicMock()
    result, status_code = main.run_sync_performance_tracker_to_firestore(mock_request)
    assert status_code == 200
    assert result == "Sync performance tracker to Firestore pipeline finished."
    mock_pipeline.run_pipeline.assert_called_once()
