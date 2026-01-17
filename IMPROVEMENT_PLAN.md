# Improvement Plan: Robust Fundamentals Ingestion

## Status: IMPLEMENTED (2026-01-16)

### Summary of Changes
The ingestion pipelines have been updated to implement a "Trust but Verify" strategy. Instead of assuming that the existence of a file means the data is valid, the system now inspects the content of existing files to detect "placeholder" records (common with FMP API immediately after earnings).

### Key Updates

1.  **`src/ingestion/core/pipelines/fundamentals.py`**
    *   Added `_is_data_incomplete(data)` helper function.
    *   **Logic:** Checks if the most recent record has `revenuePerShare == 0` AND `operatingCashFlowPerShare == 0`.
    *   **Action:** If incomplete, it forces a re-fetch of the data from the FMP API, overwriting the placeholder file.

2.  **`src/ingestion/core/pipelines/statement_loader.py`**
    *   Added `_is_statement_incomplete(data)` helper function.
    *   **Logic:** Checks specifically based on statement type:
        *   **Income Statement:** `revenue == 0`
        *   **Balance Sheet:** `totalAssets == 0`
        *   **Cash Flow:** `operatingCashFlow == 0`
    *   **Action:** If incomplete, it logs a warning and forces a re-fetch.

3.  **`src/ingestion/core/gcs.py`**
    *   Added missing `read_blob` function that was causing an `ImportError` in the ingestion pipelines.

4.  **Verification**
    *   Created `scripts/test_fmp_refresh.py` to confirm that FMP indeed updates the data after the initial "zero" placeholder period.
    *   Test confirmed that WDC (the problem case) now has valid non-zero data available via API, validating the fix strategy.
    *   Fixed an unrelated test failure in `tests/ingestion/test_ingestion_main.py` regarding `sync_spy_price_history` signature mismatch.

### Deployment Instructions
To deploy these changes, run:
```bash
./src/utils/deploy_functions.sh
```
This will redeploy the `refresh-fundamentals` and `load-statements` Cloud Functions with the new validation logic.
