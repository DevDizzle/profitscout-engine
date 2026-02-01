# Plan: Fail Fast & Move On (Zero Retries)

## Objective
Update `technicals_analyzer.py`, `news_analyzer.py`, and `page_generator.py` to ensure the entire batch of ~1000 files processes within **20-30 minutes**, regardless of AI errors or network glitches.

## The Problem
The current "hanging" behavior is caused by the Google Vertex AI client library's default **Retry Policy**. When an API call fails (e.g., rate limit, overload, or empty response), the client automatically waits and retries with exponential backoff. This can cause a single thread to hang for 2-5 minutes. With 1000 items, a few of these retries blow the 30-minute budget.

## The Solution
We will aggressively disable retries and enforce strict timeouts.

### 1. Configure "Zero Retry" Policy
We will modify the `vertex_ai.generate` (and `generate_with_tools`) calls to explicitly disable automatic retries.

*   **Current:** Default (Retries enabled, max timeout ~600s).
*   **New:** `retry=None` (or configured for 0 attempts).

### 2. Enforce Strict Timeout (15 Seconds)
We will add a hard `timeout=15` parameter to the generation request.
*   **Why:** If the model hasn't responded in 15 seconds, it's likely overloaded or hanging. Waiting longer yields diminishing returns.
*   **Result:** A stuck request dies in 15s instead of 300s.

### 3. Implementation Details

#### `src/enrichment/core/pipelines/technicals_analyzer.py`
-   Update `vertex_ai.generate()` call.
-   Add `request_options={"timeout": 15, "retry": None}` (or equivalent for the specific library version).

#### `src/enrichment/core/pipelines/news_analyzer.py`
-   Update `vertex_ai.generate_with_tools()` call.
-   Add explicit timeout and disable retries.

#### `src/serving/core/pipelines/page_generator.py`
-   Update `vertex_ai.generate()` call in `_generate_analyst_brief`.
-   Enforce the same 15s timeout.

## Expected Outcome
-   **Success:** Processed normally.
-   **Failure:** Fails in <15s, logs error, worker moves to next item.
-   **Throughput:** With 10 workers, 1000 items * (1.2s avg duration) / 10 = ~20 minutes. Even if 10% fail, the 15s timeout ensures we stay within the window.

## Execution Steps
1.  **Refactor:** Apply code changes to the 3 files.
2.  **Verify:** Run a quick test (unit or manual) to confirm options are passed.
3.  **Deploy:** Redeploy the 3 Cloud Functions.
