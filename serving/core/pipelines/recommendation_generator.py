# serving/core/pipelines/recommendation_generator.py
import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage  # storage kept for other pipeline uses; not needed for inline chart
from .. import config, gcs
from ..clients import vertex_ai
import matplotlib.pyplot as plt
import io
import base64
from datetime import datetime

# _EXAMPLE_OUTPUT and _PROMPT_TEMPLATE remain the same...

_EXAMPLE_OUTPUT = """
# American Airlines (AAL) ✈️

**HOLD** – Strong momentum, but financial risks keep us cautious.

Quick take: AAL shows near-term strength in price action and headlines, but high debt and weak equity limit long-term upside. Caution is warranted.

### Key Highlights:
- **News Buzz**: Partnership wins and stock momentum suggest optimism. 📈
- **Tech Signals**: Breakouts and positive indicators point to short-term gains. ⚡
- **Mgmt Chat**: Liquidity is solid, but rising costs and fuel risks add pressure. 😬
- **Earnings Scoop**: Record revenue and EPS, though margins remain under strain. 💵
- **Financials**: Heavy debt load and negative equity are ongoing concerns. 🚩
- **Fundamentals**: Signs of recovery, but profitability is still fragile. ⚖️

Overall: Positive momentum meets financial headwinds—best to hold for now.  
💡 Help shape the future: share your feedback to guide our next update.
"""

_PROMPT_TEMPLATE = r"""
You are a confident but approachable financial analyst writing AI-powered stock recommendations.
Tone = clear, professional, and concise. 
Think: "Smarter Investing Starts Here" — give users clarity, not noise.

### Section Mapping
Map aggregated text sections into these labels:
- **News Buzz** → "News Analysis"
- **Tech Signals** → "Technicals Analysis"
- **Mgmt Chat** → "Mda Analysis"
- **Earnings Scoop** → "Transcript Analysis"
- **Financials** → "Financials Analysis"
- **Fundamentals** → "Key Metrics Analysis" + "Ratios Analysis"

### Instructions
1. **Recommendation**: Strictly "BUY" (> 0.68), "HOLD" (0.50–0.67), "SELL" (< 0.50). 
   Add a short, confident one-liner (e.g., "Strong outlook, limited risks" or "Momentum up, but debt drags"). 
   Do not show the raw score.
2. **Quick Take**: Start with 1–2 sentences summarizing the overall outlook.
3. **Highlights**: 
   - Use concise emoji bullets (📈 bullish, 🚩 bearish, ⚖️ mixed, ⚡ momentum).
   - Each bullet = 1 line, clear and factual.
   - Keep each section under ~12 words.
4. **Wrap-Up**: End with 1 sentence summarizing why this is the right call. 
5. **Engagement Hook**: Close with a single call-to-action (e.g., "💡 Have a suggestion? Share it in the feedback box to help us improve.").
6. **Format**: 
   - H1 with ticker + relevant emoji.
   - Bold Recommendation line.
   - Section = "### Key Highlights:" followed by bullets.
   - Entire output under ~200 words, no extra fluff.

### Input Data
- **Weighted Score**: {weighted_score}
- **Aggregated Analysis Text**:
{aggregated_text}

### Example Output
{example_output}
"""


def _get_analysis_scores() -> list[dict]:
    """Fetches the latest scored records from BigQuery."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    logging.info(f"Querying for analysis scores: {config.SCORES_TABLE_ID}")
    query = f"""
        SELECT ticker, weighted_score, aggregated_text
        FROM `{config.SCORES_TABLE_ID}`
        WHERE weighted_score IS NOT NULL AND aggregated_text IS NOT NULL
    """
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} records for recommendation.")
        return df.to_dict('records')
    except Exception as e:
        logging.critical(f"Failed to query for analysis scores: {e}", exc_info=True)
        return []


def _generate_chart_data_uri(ticker: str) -> str | None:
    """
    Queries price data from a long enough window, generates a brand-aligned chart,
    and returns a base64 data URI (no external files or URLs).
    - X-axis: last ~90 trading days (from available data).
    - SMAs: 50-day and 200-day.
    - We query from a fixed early date (2020-01-01) to guarantee enough history.
    """
    try:
        logging.info(f"[{ticker}] Generating performance chart (inline base64).")
        bq_client = bigquery.Client(project=config.SOURCE_PROJECT_ID)

        # Pull from a fixed early date to guarantee warm-up for SMA(200) well before the 90-day window.
        fixed_start_date = "2020-01-01"

        query = """
            SELECT date, adj_close
            FROM `{table}`
            WHERE ticker = @ticker AND date >= @start_date
            ORDER BY date ASC
        """.format(table=config.PRICE_DATA_TABLE_ID)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                bigquery.ScalarQueryParameter("start_date", "DATE", fixed_start_date),
            ]
        )
        df = bq_client.query(query, job_config=job_config).to_dataframe()

        if df.empty or len(df) < 200:
            logging.warning(f"[{ticker}] Not enough price data (need >=200) to generate chart.")
            return None

        # Compute SMAs
        df["sma_50"] = df["adj_close"].rolling(window=50, min_periods=50).mean()
        df["sma_200"] = df["adj_close"].rolling(window=200, min_periods=200).mean()

        # Display last ~90 trading days
        plot_df = df.tail(90).copy()

        # Detect crossovers within the display window
        cross = (plot_df["sma_50"] - plot_df["sma_200"]).dropna()
        bull_idxs, bear_idxs = [], []
        if not cross.empty:
            sign = (cross > 0).astype(int)
            shifts = sign.diff()
            bull_idxs = list(shifts[shifts == 1].index)   # 50 crosses above 200
            bear_idxs = list(shifts[shifts == -1].index)  # 50 crosses below 200

        # Colors (brand-aware, dark UI)
        bg = "#20222D"
        price_c = "#9CFF0A"   # brand green
        sma50_c = "#00BFFF"   # cyan
        sma200_c = "#FF6347"  # tomato

        # Figure
        fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
        fig.patch.set_facecolor(bg)
        ax.set_facecolor(bg)

        ax.plot(plot_df["date"], plot_df["adj_close"], label="Price", color=price_c, linewidth=2.2)
        ax.plot(plot_df["date"], plot_df["sma_50"],   label="50-Day SMA",  color=sma50_c, linestyle="--", linewidth=1.6)
        ax.plot(plot_df["date"], plot_df["sma_200"],  label="200-Day SMA", color=sma200_c, linestyle="--", linewidth=1.6)

        # Crossover markers
        for idx in bull_idxs:
            ax.scatter(idx, plot_df.loc[idx, "sma_50"], s=26, marker="^", color=sma50_c, zorder=5)
        for idx in bear_idxs:
            ax.scatter(idx, plot_df.loc[idx, "sma_50"], s=26, marker="v", color=sma200_c, zorder=5)

        # Last price label
        last_x = plot_df["date"].iloc[-1]
        last_y = plot_df["adj_close"].iloc[-1]
        ax.annotate(f"${last_y:,.2f}", xy=(last_x, last_y), xytext=(8, 0),
                    textcoords="offset points", color="white", fontsize=9, va="center")

        # Styling
        ax.set_title(f"{ticker} — 90-Day Price with 50/200-Day SMA", color="white", fontsize=14, pad=12)
        ax.set_ylabel("Price (USD)", color="white")
        ax.tick_params(axis="x", colors="#C8C9CC", labelsize=9, rotation=0)
        ax.tick_params(axis="y", colors="#C8C9CC", labelsize=9)
        ax.grid(True, linestyle="--", alpha=0.15)
        ax.margins(x=0.01)

        # Legend
        leg = ax.legend(loc="upper left", framealpha=0.2, facecolor=bg, edgecolor="none", labelcolor="white")
        for text in leg.get_texts():
            text.set_color("#EDEEEF")

        plt.tight_layout()

        # Save PNG to memory buffer
        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        plt.close(fig)

        # Encode to base64 data URI
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        data_uri = f"data:image/png;base64,{b64}"
        return data_uri

    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate inline chart: {e}", exc_info=True)
        return None


def _process_ticker(ticker_data: dict):
    """Generates and uploads a Markdown recommendation for a single ticker."""
    ticker = ticker_data.get("ticker")
    weighted_score = ticker_data.get("weighted_score")
    aggregated_text = ticker_data.get("aggregated_text")

    if not all([ticker, weighted_score, aggregated_text]):
        return None

    md_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.md"
    logging.info(f"[{ticker}] Generating recommendation Markdown.")

    prompt = _PROMPT_TEMPLATE.format(
        weighted_score=str(weighted_score),
        aggregated_text=aggregated_text,
        example_output=_EXAMPLE_OUTPUT,
    )

    try:
        # Generate the text-based Markdown analysis using the LLM
        recommendation_md = vertex_ai.generate(prompt)

        # Generate inline chart (base64 data URI)
        chart_data_uri = _generate_chart_data_uri(ticker)

        # Append the chart to the markdown if it was created successfully
        final_md = recommendation_md
        if chart_data_uri:
            chart_md = (
                "\n\n### 90-Day Performance\n"
                f'<img src="{chart_data_uri}" alt="{ticker} price with 50/200-day SMA (last 90 days)" '
                'style="max-width:100%; height:auto; display:block; margin:0;"/>'
            )
            final_md += chart_md

        # Upload the final markdown file to Google Cloud Storage (/recommendation folder)
        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, final_md, "text/markdown")
        logging.info(f"[{ticker}] Successfully wrote final Markdown file.")

        return md_blob_path
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate or upload recommendation: {e}", exc_info=True)
        return None


def run_pipeline():
    """Orchestrates the recommendation generation pipeline."""
    logging.info("--- Starting Recommendation Generation Pipeline ---")
    scores_data = _get_analysis_scores()
    if not scores_data:
        logging.warning("No analysis scores found. Exiting pipeline.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        futures = [executor.submit(_process_ticker, item) for item in scores_data]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Recommendation Pipeline Finished. Processed {count} tickers. ---")
