# serving/core/pipelines/recommendation_generator.py
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs
from ..clients import vertex_ai
import io
import base64
from datetime import date, datetime
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import threading

_PLOT_LOCK = threading.Lock()

# --- Templates and other top-level definitions remain the same ---
_EXAMPLE_OUTPUT = """
# American Airlines (AAL) ✈️
**HOLD** – Strong momentum, but financial risks keep us cautious.
Quick take: AAL shows near-term strength in price action and headlines, but high debt and weak equity limit long-term upside. Caution is warranted.
### Profile
American Airlines is a major network air carrier for passengers and cargo. It operates an extensive domestic and international network, earning revenue primarily from ticket sales, fees, and its loyalty program.
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
- **Profile** → "About"
- **News Buzz** → "News Analysis"
- **Tech Signals** → "Technicals Analysis"
- **Mgmt Chat** → "Mda Analysis"
- **Earnings Scoop** → "Transcript Analysis"
- **Financials** → "Financials Analysis"
- **Fundamentals** → "Key Metrics Analysis" + "Ratios Analysis"
### Instructions
1. **Recommendation**: Strictly "BUY" (> 0.68), "HOLD" (0.50–0.67), or "SELL" (< 0.50). Add a short, confident one-liner. Do not show the raw score.
2. **Quick Take**: Start with 1–2 sentences summarizing the overall outlook.
3. **Profile**: Add a "### Profile" section. Briefly summarize the "About" text in **1-2 sentences**.
4. **Highlights**: Use concise emoji bullets (📈 bullish, 🚩 bearish, ⚖️ mixed, ⚡ momentum). Each bullet = 1 line, clear and factual. Keep each section under ~12 words.
5. **Wrap-Up**: End with 1 sentence summarizing why this is the right call.
6. **Engagement Hook**: Close with a single call-to-action.
7. **Format**: H1 with ticker + emoji. Bold Recommendation line. Section = "### Key Highlights:" followed by bullets. Entire output under ~250 words.
### Input Data
- **Weighted Score**: {weighted_score}
- **Aggregated Analysis Text**:
{aggregated_text}
### Example Output
{example_output}
"""

def _get_all_last_gen_dates() -> dict[str, date]:
    """
    Efficiently gets the last generation date for all tickers by listing blobs once.
    """
    prefix = config.RECOMMENDATION_PREFIX
    blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    date_regex = re.compile(r'([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.md$')
    last_dates = {}

    for blob_name in blobs:
        match = date_regex.search(blob_name)
        if match:
            ticker, date_str = match.groups()
            try:
                blob_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if ticker not in last_dates or blob_date > last_dates[ticker]:
                    last_dates[ticker] = blob_date
            except ValueError:
                continue
    return last_dates

def _get_daily_work_list() -> list[dict]:
    """
    Builds the list of tickers that need processing for the day.
    """
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    today_iso = date.today().isoformat()
    
    today_scores_query = f"""
        SELECT ticker, weighted_score, aggregated_text
        FROM `{config.SCORES_TABLE_ID}`
        WHERE run_date = '{today_iso}' AND weighted_score IS NOT NULL
    """
    try:
        today_df = client.query(today_scores_query).to_dataframe()
        if today_df.empty: return []
    except Exception as e:
        logging.critical(f"Failed to fetch today's scores: {e}", exc_info=True)
        return []

    prev_scores_query = f"""
        WITH RankedScores AS (
            SELECT ticker, weighted_score, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY run_date DESC) as rn
            FROM `{config.SCORES_TABLE_ID}`
            WHERE run_date < '{today_iso}' AND weighted_score IS NOT NULL
        )
        SELECT ticker, weighted_score AS prev_weighted_score FROM RankedScores WHERE rn = 1
    """
    try:
        prev_df = client.query(prev_scores_query).to_dataframe()
    except Exception:
        prev_df = pd.DataFrame(columns=['ticker', 'prev_weighted_score'])

    merged_df = pd.merge(today_df, prev_df, on="ticker", how="left")
    
    merged_df['score_diff'] = (merged_df['weighted_score'] - merged_df['prev_weighted_score']).abs()
    merged_df['needs_new_text'] = (merged_df['score_diff'] >= 0.02) | (merged_df['prev_weighted_score'].isna())
    
    last_gen_dates_map = _get_all_last_gen_dates()
    last_gen_df = pd.DataFrame(list(last_gen_dates_map.items()), columns=['ticker', 'last_gen_date'])
    merged_df = pd.merge(merged_df, last_gen_df, on='ticker', how='left')

    today_date = date.today()
    merged_df['needs_new_text'] = merged_df['needs_new_text'] | (
        merged_df['last_gen_date'].apply(lambda x: (today_date - x).days >= 7 if pd.notnull(x) else True)
    )
    
    logging.info(f"Found {len(today_df)} tickers. Flagged {merged_df['needs_new_text'].sum()} for new text generation.")
    return merged_df.to_dict('records')

def _get_all_price_histories(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """
    Fetches price history for all specified tickers in a single BigQuery call.
    """
    if not tickers:
        return {}
        
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    fixed_start_date = "2020-01-01"
    query = f"""
        SELECT ticker, date, adj_close
        FROM `{config.PRICE_DATA_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers) AND date >= @start_date
        ORDER BY ticker, date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
            bigquery.ScalarQueryParameter("start_date", "DATE", fixed_start_date),
        ]
    )
    full_df = client.query(query, job_config=job_config).to_dataframe()
    
    return {ticker: group for ticker, group in full_df.groupby("ticker")}


def _get_latest_recommendation_text_from_gcs(ticker: str) -> str | None:
    """Retrieves the text from the most recent recommendation file for a ticker."""
    prefix = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_"
    blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    if not blobs: return None
    latest_blob_name = sorted(blobs, reverse=True)[0]
    try:
        content = gcs.read_blob(config.GCS_BUCKET_NAME, latest_blob_name)
        return content.split("\n\n### 90-Day Performance")[0].strip() if content else None
    except Exception as e:
        logging.error(f"[{ticker}] Failed to read latest recommendation {latest_blob_name}: {e}")
    return None

def _delete_all_recommendations_for_ticker(ticker: str):
    """Deletes all old recommendation files for a given ticker."""
    prefix = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old file {blob_name}: {e}")

def _generate_chart_data_uri(ticker: str, price_df: pd.DataFrame) -> str | None:
    """
    Generates a chart from a pre-fetched DataFrame, conditionally plotting SMAs.
    """
    if price_df is None or price_df.empty:
        logging.warning(f"[{ticker}] No price data available to generate a chart.")
        return None

    df = price_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "adj_close"]).sort_values("date")

    # Conditionally calculate SMAs only if enough data exists
    if len(df) >= 50:
        df["sma_50"] = df["adj_close"].rolling(window=50, min_periods=50).mean()
    if len(df) >= 200:
        df["sma_200"] = df["adj_close"].rolling(window=200, min_periods=200).mean()

    plot_df = df.tail(90).copy()

    if plot_df.empty: return None
    
    with _PLOT_LOCK:
        fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
        try:
            bg, price_c, sma50_c, sma200_c = "#20222D", "#9CFF0A", "#00BFFF", "#FF6347"
            fig.patch.set_facecolor(bg)
            ax.set_facecolor(bg)
            
            # Always plot the main price
            ax.plot(plot_df["date"], plot_df["adj_close"], label="Price", color=price_c, linewidth=2.2)
            
            # Conditionally plot SMAs if the columns exist and have data
            if "sma_50" in plot_df.columns and plot_df["sma_50"].notna().any():
                ax.plot(plot_df["date"], plot_df["sma_50"], label="50-Day SMA", color=sma50_c, linestyle="--", linewidth=1.6)
            if "sma_200" in plot_df.columns and plot_df["sma_200"].notna().any():
                ax.plot(plot_df["date"], plot_df["sma_200"], label="200-Day SMA", color=sma200_c, linestyle="--", linewidth=1.6)
            
            last_y = float(plot_df["adj_close"].iloc[-1])
            ax.set_title(f"{ticker} — 90-Day Price", color="white", fontsize=14, pad=12)
            ax.tick_params(axis="both", colors="#C8C9CC", labelsize=9)
            ax.grid(True, linestyle="--", alpha=0.15)
            leg = ax.legend(loc="upper left", framealpha=0.2, facecolor=bg, edgecolor="none", labelcolor="white")
            for text in leg.get_texts(): text.set_color("#EDEEEF")
            
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)
            return f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('ascii')}"
        finally:
            plt.close(fig)

def _process_ticker(ticker_data: dict, price_histories: dict):
    """
    Main worker function for a single ticker.
    """
    ticker = ticker_data["ticker"]
    needs_new_text = ticker_data["needs_new_text"]
    today_str = date.today().strftime('%Y-%m-%d')
    md_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_{today_str}.md"
    
    try:
        recommendation_text = ""
        if needs_new_text:
            prompt = _PROMPT_TEMPLATE.format(
                weighted_score=ticker_data["weighted_score"],
                aggregated_text=ticker_data["aggregated_text"],
                example_output=_EXAMPLE_OUTPUT,
            )
            recommendation_text = vertex_ai.generate(prompt)
        else:
            recommendation_text = _get_latest_recommendation_text_from_gcs(ticker)

        if not recommendation_text:
            logging.error(f"[{ticker}] Could not get or generate text. Aborting.")
            return None

        ticker_price_df = price_histories.get(ticker)
        chart_data_uri = _generate_chart_data_uri(ticker, ticker_price_df)
        
        final_md = recommendation_text
        if chart_data_uri:
            chart_md = (
                f'\n\n### 90-Day Performance\n'
                f'<img src="{chart_data_uri}" alt="{ticker} price chart"/>'
            )
            final_md += chart_md
        
        _delete_all_recommendations_for_ticker(ticker)
        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, final_md, "text/markdown")
        
        logging.info(f"[{ticker}] Successfully generated and wrote recommendation to {md_blob_path}")
        return md_blob_path
        
    except Exception as e:
        logging.error(f"[{ticker}] Unhandled exception in processing: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting Optimized Recommendation Generation Pipeline ---")
    
    work_list = _get_daily_work_list()
    if not work_list:
        logging.warning("No tickers in daily work list. Exiting.")
        return

    tickers_to_process = [item['ticker'] for item in work_list]
    price_histories = _get_all_price_histories(tickers_to_process)
    
    # Process all tickers from the work list without filtering
    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        future_to_ticker = {
            executor.submit(_process_ticker, item, price_histories): item['ticker']
            for item in work_list
        }
        for future in as_completed(future_to_ticker):
            if future.result():
                processed_count += 1
                
    logging.info(f"--- Recommendation Pipeline Finished. Processed {processed_count} of {len(work_list)} tickers. ---")