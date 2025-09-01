# serving/core/pipelines/options_recommendation_generator.py
import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from datetime import date

from .. import config, gcs
from ..clients import vertex_ai

# --- You can copy this function directly from the main recommendation_generator.py ---
from .recommendation_generator import _generate_chart_data_uri, _get_all_price_histories

# --- Prompt Template and Example ---

_EXAMPLE_OUTPUT_OPTIONS = """
# Fifth Third Bancorp (FITB) 📊

**BUY CALL** – Affordable OTM contract with strong liquidity and steady Greeks.

**Quick take:** Momentum tilt favors upside; this contract offers leverage with controlled risk.

### Contract Profile
FITB 50C expiring 11/21/2025 (447 DTE). Strike = $50.00, premium ≈ $0.55.
This contract shows high open interest and balanced Greeks, indicating tradable liquidity.

### Key Highlights
- ⚡ Momentum-led BUY rating aligns with this bullish contract setup.
- 📈 Contract is 5.3% Out-of-the-Money, offering good leverage on a rally.
- 💧 Solid open interest (581 contracts) ensures strong liquidity for trading.
- ⚖️ Greeks show moderate delta (0.21) and contained daily theta decay (-$0.008).
- 📈 Vega is positive, offering exposure if upside volatility expands.
- ⚖️ Implied volatility is within a normal range; the option is not overly expensive.

Overall: A liquid, moderately OTM call offering convex upside that is well-aligned with the underlying stock's bullish signal.

💡 Share your feedback to refine our options analysis experience.
"""

_PROMPT_TEMPLATE_OPTIONS = r"""
You are a confident but approachable financial analyst writing AI-powered **options recommendations**.
Your tone is clear, professional, and concise. Your goal is to give users clarity, not noise, for making smarter options trades.

### Formatting Rules (Strict)
- Use this layout and spacing exactly:
  1) H1 line: "# {company_name} ({ticker}) [Emoji]"
  2) Bold recommendation line: "**{signal} {option_type_upper}** – short one-liner."
  3) A blank line, then "**Quick take:**" + 1–2 sentences summarizing the setup.
  4) A blank line, then "### Contract Profile" + contract summary including DTE.
  5) A blank line, then "### Key Highlights" followed by a bulleted list.
  6) A blank line, then one sentence starting with "Overall: ..."
  7) A blank line, then a single call-to-action line.
- Each bullet in "Key Highlights" must start with an emoji (`⚡`, `📈`, `⚖️`, `💧`) and be a concise statement.
- Total length should be under 200 words.

### Content Instructions
1.  **Recommendation**: Combine the `{signal}` and `{option_type_upper}`.
2.  **Quick Take**: Link the stock signal to the option's attractiveness.
3.  **Contract Profile**: Describe the contract with strike, type, expiry, DTE, and premium.
4.  **Highlights**: Create 5-6 emoji-led bullets covering the signal, moneyness, liquidity, greeks, and implied volatility.
5.  **Wrap-Up**: A single sentence summarizing why the option is a good fit.

### Input Data
- **Ticker**: {ticker}
- **Company Name**: {company_name}
- **Signal**: {signal}
- **Option Type**: {option_type}
- **Option Type Upper**: {option_type_upper}
- **Expiration**: {expiration_date}
- **DTE**: {dte}
- **Moneyness**: {moneyness}
- **Strike**: {strike}
- **Last Price (premium)**: {last_price}
- **Open Interest**: {open_interest}
- **Implied Volatility**: {implied_volatility}
- **Delta**: {delta}
- **Theta**: {theta}
- **Underlying Price**: {underlying_price}
- **Options Score**: {options_score}

### Example Output
{example_output}
"""

def _get_options_work_list() -> pd.DataFrame:
    """
    Fetches all of today's options candidates from BigQuery.
    """
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT
            c.*,
            s.company_name
        FROM `{config.OPTIONS_CANDIDATES_TABLE_ID}` AS c
        JOIN (
            SELECT ticker, company_name, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}`
        ) AS s ON c.ticker = s.ticker AND s.rn = 1
        WHERE c.fetch_date = CURRENT_DATE() AND c.options_score >= 0.7
        ORDER BY c.ticker, c.options_score DESC
    """
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logging.critical(f"Failed to fetch options work list: {e}", exc_info=True)
        return pd.DataFrame()


def _process_contract(contract_data: pd.Series, price_histories: dict):
    """
    Main worker function for a single option contract.
    Generates one markdown file for the contract.
    """
    ticker = contract_data['ticker']
    
    try:
        # ==============================================================================
        # --- CONSOLIDATED VARIABLE CALCULATION TO PREVENT ALL KEYERRORS ---
        # ==============================================================================
        
        # 1. Calculate DTE (Days to Expiration)
        expiration_date = pd.to_datetime(contract_data.get('expiration_date'))
        dte = (expiration_date - pd.Timestamp.now()).days if pd.notna(expiration_date) else 0

        # 2. Create Uppercase Option Type
        option_type = contract_data.get('option_type', '')
        option_type_upper = option_type.upper() if option_type else ''

        # 3. Calculate Moneyness
        moneyness_str = "N/A"
        strike = contract_data.get('strike', 0.0)
        underlying_price = contract_data.get('underlying_price', 0.0)
        
        if pd.notna(underlying_price) and underlying_price > 0 and pd.notna(strike) and strike > 0:
            if option_type == 'call':
                moneyness_val = (strike / underlying_price - 1) * 100
                moneyness_str = f"{moneyness_val:.1f}% Out-of-the-Money" if moneyness_val >= 0 else f"{-moneyness_val:.1f}% In-the-Money"
            elif option_type == 'put':
                moneyness_val = (underlying_price / strike - 1) * 100
                moneyness_str = f"{moneyness_val:.1f}% Out-of-the-Money" if moneyness_val >= 0 else f"{-moneyness_val:.1f}% In-the-Money"

        # ==============================================================================

        prompt = _PROMPT_TEMPLATE_OPTIONS.format(
            ticker=ticker,
            company_name=contract_data.get('company_name', 'N/A'),
            signal=contract_data.get('signal', 'N/A'),
            option_type=option_type,
            option_type_upper=option_type_upper,
            expiration_date=expiration_date.strftime('%m/%d/%Y') if pd.notna(expiration_date) else 'N/A',
            dte=dte,
            moneyness=moneyness_str,
            strike=strike,
            last_price=contract_data.get('last_price', 0.0),
            open_interest=contract_data.get('open_interest', 0),
            implied_volatility=contract_data.get('implied_volatility', 0.0),
            delta=contract_data.get('delta', 0.0),
            theta=contract_data.get('theta', 0.0),
            underlying_price=underlying_price,
            options_score=contract_data.get('options_score', 0.0),
            example_output=_EXAMPLE_OUTPUT_OPTIONS
        )
        
        recommendation_text = vertex_ai.generate(prompt)
        
        ticker_price_df = price_histories.get(ticker)
        chart_data_uri = _generate_chart_data_uri(ticker, ticker_price_df)
        
        final_md = recommendation_text
        if chart_data_uri:
            chart_md = (
                f'\n\n---\n\n### 90-Day Underlying Price Performance\n'
                f'<img src="{chart_data_uri}" alt="{ticker} price chart"/>'
            )
            final_md += chart_md
            
        today_str = date.today().strftime('%Y-%m-%d')
        contract_symbol = contract_data.get('contract_symbol', '').replace(':', '_')
        md_blob_path = f"{config.OPTIONS_MD_PREFIX}{ticker}/{contract_symbol}_{today_str}.md"
        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, final_md, "text/markdown")
        
        logging.info(f"[{ticker}] Successfully generated recommendation for {contract_symbol}")
        return md_blob_path

    except Exception as e:
        logging.error(f"[{ticker}] Failed during options recommendation generation: {e}", exc_info=True)
        return None

def run_pipeline():
    """Main pipeline for options recommendation generation."""
    logging.info("--- Starting Single-Contract Options Recommendation Pipeline ---")
    
    work_df = _get_options_work_list()
    if work_df.empty:
        logging.warning("No options candidates found for today. Exiting.")
        return

    tickers_to_process = work_df['ticker'].unique().tolist()
    price_histories = _get_all_price_histories(tickers_to_process)
    
    work_items = [row for index, row in work_df.iterrows()]
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_process_contract, item, price_histories) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
                
    logging.info(f"--- Options Recommendation Pipeline Finished. Processed {count} individual contracts. ---")