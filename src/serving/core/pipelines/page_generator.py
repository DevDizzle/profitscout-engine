# serving/core/pipelines/page_generator.py
import logging
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from typing import Dict, Optional, List

from .. import config, gcs
from .. import bq
from ..clients import vertex_ai

INPUT_PREFIX = config.RECOMMENDATION_PREFIX
OUTPUT_PREFIX = config.PAGE_JSON_PREFIX

# --- MICRO-PROMPT for Analyst Brief ---
# We only ask the LLM for the text content, not the JSON structure.
_BRIEF_PROMPT = """
You are a Senior Derivatives Analyst. Write a concise, 2-paragraph market update for {ticker} ({company_name}) in HTML format (<p>, <strong>).

**Data Context:**
- Signal: {signal}
- Price Trend: {technicals_snippet}
- Options Structure: Call Wall ${call_wall}, Put Wall ${put_wall}, Net Call Gamma {net_call_gamma:.2f}, Net Put Gamma {net_put_gamma:.2f}.
- Top Activity: {top_contract_desc}

**Instructions:**
1. Start with the directional bias (Signal) and the key technical trend.
2. Analyze the Options Structure (Call/Put Walls) and how they might act as magnets or barriers.
3. Mention the "Smart Money" positioning based on the top contract activity.
4. Keep it professional, objective, and under 200 words.

**Output:**
Return ONLY the HTML string. No Markdown blocks.
"""

def _clean_text(text: str) -> str:
    if not text: return ""
    return text.replace('"', "'").replace('\n', ' ').strip()

def _split_aggregated_text(aggregated_text: str) -> Dict[str, str]:
    """
    Parses the massive aggregated text block into specific sections.
    Maps common headers to the keys required by the frontend.
    """
    section_map = {}
    
    # Mapping table: 'Header Keyword' -> 'Output Key'
    # The pipeline usually produces headers like "## News Analysis", "## Technicals Analysis"
    key_mapping = {
        "news": "news",
        "technicals": "technicals",
        "md&a": "md&a",
        "earnings transcript": "transcript",
        "transcript": "transcript", 
        "financials": "financials",
        "fundamentals": "fundamentals"
    }

    if aggregated_text:
        sections = re.split(r'\n\n---\n\n', aggregated_text.strip())
        for section in sections:
            # Match "## HEADER Analysis" or just "## HEADER"
            match = re.match(r'## (.*?)(?: Analysis)?\n\n(.*)', section, re.DOTALL | re.IGNORECASE)
            if match:
                raw_header = match.group(1).lower().strip()
                content = match.group(2).strip()
                
                # Find the best matching key
                for k, v in key_mapping.items():
                    if k in raw_header:
                        section_map[v] = content
                        break
    
    # Fill missing keys with empty strings to prevent frontend errors
    for v in key_mapping.values():
        if v not in section_map:
            section_map[v] = ""
            
    return section_map

def _generate_seo(ticker: str, company: str, signal: str, call_wall: float) -> Dict:
    """Deterministically generates SEO metadata."""
    bias = "Bullish" if signal == "Bullish" else "Bearish" if signal == "Bearish" else "Neutral"
    cw_str = f"${call_wall}" if call_wall else "Key Levels"
    
    return {
        "title": f"{ticker} Options Flow: {bias} Gamma Setup & Targets | GammaRips",
        "metaDescription": f"{company} ({ticker}) displays a {bias.lower()} gamma setup. Analysts project a test of the {cw_str} Call Wall. See the full options analysis.",
        "keywords": [
            f"{ticker} options flow",
            f"{ticker} gamma squeeze",
            f"{company} stock forecast",
            f"{ticker} earnings analysis",
            "institutional order flow"
        ],
        "h1": f"{ticker} Targets {cw_str}: {bias} Momentum Signal"
    }

def _generate_faq(ticker: str, ms: Dict) -> List[Dict]:
    """Deterministically generates FAQ based on market structure."""
    cw = ms.get('call_wall', 'N/A')
    pw = ms.get('put_wall', 'N/A')
    
    return [
        {
            "question": f"Is {ticker} seeing unusual call volume?",
            "answer": f"We are tracking significant activity in the options chain. The Call Wall is currently at ${cw}, which often acts as a magnet or resistance level."
        },
        {
            "question": f"What are the key support and resistance levels for {ticker}?",
            "answer": f"Based on current dealer positioning, the primary resistance (Call Wall) is at ${cw}, while strong support (Put Wall) is found at ${pw}."
        }
    ]

def _generate_analyst_brief(ticker: str, company: str, signal: str, ms: Dict, tech_snippet: str) -> Dict:
    """
    Generates the brief using LLM (Micro-Prompt) or Fallback.
    """
    # Prepare Context
    call_wall = ms.get('call_wall', 0)
    put_wall = ms.get('put_wall', 0)
    net_call_gamma = ms.get('net_call_gamma', 0)
    net_put_gamma = ms.get('net_put_gamma', 0)
    
    top_contracts = ms.get('top_active_contracts', [])
    top_contract_desc = "N/A"
    if top_contracts:
        c = top_contracts[0]
        top_contract_desc = f"{c.get('expiration_date', '')[:10]} ${c.get('strike')} {c.get('option_type')} (Vol: {c.get('volume')})"

    # 1. Try LLM
    try:
        # If technicals are missing, provide a generic "Monitoring Price Action" string
        # to ensure the prompt still makes sense.
        if not tech_snippet:
            tech_snippet = "Price action is under observation."

        prompt = _BRIEF_PROMPT.format(
            ticker=ticker,
            company_name=company,
            signal=signal,
            technicals_snippet=tech_snippet[:500], # Keep it short
            call_wall=call_wall,
            put_wall=put_wall,
            net_call_gamma=net_call_gamma,
            net_put_gamma=net_put_gamma,
            top_contract_desc=top_contract_desc
        )
        
        # Use flash model for speed and low cost
        content = vertex_ai.generate(prompt)
        
        # Simple cleanup if the model returns markdown code blocks despite instructions
        content = content.replace("```html", "").replace("```", "").strip()
        
        return {
            "headline": f"{signal} Setup: Eyes on ${call_wall} Call Wall",
            "content": content
        }
        
    except Exception as e:
        logging.warning(f"[{ticker}] LLM Brief Gen failed ({e}). Using fallback.")
        
    # 2. Fallback Template
    fallback_content = (
        f"<p><strong>{ticker}</strong> is showing a <strong>{signal}</strong> setup. "
        f"Order flow analysis identifies the <strong>${call_wall} Call Wall</strong> as a key target. "
        f"Support is firm at the <strong>${put_wall} Put Wall</strong>. "
        f"Net Call Gamma is {net_call_gamma:.2f}, suggesting positive dealer hedging flows may support price action.</p>"
    )
    
    return {
        "headline": f"Market Update: {ticker} Testing Key Levels",
        "content": fallback_content
    }

def process_blob(blob_name: str) -> Optional[str]:
    # 1. Parse Filename
    file_name = os.path.basename(blob_name)
    match = re.match(r'([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.json', file_name)
    if not match: return None
    ticker_filename, date_filename = match.groups()

    try:
        # 2. Read Recommendation (GCS)
        rec_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
        if not rec_json_str: return None
        try:
            rec_data = json.loads(rec_json_str)
        except json.JSONDecodeError: return None

        # Resolve Ticker/Date
        ticker = rec_data.get("ticker", ticker_filename)
        run_date = rec_data.get("run_date", date_filename)
        
        # 3. Fetch Options Market Structure (BQ) - CRITICAL
        # We fail if this is missing because the product IS options analysis.
        market_structure = bq.fetch_options_market_structure(ticker)
        if not market_structure:
            logging.warning(f"[{ticker}] No Options Market Structure found. Skipping page gen.")
            return None

        # 4. Fetch Analysis Scores (BQ) - OPTIONAL/ENRICHMENT
        # We do NOT fail if this is missing. We just use empty defaults.
        bq_data = bq.fetch_analysis_scores(ticker, run_date)
        
        # 5. Assembly Phase
        company_name = bq_data.get("company_name", ticker)
        weighted_score = bq_data.get("weighted_score", 50.0)
        
        # If aggregated_text is missing, this returns a dict with empty strings for all keys
        full_analysis_map = _split_aggregated_text(bq_data.get("aggregated_text", ""))
        
        # Trade Setup (from Recommendation)
        trade_setup = {
            "signal": rec_data.get("outlook_signal", "Neutral"),
            "confidence": rec_data.get("confidence", "Medium"),
            "strategy": rec_data.get("strategy", "Observation"),
            "catalyst": rec_data.get("primary_driver", "Market Structure")
        }

        # SEO & Brief
        seo_data = _generate_seo(ticker, company_name, trade_setup['signal'], market_structure.get('call_wall'))
        faq_data = _generate_faq(ticker, market_structure)
        
        # Use Technicals snippet for context in Brief
        tech_snippet = full_analysis_map.get("technicals", "")
        if not tech_snippet:
             # Try to provide at least price context if technicals text is missing
             last_price = market_structure.get("top_active_contracts", [{}])[0].get("last_price", "N/A")
             tech_snippet = f"Stock is trading near options activity levels."

        analyst_brief = _generate_analyst_brief(ticker, company_name, trade_setup['signal'], market_structure, tech_snippet)

        # 6. Final JSON Construction
        final_json = {
            "symbol": ticker,
            "date": run_date,
            "bullishScore": round(weighted_score * 100, 2),
            "fullAnalysis": full_analysis_map,
            "marketStructure": market_structure,
            "seo": seo_data,
            "analystBrief": analyst_brief,
            "tradeSetup": trade_setup,
            "faq": faq_data
        }

        # 7. Write Output
        output_path = f"{OUTPUT_PREFIX}{ticker}_page_{run_date}.json"
        gcs.write_text(config.GCS_BUCKET_NAME, output_path, json.dumps(final_json, indent=2), "application/json")
        
        logging.info(f"[{ticker}] Page Gen Success: {output_path}")
        return output_path
    
    except Exception as e:
        logging.error(f"[{ticker}] Page Gen Failed: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting Optimized Page Generator ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    
    if not work_items:
        logging.info("No work items found.")
        return

    processed_count = 0
    # Use ThreadPool with higher concurrency now that BQ client is Singleton
    max_workers = config.MAX_WORKERS_RECOMMENDER
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_blob, item) for item in work_items}
        for future in as_completed(futures):
            if future.result():
                processed_count += 1

    logging.info(f"--- Page Gen Finished. Created {processed_count} pages. ---")