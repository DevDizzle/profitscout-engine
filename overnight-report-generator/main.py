import os
import json
import logging
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
from google.cloud import bigquery, firestore
from google import genai
from google.genai import types

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PROJECT_ID = os.environ.get("PROJECT_ID", "profitscout-fida8")
DATASET = os.environ.get("DATASET", "profit_scout")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "").strip() or None

try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    db = firestore.Client(project=PROJECT_ID)
except Exception as e:
    logger.error(f"Failed to initialize GCP clients: {e}")

# Use Vertex AI backend for google-genai
try:
    ai_client = genai.Client(vertexai=True, project=PROJECT_ID, location="global")
except Exception as e:
    logger.error(f"Failed to initialize Vertex AI client: {e}")
    ai_client = None

def get_report_dates(req_data):
    if "report_date" in req_data:
        report_date = req_data["report_date"]
        underlying_scan_date = req_data.get("underlying_scan_date")
        if not underlying_scan_date:
            rd = datetime.strptime(report_date, "%Y-%m-%d")
            days_to_subtract = 3 if rd.weekday() == 0 else 1
            underlying_scan_date = (rd - timedelta(days=days_to_subtract)).strftime("%Y-%m-%d")
    else:
        # User specified "today is Tuesday, March 10, 2026"
        # We will use UTC date if no request data provided, but let's default to system date
        now = datetime.now()
        report_date = now.strftime("%Y-%m-%d")
        days_to_subtract = 3 if now.weekday() == 0 else 1
        underlying_scan_date = (now - timedelta(days=days_to_subtract)).strftime("%Y-%m-%d")
    return report_date, underlying_scan_date

def fetch_signals(underlying_scan_date):
    query = f"""
        SELECT 
            ticker, direction, key_headline, thesis, news_summary,
            overnight_score, is_premium_signal, recommended_contract, 
            recommended_strike, CAST(recommended_expiration AS STRING) as recommended_expiration, 
            premium_high_rr, premium_high_atr, recommended_spread_pct, 
            recommended_mid_price
        FROM `{PROJECT_ID}.{DATASET}.overnight_signals_enriched`
        WHERE scan_date = '{underlying_scan_date}'
    """
    logger.info(f"Running query: {query}")
    query_job = bq_client.query(query)
    results = query_job.result()
    signals = [dict(row) for row in results]
    return signals

from pydantic import BaseModel, Field

class ReportResponse(BaseModel):
    title: str = Field(description='A punchy, thematic title (e.g., "The Tariff Shakeout").')
    headline: str = Field(description='A 2-3 sentence summary of the market split and key directional plays.')
    content: str = Field(description='The full markdown body of the report.')

def generate_report_content(payload):
    prompt = f"""
    You are GammaMolt, the AI CEO and quantitative editor for GammaRips.
    You are writing the 'Overnight Edge' daily report.
    The report must have an intelligent, thematic, market-structure aware, and concise but rich tone. 
    It is read by options traders and operators.
    
    Here is the structured payload of the overnight scan:
    {json.dumps(payload, indent=2)}

    Please generate a JSON response with exactly three keys:
    "title": A punchy, thematic title (e.g., "The Tariff Shakeout").
    "headline": A 2-3 sentence summary of the market split and key directional plays.
    "content": The full markdown body of the report. Must include sections like:
      - Title as a header (e.g., # The Tariff Shakeout — Overnight Edge Report, [Date])
      - Market Pulse (synthesize the total signals and bullish/bearish split)
      - Key Themes (based on the provided news and theses)
      - Top Bullish Signals
      - Top Bearish Signals
      - Best Contract Recommendations
      - Divergence Watch (if any signals stand out)
      - Summary / Bias
    
    Ensure the JSON is strictly formatted and follows the required schema.
    """
    
    response = ai_client.models.generate_content(
        model='gemini-3-flash-preview',
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=ReportResponse,
            temperature=0.4,
        )
    )
    
    try:
        if response.parsed:
            return response.parsed.model_dump()
        return json.loads(response.text)
    except Exception as e:
        logger.error(f"Failed to parse JSON response. Raw text: {response.text}")
        raise e

@app.route("/", methods=["POST", "GET"])
def generate_report():
    if request.method == "GET":
        return "Overnight Report Generator is running", 200

    req_data = request.get_json(silent=True) or {}
    report_date, underlying_scan_date = get_report_dates(req_data)
    force = req_data.get("force", False)

    # Idempotency check
    doc_ref = db.collection("daily_reports").document(report_date)
    if not force and doc_ref.get().exists:
        return jsonify({"status": "skipped", "message": f"Report for {report_date} already exists"}), 200

    logger.info(f"Generating report for {report_date} (underlying: {underlying_scan_date})")

    # Stage 1: Data aggregation
    signals = fetch_signals(underlying_scan_date)
    
    if not signals:
        return jsonify({"status": "error", "message": f"No signals found for underlying_scan_date: {underlying_scan_date}"}), 404

    total_signals = len(signals)
    bullish_signals = [s for s in signals if s['direction'] == 'BULLISH']
    bearish_signals = [s for s in signals if s['direction'] == 'BEARISH']
    bullish_count = len(bullish_signals)
    bearish_count = len(bearish_signals)
    
    # Get top signals by score
    sorted_signals = sorted(signals, key=lambda x: x.get('overnight_score') or 0, reverse=True)
    top_bullish = [s for s in sorted_signals if s['direction'] == 'BULLISH'][:5]
    top_bearish = [s for s in sorted_signals if s['direction'] == 'BEARISH'][:5]
    premium_signals = [s for s in signals if s.get('is_premium_signal')]
    
    payload = {
        "report_date": report_date,
        "underlying_scan_date": underlying_scan_date,
        "total_signals": total_signals,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "top_bullish": top_bullish,
        "top_bearish": top_bearish,
        "premium_signals": premium_signals,
    }

    # Stage 2: Editorial composition
    if not ai_client:
        return jsonify({"status": "error", "message": "Vertex AI Client not configured"}), 500
        
    try:
        generated = generate_report_content(payload)
    except Exception as e:
        logger.error(f"Generation failed: {e}")
        return jsonify({"status": "error", "message": "Failed to generate content", "details": str(e)}), 500

    # Stage 3: Validation
    title = generated.get("title")
    headline = generated.get("headline")
    content = generated.get("content")

    if not title or not headline or not content:
        return jsonify({"status": "error", "message": "Generated content missing required fields"}), 500

    # Clean up escaped newlines that the model sometimes returns when using JSON schemas
    title = title.replace('\\n', '\n').replace('\\"', '"')
    headline = headline.replace('\\n', '\n').replace('\\"', '"')
    content = content.replace('\\n', '\n').replace('\\"', '"')

    # Stage 4: Firestore publication
    doc_data = {
        "title": title,
        "headline": headline,
        "content": content,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "total_signals": total_signals,
        "scan_date": report_date,
        "underlying_scan_date": underlying_scan_date,
        "published": True,
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP
    }

    try:
        doc_ref.set(doc_data)
        logger.info(f"Successfully saved report to daily_reports/{report_date}")
    except Exception as e:
        logger.error(f"Failed to save to Firestore: {e}")
        return jsonify({"status": "error", "message": "Failed to save to Firestore", "details": str(e)}), 500

    return jsonify({
        "status": "success",
        "message": f"Report generated and saved to daily_reports/{report_date}",
        "report_date": report_date,
        "data": {
            "title": title,
            "headline": headline
        }
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
