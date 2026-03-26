import pandas as pd
from google.cloud import bigquery
import warnings

warnings.filterwarnings('ignore')

PROJECT_ID = "profitscout-fida8"
client = bigquery.Client(project=PROJECT_ID)

queries = {
    "Query 1: Win Rate by Premium Score": """
SELECT 
  premium_score,
  COUNT(*) as total,
  COUNTIF(is_win) as wins,
  ROUND(COUNTIF(is_win) / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
  ROUND(AVG(peak_return_3d), 2) as avg_peak
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL
GROUP BY premium_score
ORDER BY premium_score DESC;
    """,
    "Query 2: Win Rate by Individual Pattern": """
SELECT 'hedge' as pattern, COUNT(*) as total, COUNTIF(is_win) as wins, 
  ROUND(COUNTIF(is_win)/NULLIF(COUNT(*),0)*100,1) as win_pct, ROUND(AVG(peak_return_3d),2) as avg_peak
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_hedge = true
UNION ALL
SELECT 'high_rr', COUNT(*), COUNTIF(is_win), 
  ROUND(COUNTIF(is_win)/NULLIF(COUNT(*),0)*100,1), ROUND(AVG(peak_return_3d),2)
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_high_rr = true
UNION ALL
SELECT 'bull_flow', COUNT(*), COUNTIF(is_win),
  ROUND(COUNTIF(is_win)/NULLIF(COUNT(*),0)*100,1), ROUND(AVG(peak_return_3d),2)
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_bull_flow = true
UNION ALL
SELECT 'high_atr', COUNT(*), COUNTIF(is_win),
  ROUND(COUNTIF(is_win)/NULLIF(COUNT(*),0)*100,1), ROUND(AVG(peak_return_3d),2)
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_high_atr = true
UNION ALL
SELECT 'bear_flow', COUNT(*), COUNTIF(is_win),
  ROUND(COUNTIF(is_win)/NULLIF(COUNT(*),0)*100,1), ROUND(AVG(peak_return_3d),2)
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_bear_flow = true
ORDER BY win_pct DESC;
    """,
    "Query 3: Premium vs Non-Premium Overall": """
SELECT 
  is_premium_signal,
  COUNT(*) as total,
  COUNTIF(is_win) as wins,
  ROUND(COUNTIF(is_win) / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
  ROUND(AVG(peak_return_3d), 2) as avg_peak
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL
GROUP BY is_premium_signal;
    """,
    "Query 4: Best 2-Pattern Combos": """
SELECT
  CONCAT(
    CASE WHEN premium_hedge THEN 'HEDGE+' ELSE '' END,
    CASE WHEN premium_high_rr THEN 'HIGH_RR+' ELSE '' END,
    CASE WHEN premium_bull_flow THEN 'BULL_FLOW+' ELSE '' END,
    CASE WHEN premium_high_atr THEN 'HIGH_ATR+' ELSE '' END,
    CASE WHEN premium_bear_flow THEN 'BEAR_FLOW+' ELSE '' END
  ) as combo,
  COUNT(*) as total,
  COUNTIF(is_win) as wins,
  ROUND(COUNTIF(is_win) / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
  ROUND(AVG(peak_return_3d), 2) as avg_peak
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE performance_updated IS NOT NULL AND premium_score >= 2
GROUP BY combo
HAVING COUNT(*) >= 3
ORDER BY win_pct DESC;
    """,
    "Query 5: Score Distribution (How Many Signals Per Tier)": """
SELECT 
  premium_score,
  COUNT(*) as total_signals,
  COUNTIF(is_premium_signal) as premium_signals,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct_of_all
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
GROUP BY premium_score
ORDER BY premium_score DESC;
    """
}

for title, q in queries.items():
    print(f"\n{'='*50}\n{title}\n{'='*50}")
    df = client.query(q).to_dataframe()
    print(df.to_markdown(index=False))
