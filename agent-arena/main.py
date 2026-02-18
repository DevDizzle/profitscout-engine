"""
Agent Arena — Main Orchestrator
================================
4-round adversarial debate among 3 AI agents on overnight options signals.

Rounds:
  1. Pick — Each agent independently picks top trades
  2. Attack — Agents cross-examine each other's picks
  3. Defend — Agents defend or revise under attack
  4. Final — Final vote with updated convictions

Can run as:
  - FastAPI HTTP endpoint (Cloud Run via uvicorn)
  - CLI for testing: python main.py [--date YYYY-MM-DD] [--dry-run]
"""

import os
import sys
import json
import asyncio
import logging
import argparse
from datetime import datetime, date, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from google.cloud import bigquery, firestore

from config import (
    AGENTS, MAX_SIGNALS, MIN_SIGNAL_SCORE, MAX_PICKS_PER_AGENT,
    CONSENSUS_THRESHOLD, SUPERMAJORITY,
    LARGE_MOVE_THRESHOLD, EXTREME_MOVE_THRESHOLD,
    SYSTEM_PROMPT, ROUND1_PICK_PROMPT, ROUND2_ATTACK_PROMPT,
    ROUND3_DEFEND_PROMPT, ROUND4_FINAL_PROMPT,
    GCP_PROJECT, BQ_DATASET, BQ_ROUNDS_TABLE, BQ_PICKS_TABLE,
    BQ_CONSENSUS_TABLE, FIRESTORE_COLLECTION,
)
from agents import call_agent, call_all_agents, parse_agent_response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="GammaRips Agent Arena", version="1.0.0")

bq_client = bigquery.Client(project=GCP_PROJECT)
fs_client = firestore.Client(project=GCP_PROJECT)


# ============================================================
# Data Loading
# ============================================================

def load_signals(scan_date: str = None) -> list[dict]:
    """Load top enriched signals from BQ for a given date."""
    if scan_date:
        date_filter = f"scan_date = '{scan_date}'"
    else:
        date_filter = f"scan_date = (SELECT MAX(scan_date) FROM `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals_enriched`)"

    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals_enriched`
    WHERE {date_filter}
      AND overnight_score >= {MIN_SIGNAL_SCORE}
    ORDER BY enrichment_quality_score DESC, overnight_score DESC
    LIMIT {MAX_SIGNALS}
    """
    rows = list(bq_client.query(query).result())
    signals = []
    for r in rows:
        signals.append({
            "ticker": r.ticker,
            "direction": r.direction,
            "overnight_score": r.overnight_score,
            "call_dollar_volume": float(r.call_dollar_volume or 0),
            "put_dollar_volume": float(r.put_dollar_volume or 0),
            "call_vol_oi_ratio": round(float(r.call_vol_oi_ratio or 0), 2),
            "put_vol_oi_ratio": round(float(r.put_vol_oi_ratio or 0), 2),
            "call_active_strikes": r.call_active_strikes,
            "put_active_strikes": r.put_active_strikes,
            "price_change_pct": round(float(r.price_change_pct or 0), 2),
            "underlying_price": round(float(r.underlying_price or 0), 2),
            "rsi_14": round(float(r.rsi_14 or 0), 1),
            "macd_hist": round(float(r.macd_hist or 0), 4),
            "news_summary": r.news_summary or "",
            "key_headline": r.key_headline or "",
            "catalyst_type": r.catalyst_type or "",
            "catalyst_score": round(float(r.catalyst_score or 0), 2),
            "scan_date": str(r.scan_date),
            # New v2 fields (may not exist in older data)
            "flow_intent": getattr(r, "flow_intent", None) or "UNKNOWN",
            "flow_intent_reasoning": getattr(r, "flow_intent_reasoning", None) or "",
            "mean_reversion_risk": round(float(getattr(r, "mean_reversion_risk", None) or 0), 3),
            "atr_normalized_move": round(float(getattr(r, "atr_normalized_move", None) or 0), 2),
            "reversal_probability": round(float(getattr(r, "reversal_probability", None) or 0), 3),
            "enrichment_quality_score": round(float(getattr(r, "enrichment_quality_score", None) or 5), 1),
            # Price structure for contract selection
            "support": round(float(getattr(r, "support", None) or 0), 2),
            "resistance": round(float(getattr(r, "resistance", None) or 0), 2),
            "high_52w": round(float(getattr(r, "high_52w", None) or 0), 2),
            "low_52w": round(float(getattr(r, "low_52w", None) or 0), 2),
            "sma_50": round(float(getattr(r, "sma_50", None) or 0), 2),
            "sma_200": round(float(getattr(r, "sma_200", None) or 0), 2),
            "risk_reward_ratio": round(float(getattr(r, "risk_reward_ratio", None) or 0), 2),
            "thesis": getattr(r, "thesis", None) or "",
            "recommended_contract": getattr(r, "recommended_contract", None) or "",
            # Scanner contract Greeks
            "recommended_strike": round(float(getattr(r, "recommended_strike", None) or 0), 2),
            "recommended_expiration": str(getattr(r, "recommended_expiration", None) or ""),
            "recommended_dte": int(getattr(r, "recommended_dte", None) or 0),
            "recommended_mid_price": round(float(getattr(r, "recommended_mid_price", None) or 0), 2),
            "recommended_spread_pct": round(float(getattr(r, "recommended_spread_pct", None) or 0), 4),
            "recommended_delta": round(float(getattr(r, "recommended_delta", None) or 0), 4),
            "recommended_gamma": round(float(getattr(r, "recommended_gamma", None) or 0), 6),
            "recommended_theta": round(float(getattr(r, "recommended_theta", None) or 0), 4),
            "recommended_vega": round(float(getattr(r, "recommended_vega", None) or 0), 4),
            "recommended_iv": round(float(getattr(r, "recommended_iv", None) or 0), 4),
            "recommended_volume": int(getattr(r, "recommended_volume", None) or 0),
            "recommended_oi": int(getattr(r, "recommended_oi", None) or 0),
            "contract_score": round(float(getattr(r, "contract_score", None) or 0), 3),
        })
    logger.info(f"Loaded {len(signals)} signals for {scan_date or 'latest'}")
    return signals


def format_signals_text(signals: list[dict]) -> str:
    """Format signals with mean-reversion risk flags."""
    lines = []
    for i, s in enumerate(signals, 1):
        vol = s["call_dollar_volume"] if s["direction"] == "BULLISH" else s["put_dollar_volume"]
        vol_oi = s["call_vol_oi_ratio"] if s["direction"] == "BULLISH" else s["put_vol_oi_ratio"]
        strikes = s["call_active_strikes"] if s["direction"] == "BULLISH" else s["put_active_strikes"]

        # --- Mean-reversion risk assessment ---
        pct = s["price_change_pct"]
        rsi = s["rsi_14"]
        direction = s["direction"]
        risk_flags = []

        # Check if price already moved significantly IN the flow direction
        if direction == "BEARISH" and pct < -LARGE_MOVE_THRESHOLD:
            risk_flags.append(f"⚠️ MEAN-REVERSION RISK: Stock already DOWN {pct:.1f}% — bearish flow may be HEDGING, not directional")
        if direction == "BULLISH" and pct > LARGE_MOVE_THRESHOLD:
            risk_flags.append(f"⚠️ MEAN-REVERSION RISK: Stock already UP +{pct:.1f}% — bullish flow may be CHASING, not initiating")

        if direction == "BEARISH" and pct < -EXTREME_MOVE_THRESHOLD:
            risk_flags.append(f"🚨 EXTREME MOVE: -{abs(pct):.1f}% crash already priced in — very high bounce probability")
        if direction == "BULLISH" and pct > EXTREME_MOVE_THRESHOLD:
            risk_flags.append(f"🚨 EXTREME MOVE: +{pct:.1f}% rally may be exhausted — very high pullback probability")

        # RSI extremes
        if direction == "BEARISH" and rsi < 35:
            risk_flags.append(f"⚠️ RSI OVERSOLD ({rsi}) — bearish flow at oversold levels often signals capitulation BOTTOM")
        if direction == "BULLISH" and rsi > 70:
            risk_flags.append(f"⚠️ RSI OVERBOUGHT ({rsi}) — bullish flow at overbought levels risks euphoria TOP")

        # Flow-after-move skepticism flag
        if abs(pct) > LARGE_MOVE_THRESHOLD:
            risk_flags.append(f"❓ FLOW TIMING: Was this flow placed BEFORE or AFTER the {pct:+.1f}% move? Post-move flow is likely hedging.")

        risk_text = "\n   ".join(risk_flags) if risk_flags else "✅ No mean-reversion flags"

        # Enrichment v2 fields
        flow_intent = s.get("flow_intent", "UNKNOWN")
        flow_reasoning = s.get("flow_intent_reasoning", "")
        mr_risk = s.get("mean_reversion_risk", 0)
        atr_norm = s.get("atr_normalized_move", 0)
        quality = s.get("enrichment_quality_score", 5)
        reversal = s.get("reversal_probability", 0)

        # Price structure context
        support = s.get("support", 0)
        resistance = s.get("resistance", 0)
        high_52w = s.get("high_52w", 0)
        low_52w = s.get("low_52w", 0)
        sma_50 = s.get("sma_50", 0)
        sma_200 = s.get("sma_200", 0)
        rr = s.get("risk_reward_ratio", 0)
        thesis = s.get("thesis", "")
        rec_contract = s.get("recommended_contract", "")
        rec_strike = s.get("recommended_strike", 0)
        rec_exp = s.get("recommended_expiration", "")
        rec_dte = s.get("recommended_dte", 0)
        rec_mid = s.get("recommended_mid_price", 0)
        rec_spread = s.get("recommended_spread_pct", 0)
        rec_delta = s.get("recommended_delta", 0)
        rec_gamma = s.get("recommended_gamma", 0)
        rec_theta = s.get("recommended_theta", 0)
        rec_vega = s.get("recommended_vega", 0)
        rec_iv = s.get("recommended_iv", 0)
        rec_vol = s.get("recommended_volume", 0)
        rec_oi = s.get("recommended_oi", 0)
        c_score = s.get("contract_score", 0)

        # Build contract line with Greeks
        if rec_contract and rec_strike:
            contract_line = (
                f"🎯 RECOMMENDED CONTRACT: {rec_contract} | "
                f"Strike: ${rec_strike:.2f} | Exp: {rec_exp} ({rec_dte} DTE) | "
                f"Mid: ${rec_mid:.2f} | Spread: {rec_spread:.1%} | Score: {c_score}\n"
                f"   📐 Greeks: Δ={rec_delta:.4f} | Γ={rec_gamma:.6f} | Θ={rec_theta:.4f} | "
                f"V={rec_vega:.4f} | IV={rec_iv:.1%} | Vol: {rec_vol:,} | OI: {rec_oi:,}"
            )
        else:
            contract_line = "🎯 RECOMMENDED CONTRACT: None — scanner could not find a qualifying contract"

        lines.append(
            f"{i}. {s['ticker']} | {s['direction']} | Score: {s['overnight_score']}/10 | Quality: {quality}/10 | "
            f"Flow: ${vol:,.0f} | Vol/OI: {vol_oi:.2f} | Strikes: {strikes} | "
            f"Price: ${s['underlying_price']:.2f} ({s['price_change_pct']:+.1f}%) | "
            f"RSI: {s['rsi_14']} | MACD Hist: {s['macd_hist']} | ATR Move: {atr_norm}x\n"
            f"   📈 Price Structure: Support ${support:.2f} | Resistance ${resistance:.2f} | "
            f"SMA50 ${sma_50:.2f} | SMA200 ${sma_200:.2f} | 52w High ${high_52w:.2f} | 52w Low ${low_52w:.2f}\n"
            f"   📊 Risk/Reward: {rr:.1f}:1 | Thesis: {thesis[:200]}\n"
            f"   {contract_line}\n"
            f"   Catalyst ({s['catalyst_type']}, score {s['catalyst_score']}): {s['key_headline']}\n"
            f"   News: {s['news_summary'][:300]}\n"
            f"   📊 Flow Intent: {flow_intent} — {flow_reasoning}\n"
            f"   📊 Mean-Reversion Risk: {mr_risk:.1%} | Reversal Prob: {reversal:.1%}\n"
            f"   {risk_text}"
        )
    return "\n\n".join(lines)


# ============================================================
# Performance Feedback Loop
# ============================================================

def load_performance_context() -> str:
    """
    Pull historical win rate data from BQ to feed back into agent prompts.
    Returns empty string if no data yet (cold start).
    """
    try:
        query = f"""
        WITH picks_with_perf AS (
            SELECT 
                p.agent_id,
                p.ticker,
                p.direction,
                p.conviction,
                sp.return_1d,
                sp.return_3d,
                CASE 
                    WHEN p.direction = 'bull' AND SAFE_CAST(sp.return_1d AS FLOAT64) > 0 THEN 1
                    WHEN p.direction = 'bear' AND SAFE_CAST(sp.return_1d AS FLOAT64) < 0 THEN 1
                    ELSE 0
                END as win_1d
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_PICKS_TABLE}` p
            LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.signal_performance` sp
                ON p.ticker = sp.ticker AND CAST(p.scan_date AS STRING) = CAST(sp.scan_date AS STRING)
            WHERE sp.return_1d IS NOT NULL
        ),
        agent_stats AS (
            SELECT 
                agent_id,
                COUNT(*) as total_picks,
                SUM(win_1d) as wins,
                ROUND(AVG(win_1d) * 100, 1) as win_rate_pct,
                ROUND(AVG(SAFE_CAST(return_1d AS FLOAT64)), 2) as avg_return_1d
            FROM picks_with_perf
            GROUP BY agent_id
        ),
        direction_stats AS (
            SELECT
                direction,
                COUNT(*) as total,
                SUM(win_1d) as wins,
                ROUND(AVG(win_1d) * 100, 1) as win_rate_pct
            FROM picks_with_perf
            GROUP BY direction
        ),
        post_crash_stats AS (
            SELECT
                'post_crash_bear' as category,
                COUNT(*) as total,
                SUM(win_1d) as wins,
                ROUND(AVG(win_1d) * 100, 1) as win_rate_pct
            FROM picks_with_perf p
            JOIN `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals_enriched` e
                ON p.ticker = e.ticker AND CAST(p.scan_date AS STRING) = CAST(e.scan_date AS STRING)
            WHERE p.direction = 'bear' 
                AND SAFE_CAST(e.price_change_pct AS FLOAT64) < -10
        )
        SELECT 'agent' as stat_type, agent_id as label, total_picks as total, wins, win_rate_pct, avg_return_1d
        FROM agent_stats
        UNION ALL
        SELECT 'direction', direction, total, wins, win_rate_pct, NULL
        FROM direction_stats
        UNION ALL
        SELECT 'post_crash', category, total, wins, win_rate_pct, NULL
        FROM post_crash_stats
        """
        rows = list(bq_client.query(query).result())
        if not rows:
            return ""

        lines = ["\n=== HISTORICAL PERFORMANCE (use this to calibrate your confidence) ==="]
        for r in rows:
            if r.stat_type == "agent":
                lines.append(f"Agent {r.label}: {r.wins}/{r.total} wins ({r.win_rate_pct}%), avg 1d return: {r.avg_return_1d}%")
            elif r.stat_type == "direction":
                lines.append(f"{r.label.upper()} picks: {r.wins}/{r.total} wins ({r.win_rate_pct}%)")
            elif r.stat_type == "post_crash":
                lines.append(f"⚠️ BEAR picks after >10% drops: {r.wins}/{r.total} wins ({r.win_rate_pct}%) — BE SKEPTICAL of post-crash bear flow")

        lines.append("=== END HISTORICAL PERFORMANCE ===")
        context = "\n".join(lines)
        logger.info(f"Loaded performance context: {len(rows)} stat rows")
        return context

    except Exception as e:
        logger.warning(f"Could not load performance context (cold start?): {e}")
        return ""


# ============================================================
# Debate Orchestration
# ============================================================

async def run_debate(signals: list[dict], dry_run: bool = False) -> dict:
    """Run the full 4-round debate. Returns complete debate record."""
    signals_text = format_signals_text(signals)
    scan_date = signals[0]["scan_date"] if signals else str(date.today())
    debate_id = f"arena_{scan_date}_{datetime.now(timezone.utc).strftime('%H%M%S')}"

    performance_context = load_performance_context()

    debate = {
        "debate_id": debate_id,
        "scan_date": scan_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "agents": [a["id"] for a in AGENTS],
        "signal_count": len(signals),
        "rounds": {},
    }

    def build_system_prompt(agent):
        """Build role-specific system prompt for an agent."""
        return SYSTEM_PROMPT.format(
            agent_name=agent["name"],
            agent_role_label=agent["role_label"],
            agent_role_prompt=agent["role_prompt"],
            performance_context=performance_context,
        )

    # --- ROUND 1: Pick ---
    logger.info("=== ROUND 1: PICK ===")
    r1_responses = {}
    r1_parsed = {}
    for agent in AGENTS:
        prompt = ROUND1_PICK_PROMPT.format(
            agent_name=agent["name"],
            agent_role_label=agent["role_label"],
            signals=signals_text,
            max_picks=MAX_PICKS_PER_AGENT,
        )
        full_prompt = build_system_prompt(agent) + "\n\n" + prompt
        logger.info(f"  Calling {agent['id']} ({agent['role_label']})...")
        resp = await call_agent(agent, full_prompt, temperature=0.7)
        r1_responses[agent["id"]] = resp
        r1_parsed[agent["id"]] = parse_agent_response(resp)
        logger.info(f"  {agent['id']}: {len(r1_parsed[agent['id']])} picks")

    debate["rounds"]["round1_picks"] = r1_parsed

    # --- ROUND 2: Attack ---
    logger.info("=== ROUND 2: ATTACK ===")
    r2_responses = {}
    r2_parsed = {}
    for agent in AGENTS:
        own_picks = json.dumps(r1_parsed[agent["id"]], indent=2)
        other_picks_parts = []
        for other in AGENTS:
            if other["id"] != agent["id"]:
                other_picks_parts.append(
                    f"### {other['name']} {other['emoji']}\n{json.dumps(r1_parsed[other['id']], indent=2)}"
                )
        other_picks_text = "\n\n".join(other_picks_parts)

        prompt = ROUND2_ATTACK_PROMPT.format(
            agent_name=agent["name"],
            agent_role_label=agent["role_label"],
            own_picks=own_picks,
            other_picks=other_picks_text,
            signals=signals_text,
        )
        full_prompt = build_system_prompt(agent) + "\n\n" + prompt
        logger.info(f"  Calling {agent['id']} ({agent['role_label']})...")
        resp = await call_agent(agent, full_prompt, temperature=0.7)
        r2_responses[agent["id"]] = resp
        r2_parsed[agent["id"]] = parse_agent_response(resp)
        logger.info(f"  {agent['id']}: {len(r2_parsed[agent['id']])} actions")

    debate["rounds"]["round2_attacks"] = r2_parsed

    # --- ROUND 3: Defend ---
    logger.info("=== ROUND 3: DEFEND ===")
    r3_responses = {}
    r3_parsed = {}
    for agent in AGENTS:
        own_picks = json.dumps(r1_parsed[agent["id"]], indent=2)
        # Collect attacks targeting this agent
        attacks = []
        for other in AGENTS:
            if other["id"] != agent["id"]:
                for action in r2_parsed[other["id"]]:
                    if action.get("target_agent") == agent["id"] and action.get("action") == "attack":
                        attacks.append({
                            "from": other["id"],
                            "ticker": action.get("target_ticker"),
                            "argument": action.get("argument"),
                        })

        if not attacks:
            # No attacks — carry forward picks unchanged
            r3_parsed[agent["id"]] = [
                {"ticker": p.get("ticker"), "action": "hold",
                 "original_conviction": int(p.get("conviction", 7)),
                 "new_conviction": int(p.get("conviction", 7)),
                 "defense": "No attacks received."}
                for p in r1_parsed[agent["id"]]
            ]
            logger.info(f"  {agent['id']}: no attacks received, holding all picks")
            continue

        prompt = ROUND3_DEFEND_PROMPT.format(
            agent_name=agent["name"],
            agent_role_label=agent["role_label"],
            own_picks=own_picks,
            attacks_received=json.dumps(attacks, indent=2),
            signals=signals_text,
        )
        full_prompt = build_system_prompt(agent) + "\n\n" + prompt
        logger.info(f"  Calling {agent['id']} ({agent['role_label']})...")
        resp = await call_agent(agent, full_prompt, temperature=0.7)
        r3_responses[agent["id"]] = resp
        r3_parsed[agent["id"]] = parse_agent_response(resp)
        logger.info(f"  {agent['id']}: {len(r3_parsed[agent['id']])} defenses")

    debate["rounds"]["round3_defenses"] = r3_parsed

    # --- ROUND 4: Final Vote ---
    logger.info("=== ROUND 4: FINAL VOTE ===")
    r4_responses = {}
    r4_parsed = {}

    # Build debate summary
    summary_parts = []
    for agent in AGENTS:
        agent_summary = f"### {agent['name']} {agent['emoji']}\n"
        agent_summary += f"**Round 1 Picks:** {json.dumps(r1_parsed[agent['id']])}\n"
        agent_summary += f"**Round 2 Actions:** {json.dumps(r2_parsed[agent['id']])}\n"
        agent_summary += f"**Round 3 Defenses:** {json.dumps(r3_parsed[agent['id']])}\n"
        summary_parts.append(agent_summary)
    debate_summary = "\n\n".join(summary_parts)

    for agent in AGENTS:
        prompt = ROUND4_FINAL_PROMPT.format(
            agent_name=agent["name"],
            agent_role_label=agent["role_label"],
            debate_summary=debate_summary,
            signals=signals_text,
        )
        full_prompt = build_system_prompt(agent) + "\n\n" + prompt
        logger.info(f"  Calling {agent['id']} ({agent['role_label']})...")
        resp = await call_agent(agent, full_prompt, temperature=0.5)  # Lower temp for final
        r4_responses[agent["id"]] = resp
        r4_parsed[agent["id"]] = parse_agent_response(resp)
        logger.info(f"  {agent['id']}: {len(r4_parsed[agent['id']])} final picks")

    debate["rounds"]["round4_final"] = r4_parsed

    # --- Consensus Calculation ---
    consensus = calculate_consensus(r4_parsed)
    debate["consensus"] = consensus
    debate["completed_at"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"=== DEBATE COMPLETE === {len(consensus)} consensus tickers")

    # --- Storage ---
    if not dry_run:
        await asyncio.to_thread(store_debate, debate)
        logger.info("Debate stored to BQ + Firestore")
    else:
        logger.info("DRY RUN — skipping storage")

    return debate


def calculate_consensus(final_picks: dict) -> list[dict]:
    """
    Calculate consensus from Round 4 final picks.
    A ticker has consensus if >= CONSENSUS_THRESHOLD agents picked it in the same direction.
    """
    tally = {}
    for agent_id, picks in final_picks.items():
        for pick in picks:
            ticker = pick.get("ticker", "").upper()
            direction = pick.get("direction", "").lower()
            key = (ticker, direction)
            if key not in tally:
                tally[key] = []
            tally[key].append({
                "agent": agent_id,
                "conviction": int(pick.get("conviction", 5)),
                "contract": pick.get("contract", ""),
                "strike": pick.get("strike"),
                "expiration": pick.get("expiration", ""),
                "option_type": pick.get("option_type", ""),
                "delta_target": pick.get("delta_target"),
                "contract_reasoning": pick.get("contract_reasoning", ""),
                "reasoning": pick.get("reasoning", ""),
            })

    n_agents = len(AGENTS)
    consensus = []
    for (ticker, direction), votes in tally.items():
        ratio = len(votes) / n_agents
        if ratio >= CONSENSUS_THRESHOLD:
            avg_conviction = sum(v["conviction"] for v in votes) / len(votes)
            level = "unanimous" if ratio >= 1.0 else (
                "supermajority" if ratio >= SUPERMAJORITY else "majority"
            )
            # Pick the contract string from the highest-conviction voter
            best_vote = max(votes, key=lambda v: v["conviction"])
            contract = best_vote.get("contract", "")
            
            consensus.append({
                "ticker": ticker,
                "direction": direction,
                "consensus_level": level,
                "vote_count": len(votes),
                "total_agents": n_agents,
                "avg_conviction": round(avg_conviction, 1),
                "contract": contract,
                "votes": votes,
            })

    consensus.sort(key=lambda x: (x["vote_count"], x["avg_conviction"]), reverse=True)
    # Arena outputs ONE trade — return only the top consensus pick
    if consensus:
        return consensus[:1]

    # SPLIT DECISION FALLBACK: No multi-agent consensus.
    # Surface the highest-conviction individual pick.
    all_picks = []
    for agent_id, picks in final_picks.items():
        for pick in picks:
            all_picks.append({
                "agent": agent_id,
                "ticker": pick.get("ticker", "").upper(),
                "direction": pick.get("direction", "").lower(),
                "conviction": int(pick.get("conviction", 5)),
                "contract": pick.get("contract", ""),
                "reasoning": pick.get("reasoning", ""),
            })

    if not all_picks:
        return []

    best = max(all_picks, key=lambda p: p["conviction"])
    logger.info(f"SPLIT DECISION: No consensus. Surfacing highest conviction: "
                f"{best['ticker']} {best['direction']} ({best['conviction']}/10) by {best['agent']}")
    return [{
        "ticker": best["ticker"],
        "direction": best["direction"],
        "consensus_level": "split_decision",
        "vote_count": 1,
        "total_agents": n_agents,
        "avg_conviction": float(best["conviction"]),
        "contract": best["contract"],
        "votes": [best],
        "note": f"No multi-agent consensus. Highest conviction pick by {best['agent']} ({best['conviction']}/10).",
    }]


# ============================================================
# Storage
# ============================================================

def store_debate(debate: dict):
    """Write debate results to BigQuery and Firestore."""
    scan_date = debate["scan_date"]
    debate_id = debate["debate_id"]

    # --- Firestore: full debate document ---
    doc_ref = fs_client.collection(FIRESTORE_COLLECTION).document(debate_id)
    doc_ref.set(debate)
    logger.info(f"Firestore: wrote {FIRESTORE_COLLECTION}/{debate_id}")

    # --- BQ: consensus table ---
    rows = []
    for c in debate.get("consensus", []):
        rows.append({
            "debate_id": debate_id,
            "scan_date": scan_date,
            "ticker": c["ticker"],
            "direction": c["direction"],
            "consensus_level": c["consensus_level"],
            "vote_count": c["vote_count"],
            "total_agents": c["total_agents"],
            "avg_conviction": c["avg_conviction"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    if rows:
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_CONSENSUS_TABLE}"
        _ensure_bq_table(table_id, [
            bigquery.SchemaField("debate_id", "STRING"),
            bigquery.SchemaField("scan_date", "DATE"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("consensus_level", "STRING"),
            bigquery.SchemaField("vote_count", "INTEGER"),
            bigquery.SchemaField("total_agents", "INTEGER"),
            bigquery.SchemaField("avg_conviction", "FLOAT"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ])
        errors = bq_client.insert_rows_json(table_id, rows)
        if errors:
            logger.error(f"BQ insert errors: {errors}")
        else:
            logger.info(f"BQ: wrote {len(rows)} consensus rows")

    # --- BQ: per-agent picks (round 4 final) ---
    pick_rows = []
    for agent_id, picks in debate["rounds"].get("round4_final", {}).items():
        for pick in picks:
            pick_rows.append({
                "debate_id": debate_id,
                "scan_date": scan_date,
                "agent_id": agent_id,
                "ticker": pick.get("ticker", "").upper(),
                "direction": pick.get("direction", "").lower(),
                "conviction": int(pick.get("conviction", 5)),
                "contract": pick.get("contract", ""),
                "strike": float(pick.get("strike", 0) or 0),
                "expiration": pick.get("expiration", ""),
                "option_type": pick.get("option_type", ""),
                "delta_target": float(pick.get("delta_target", 0) or 0),
                "contract_reasoning": pick.get("contract_reasoning", ""),
                "reasoning": pick.get("reasoning", ""),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })

    if pick_rows:
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_PICKS_TABLE}"
        _ensure_bq_table(table_id, [
            bigquery.SchemaField("debate_id", "STRING"),
            bigquery.SchemaField("scan_date", "DATE"),
            bigquery.SchemaField("agent_id", "STRING"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("conviction", "INTEGER"),
            bigquery.SchemaField("contract", "STRING"),
            bigquery.SchemaField("strike", "FLOAT"),
            bigquery.SchemaField("expiration", "STRING"),
            bigquery.SchemaField("option_type", "STRING"),
            bigquery.SchemaField("delta_target", "FLOAT"),
            bigquery.SchemaField("contract_reasoning", "STRING"),
            bigquery.SchemaField("reasoning", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ])
        errors = bq_client.insert_rows_json(table_id, pick_rows)
        if errors:
            logger.error(f"BQ pick insert errors: {errors}")
        else:
            logger.info(f"BQ: wrote {len(pick_rows)} agent pick rows")

    # --- BQ: debate rounds (all 4 rounds, every action) ---
    round_rows = []
    round_map = {
        "round1_picks": 1,
        "round2_attacks": 2,
        "round3_defenses": 3,
        "round4_final": 4,
    }
    for round_key, round_num in round_map.items():
        round_data = debate.get("rounds", {}).get(round_key, {})
        for agent_id, items in round_data.items():
            if not isinstance(items, list):
                continue
            for item in items:
                round_rows.append({
                    "debate_id": debate_id,
                    "scan_date": scan_date,
                    "round_num": round_num,
                    "round_name": round_key,
                    "agent_id": agent_id,
                    "ticker": item.get("ticker", item.get("target_ticker", "")),
                    "direction": item.get("direction", item.get("action", "")),
                    "conviction": int(item.get("conviction", item.get("new_conviction", item.get("original_conviction", 0))) or 0),
                    "contract": item.get("contract", ""),
                    "flow_intent": item.get("flow_intent", ""),
                    "target_agent": item.get("target_agent", ""),
                    "reasoning": item.get("reasoning", item.get("argument", item.get("defense", ""))),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })

    if round_rows:
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_ROUNDS_TABLE}"
        _ensure_bq_table(table_id, [
            bigquery.SchemaField("debate_id", "STRING"),
            bigquery.SchemaField("scan_date", "DATE"),
            bigquery.SchemaField("round_num", "INTEGER"),
            bigquery.SchemaField("round_name", "STRING"),
            bigquery.SchemaField("agent_id", "STRING"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("direction", "STRING"),
            bigquery.SchemaField("conviction", "INTEGER"),
            bigquery.SchemaField("contract", "STRING"),
            bigquery.SchemaField("flow_intent", "STRING"),
            bigquery.SchemaField("target_agent", "STRING"),
            bigquery.SchemaField("reasoning", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ])
        errors = bq_client.insert_rows_json(table_id, round_rows)
        if errors:
            logger.error(f"BQ rounds insert errors: {errors}")
        else:
            logger.info(f"BQ: wrote {len(round_rows)} round rows")


def _ensure_bq_table(table_id: str, schema: list):
    """Create BQ table if it doesn't exist."""
    try:
        bq_client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        logger.info(f"Created BQ table: {table_id}")


# ============================================================
# FastAPI Endpoints
# ============================================================

@app.post("/")
async def run_arena(request: Request):
    """HTTP trigger for Cloud Scheduler or manual invocation."""
    data = await request.json() if request.headers.get("content-type") == "application/json" else {}
    scan_date = data.get("scan_date")
    dry_run = data.get("dry_run", False)

    try:
        signals = await asyncio.to_thread(load_signals, scan_date)
        if not signals:
            return JSONResponse({"status": "skipped", "reason": "No signals found"})

        debate = await run_debate(signals, dry_run=dry_run)

        return JSONResponse({
            "status": "ok",
            "debate_id": debate["debate_id"],
            "scan_date": debate["scan_date"],
            "signal_count": debate["signal_count"],
            "consensus_count": len(debate.get("consensus", [])),
            "consensus": debate.get("consensus", []),
        })

    except Exception as e:
        logger.exception("Arena debate failed")
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "agent-arena"}


# ============================================================
# CLI
# ============================================================

def main_cli():
    parser = argparse.ArgumentParser(description="Agent Arena — CLI Runner")
    parser.add_argument("--date", help="Scan date (YYYY-MM-DD). Default: latest.", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Skip BQ/Firestore writes")
    parser.add_argument("--json", action="store_true", help="Output full JSON")
    args = parser.parse_args()

    signals = load_signals(args.date)
    if not signals:
        print("No signals found. Exiting.")
        sys.exit(0)

    print(f"\n🏟️  Agent Arena — {len(signals)} signals for {signals[0]['scan_date']}")
    print(f"   Agents: {', '.join(a['name'] + ' ' + a['emoji'] for a in AGENTS)}")
    print(f"   Dry run: {args.dry_run}\n")

    debate = asyncio.run(run_debate(signals, dry_run=args.dry_run))

    if args.json:
        print(json.dumps(debate, indent=2, default=str))
    else:
        consensus = debate.get("consensus", [])
        if consensus:
            print(f"\n{'='*60}")
            print(f"🏆 CONSENSUS PICKS ({len(consensus)} tickers)")
            print(f"{'='*60}")
            for c in consensus:
                emoji = "🟢" if c["direction"] == "bull" else "🔴"
                print(f"\n{emoji} {c['ticker']} — {c['direction'].upper()}")
                print(f"   Level: {c['consensus_level']} ({c['vote_count']}/{c['total_agents']})")
                print(f"   Avg Conviction: {c['avg_conviction']}/10")
                for v in c["votes"]:
                    agent_cfg = next((a for a in AGENTS if a["id"] == v["agent"]), None)
                    name = agent_cfg["name"] if agent_cfg else v["agent"]
                    emoji_a = agent_cfg["emoji"] if agent_cfg else ""
                    print(f"   {emoji_a} {name}: conviction {v['conviction']}/10 — {v.get('contract', '')}")
        else:
            print("\n⚠️  No consensus reached.")

        print(f"\n{'='*60}")
        print("📋 ALL FINAL PICKS (Round 4)")
        print(f"{'='*60}")
        for agent_id, picks in debate["rounds"].get("round4_final", {}).items():
            agent_cfg = next((a for a in AGENTS if a["id"] == agent_id), None)
            name = agent_cfg["name"] if agent_cfg else agent_id
            emoji_a = agent_cfg["emoji"] if agent_cfg else ""
            print(f"\n{emoji_a} {name}:")
            for p in picks:
                dir_emoji = "🟢" if p.get("direction", "").lower() == "bull" else "🔴"
                print(f"  {dir_emoji} {p.get('ticker')} | {p.get('direction')} | "
                      f"Conviction: {p.get('conviction')}/10 | {p.get('contract', '')}")
                print(f"     {p.get('reasoning', '')[:200]}")

    print(f"\n⏱️  Started: {debate['started_at']}")
    print(f"⏱️  Completed: {debate['completed_at']}")


if __name__ == "__main__":
    main_cli()
