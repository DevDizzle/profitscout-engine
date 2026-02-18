"""
Agent Arena — Storage Layer
==============================
Writes debate results to BigQuery and Firestore.
"""

import json
import logging
from datetime import datetime, timezone

from google.cloud import bigquery, firestore

from config import (
    GCP_PROJECT, BQ_DATASET, 
    BQ_ROUNDS_TABLE, BQ_PICKS_TABLE, BQ_CONSENSUS_TABLE,
    FIRESTORE_COLLECTION,
)

logger = logging.getLogger(__name__)


def save_to_bigquery(debate_result: dict):
    """
    Save debate results to BigQuery tables for research analysis.
    
    Tables:
    - agent_arena_picks: Individual agent picks across all rounds
    - agent_arena_consensus: Daily consensus results
    """
    client = bigquery.Client(project=GCP_PROJECT)
    scan_date = debate_result["scan_date"]
    
    # ── Save individual picks (Round 1 + Round 4) ───────────
    picks_rows = []
    
    # Round 1 picks
    for agent_id, picks in debate_result["rounds"]["round1_picks"].items():
        for pick in picks:
            picks_rows.append({
                "scan_date": scan_date,
                "round": 1,
                "agent_id": agent_id,
                "ticker": pick.get("ticker", "").upper(),
                "direction": pick.get("direction", ""),
                "conviction": pick.get("conviction", 0),
                "contract": pick.get("contract", ""),
                "reasoning": pick.get("reasoning", ""),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
    
    # Round 4 final picks
    for agent_id, picks in debate_result["rounds"]["round4_final"].items():
        for pick in picks:
            picks_rows.append({
                "scan_date": scan_date,
                "round": 4,
                "agent_id": agent_id,
                "ticker": pick.get("ticker", "").upper(),
                "direction": pick.get("direction", ""),
                "conviction": pick.get("conviction", 0),
                "contract": pick.get("contract", ""),
                "reasoning": pick.get("reasoning", ""),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
    
    if picks_rows:
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_PICKS_TABLE}"
        # Delete existing rows for this scan_date to avoid dupes
        delete_query = f"DELETE FROM `{table_id}` WHERE scan_date = '{scan_date}'"
        try:
            client.query(delete_query).result()
        except Exception:
            pass  # Table might not exist yet
        
        errors = client.insert_rows_json(table_id, picks_rows)
        if errors:
            logger.error(f"BQ insert errors (picks): {errors}")
        else:
            logger.info(f"Saved {len(picks_rows)} picks to BQ")
    
    # ── Save consensus ──────────────────────────────────────
    consensus = debate_result["consensus"]
    ct = consensus.get("consensus_trade")
    
    consensus_row = {
        "scan_date": scan_date,
        "has_consensus": ct is not None,
        "consensus_ticker": ct["ticker"] if ct else None,
        "consensus_direction": ct["direction"] if ct else None,
        "consensus_agent_count": ct["agent_count"] if ct else 0,
        "consensus_avg_conviction": ct["avg_conviction"] if ct else 0,
        "total_agents": consensus["total_agents"],
        "total_unique_tickers": consensus["total_unique_tickers"],
        "unanimous_count": len(consensus["unanimous"]),
        "supermajority_count": len(consensus["supermajority"]),
        "majority_count": len(consensus["majority"]),
        "split_count": len(consensus["split"]),
        "solo_count": len(consensus["solo"]),
        "duration_seconds": debate_result["duration_seconds"],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_CONSENSUS_TABLE}"
    delete_query = f"DELETE FROM `{table_id}` WHERE scan_date = '{scan_date}'"
    try:
        client.query(delete_query).result()
    except Exception:
        pass
    
    errors = client.insert_rows_json(table_id, [consensus_row])
    if errors:
        logger.error(f"BQ insert errors (consensus): {errors}")
    else:
        logger.info(f"Saved consensus to BQ")


def save_to_firestore(debate_result: dict):
    """
    Save debate results to Firestore for webapp consumption.
    Document ID = scan_date.
    """
    db = firestore.Client(project=GCP_PROJECT)
    scan_date = debate_result["scan_date"]
    
    consensus = debate_result["consensus"]
    ct = consensus.get("consensus_trade")
    
    # Build the debate transcript for the webapp
    # (Full rounds stored for /arena page)
    doc_data = {
        "scan_date": scan_date,
        "status": "complete",
        "started_at": debate_result["started_at"],
        "finished_at": debate_result["finished_at"],
        "duration_seconds": debate_result["duration_seconds"],
        "signals_count": debate_result["signals_count"],
        "agents": debate_result["agents"],
        
        # Consensus summary
        "has_consensus": ct is not None,
        "consensus_trade": {
            "ticker": ct["ticker"],
            "direction": ct["direction"],
            "agent_count": ct["agent_count"],
            "total_agents": ct["total_agents"],
            "avg_conviction": ct["avg_conviction"],
            "votes": ct["votes"],
        } if ct else None,
        
        # Full categorized results
        "unanimous": consensus["unanimous"],
        "supermajority": consensus["supermajority"],
        "majority": consensus["majority"],
        "split": consensus["split"],
        "solo": consensus["solo"],
        
        # Round data (parsed JSON, not raw text)
        "round1_picks": debate_result["rounds"]["round1_picks"],
        "round2_attacks": debate_result["rounds"]["round2_attacks"],
        "round3_defense": debate_result["rounds"]["round3_defense"],
        "round4_final": debate_result["rounds"]["round4_final"],
        
        # Timestamps
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    
    db.collection(FIRESTORE_COLLECTION).document(scan_date).set(doc_data)
    logger.info(f"Saved debate to Firestore: {FIRESTORE_COLLECTION}/{scan_date}")


def format_whatsapp_messages(debate_result: dict) -> list:
    """
    Format the debate results into 4 WhatsApp messages for War Room delivery.
    Returns list of message strings.
    """
    consensus = debate_result["consensus"]
    ct = consensus.get("consensus_trade")
    agents = debate_result["agents"]
    agent_names = " · ".join(a["name"] for a in agents)
    scan_date = debate_result["scan_date"]
    
    messages = []
    
    # ── Message 1: The Setup ────────────────────────────────
    msg1 = (
        f"🏟️ *AGENT ARENA* — {scan_date}\n\n"
        f"{len(agents)} agents. Same data. Same question.\n"
        f"\"What's the best trade from last night's institutional flow?\"\n\n"
        f"*Agents:* {agent_names}\n"
        f"*Signals analyzed:* {debate_result['signals_count']} (score 6+)\n"
        f"*Debate duration:* {debate_result['duration_seconds']:.0f}s\n\n"
        f"Results incoming..."
    )
    messages.append(msg1)
    
    # ── Message 2: The Fight (key attacks from Round 2) ─────
    r2 = debate_result["rounds"]["round2_attacks"]
    agent_lookup = {a["id"]: a for a in agents}
    
    attack_lines = []
    for agent_id, attacks in r2.items():
        agent = agent_lookup.get(agent_id, {})
        for attack in attacks:
            if attack.get("action") == "attack":
                attack_lines.append(
                    f"{agent.get('emoji', '')} *{agent.get('name', agent_id)}* attacks "
                    f"{attack.get('target_agent', '?')}'s {attack.get('target_ticker', '?')}:\n"
                    f"_{attack.get('argument', '')[:200]}_"
                )
    
    # Take top 3-4 most interesting attacks
    fight_text = "\n\n".join(attack_lines[:4]) if attack_lines else "All agents agreed — no major attacks."
    
    msg2 = f"🥊 *ROUND 2: Cross-Examination*\n\n{fight_text}"
    messages.append(msg2)
    
    # ── Message 3: The Verdict ──────────────────────────────
    if ct:
        votes_text = ", ".join(
            f"{agent_lookup.get(v['agent_id'], {}).get('emoji', '')} {agent_lookup.get(v['agent_id'], {}).get('name', v['agent_id'])}"
            for v in ct["votes"]
        )
        msg3 = (
            f"🏆 *CONSENSUS TRADE*\n\n"
            f"*{ct['ticker']}* — {ct['direction'].upper()}\n"
            f"{ct['agent_count']}/{ct['total_agents']} agents agree\n"
            f"Avg conviction: {ct['avg_conviction']}/10\n\n"
            f"*Agents in favor:* {votes_text}\n\n"
            f"Full debate transcript → gammarips.com/arena"
        )
    else:
        msg3 = (
            f"⚖️ *NO CONSENSUS TODAY*\n\n"
            f"Agents couldn't agree on a single trade. "
            f"Split decisions across {consensus['total_unique_tickers']} tickers.\n\n"
            f"No consensus = no trade. That's discipline.\n\n"
            f"Full debate transcript → gammarips.com/arena"
        )
    messages.append(msg3)
    
    # ── Message 4: Other Picks ──────────────────────────────
    other_lines = []
    for pool, label in [
        (consensus["supermajority"], "Strong"),
        (consensus["majority"], "Majority"),
        (consensus["split"], "Split"),
    ]:
        for entry in pool:
            if ct and entry["ticker"] == ct["ticker"]:
                continue  # Skip the consensus trade
            emoji = "🟢" if entry["direction"] == "bull" else "🔴"
            other_lines.append(
                f"*{label}* ({entry['agent_count']}/{entry['total_agents']}): "
                f"{entry['ticker']} {emoji} — conv. {entry['avg_conviction']}"
            )
    
    if other_lines:
        msg4 = f"📊 *OTHER PICKS*\n\n" + "\n".join(other_lines[:6])
    else:
        msg4 = f"📊 *OTHER PICKS*\n\nNo other trades reached even 2-agent agreement today."
    
    messages.append(msg4)
    
    return messages
