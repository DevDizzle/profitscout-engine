"""
Agent Arena — Debate Engine
=============================
Orchestrates the 4-round adversarial debate between agents.
"""

import json
import logging
import asyncio
from datetime import datetime, timezone

from config import (
    AGENTS, MAX_SIGNALS, MIN_SIGNAL_SCORE, MAX_PICKS_PER_AGENT,
    CONSENSUS_THRESHOLD, SUPERMAJORITY,
    ROUND1_PICK_PROMPT, ROUND2_ATTACK_PROMPT,
    ROUND3_DEFEND_PROMPT, ROUND4_FINAL_PROMPT,
)
from agents import call_all_agents, parse_agent_response

logger = logging.getLogger(__name__)


def format_signals_for_prompt(signals: list) -> str:
    """Format enriched signals into a readable string for prompts."""
    lines = []
    for i, s in enumerate(signals, 1):
        lines.append(f"""
Signal {i}: {s.get('ticker', 'N/A')}
  Direction: {s.get('direction', 'N/A')}
  Overnight Score: {s.get('overnight_score', s.get('signal_score', 'N/A'))}/10
  Move: {s.get('move_pct', 0):.1f}%
  New Positioning: ${s.get('new_positioning_usd', 0):,.0f}
  Active Strikes: {s.get('active_strikes', 'N/A')}
  Vol/OI Ratio: {s.get('vol_oi_ratio', s.get('max_vol_oi', 'N/A'))}
  AI Thesis: {s.get('ai_thesis', s.get('thesis', 'N/A'))}
  Technical Levels: Support {s.get('key_levels', {}).get('support', 'N/A')} | Resistance {s.get('key_levels', {}).get('resistance', 'N/A')}
  News: {s.get('news_summary', s.get('catalyst', 'N/A'))}
""".strip())
    return "\n\n".join(lines)


def format_picks_for_prompt(picks_by_agent: dict) -> str:
    """Format all agents' picks into a readable string."""
    lines = []
    agent_lookup = {a["id"]: a for a in AGENTS}
    for agent_id, picks in picks_by_agent.items():
        agent = agent_lookup.get(agent_id, {})
        emoji = agent.get("emoji", "")
        name = agent.get("name", agent_id)
        lines.append(f"{emoji} {name}:")
        if not picks:
            lines.append("  No trades selected.")
        for p in picks:
            lines.append(
                f"  • {p.get('ticker', '?')} {p.get('direction', '?').upper()} "
                f"— conviction {p.get('conviction', '?')}/10"
                f" — {p.get('contract', 'no contract specified')}"
                f"\n    Reasoning: {p.get('reasoning', 'N/A')}"
            )
        lines.append("")
    return "\n".join(lines)


def format_attacks_for_agent(attacks_by_agent: dict, target_agent_id: str) -> str:
    """Get all attacks directed at a specific agent."""
    lines = []
    agent_lookup = {a["id"]: a for a in AGENTS}
    for attacker_id, attacks in attacks_by_agent.items():
        if attacker_id == target_agent_id:
            continue
        attacker = agent_lookup.get(attacker_id, {})
        for attack in attacks:
            if attack.get("target_agent") == target_agent_id and attack.get("action") == "attack":
                lines.append(
                    f"{attacker.get('emoji', '')} {attacker.get('name', attacker_id)} attacks "
                    f"your {attack.get('target_ticker', '?')} pick:\n"
                    f"  \"{attack.get('argument', 'N/A')}\""
                )
    return "\n\n".join(lines) if lines else "No attacks on your picks. All agents supported your positions."


def build_debate_summary(round1: dict, round2: dict, round3: dict) -> str:
    """Build a comprehensive debate summary for the final vote."""
    lines = ["=== ROUND 1: INITIAL PICKS ==="]
    lines.append(format_picks_for_prompt(round1))
    
    lines.append("\n=== ROUND 2: CROSS-EXAMINATION ===")
    agent_lookup = {a["id"]: a for a in AGENTS}
    for agent_id, attacks in round2.items():
        agent = agent_lookup.get(agent_id, {})
        for a in attacks:
            action_emoji = "⚔️" if a.get("action") == "attack" else "🤝"
            lines.append(
                f"{agent.get('emoji', '')} {agent.get('name', agent_id)} "
                f"{action_emoji} {a.get('target_agent', '?')}'s {a.get('target_ticker', '?')}: "
                f"{a.get('argument', '')}"
            )
    
    lines.append("\n=== ROUND 3: DEFENSE & REVISION ===")
    for agent_id, defenses in round3.items():
        agent = agent_lookup.get(agent_id, {})
        for d in defenses:
            action = d.get("action", "hold").upper()
            conv_change = ""
            if d.get("original_conviction") and d.get("new_conviction"):
                if d["new_conviction"] != d["original_conviction"]:
                    conv_change = f" (conviction {d['original_conviction']} → {d['new_conviction']})"
            lines.append(
                f"{agent.get('emoji', '')} {agent.get('name', agent_id)} "
                f"{action} {d.get('ticker', '?')}{conv_change}: "
                f"{d.get('defense', '')}"
            )
    
    return "\n".join(lines)


def compute_consensus(final_picks: dict) -> dict:
    """
    Compute consensus from final round picks.
    Returns structured consensus data.
    """
    # Tally votes by ticker+direction
    ticker_votes = {}  # {(ticker, direction): [list of agent picks]}
    
    for agent_id, picks in final_picks.items():
        for pick in picks:
            key = (pick.get("ticker", "").upper(), pick.get("direction", "").lower())
            if key[0]:  # valid ticker
                if key not in ticker_votes:
                    ticker_votes[key] = []
                ticker_votes[key].append({
                    "agent_id": agent_id,
                    "conviction": pick.get("conviction", 5),
                    "contract": pick.get("contract", ""),
                    "reasoning": pick.get("reasoning", ""),
                })
    
    total_agents = len(AGENTS)
    
    # Classify consensus levels
    unanimous = []
    supermajority = []
    majority = []
    split = []
    solo = []
    
    for (ticker, direction), votes in sorted(ticker_votes.items(), 
                                              key=lambda x: len(x[1]), reverse=True):
        ratio = len(votes) / total_agents
        avg_conviction = sum(v["conviction"] for v in votes) / len(votes)
        
        entry = {
            "ticker": ticker,
            "direction": direction,
            "agent_count": len(votes),
            "total_agents": total_agents,
            "ratio": ratio,
            "avg_conviction": round(avg_conviction, 1),
            "votes": votes,
        }
        
        if ratio >= 1.0:
            unanimous.append(entry)
        elif ratio >= SUPERMAJORITY:
            supermajority.append(entry)
        elif ratio >= CONSENSUS_THRESHOLD:
            majority.append(entry)
        elif len(votes) >= 2:
            split.append(entry)
        else:
            solo.append(entry)
    
    # The consensus trade = highest voted, then highest conviction
    consensus_trade = None
    for pool in [unanimous, supermajority, majority]:
        if pool:
            consensus_trade = pool[0]
            break
    
    return {
        "consensus_trade": consensus_trade,
        "unanimous": unanimous,
        "supermajority": supermajority,
        "majority": majority,
        "split": split,
        "solo": solo,
        "total_agents": total_agents,
        "total_unique_tickers": len(ticker_votes),
    }


async def run_debate(signals: list) -> dict:
    """
    Run the full 4-round adversarial debate.
    Returns complete debate record with all rounds and consensus.
    """
    started_at = datetime.now(timezone.utc)
    signals_text = format_signals_for_prompt(signals)
    
    logger.info(f"Starting Agent Arena debate with {len(signals)} signals and {len(AGENTS)} agents")
    
    # ── ROUND 1: Independent Picks ──────────────────────────
    logger.info("Round 1: Independent Picks")
    round1_raw = await call_all_agents(
        ROUND1_PICK_PROMPT,
        {"signals": signals_text, "max_picks": MAX_PICKS_PER_AGENT}
    )
    round1 = {aid: parse_agent_response(resp) for aid, resp in round1_raw.items()}
    logger.info(f"Round 1 complete. Picks: {sum(len(p) for p in round1.values())}")
    
    # ── ROUND 2: Cross-Examination ──────────────────────────
    logger.info("Round 2: Cross-Examination")
    round2_tasks = {}
    for agent in AGENTS:
        own = json.dumps(round1.get(agent["id"], []), indent=2)
        others = {aid: picks for aid, picks in round1.items() if aid != agent["id"]}
        others_text = format_picks_for_prompt(others)
        
        prompt = ROUND2_ATTACK_PROMPT.format(
            own_picks=own,
            other_picks=others_text,
            signals=signals_text,
        )
        round2_tasks[agent["id"]] = (agent, prompt)
    
    round2_results = await asyncio.gather(*[
        _call_single(agent, prompt) for agent, prompt in round2_tasks.values()
    ])
    round2_raw = {agent_id: resp for (agent_id, resp) in 
                  zip(round2_tasks.keys(), round2_results)}
    round2 = {aid: parse_agent_response(resp) for aid, resp in round2_raw.items()}
    logger.info(f"Round 2 complete. Attacks/supports: {sum(len(a) for a in round2.values())}")
    
    # ── ROUND 3: Defense & Revision ─────────────────────────
    logger.info("Round 3: Defense & Revision")
    round3_tasks = {}
    for agent in AGENTS:
        own = json.dumps(round1.get(agent["id"], []), indent=2)
        attacks = format_attacks_for_agent(round2, agent["id"])
        
        prompt = ROUND3_DEFEND_PROMPT.format(
            own_picks=own,
            attacks_received=attacks,
            signals=signals_text,
        )
        round3_tasks[agent["id"]] = (agent, prompt)
    
    round3_results = await asyncio.gather(*[
        _call_single(agent, prompt) for agent, prompt in round3_tasks.values()
    ])
    round3_raw = {agent_id: resp for (agent_id, resp) in 
                  zip(round3_tasks.keys(), round3_results)}
    round3 = {aid: parse_agent_response(resp) for aid, resp in round3_raw.items()}
    logger.info(f"Round 3 complete. Defenses: {sum(len(d) for d in round3.values())}")
    
    # ── ROUND 4: Final Vote ─────────────────────────────────
    logger.info("Round 4: Final Vote")
    debate_summary = build_debate_summary(round1, round2, round3)
    
    round4_raw = await call_all_agents(
        ROUND4_FINAL_PROMPT,
        {"debate_summary": debate_summary, "signals": signals_text}
    )
    round4 = {aid: parse_agent_response(resp) for aid, resp in round4_raw.items()}
    logger.info(f"Round 4 complete. Final picks: {sum(len(p) for p in round4.values())}")
    
    # ── Consensus ───────────────────────────────────────────
    consensus = compute_consensus(round4)
    
    finished_at = datetime.now(timezone.utc)
    duration_sec = (finished_at - started_at).total_seconds()
    logger.info(f"Debate complete in {duration_sec:.1f}s. "
                f"Consensus trade: {consensus['consensus_trade']['ticker'] if consensus['consensus_trade'] else 'NONE'}")
    
    return {
        "scan_date": signals[0].get("scan_date", datetime.now().strftime("%Y-%m-%d")) if signals else None,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration_sec, 1),
        "signals_count": len(signals),
        "agents": [{"id": a["id"], "name": a["name"], "emoji": a["emoji"], 
                     "model": a["model"], "origin": a["origin"]} for a in AGENTS],
        "rounds": {
            "round1_picks": round1,
            "round1_raw": round1_raw,
            "round2_attacks": round2,
            "round2_raw": round2_raw,
            "round3_defense": round3,
            "round3_raw": round3_raw,
            "round4_final": round4,
            "round4_raw": round4_raw,
        },
        "consensus": consensus,
    }


async def _call_single(agent: dict, prompt: str) -> str:
    """Call a single agent with a fully formed prompt."""
    from agents import call_agent
    return await call_agent(agent, prompt)
