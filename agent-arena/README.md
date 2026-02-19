# 🏟️ Agent Arena — Multi-Agent Adversarial Consensus System

**5 AI agents. Same data. Adversarial debate. One consensus trade.**

Agent Arena is a production multi-agent orchestration system where 5 LLMs with distinct analytical roles debate overnight institutional options flow data through structured rounds to produce a single high-conviction consensus trade.

## The Agents

| ID | Name | Model | Role | Lens |
|----|------|-------|------|------|
| grok | Grok | `grok-4-1-fast-reasoning` | 🔴 Momentum Trader | Trend following, volume confirmation |
| gemini | Gemini | `gemini-3-flash-preview` (thinking=high) | 🟡 Contrarian | Mean-reversion, oversold/overbought extremes |
| claude | Claude | `claude-sonnet-4` | 🟣 Risk Manager | Flow intent classification, risk/reward |
| deepseek | DeepSeek | `DeepSeek-V3` | 🔵 Catalyst Hunter | Event-driven, catalyst timing |
| gpt | GPT-5.2 | `gpt-5.2-2025-12-11` | 🟢 Technical Analyst | Price structure, support/resistance |

### Why Roles Matter

Without roles, agents converge on the same analysis (groupthink). With assigned roles, each agent applies a different analytical lens to the same data — creating genuine adversarial tension that produces better consensus.

## The Debate Protocol

```
Round 1: PICK      → Each agent independently picks their single best trade
Round 2: ATTACK    → Agents cross-examine each other's picks using their role's expertise
Round 3: DEFEND    → Agents hold, revise conviction, or drop picks based on attacks
Round 4: VOTE      → Final vote — one pick per agent, informed by full debate
         CONSENSUS → Tally votes, output single consensus trade
```

### Flow Intent Framework

Every pick requires flow intent classification before trading:

| Intent | Meaning | Tradeable? |
|--------|---------|------------|
| **Directional** | New positions betting on a move, usually pre-catalyst | ✅ Yes |
| **Hedging** | Protection on existing positions, usually post-move | ❌ No |
| **Mechanical** | Market maker delta-hedging, gamma exposure | ❌ No |
| **Rolling** | Closing + opening new positions | ⚠️ Mixed |

## Consensus Levels

| Level | Threshold | Action |
|-------|-----------|--------|
| Unanimous | 5/5 | Strongest signal — published with full conviction |
| Supermajority | 4/5 (80%+) | Strong consensus — published |
| Majority | 3/5 (60%+) | Consensus trade — published |
| Split | 2/5 | No consensus — highest individual conviction pick surfaces |
| No Trade | 0-1/5 | Agents agree nothing is worth trading today |

## Architecture

```
FastAPI (async) → Cloud Run (auto-scaling)
     │
     ├── 5 LLM providers (xAI, Google, Anthropic, HuggingFace, OpenAI)
     │
     ├── BigQuery (picks, rounds, consensus tables)
     │
     ├── Firestore (arena_debates collection)
     │
     └── GCP Secret Manager (API keys)
```

## Stack

- **Runtime:** Python 3.11 + FastAPI + uvicorn
- **Deployment:** GCP Cloud Run (containerized)
- **Storage:** BigQuery (structured) + Firestore (documents)
- **Secrets:** GCP Secret Manager
- **CI/CD:** Cloud Build
- **LLM SDKs:** Anthropic, Google GenAI, OpenAI-compatible (xAI, DeepSeek, GPT)

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/` | Run full 4-round debate (optional `{"scan_date": "YYYY-MM-DD"}`) |
| `GET` | `/health` | Health check + configured agent status |
| `GET` | `/latest` | Most recent debate from Firestore |

## Pipeline Integration

```
4:00 UTC  │ Scanner → overnight_signals (BigQuery)
4:30 UTC  │ Enrichment → overnight_signals_enriched (BigQuery + Firestore)
5:00 UTC  │ Agent Arena → /debate triggered by Cloud Scheduler
           │   → agent_arena_picks, agent_arena_consensus, agent_arena_rounds (BigQuery)
           │   → arena_debates (Firestore)
6:00 AM EST │ Daily report includes consensus trade
           │ War Room WhatsApp receives debate highlights
```

## API Keys

Stored in GCP Secret Manager, mounted as environment variables:

| Secret | Provider |
|--------|----------|
| `XAI_API_KEY` | xAI (Grok) |
| `ARENA_GOOGLE_API_KEY` | Google (Gemini) |
| `ANTHROPIC_API_KEY` | Anthropic (Claude) |
| `HF_TOKEN` | HuggingFace (DeepSeek V3) |
| `OPENAI_API_KEY` | OpenAI (GPT-5.2) |

## Deploy

```bash
./deploy.sh
```

Builds container, pushes to Artifact Registry, deploys to Cloud Run with secrets.

## Key Design Decisions

- **1 pick per agent, 1 consensus output** — One high-conviction trade beats three diluted ones
- **Flow intent is mandatory** — Agents must classify flow as directional/hedging before picking
- **Mean-reversion risk flags** — Signals with >10% price moves get ⚠️ flags requiring extra justification
- **Split-decision fallback** — When no consensus, the highest individual conviction pick surfaces
- **Structured JSON throughout** — Every round uses strict JSON schemas for reliable parsing

## Cost

~$0.50-1.00/day for 5 agents × 4 rounds = 20 API calls.
DeepSeek V3 and Gemini Flash are near-free. Claude and GPT-5.2 are the main cost drivers.

## Research Applications

BQ tables support analysis of:
- Does multi-agent consensus outperform individual models?
- Which roles catch which failure modes?
- Does adversarial debate improve prediction accuracy?
- Is agent disagreement a volatility predictor?
