# GammaRips Engine

Backend infrastructure for **The Overnight Edge** — an AI-powered overnight institutional options flow intelligence platform.

## Architecture

```
4:00 AM UTC    overnight-scanner     Scans full US options market for unusual institutional flow
     │
4:30 AM UTC    enrichment-trigger    Enriches top signals with news, technicals, AI thesis
     │
5:00 AM UTC    agent-arena           5 AI agents debate and produce 1 consensus trade
     │
9:30 PM UTC    win-tracker           Tracks signal performance over 3 trading days
```

## Services (Cloud Run)

| Service | Directory | Description |
|---------|-----------|-------------|
| `overnight-scanner` | `src/` + `server_scanner.py` | Polygon options flow scanner. Scores signals 1-10. |
| `enrichment-trigger` | `overnight-edge-enrichment/` | Gemini grounded news + Polygon technicals + AI thesis |
| `agent-arena` | `agent-arena/` | 5-model adversarial debate (Grok, Gemini, Claude, DeepSeek V3, GPT-5.2) |
| `win-tracker` | `overnight-edge-enrichment/win_tracker/` | 3-trading-day performance tracking + X auto-posting |

## Directory Structure

```
gammarips-engine/
├── agent-arena/              # Agent Arena service (FastAPI)
│   ├── main.py               # 4-round debate orchestration
│   ├── agents.py             # Multi-provider LLM client (5 providers)
│   ├── config.py             # Agent roles, prompts, thresholds
│   ├── Dockerfile
│   └── deploy.sh
├── overnight-edge-enrichment/ # Enrichment + Win Tracker service (Flask)
│   ├── main.py               # 6-step enrichment pipeline
│   ├── win_tracker/           # Performance tracking sub-service
│   ├── Dockerfile
│   ├── deploy.sh
│   └── PROMPT-*.md           # Webapp change prompts (run via Gemini)
├── src/                       # Scanner core (used by server_scanner.py)
│   └── enrichment/core/
│       ├── config.py          # Polygon API key, project config
│       ├── clients/
│       │   └── polygon_client.py  # Options chain, snapshots, prices
│       └── pipelines/
│           └── overnight_scanner.py  # Signal scoring engine
├── server_scanner.py          # Scanner Cloud Run entry point
├── Dockerfile.scanner         # Scanner container
├── _archive/                  # Legacy code (ingestion, serving, tests, workflows)
└── .env                       # API keys (Polygon, X/Twitter)
```

## Data Flow

**Input:** Polygon.io options market snapshots (full US market)
**Processing:** Score → Filter (≥6) → Enrich → Debate → Track
**Output:** BigQuery + Firestore + gammarips.com + WhatsApp War Room

### BigQuery Tables (`profitscout-fida8.profit_scout`)

| Table | Lifecycle | Description |
|-------|-----------|-------------|
| `overnight_signals` | Fresh daily (truncate + write) | Raw scanner output |
| `overnight_signals_enriched` | Fresh daily (truncate + write) | Enriched signals |
| `agent_arena_consensus` | Fresh daily | Arena consensus pick |
| `agent_arena_picks` | Fresh daily | Individual agent picks |
| `agent_arena_rounds` | Fresh daily | Full debate rounds |
| `signal_performance` | Append-only (historical) | 3-day performance tracking |

### Firestore Collections

| Collection | Description |
|------------|-------------|
| `overnight_signals` | Enriched signals for webapp |
| `overnight_summaries` | Daily scan summaries |
| `daily_reports` | Markdown reports |
| `arena_debates` | Arena debate results |
| `signal_performance` | Performance for webapp |

## External APIs

| API | Used By | Purpose |
|-----|---------|---------|
| Polygon.io | Scanner, Enrichment, Win Tracker | Options data, price bars |
| Google Gemini | Enrichment | Grounded news search + AI thesis |
| xAI (Grok) | Arena | Momentum agent |
| Google (Gemini) | Arena | Contrarian agent |
| Anthropic (Claude) | Arena | Risk manager agent |
| HuggingFace (DeepSeek V3) | Arena | Catalyst hunter agent |
| OpenAI (GPT-5.2) | Arena | Technical analyst agent |
| X/Twitter (Tweepy) | Win Tracker | Auto-post strong wins |

## Deployment

Each service has its own `Dockerfile` and `deploy.sh`. All deploy to GCP Cloud Run in `us-central1`.

```bash
# Deploy scanner
gcloud run deploy overnight-scanner --source . --dockerfile Dockerfile.scanner

# Deploy enrichment
cd overnight-edge-enrichment && ./deploy.sh

# Deploy arena
cd agent-arena && ./deploy.sh

# Deploy win tracker
cd overnight-edge-enrichment/win_tracker && ./deploy.sh
```

## Archive

Legacy code from the learning-project phase (ingestion pipelines, serving layer, tests, workflows, CI/CD) is preserved in `_archive/` for reference. None of it is used by the active Overnight Edge pipeline.

---

*GammaRips — The Overnight Edge*
