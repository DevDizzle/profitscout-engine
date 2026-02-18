# 🏟️ Agent Arena — The War Room

**7 AI agents. Same data. Adversarial debate. Consensus trade.**

## The Agents

| ID | Name | Model | Provider | Origin |
|----|------|-------|----------|--------|
| claude | Claude | claude-sonnet-4 | Anthropic | 🇺🇸 USA |
| gpt | GPT | gpt-4o | OpenAI | 🇺🇸 USA |
| grok | Grok | grok-3-mini | xAI | 🇺🇸 USA |
| gemini | Gemini | gemini-2.0-flash | Google | 🇺🇸 USA |
| deepseek | DeepSeek | deepseek-chat (V3) | DeepSeek | 🇨🇳 China |
| llama | Llama | llama-3.3-70b | Meta via Groq | 🇺🇸 USA |
| mistral | Mistral | mistral-large | Mistral AI | 🇫🇷 France |

## The Debate Protocol

```
Round 1: PICK      — Each agent independently picks top 3 trades
Round 2: ATTACK    — Agents challenge each other's picks  
Round 3: DEFEND    — Agents defend, revise, or drop picks
Round 4: VOTE      — Final picks with updated conviction
         CONSENSUS — Tally votes, classify agreement level
```

## Consensus Levels

| Level | Threshold | Meaning |
|-------|-----------|---------|
| Unanimous | 7/7 | All agents agree — strongest signal |
| Supermajority | 6/7 (80%+) | Strong consensus |
| Majority | 4-5/7 (60%+) | Consensus trade — this gets published |
| Split | 2-3/7 | Disagreement — caution flag |
| Solo | 1/7 | Contrarian pick — tracked separately |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /debate | Run the full 4-round debate |
| GET | /health | Check which agents are configured |
| GET | /latest | Get most recent debate from Firestore |

## API Keys Required

Set in GCP Secret Manager, mounted as env vars:

| Secret Name | Provider | Get Key At |
|-------------|----------|------------|
| ANTHROPIC_API_KEY | Anthropic (Claude) | https://console.anthropic.com/ |
| OPENAI_API_KEY | OpenAI (GPT) | https://platform.openai.com/ |
| XAI_API_KEY | xAI (Grok) | https://console.x.ai/ |
| GOOGLE_API_KEY | Google (Gemini) | https://aistudio.google.com/apikey |
| DEEPSEEK_API_KEY | DeepSeek | https://platform.deepseek.com/ |
| GROQ_API_KEY | Groq (Llama) | https://console.groq.com/ |
| MISTRAL_API_KEY | Mistral | https://console.mistral.ai/ |

## Deploy

```bash
cd agent-arena
./deploy.sh
```

## Pipeline Integration

```
4:00 AM  Scanner runs → overnight_signals
4:25 AM  Enrichment → overnight_signals_enriched  
4:30 AM  Agent Arena → /debate triggered by Cloud Scheduler
4:33 AM  Results in BQ + Firestore
6:00 AM  GammaMolt delivers highlights to War Room WhatsApp
6:00 AM  Daily report includes consensus trade
```

## Cost Estimate

~$1-2/day for all 7 agents × 4 rounds = 28 API calls.
DeepSeek, Groq, and Gemini Flash are near-free.
Claude and GPT-4o are the main cost drivers (~$0.15 each).

## Research Output

BQ tables for PhD analysis:
- `agent_arena_picks` — Every pick, every round, with performance tracking
- `agent_arena_consensus` — Daily consensus metrics
- `agent_arena_rounds` — Full raw transcripts

Questions this data answers:
1. Does multi-agent consensus outperform individual models?
2. Which architectures are best at which signal types?
3. Does adversarial debate improve prediction accuracy?
4. Is agent disagreement a volatility predictor?
