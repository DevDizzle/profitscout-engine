# PROMPT: Build Agent Arena Page

## Context
GammaRips has an "Agent Arena" — 3 AI agents (Grok/Momentum, Gemini/Contrarian, Claude/Risk Manager) that debate overnight options flow signals and converge on a single consensus trade. Debate data lives in Firestore `arena_debates` collection.

## Task
Create a `/arena` page that displays the latest Agent Arena debate and consensus trade.

## Firestore Schema: `arena_debates`
Each document has:
```typescript
{
  debate_id: string;          // e.g. "arena_2026-02-14_124934"
  scan_date: string;          // e.g. "2026-02-14"
  signal_count: number;       // signals fed to agents
  started_at: string;
  completed_at: string;
  agents: string[];           // ["grok", "gemini", "claude"]
  consensus: [{
    ticker: string;
    direction: "bull" | "bear";
    contract: string;         // e.g. "$185C March 21"
    consensus_level: "unanimous" | "majority" | "split_decision";
    vote_count: number;
    total_agents: number;
    avg_conviction: number;   // 1-10
    votes: [{
      agent: string;
      conviction: number;
      contract: string;
      reasoning: string;
    }];
    note?: string;            // editorial context
  }];
  rounds: {                   // keyed by round name
    round1_picks: [{agent_id, ticker, direction, conviction, reasoning, contract}];
    round2_attacks: [{agent_id, ticker, direction, reasoning}];
    round3_defenses: [{agent_id, ticker, direction, conviction, reasoning}];
    round4_final: [{agent_id, ticker, direction, conviction, reasoning, contract}];
  };
}
```

## Page Requirements

### Route: `/arena`
### Navigation: Add "Arena" link to main nav between "Signals" and "Reports"

### Layout (mobile-first):

#### 1. Hero Section
- Title: "Agent Arena"
- Subtitle: "3 AI agents. 1 consensus trade. Every night."
- Scan date badge showing which date's data

#### 2. Consensus Trade Card (TOP, most prominent)
- Large card with the consensus pick: ticker, direction (BULL/BEAR with color), contract
- Conviction meter (visual bar or gauge, avg_conviction out of 10)
- Consensus level badge: "Unanimous ✅", "Majority 🤝", "Split Decision ⚔️"
- The `note` field if present (italic, smaller text)

#### 3. Agent Breakdown (3 columns on desktop, stacked on mobile)
For each agent, show a card:
- **Grok** — Role: "Momentum Hunter" — Color: Red/Orange
- **Gemini** — Role: "Contrarian Analyst" — Color: Blue
- **Claude** — Role: "Risk Manager" — Color: Purple

Each card shows:
- Agent name + role
- Their final pick (ticker, direction, conviction)
- Their contract
- Their reasoning (truncated with "Read more" expand)

#### 4. Debate Timeline (expandable/collapsible)
Show the 4 rounds as a timeline:
- Round 1: Initial Picks — who picked what
- Round 2: Cross-Examination — who attacked/supported what
- Round 3: Defense — who held, who revised
- Round 4: Final Vote — final picks + convictions

Use icons/colors to show attacks (⚔️), defenses (🛡️), holds (✊), revisions (🔄)

#### 5. Historical Debates (below)
- List of previous debates by scan_date
- Click to expand/view past consensus picks
- Show win/loss status if signal_performance data exists

### Gating
- Consensus card: **visible to all** (free tier teaser)
- Agent reasoning + debate timeline: **War Room only** (gated behind subscription check)
- Use same auth/subscription pattern as other gated pages

### Styling
- Match existing GammaRips dark theme
- Use the brand green (#00FF88 or whatever the existing accent is)
- Card-based layout with subtle borders
- Conviction bars should be gradient (red → yellow → green)

### Data Fetching
- Server component: fetch latest `arena_debates` doc from Firestore (ordered by scan_date DESC, limit 1)
- For historical: fetch last 10 debates
- Handle empty state: "No debates yet. The Arena runs every market night at 11:30 PM EST."

### SEO
- Page title: "Agent Arena — AI Consensus Trade | GammaRips"
- Meta description: "Watch 3 AI agents debate overnight options flow and converge on one high-conviction trade. Updated every market night."

## Files to Create/Modify
1. `src/app/arena/page.tsx` — Main arena page (server component)
2. `src/app/arena/arena-client.tsx` — Client component for interactivity (expand/collapse, gating)
3. Update navigation in `src/components/layout/public-header.tsx` — Add "Arena" link
4. Update navigation in any mobile menu component

## Important
- Do NOT add any mock/hardcoded data. Read everything from Firestore `arena_debates` collection.
- The page should work with the existing data structure shown above.
- Keep the consensus card ungated — it's the hook that converts free users to War Room subscribers.
