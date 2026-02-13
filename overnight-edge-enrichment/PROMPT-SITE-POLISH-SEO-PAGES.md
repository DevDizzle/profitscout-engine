# PROMPT: Site Polish — About Page, Missing Pages, and SEO Fixes

## Context
GammaRips has pivoted from an AI stock analysis tool ("Rips") to **The Overnight Edge** — an institutional options flow scanner. The webapp has been rebuilt with real data from Firestore, but several pages are outdated or missing. The current About page still references the old product (SEC filings, "Rips", "Confluence Dashboard"). It needs a complete rewrite.

**Critical SEO issue:** All pages currently return only the footer disclaimer when fetched by crawlers/bots. This means Google sees ZERO content. The landing page, reports, and signals pages MUST be server-rendered (not client-only). Verify that key content pages use Next.js Server Components (not `"use client"` at the top). The page content must be in the initial HTML response, not hydrated client-side.

## Changes Required

### 1. REWRITE: About Page — `src/app/about/page.tsx`

Delete everything in the current file. Replace with a completely new page:

**Structure:**

```
┌─────────────────────────────────────────────────┐
│ ABOUT THE OVERNIGHT EDGE                        │
│                                                 │
│ The Overnight Edge is an institutional options   │
│ flow intelligence platform. Every night, we     │
│ scan options activity across 5,230+ tickers to  │
│ surface what smart money did while you slept.    │
│                                                 │
│ ─── HOW IT WORKS ───                            │
│                                                 │
│ 1. SCAN (4:00 AM EST)                           │
│    Our scanner analyzes overnight options flow   │
│    across the entire market — volume, open       │
│    interest, unusual activity, dollar flow.      │
│                                                 │
│ 2. SCORE (4:25 AM EST)                          │
│    Each signal is scored 1-10 based on           │
│    institutional conviction: positioning size,   │
│    strike breadth, vol/OI ratio, directional     │
│    flow imbalance.                               │
│                                                 │
│ 3. ENRICH (4:30 AM EST)                         │
│    Top signals (score 6+) get AI-powered         │
│    analysis: news context, technical levels,     │
│    trade thesis, recommended contracts.           │
│                                                 │
│ 4. DELIVER (Before Market Open)                 │
│    Signals land on gammarips.com and via alerts  │
│    — before the opening bell.                    │
│                                                 │
│ ─── MEET THE TEAM ───                           │
│                                                 │
│ [Photo/Avatar] Evan Parra — Founder & Chairman  │
│ ML engineer and data architect. Built the        │
│ scanner pipeline, enrichment engine, and data    │
│ infrastructure that powers The Overnight Edge.   │
│ Background in machine learning, data             │
│ engineering, and quantitative analysis.          │
│                                                 │
│ [Avatar] GammaMolt — CEO & Chief Analyst        │
│ AI-powered trading analyst and the operational   │
│ brain behind GammaRips. GammaMolt runs the       │
│ daily signal generation, market analysis, X      │
│ engagement, and content engine. Built on          │
│ Claude (Anthropic) via OpenClaw, GammaMolt is    │
│ not a chatbot — it's an autonomous operator      │
│ with skin in the game. Every signal call is      │
│ timestamped and tracked. No hiding from the      │
│ results.                                         │
│                                                 │
│ "I don't talk about trading. I trade. Results    │
│  over rhetoric." — GammaMolt                     │
│                                                 │
│ ─── WHAT MAKES US DIFFERENT ───                 │
│                                                 │
│ • We scan 5,230+ tickers overnight (not just    │
│   the popular 50)                                │
│ • Every signal is timestamped and publicly       │
│   tracked — no cherry-picking                    │
│ • AI analysis on every enriched signal — not     │
│   just raw data dumps                            │
│ • An AI CEO that operates 24/7 with full         │
│   accountability                                 │
│ • Free daily previews — we prove value before    │
│   asking for payment                             │
│                                                 │
│ ─── PRICING ───                                 │
│                                                 │
│ FREE: Daily signal previews, top movers,         │
│ market themes, public reports                    │
│                                                 │
│ THE OVERNIGHT EDGE ($49/mo): Full AI thesis,     │
│ recommended contracts, key levels,               │
│ support/resistance, alerts                       │
│                                                 │
│ THE WAR ROOM ($149/mo): Everything in Edge +     │
│ real-time flow alerts, direct analyst access,    │
│ priority signals                                 │
│                                                 │
│ [Subscribe Now →]                                │
│                                                 │
│ ─── FAQ ───                                     │
│ (Update FAQs for Overnight Edge product)         │
│                                                 │
│ ─── CONTACT ───                                 │
│ support@gammarips.com                            │
│ @GammaRips on X                                 │
└─────────────────────────────────────────────────┘
```

**SEO metadata:**
```typescript
export const metadata: Metadata = {
  title: 'About The Overnight Edge | GammaRips',
  description: 'Learn how The Overnight Edge scans institutional options flow across 5,230+ tickers every night. Meet the team — a founder-engineer and an AI CEO tracking every signal.',
  alternates: { canonical: '/about' },
};
```

**JSON-LD structured data:**
```json
{
  "@context": "https://schema.org",
  "@type": "Organization",
  "name": "GammaRips",
  "alternateName": "The Overnight Edge",
  "url": "https://gammarips.com",
  "email": "support@gammarips.com",
  "description": "Institutional options flow intelligence platform",
  "founder": {
    "@type": "Person",
    "name": "Evan Parra",
    "jobTitle": "Founder & Chairman"
  },
  "sameAs": [
    "https://twitter.com/GammaRips"
  ]
}
```

### 2. NEW: Pricing Page — `src/app/pricing/page.tsx`

Dedicated pricing page with three tiers:

| | Free | The Overnight Edge ($49/mo) | The War Room ($149/mo) |
|---|---|---|---|
| Daily signal previews | ✅ | ✅ | ✅ |
| Top movers + themes | ✅ | ✅ | ✅ |
| Public reports archive | ✅ | ✅ | ✅ |
| AI trade thesis | ❌ | ✅ | ✅ |
| Recommended contracts | ❌ | ✅ | ✅ |
| Key levels (S/R) | ❌ | ✅ | ✅ |
| Technical analysis | ❌ | ✅ | ✅ |
| News deep-dive | ❌ | ✅ | ✅ |
| WhatsApp alerts | ❌ | ❌ | ✅ |
| Priority signals | ❌ | ❌ | ✅ |
| Direct analyst access | ❌ | ❌ | ✅ |

Each paid tier gets a "Subscribe" button linking to Stripe checkout.

**SEO metadata:**
```
Title: "Pricing | The Overnight Edge by GammaRips"
Description: "See what institutions did last night. Free daily previews. Full analysis from $49/mo."
```

### 3. NEW: How It Works Page — `src/app/how-it-works/page.tsx`

An educational, SEO-rich page explaining the scanner methodology:

**Sections:**
1. **What is Unusual Options Activity?** — Explain UOA, vol/OI ratio, dollar flow
2. **Our Scoring System** — How signals get scored 1-10 (positioning size, strike breadth, directional imbalance, vol/OI)
3. **The Enrichment Layer** — How AI adds news context, technicals, thesis
4. **Reading a Signal** — Walk through a sample signal (use FSLY as example: score 9, +76%, $12.4M positioning, 58 strikes, agentic AI thesis)
5. **Signal vs. Trade Recommendation** — Disclaimer: signals are institutional flow data, not financial advice

This page targets educational long-tail keywords: "what is unusual options activity", "how to read institutional flow", "options flow scoring system"

**SEO metadata:**
```
Title: "How The Overnight Edge Works | Institutional Options Flow Analysis"
Description: "Learn how our scanner analyzes overnight institutional options flow across 5,230+ tickers. Understand our scoring system, enrichment process, and what makes a high-conviction signal."
```

### 4. NEW: Scorecard Page — `src/app/scorecard/page.tsx`

Public performance tracking page (reads from Firestore `signal_performance` collection once win tracker is deployed). For now, create the page structure with placeholder messaging:

```
THE OVERNIGHT EDGE — SIGNAL SCORECARD

Every signal is timestamped. Every result is tracked.
No cherry-picking. No hindsight bias. Just data.

[Performance stats will populate as signals are tracked]

Win tracking begins February 2026. Check back for verified results.
```

Later this will show: overall win rate, win rate by score tier, recent wins, best calls.

**SEO metadata:**
```
Title: "Signal Scorecard | Verified Performance | GammaRips"
Description: "Every Overnight Edge signal is timestamped and tracked. See our verified win rate and performance history. No cherry-picking — just data."
```

### 5. UPDATE: FAQ Component — `src/components/landing/faq.tsx`

Replace all old FAQs with Overnight Edge questions:

```typescript
export const faqs = [
  {
    question: "What is The Overnight Edge?",
    answer: "The Overnight Edge is an institutional options flow scanner that analyzes activity across 5,230+ tickers every night. We surface what smart money did overnight — before the market opens."
  },
  {
    question: "How are signals scored?",
    answer: "Each signal is scored 1-10 based on institutional conviction indicators: new options positioning size, number of active strike prices, volume-to-open-interest ratio, and directional flow imbalance (call vs put dollar flow)."
  },
  {
    question: "What do I get with a free account?",
    answer: "Free users see daily signal previews including ticker, score, direction, percent move, and positioning size. You also get access to daily market themes and the public reports archive."
  },
  {
    question: "What does the $49/mo subscription include?",
    answer: "The Overnight Edge subscription unlocks the full AI-generated trade thesis for each signal, recommended contracts with specific strikes and expiries, key support/resistance levels, and detailed technical and news analysis."
  },
  {
    question: "What is The War Room ($149/mo)?",
    answer: "The War Room includes everything in The Overnight Edge plus real-time flow alerts via WhatsApp, priority access to the highest-conviction signals, and direct access to our analyst for questions."
  },
  {
    question: "When are signals delivered?",
    answer: "Our scanner runs at approximately 4:00 AM EST. Enriched signals are available on gammarips.com before the market opens at 9:30 AM EST."
  },
  {
    question: "Is this financial advice?",
    answer: "No. The Overnight Edge provides institutional flow data and AI-generated analysis for informational purposes only. All trading decisions are yours. We track our signals publicly so you can evaluate our accuracy, but past performance does not guarantee future results."
  },
  {
    question: "Can I cancel anytime?",
    answer: "Yes. All subscriptions are month-to-month with no contracts. Cancel anytime from your account page."
  },
  {
    question: "Who runs GammaRips?",
    answer: "GammaRips was founded by Evan Parra (ML engineer and data architect) and is operated by GammaMolt, an AI-powered trading analyst. Every signal is generated by our automated pipeline and tracked publicly."
  },
];
```

### 6. UPDATE: Navigation

Update the header/nav to include new pages:
```
Logo | Signals | Reports | How It Works | Pricing | About | [Sign In]
```

### 7. UPDATE: Sitemap — `src/app/sitemap.ts`

Add new pages:
```typescript
{ url: 'https://gammarips.com/about', changeFrequency: 'monthly', priority: 0.7 },
{ url: 'https://gammarips.com/pricing', changeFrequency: 'monthly', priority: 0.8 },
{ url: 'https://gammarips.com/how-it-works', changeFrequency: 'monthly', priority: 0.7 },
{ url: 'https://gammarips.com/scorecard', changeFrequency: 'daily', priority: 0.8 },
```

### 8. CRITICAL: Fix Server-Side Rendering

The current site returns only footer content to crawlers. This means Google indexes NOTHING.

**Check every key page and ensure the main content uses Server Components:**
- `src/app/page.tsx` — Landing page MUST be a Server Component (no `"use client"` directive)
- `src/app/reports/page.tsx` — Server Component
- `src/app/reports/[date]/page.tsx` — Server Component
- `src/app/signals/page.tsx` — Server Component
- `src/app/about/page.tsx` — Server Component

If any of these have `"use client"` at the top, the content won't be in the initial HTML and Google won't index it.

**Test:** After building, run:
```bash
curl -s https://gammarips.com | grep -i "overnight edge"
```
If this returns nothing, the content is client-rendered and invisible to Google.

For pages that need client interactivity (auth checks, subscription gating), use the pattern:
- Server Component renders all PUBLIC content (indexable)
- Client Component handles only the interactive/gated parts (subscription check, click handlers)

### 9. UPDATE: Old Pages — Remove or Redirect

These pages reference the dead product and should redirect:
- `/feedback` → redirect to `/about#contact`
- `/api` → redirect to `/developers`
- `/api/cron` → remove (internal tool, not public)
- `/[ticker]` (old ticker pages) → redirect to `/signals` with a catch-all

Add redirects in `next.config.ts`:
```typescript
async redirects() {
  return [
    { source: '/feedback', destination: '/about#contact', permanent: true },
    { source: '/api', destination: '/developers', permanent: true },
    { source: '/dashboard', destination: '/', permanent: true },
    { source: '/options/:path*', destination: '/', permanent: true },
  ];
}
```

### 10. UPDATE: Footer

Ensure footer links include:
- Product: Signals | Reports | Pricing
- Company: About | How It Works | Scorecard
- Legal: Privacy | Terms
- Connect: X (@GammaRips) | support@gammarips.com | Developers (MCP API)

## DO NOT
- Remove auth/subscription/Stripe logic
- Change Firebase project configuration  
- Reference old product features (Rips, Confluence Dashboard, SEC filings, etc.)
- Use `"use client"` on pages that should be server-rendered for SEO
- Remove the disclaimer footer

## Verification
1. `npm run build` passes
2. `curl -s https://gammarips.com | grep -i "overnight"` returns content (SSR check)
3. All new pages render correctly
4. Navigation includes all pages
5. Sitemap includes all new URLs
6. No references to old product remain on any public page
