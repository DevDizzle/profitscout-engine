# PROMPT: Fix Signals Pages + Field Mapping + Data Freshness

## Context
The signals pages at `/signals` and `/signals/[ticker]` are broken because the Firestore field names written by our enrichment pipeline don't match what the webapp components expect. Additionally, we need the signals page to only show 1 day of data (the latest scan), and clean up how data flows.

## Problem 1: Field Name Mismatches

The enrichment pipeline (`overnight-edge-enrichment/main.py`) writes these fields to the `overnight_signals` Firestore collection:

```
thesis              → webapp expects: ai_thesis
overnight_score     → webapp expects: signal_score
price_change_pct    → webapp expects: move_pct
support             → webapp expects: key_levels.support (number[])
resistance          → webapp expects: key_levels.resistance (number[])
risk_reward_ratio   → webapp expects: risk_reward
call_dollar_volume  → webapp has no mapping (used for positioning calc)
put_dollar_volume   → webapp has no mapping (used for positioning calc)
```

The webapp interface `OvernightSignal` in `src/lib/firebase-admin.ts` doesn't match reality.

### Fix: Update `OvernightSignal` interface and mapping

In `src/lib/firebase-admin.ts`, update the `OvernightSignal` interface to match actual Firestore fields:

```typescript
export interface OvernightSignal {
  id: string;
  ticker: string;
  scan_date: string;
  direction: string; // "BULLISH" | "BEARISH" (stored uppercase in Firestore)
  // Scores
  overnight_score: number;
  enrichment_quality_score?: number;
  contract_score?: number;
  // Price & Flow
  underlying_price?: number;
  price_change_pct?: number;
  call_dollar_volume?: number;
  put_dollar_volume?: number;
  call_uoa_depth?: number;
  put_uoa_depth?: number;
  call_active_strikes?: number;
  put_active_strikes?: number;
  // Contract
  recommended_contract?: string;
  recommended_strike?: number;
  recommended_expiration?: string;
  recommended_mid_price?: number;
  recommended_delta?: number;
  // Enrichment
  thesis?: string;
  news_summary?: string;
  catalyst_score?: number;
  catalyst_type?: string;
  key_headline?: string;
  flow_intent?: string;
  flow_intent_reasoning?: string;
  mean_reversion_risk?: number;
  // Technicals
  support?: number;
  resistance?: number;
  high_52w?: number;
  low_52w?: number;
  sma_50?: number;
  sma_200?: number;
  rsi_14?: number;
  risk_reward_ratio?: number;
  // Meta
  enriched_at?: any;
}
```

### Fix `getOvernightSignals` query

The function currently filters `direction == 'BULLISH'` or `direction == 'BEARISH'`, but the caller passes `'bull'` or `'bear'`. Update the mapping:

```typescript
export async function getOvernightSignals(
  scanDate: string,
  direction: 'bull' | 'bear',
  offset: number = 0,
  limit: number = 20
): Promise<OvernightSignal[]> {
  noStore();
  try {
    const dirValue = direction === 'bull' ? 'BULLISH' : 'BEARISH';
    const snapshot = await getDb().collection('overnight_signals')
      .where('scan_date', '==', scanDate)
      .where('direction', '==', dirValue)
      .orderBy('overnight_score', 'desc')
      .offset(offset)
      .limit(limit)
      .get();

    return snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as OvernightSignal));
  } catch (error) {
    console.error('Error fetching overnight signals:', error);
    return [];
  }
}
```
This part is already correct — keep it as-is.

## Problem 2: Signals Table Component

In `src/components/overnight/signals-table.tsx`, the table references old field names. Update:

```tsx
'use client';

import React from 'react';
import Link from 'next/link';
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ArrowRight, TrendingUp, TrendingDown, Lock } from "lucide-react";
import { type OvernightSignal } from '@/lib/firebase-admin';

interface SignalsTableProps {
  signals: OvernightSignal[];
  title: string;
  isSubscribed?: boolean;
}

export function SignalsTable({ signals, title, isSubscribed = false }: SignalsTableProps) {
  const formatMoney = (amount: number) => {
    if (!amount || amount === 0) return '—';
    if (Math.abs(amount) >= 1_000_000_000) return `$${(amount / 1_000_000_000).toFixed(1)}B`;
    if (Math.abs(amount) >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
    if (Math.abs(amount) >= 1_000) return `$${(amount / 1_000).toFixed(0)}K`;
    return `$${amount.toFixed(0)}`;
  };

  const getScoreColor = (score: number) => {
    if (score >= 9) return "bg-green-500 hover:bg-green-600";
    if (score >= 7) return "bg-amber-500 hover:bg-amber-600";
    return "bg-slate-500 hover:bg-slate-600";
  };

  // Calculate total dollar volume as positioning
  const getPositioning = (signal: OvernightSignal) => {
    const callVol = signal.call_dollar_volume || 0;
    const putVol = signal.put_dollar_volume || 0;
    return callVol + putVol;
  };

  return (
    <div className="rounded-md border bg-card">
      <div className="p-4 border-b">
        <h3 className="font-semibold text-lg">{title}</h3>
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Ticker</TableHead>
            <TableHead className="text-center">Score</TableHead>
            <TableHead className="text-right">Move</TableHead>
            <TableHead className="text-right">Flow</TableHead>
            <TableHead className="hidden md:table-cell">Thesis</TableHead>
            <TableHead className="w-[50px]"></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {signals.length === 0 ? (
            <TableRow>
              <TableCell colSpan={6} className="h-24 text-center text-muted-foreground">
                No signals found for this category.
              </TableCell>
            </TableRow>
          ) : (
            signals.map((signal) => {
              const movePct = signal.price_change_pct || 0;
              return (
                <TableRow key={signal.id} className="cursor-pointer hover:bg-muted/50 transition-colors group">
                  <TableCell className="font-bold font-mono text-base">
                    <Link href={`/signals/${signal.ticker}`} className="hover:underline underline-offset-4">
                      {signal.ticker}
                    </Link>
                  </TableCell>
                  <TableCell className="text-center">
                    <Badge className={`${getScoreColor(signal.overnight_score)} text-white border-0`}>
                      {signal.overnight_score}
                    </Badge>
                  </TableCell>
                  <TableCell className={`text-right font-medium ${movePct >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                    <div className="flex items-center justify-end gap-1">
                      {movePct >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                      {Math.abs(movePct).toFixed(1)}%
                    </div>
                  </TableCell>
                  <TableCell className="text-right font-mono text-muted-foreground">
                    {formatMoney(getPositioning(signal))}
                  </TableCell>
                  <TableCell className="hidden md:table-cell max-w-[300px] text-sm text-muted-foreground">
                    <span className="line-clamp-1">{signal.thesis || "Thesis available on detail page"}</span>
                  </TableCell>
                  <TableCell>
                    <Link href={`/signals/${signal.ticker}`} className="opacity-0 group-hover:opacity-100 transition-opacity">
                      <ArrowRight className="w-4 h-4 text-muted-foreground" />
                    </Link>
                  </TableCell>
                </TableRow>
              );
            })
          )}
        </TableBody>
      </Table>
    </div>
  );
}
```

**Key changes:**
- `signal.signal_score` → `signal.overnight_score`
- `signal.move_pct` → `signal.price_change_pct`
- `signal.new_positioning_usd` → calculated from `call_dollar_volume + put_dollar_volume`
- `signal.ai_thesis` → `signal.thesis`
- Removed the `isSubscribed` gate on thesis in the table — show truncated thesis preview for everyone (1 line). The detail page handles gating.

## Problem 3: Signal Detail Page (`/signals/[ticker]`)

Update `src/app/signals/[ticker]/signal-client.tsx` to use actual Firestore field names:

```tsx
'use client';

import { useAuth } from "@/hooks/use-auth";
import { type OvernightSignal } from "@/lib/firebase-admin";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Lock, TrendingUp, TrendingDown, ArrowLeft } from "lucide-react";
import Link from "next/link";
import { Markdown } from "@/components/markdown";
import { PublicHeader } from "@/components/layout/public-header";
import { EmailCapture } from "@/components/email-capture";

export default function SignalClientPage({ signal }: { signal: OvernightSignal }) {
  const { user, loading } = useAuth();
  const isSubscribed = !!user?.isSubscribed;
  const isBullish = signal.direction === 'BULLISH';
  const movePct = signal.price_change_pct || 0;

  const formatMoney = (amount: number) => {
    if (!amount || amount === 0) return '—';
    if (Math.abs(amount) >= 1_000_000_000) return `$${(amount / 1_000_000_000).toFixed(1)}B`;
    if (Math.abs(amount) >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
    if (Math.abs(amount) >= 1_000) return `$${(amount / 1_000).toFixed(0)}K`;
    return `$${amount.toFixed(0)}`;
  };

  const totalFlow = (signal.call_dollar_volume || 0) + (signal.put_dollar_volume || 0);

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <PublicHeader />

      <main className="flex-1 container mx-auto px-4 py-8 max-w-4xl">
        <Link href="/signals" className="inline-flex items-center text-sm text-muted-foreground hover:text-primary mb-6">
          <ArrowLeft className="w-4 h-4 mr-1" /> Back to Signals
        </Link>

        {/* Header */}
        <div className="flex flex-col md:flex-row gap-6 justify-between items-start md:items-center mb-8">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-4xl font-bold font-headline tracking-tight">{signal.ticker}</h1>
              <Badge variant={isBullish ? "default" : "destructive"} className={isBullish ? "bg-green-500 hover:bg-green-600" : "bg-red-500 hover:bg-red-600"}>
                {isBullish ? 'BULL' : 'BEAR'}
              </Badge>
              <Badge variant="outline" className="text-muted-foreground">
                {signal.scan_date}
              </Badge>
            </div>
            <p className="text-lg text-muted-foreground">Overnight Institutional Flow Signal</p>
          </div>

          <div className="flex gap-4 text-center">
            <div className="p-3 bg-card rounded-lg border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Score</div>
              <div className={`text-2xl font-bold font-code ${signal.overnight_score >= 7 ? (isBullish ? 'text-green-500' : 'text-red-500') : 'text-foreground'}`}>
                {signal.overnight_score}/10
              </div>
            </div>
            <div className="p-3 bg-card rounded-lg border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Move</div>
              <div className={`text-2xl font-bold font-code flex items-center justify-center gap-1 ${movePct >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {movePct >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                {Math.abs(movePct).toFixed(1)}%
              </div>
            </div>
            <div className="p-3 bg-card rounded-lg border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Flow</div>
              <div className="text-2xl font-bold font-code text-primary">
                {formatMoney(totalFlow)}
              </div>
            </div>
          </div>
        </div>

        {/* Main Content Grid */}
        <div className="grid gap-6">

          {/* AI Trade Thesis — always visible (this is the teaser that sells subscriptions) */}
          <Card>
            <CardHeader>
              <CardTitle>AI Trade Thesis</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="prose prose-invert max-w-none">
                <Markdown content={signal.thesis || "No thesis generated for this signal."} />
              </div>
            </CardContent>
          </Card>

          {/* Flow Breakdown — free */}
          <Card>
            <CardHeader>
              <CardTitle>Flow Breakdown</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <div className="text-xs text-muted-foreground uppercase mb-1">Call Volume</div>
                  <div className="font-mono font-bold text-green-500">{formatMoney(signal.call_dollar_volume || 0)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground uppercase mb-1">Put Volume</div>
                  <div className="font-mono font-bold text-red-500">{formatMoney(signal.put_dollar_volume || 0)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground uppercase mb-1">Flow Intent</div>
                  <div className="font-mono font-bold">{signal.flow_intent || '—'}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground uppercase mb-1">Catalyst</div>
                  <div className="font-mono font-bold">{signal.catalyst_type || '—'}</div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Locked Details Section */}
          <div className="grid md:grid-cols-2 gap-6">
            {/* Contract Setup */}
            <Card className={`relative overflow-hidden ${!isSubscribed && 'border-primary/20'}`}>
              {!isSubscribed && !loading && (
                <div className="absolute inset-0 z-10 bg-background/80 backdrop-blur-sm flex flex-col items-center justify-center p-6 text-center">
                  <Lock className="w-8 h-8 text-primary mb-4" />
                  <h3 className="text-lg font-bold mb-2">Subscribe to Unlock</h3>
                  <p className="text-sm text-muted-foreground mb-4">Get the exact contract, strike price, and risk/reward analysis.</p>
                  <Button asChild>
                    <Link href="/#pricing">Upgrade to Edge</Link>
                  </Button>
                </div>
              )}
              <CardHeader>
                <CardTitle>Recommended Setup</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">Contract</span>
                  <span className="font-mono font-bold">{signal.recommended_contract || "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">Contract Score</span>
                  <span className="font-mono">{signal.contract_score ? `${signal.contract_score}/10` : "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">Risk/Reward</span>
                  <span className="font-mono">{signal.risk_reward_ratio ? `${signal.risk_reward_ratio.toFixed(1)}:1` : "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">Delta</span>
                  <span className="font-mono">{signal.recommended_delta ? signal.recommended_delta.toFixed(2) : "—"}</span>
                </div>
              </CardContent>
            </Card>

            {/* Key Levels */}
            <Card className="relative overflow-hidden">
              {!isSubscribed && !loading && (
                <div className="absolute inset-0 z-10 bg-background/80 backdrop-blur-sm flex flex-col items-center justify-center p-6 text-center">
                  <Lock className="w-8 h-8 text-primary mb-4" />
                  <h3 className="text-lg font-bold mb-2">Unlock Technicals</h3>
                  <p className="text-sm text-muted-foreground mb-4">See key support & resistance levels, technicals, and 52-week range.</p>
                  <Button asChild>
                    <Link href="/#pricing">Upgrade to Edge</Link>
                  </Button>
                </div>
              )}
              <CardHeader>
                <CardTitle>Key Levels</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex justify-between py-2 border-b">
                  <span className="text-xs font-semibold text-red-400 uppercase tracking-wider">Resistance</span>
                  <span className="font-mono text-lg">{signal.resistance ? `$${signal.resistance.toFixed(2)}` : "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-xs font-semibold text-green-400 uppercase tracking-wider">Support</span>
                  <span className="font-mono text-lg">{signal.support ? `$${signal.support.toFixed(2)}` : "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">Price</span>
                  <span className="font-mono">{signal.underlying_price ? `$${signal.underlying_price.toFixed(2)}` : "—"}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-muted-foreground">SMA 50 / 200</span>
                  <span className="font-mono">
                    {signal.sma_50 ? `$${signal.sma_50.toFixed(0)}` : "—"} / {signal.sma_200 ? `$${signal.sma_200.toFixed(0)}` : "—"}
                  </span>
                </div>
                <div className="flex justify-between py-2">
                  <span className="text-muted-foreground">52W Range</span>
                  <span className="font-mono">
                    {signal.low_52w ? `$${signal.low_52w.toFixed(0)}` : "—"} — {signal.high_52w ? `$${signal.high_52w.toFixed(0)}` : "—"}
                  </span>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Extended Analysis */}
          <Card className="relative overflow-hidden">
            {!isSubscribed && !loading && (
              <div className="absolute inset-0 z-10 bg-background/80 backdrop-blur-sm flex flex-col items-center justify-center p-6 text-center">
                <Button variant="outline" asChild>
                  <Link href="/#pricing">Unlock Full Analysis</Link>
                </Button>
              </div>
            )}
            <CardHeader>
              <CardTitle>News & Catalyst Analysis</CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              {signal.key_headline && (
                <div>
                  <h4 className="font-semibold mb-2">Key Headline</h4>
                  <p className="text-sm text-muted-foreground">{signal.key_headline}</p>
                </div>
              )}
              <div>
                <h4 className="font-semibold mb-2">News Summary</h4>
                <div className="text-sm text-muted-foreground leading-relaxed">
                  <Markdown content={signal.news_summary || "No news analysis available."} />
                </div>
              </div>
              {signal.flow_intent_reasoning && (
                <div>
                  <h4 className="font-semibold mb-2">Flow Intent Analysis</h4>
                  <p className="text-sm text-muted-foreground">{signal.flow_intent_reasoning}</p>
                </div>
              )}
            </CardContent>
          </Card>

          <div className="mt-8 max-w-xl mx-auto w-full">
            <EmailCapture />
          </div>
        </div>
      </main>
    </div>
  );
}
```

**Key changes:**
- All field references now match actual Firestore field names
- `direction` compared as `'BULLISH'`/`'BEARISH'` (not `'bull'`/`'bear'`)
- `signal.thesis` instead of `signal.ai_thesis`
- `signal.overnight_score` instead of `signal.signal_score`
- `signal.price_change_pct` instead of `signal.move_pct`
- `signal.support` / `signal.resistance` as flat numbers (not `key_levels` object)
- `signal.risk_reward_ratio` instead of `signal.risk_reward`
- Added Flow Breakdown card (free tier — this is the hook)
- Added Key Headline, Flow Intent Analysis, SMA 50/200, 52W Range, Delta
- Back link goes to `/signals` not `/`
- Thesis card is always visible (free teaser — this sells subscriptions)

## Problem 4: Signals Page Only Shows Latest Day

The `/signals/page.tsx` already queries by `scanDate` from `getLatestOvernightSummary()`, so it only shows the latest scan. This is correct. No change needed.

However, update the page heading to make the date prominent:

```tsx
// In src/app/signals/page.tsx, update the heading section:
<div className="mb-8">
  <h1 className="text-3xl font-bold font-headline mb-2">Overnight Signals</h1>
  <p className="text-muted-foreground">
    Institutional options flow detected overnight — {scanDate}
  </p>
</div>
```

## Problem 5: Delete `mock-data.ts`

Delete `src/app/signals/mock-data.ts` — we're past mock data.

## Summary of Files to Change

1. **`src/lib/firebase-admin.ts`** — Update `OvernightSignal` interface to match actual Firestore fields
2. **`src/components/overnight/signals-table.tsx`** — Full replacement (field name fixes)
3. **`src/app/signals/[ticker]/signal-client.tsx`** — Full replacement (field name fixes + new sections)
4. **`src/app/signals/page.tsx`** — Minor heading update
5. **`src/app/signals/mock-data.ts`** — DELETE this file

No backend/enrichment pipeline changes needed — the data is correct in Firestore, the webapp just wasn't reading it properly.
