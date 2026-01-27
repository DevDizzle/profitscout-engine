# Backend Data Integrity & Schema Alignment Plan

## Overview
This document outlines the required backend fixes to resolve data inconsistencies causing errors in the frontend application (`getDashboardData` fallbacks and `OptionCandidateSchema` validation failures).

## 1. Fix `winners_dashboard` Data Promotion
**Issue:**
The `winners_dashboard` collection documents frequently miss the `dashboard_json` field. This forces the frontend to perform a fallback query to the `tickers` collection, increasing latency and logging warnings (e.g., `Winner contract for SCHW is incomplete`).

**Action Required:**
Update the backend process (Cloud Function/Script) responsible for promoting a stock to "Winner" status to explicitly include the `dashboard_json` field.

**Implementation Logic:**
When creating/updating a document in `winners_dashboard`:
1.  Fetch the source data (likely from `tickers/{ticker_id}`).
2.  Ensure `dashboard_json` (the GCS URI) is read from the source.
3.  Write `dashboard_json` to the `winners_dashboard` document.

**Target Data Structure (`winners_dashboard`):**
```json
{
  "ticker": "SCHW",
  "dashboard_json": "gs://bucket/path/to/analysis.json", 
  // ... other existing fields
}
```

---

## 2. Standardize `options_candidates` Schema
**Issue:**
The frontend logs `Invalid options candidate data` because the Zod schema validation fails. This is primarily due to field name mismatches (e.g., `strike` vs `strike_price`) or missing optional fields.

**Action Required:**
1.  **Standardize Field Names:** Ensure the backend writes consistent field names. The Typescript interface expects `strike` (number), but Firestore often contains `strike_price`.
2.  **Update DB Mapping (Immediate Fix):** Update the `getOptionsCandidatesAdmin` function in `src/lib/firebase-admin.ts` to map fields robustly.

**Code Update (`src/lib/firebase-admin.ts`):**
Update the mapping logic inside `getOptionsCandidatesAdmin` to handle variations:

```typescript
// Current mapping (prone to failure if strike_price is missing)
strike: data.strike_price, 

// Robust mapping
strike: data.strike ?? data.strike_price ?? 0, 
```

**Schema Update (`src/lib/schemas.ts`):**
Review `OptionCandidateSchema` to ensure nullable fields match the database reality.
- `industry`: `z.string().nullable().optional()`
- `image_uri`: `z.string().nullable().optional()`
- `implied_volatility`: `z.number().nullable().optional()`

---

## 3. Data Cleanup Script (One-Off)
**Objective:** Retroactively fix existing broken documents.

**Script Logic:**
1.  **Scan `winners_dashboard`:**
    - Find documents missing `dashboard_json`.
    - For each, query `tickers/{ticker}`.
    - If the ticker has `dashboard_json`, update the winner doc.
2.  **Scan `options_candidates`:**
    - Check for documents where `strike` is missing but `strike_price` exists (or vice versa).
    - Normalize the field to `strike` (or whatever the canonical field should be).

## Summary of Files to Touch
- **Backend Script/Function:** (Location depends on your repo structure, e.g., `functions/src/...`) -> Fix promotion logic.
- **`src/lib/firebase-admin.ts`:** Update `getOptionsCandidatesAdmin` mapping.
- **`src/lib/schemas.ts`:** Relax `OptionCandidateSchema` validation rules.
