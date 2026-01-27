# Traffic Source Tracking Plan (UTM Strategy)

This document outlines the strategy for tracking traffic sources using UTM parameters to distinguish between Email, Social, SEO, and AI Agent traffic in Google Analytics 4 (GA4).

## The Core Concept
Append specific query parameters to the end of your URLs. Google Analytics captures these automatically.
**Format:** `?utm_source=...&utm_medium=...&utm_campaign=...`

---

## 1. Email Campaigns (Internal & Marketing)
**Goal:** Track users clicking links inside your daily emails, welcome emails, or alerts.

*   **Variables:**
    *   `utm_source`: `newsletter` (or `transactional`)
    *   `utm_medium`: `email`
    *   `utm_campaign`: `daily_setups` (or `welcome_email`, `win_back`, `price_alert`)

*   **Implementation:**
    *   Update the link strings in your email templates (`src/lib/email-templates` / Mailgun).
    *   **Example:** `https://gammarips.com/NVDA?utm_source=newsletter&utm_medium=email&utm_campaign=daily_setups`

## 2. Organic Social (X / Twitter / LinkedIn)
**Goal:** Track traffic from your manual social media posts.

*   **Variables:**
    *   `utm_source`: `x.com` (or `linkedin`, `reddit`)
    *   `utm_medium`: `social`
    *   `utm_campaign`: Description of the post (e.g., `launch_post`, `market_update_jan26`, `feature_highlight`)

*   **Implementation:**
    *   **Never paste a "naked" link on social media.**
    *   Use a UTM Builder or manually construct the link before posting.
    *   **Example:** `https://gammarips.com/TSLA?utm_source=x.com&utm_medium=social&utm_campaign=market_update`

## 3. Organic SEO (Google Search)
**Goal:** Track users finding you via Google Search.

*   **Action:** **None required.**
*   **Mechanism:** GA4 automatically detects traffic with an empty source but a recognized referrer (like `google.com`) and classifies it as `google / organic`. Do not use UTMs for SEO.

## 4. AI Agents (ChatGPT, Perplexity, etc.)
**Goal:** Track users clicking through from an AI interface (if your API/MCP serves them links).

*   **Variables:**
    *   `utm_source`: `ai_agent` (or specific model like `chatgpt`, `perplexity`)
    *   `utm_medium`: `referral`
    *   `utm_campaign`: `mcp_tool_response`

*   **Implementation:**
    *   In your API or MCP tool logic, ensure any URL returned to the LLM includes these tags.
    *   **Example:** `https://gammarips.com/AMD?utm_source=chatgpt&utm_medium=referral&utm_campaign=mcp_tool_response`

---

## 5. Reporting (How to View Data)
1.  Log in to **Google Analytics**.
2.  Navigate to **Reports** > **Acquisition** > **Traffic acquisition**.
3.  Change the primary column dropdown to **Session source / medium**.
4.  You will see clear buckets:
    *   `newsletter / email`
    *   `x.com / social`
    *   `google / organic`
    *   `chatgpt / referral`

## 6. Critical Next Steps
1.  [ ] **Modify Email Templates:** Edit the code generating email HTML to append the query strings.
2.  [ ] **Establish Social Workflow:** Save these standard UTM strings in a note for your social media team/process.
