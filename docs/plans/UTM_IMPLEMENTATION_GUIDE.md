# UTM Tracking Implementation Guide

This document outlines the required code changes to implement the tracking strategy defined in `@UTM_TRACKING_PLAM.md`.

## 1. Update Configuration

Add the base URL constant to the central configuration file to avoid hardcoding strings across the codebase.

**File:** `src/serving/core/config.py`

**Action:** Add the following lines to the "Social Media" or "Global" section:

```python
# --- Global Project/Service Configuration ---
BASE_URL = "https://gammarips.com"  # Add this

# ... (rest of the file)
```

## 2. Update Social Media Poster Pipeline

Modify the poster logic to programmatically construct the URL with UTM parameters *before* generating the tweet.

**File:** `src/serving/core/pipelines/social_media_poster.py`

### Step 2.1: Import Config
Ensure `config` is imported (it usually is).

### Step 2.2: Implement URL Construction
Locate the loop where `winners` are processed (inside `run_pipeline`). Replace the hardcoded link logic.

**Current Code (Approximate):**
```python
        # ... inside the loop ...
        
        # Generate Tweet
        prompt = f"""
        ...
        - Include the link: https://gammarips.com/{ticker}
        ...
        """
```

**New Code:**
```python
        # ... inside the loop ...

        # 1. Construct Tracking URL
        # Pattern: ?utm_source=x.com&utm_medium=social&utm_campaign={ticker}_auto_post
        utm_source = "x.com"
        utm_medium = "social"
        utm_campaign = f"{ticker}_auto_post"  # Or "daily_winners_feed" if you prefer a static campaign
        
        tracking_url = f"{config.BASE_URL}/{ticker}?utm_source={utm_source}&utm_medium={utm_medium}&utm_campaign={utm_campaign}"

        # 2. Update Prompt
        prompt = f"""
        You are a professional financial analyst for GammaRips. Write a catchy, professional "FinTwit" style tweet for the stock ${ticker}.
        
        Context:
        - Title: {seo_title}
        - Analyst Brief: {analyst_brief}
        - Trade Setup: {trade_setup}
        
        Requirements:
        - Start with the Cashtag ${ticker} and a relevant emoji.
        - Highlight the key level or direction (Call Wall, Support, etc.).
        - Keep it under 260 characters.
        - Include the link: {tracking_url}  <-- USE VARIABLE HERE
        - Do NOT use hashtags other than the Cashtag.
        - Tone: Confident, actionable, data-driven.
        """
```

### Step 2.3: Verification
After deploying the change, monitor the logs or the Firestore `social_media_history` collection. The `text` field in Firestore should now contain the full URL with `?utm_source=...`.

## 3. Future Implementation (Email & AI Agents)

### Email
*   **Action:** When working on the frontend/email service (`src/lib/email-templates` in the web repo), ensure links use:
    *   `utm_source=newsletter`
    *   `utm_medium=email`
    *   `utm_campaign=daily_setups` (or dynamic campaign name)

### AI Agents / MCP
*   **Action:** If `page_generator.py` or a dedicated API endpoint serves data to the MCP, add a `shareable_link` field to the JSON response.
*   **Logic:**
    ```python
    # In API response builder
    return {
        "ticker": ticker,
        # ... other data ...
        "link": f"{config.BASE_URL}/{ticker}?utm_source=ai_agent&utm_medium=referral&utm_campaign=mcp_tool_response"
    }
    ```
