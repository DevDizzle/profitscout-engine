"""
Builds the prompt and calls the GenAI client for profile summarization.
"""
from . import client

_BASE_PROMPT = """
You are a financial-data extraction assistant.

TASK  
Read {{business_profile}} (the full “Item 1 – Business” section from a U.S. Form 10-K)  
and return ONE valid JSON object that matches the schema below.

RULES  
1. Decode HTML entities (e.g., &#8220; → “).  
2. If a field is absent, output null (or [] for arrays); do **not** invent data.  
3. Keep all strings plain text—no line breaks, quotes, or markdown inside values.  
4. Do not add comments, keys, or text before/after the JSON.  
5. Make sure the JSON is syntactically valid (no trailing commas).

JSON SCHEMA
───────────
{{
  "company_name": string,                 // e.g. "American Airlines Group Inc."
  "industry": string | null,              // Short label, e.g. "Airlines"
  "products_or_services": string[] | null,// ≤ 8 core offerings
  "major_brands": string[] | null,        // Key brands/trademarks if named
  "customer_segments": string[] | null    // e.g. ["Retail", "Enterprise"]
}}

RETURN ONLY THE JSON OBJECT.
"""

def build_prompt(business_profile: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(business_profile=business_profile)

def summarise(business_profile: str) -> str:
    """Generates a summary by building a prompt and calling the AI client."""
    if not business_profile:
        raise ValueError("Business profile cannot be empty.")

    prompt = build_prompt(business_profile)

    return client.generate(prompt)