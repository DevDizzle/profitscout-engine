"""
Builds the prompt and calls the GenAI client to generate a keyword list for the FMP news API.
"""
from . import client

_BASE_PROMPT = """
You are a financial news analyst bot. Your sole task is to read a company's business profile and generate a list of key topics for a news search.

**Analysis Steps (Internal):**
1.  Read the business description to understand the company's core operations.
2.  Identify 4-6 key topics, themes, or economic factors that would directly impact this company's stock price. For an airline, this might include "jet fuel prices," "passenger demand," or "pilot union."

**Output Requirements:**
- Your entire output must be ONLY a comma-separated list of these key topics.
- Do not include any other text, explanations, or markdown.
- Do not include the company's name in the topics.

**Example:**
- **Input:** (Business description for American Airlines)
- **Output:** passenger demand,jet fuel prices,pilot contract,international travel,airline safety,tourism trends

Now, generate the key topics for the following business profile.

**Business Profile:**
{business_profile}
"""

def build_prompt(business_profile: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(business_profile=business_profile)

def summarise(business_profile: str) -> str:
    """Generates the FMP query string by building a prompt and calling the AI client."""
    if not business_profile:
        raise ValueError("Business profile cannot be empty.")
    prompt = build_prompt(business_profile)
    # The result will be a simple string of comma-separated keywords
    return client.generate(prompt)