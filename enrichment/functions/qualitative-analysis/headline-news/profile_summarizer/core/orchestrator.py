"""
Builds the prompt and calls the GenAI client to generate the FMP query string directly.
"""
from . import client

_BASE_PROMPT = """
You are a financial news analyst bot. Your sole task is to read a company's business profile and generate a single, perfectly formatted boolean query string for the Financial Modeling Prep (FMP) news API.

**Analysis Steps (Internal):**
1.  Identify the company's full legal name.
2.  Based on the business description, determine 4-6 key topics, themes, or economic factors that would directly impact this company's stock price (e.g., for an airline: "jet fuel prices", "passenger demand", "pilot union").

**Output Requirements:**
- Your entire output must be ONLY the final query string.
- Do not include any other text, explanations, or markdown.
- The format must be: `"Company Name" AND ("term1" OR "term2" OR "term3")`

**Example:**
- **Input:** (Business description for American Airlines)
- **Output:** "American Airlines Group Inc." AND ("passenger demand" OR "jet fuel prices" OR "pilot contract" OR "international travel")

Now, generate the query string for the following business profile.

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
    return client.generate(prompt)