"""
Contains the core logic for building the FMP boolean query.
"""

def build_fmp_boolean_query(profile: dict) -> str:
    """
    Create a Boolean query for FMP /search_stock_news from a parsed
    10-K Business-section profile.

    Parameters
    ----------
    profile : dict
        {
          "company_name": str,
          "industry": str | None,
          "primary_products_or_services": list[str] | None,
          "recent_major_events": list[dict] | None
        }

    Returns
    -------
    str
        Example:
        "\"American Airlines Group Inc.\" AND (AAdvantage OR Passenger flights "
        "OR fuel prices OR pilot contract)"
    """

    # 1. Always quote the legal name
    name_block = f"\"{profile['company_name']}\""

    # 2. Use headline search terms from the profile summary
    extras = (profile.get("headlines_search_terms") or [])[:4]

    # 3. Add up to two sector catalysts based on industry
    sector_keywords = {
        "Airlines": ["fuel prices", "pilot contract", "capacity cuts"],
        "Semiconductors": ["chip shortage", "export controls", "node transition"],
        "Retail": ["same-store sales", "store closures", "inventory gluts"],
        # extend dictionary as needed
    }
    catalysts = sector_keywords.get(profile.get("industry"), [])[:2]

    # 4. Assemble the OR block and final query
    terms = extras + catalysts
    if not terms:
        return name_block  # fallback: company name only

    or_block = " OR ".join(terms)
    return f"{name_block} AND ({or_block})"