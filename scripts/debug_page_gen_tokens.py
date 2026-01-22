
import logging
import sys
import os

# Add src to path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from serving.core.pipelines import page_generator

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_token_usage():
    print("--- TESTING PAGE GENERATOR PROMPT & TOKEN USAGE ---")

    # Mock Data
    ticker = "TEST"
    company = "Test Corp"
    signal = "Bullish"
    
    market_structure = {
        'call_wall': 150.0,
        'put_wall': 130.0,
        'net_call_gamma': 5000.0,
        'net_put_gamma': 2000.0,
        'top_active_contracts': [
            {'expiration_date': '2026-02-20', 'strike': 155, 'option_type': 'call', 'volume': 15000}
        ]
    }
    
    tech_snippet = "Price is trending above the 50-day moving average. RSI is neutral at 55."

    # Generate the prompt internally (we access the private template for demo)
    # We replicate the logic inside _generate_analyst_brief just to show the prompt
    
    top_contract_desc = "2026-02-20 $155 call (Vol: 15000)"
    
    prompt = page_generator._BRIEF_PROMPT.format(
        ticker=ticker,
        company_name=company,
        signal=signal,
        technicals_snippet=tech_snippet,
        call_wall=market_structure['call_wall'],
        put_wall=market_structure['put_wall'],
        net_call_gamma=market_structure['net_call_gamma'],
        net_put_gamma=market_structure['net_put_gamma'],
        top_contract_desc=top_contract_desc
    )

    print("\n[GENERATED PROMPT]:")
    print("-" * 40)
    print(prompt)
    print("-" * 40)
    
    # Simple estimation
    char_count = len(prompt)
    est_tokens = char_count / 4
    
    print(f"\n[METRICS]")
    print(f"Character Count: {char_count}")
    print(f"Est. Input Tokens: ~{int(est_tokens)}")
    print(f"Previous Approach: ~15,000+ Tokens")
    # ... (previous code) ...
    
    print(f"Reduction: ~{((15000 - est_tokens)/15000)*100:.2f}%")

    print("\n--- TESTING FULL JSON ASSEMBLY ---")
    
    # 1. SEO
    seo = page_generator._generate_seo(ticker, company, signal, market_structure['call_wall'])
    
    # 2. FAQ
    faq = page_generator._generate_faq(ticker, market_structure)
    
    # 3. Brief (Simulated Output)
    simulated_llm_response = {
        "headline": f"{signal} Setup: Eyes on ${market_structure['call_wall']} Call Wall",
        "content": "<p>This is a simulated LLM response demonstrating the html content.</p>"
    }
    
    # 4. Assemble Final JSON
    final_json = {
        "symbol": ticker,
        "date": "2026-01-22",
        "bullishScore": 75.50,
        "fullAnalysis": {"technicals": tech_snippet},
        "marketStructure": market_structure,
        "seo": seo,
        "analystBrief": simulated_llm_response,
        "tradeSetup": {"signal": signal, "confidence": "High", "strategy": "Long Calls", "catalyst": "Test Catalyst"},
        "faq": faq
    }
    
    import json
    print(json.dumps(final_json, indent=2))

if __name__ == "__main__":
    test_token_usage()
