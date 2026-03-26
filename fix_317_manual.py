import os
from google.cloud import firestore

PROJECT_ID = "profitscout-fida8"
db = firestore.Client(project=PROJECT_ID)

original = "# The Infrastructure Supercycle — Overnight Edge Report, March 17, 2026. ## Market Pulse: The overnight scan identifies 80 signals with a decisive 75% bullish bias (60 bullish, 20 bearish). Institutional flow is concentrating in 'Hard Tech' and energy infrastructure, moving away from high-multiple software and consumer discretionary. ## Key Themes: 1. AI Backbone Re-rating: Prologis (PLD), Corning (GLW), and Texas Instruments (TXN) are being re-valued as mission-critical components of the AI economy. 2. Geopolitical Risk Premium: Frontline (FRO) and Genco (GNK) are capturing massive flow as shipping disruptions in the Strait of Hormuz persist. 3. Commodity Bottoms: Albemarle (ALB) and AngloGold (AU) show high-conviction accumulation as lithium and gold cycles turn. ## Top Bullish Signals: - **PLD (Score 9)**: Aggressive institutional accumulation on the ex-dividend date confirms its transition to an AI-infrastructure provider. - **ORBS (Score 8)**: Scarcity value as the 'only public OpenAI proxy' is driving a high-beta chase. - **FRO (Score 8)**: Tanker rates hitting decade highs on geopolitical blockades provide a fundamental floor at $30. ## Top Bearish Signals: - **ULTA (Score 8)**: Guidance cuts and rising SG&A expenses have triggered a fundamental repricing. - **IT (Score 8)**: Institutional exhaustion as Gartner's research dominance is challenged by AI alternatives. - **WHR (Score 7)**: A breach of multi-year support at $57 signals further downside for the appliance giant. ## Best Contract Recommendations: - **O:PLD260417C00140000**: The $140 Call offers exposure to the data center re-rating. - **O:ALB260410C00170000**: Positioning for the lithium spot price recovery. - **O:ULTA260402P00555000**: Capitalizing on the technical breakdown below $520. ## Divergence Watch: **Salesforce (CRM)**: Despite the largest buyback in history ($25B), institutional flow is $79M bearish, signaling significant skepticism regarding the debt used to fund the program. **MicroStrategy (MSTR)**: The stock continues to rally on BTC strength, but $380M in bearish hedging suggests large players are bracing for a 'sell the news' event. ## Summary / Bias: Tactical Bullish. We favor 'Hard Assets' and infrastructure over consumer-facing growth. The bias remains long on energy and data center logistics while maintaining short hedges on high-multiple retail."

# Manual formatting replacements
content = original.replace(". ## ", ".\n\n### ")
content = content.replace(" ## ", "\n\n### ")
content = content.replace(" 1. ", "\n\n1. ")
content = content.replace(" 2. ", "\n2. ")
content = content.replace(" 3. ", "\n3. ")
content = content.replace(" - **", "\n* **")
content = content.replace(" - **O:", "\n* **O:")
content = content.replace(" Watch: **", " Watch:\n\n**")
content = content.replace(" program. **", " program.\n\n**")

doc_ref = db.collection("daily_reports").document("2026-03-17")
doc_ref.update({"content": content})
print("Fixed 3/17 report manually.")
