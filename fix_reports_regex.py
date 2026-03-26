import os
import re
from google.cloud import firestore

PROJECT_ID = "profitscout-fida8"
db = firestore.Client(project=PROJECT_ID)

docs = db.collection("daily_reports").where("scan_date", ">=", "2026-03-12").where("scan_date", "<=", "2026-03-17").stream()

for doc in docs:
    data = doc.to_dict()
    content = data.get("content", "")
    original_content = content
    
    if "\n" not in content and "##" in content:
        # Add newlines before headers
        content = re.sub(r'(?<!\n)(?<!\n\n)\s*##\s+', r'\n\n### ', content)
        content = re.sub(r'(?<!\n)(?<!\n\n)\s*#\s+', r'\n\n# ', content)
        
        # Add newlines before bullet points
        content = re.sub(r'(?<!\n)\s*-\s*\*\*', r'\n* **', content)
        content = re.sub(r'(?<!\n)\s*-\s+O:', r'\n* O:', content)
        
        # Add newlines before numbered lists
        content = re.sub(r'(?<!\n)\s*(\d+)\.\s+', r'\n\n\1. ', content)
        
        # Divergence watch items
        content = re.sub(r'(?<!\n)\s*\*\*([A-Za-z]+ \([A-Z]+\))\*\*:', r'\n\n**\1**:', content)
        
        if content != original_content:
            print(f"Updating {doc.id}")
            doc.reference.update({"content": content.strip()})
        else:
            print(f"No changes matched for {doc.id}")
    else:
        print(f"Document {doc.id} already has newlines or no headers.")
