import os
from google.cloud import firestore

PROJECT_ID = "profitscout-fida8"
db = firestore.Client(project=PROJECT_ID)

docs = db.collection("daily_reports").where("scan_date", ">=", "2026-03-12").stream()
updated = 0
for doc in docs:
    data = doc.to_dict()
    changed = False
    
    for key in ["title", "headline", "content"]:
        if key in data and isinstance(data[key], str):
            if '\\n' in data[key] or '\\"' in data[key]:
                data[key] = data[key].replace('\\n', '\n').replace('\\"', '"')
                changed = True
    
    if changed:
        print(f"Updating document {doc.id}")
        doc.reference.update({
            "title": data.get("title"),
            "headline": data.get("headline"),
            "content": data.get("content")
        })
        updated += 1

print(f"Updated {updated} documents.")
