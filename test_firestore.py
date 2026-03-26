import os
from google.cloud import firestore

PROJECT_ID = "profitscout-fida8"
db = firestore.Client(project=PROJECT_ID)

doc_ref = db.collection("daily_reports").document("2026-03-19")
doc = doc_ref.get()
if doc.exists:
    content = doc.to_dict().get("content", "")
    print("Content preview:")
    print(repr(content[:200]))
else:
    print("Document 2026-03-19 not found.")
