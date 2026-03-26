import os
from google.cloud import firestore

PROJECT_ID = "profitscout-fida8"
db = firestore.Client(project=PROJECT_ID)

doc_ref = db.collection("daily_reports").document("2026-03-17")
doc = doc_ref.get()
if doc.exists:
    data = doc.to_dict()
    print("Content preview:")
    print(repr(data.get("content", "")[:200]))
else:
    print("Document 2026-03-17 not found.")
