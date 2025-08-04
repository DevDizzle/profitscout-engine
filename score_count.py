from google.cloud import storage
import json
from collections import Counter

# Initialize GCS client
client = storage.Client()

# --- MODIFIED: Point to the key-metrics-analysis folder ---
bucket_name = 'profit-scout-data'
folder_prefix = 'key-metrics-analysis/'

# List all blobs in the folder
blobs = client.list_blobs(bucket_name, prefix=folder_prefix)

# Collect scores
scores = []

for blob in blobs:
    if blob.name.endswith('.json'):
        # Download content and decode from bytes to string
        content_str = blob.download_as_string().decode('utf-8')

        # Clean the string to remove markdown code blocks
        if '```json' in content_str:
            clean_content = content_str.replace('```json', '').replace('```', '').strip()
        else:
            clean_content = content_str

        try:
            data = json.loads(clean_content)
            score = data.get('score')
            if score is not None:
                scores.append(score)
        except json.JSONDecodeError:
            # Silently skip files that are still malformed
            continue

# Compute distribution
distribution = Counter(scores)

# Sort by score for nice output
sorted_dist = sorted(distribution.items())

# --- Final Output ---
print("Score Distribution:")
for score, count in sorted_dist:
    print(f"Score {score}: {count} files")

print(f"\nTotal files processed successfully: {len(scores)}")