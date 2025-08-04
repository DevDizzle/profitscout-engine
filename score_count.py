from google.cloud import storage
import json
from collections import Counter

# Initialize GCS client
client = storage.Client()

# Bucket and folder
bucket_name = 'profit-scout-data'
folder_prefix = 'financials-analysis/'

# List all blobs in the folder
blobs = client.list_blobs(bucket_name, prefix=folder_prefix)

# Collect scores
scores = []

for blob in blobs:
    if blob.name.endswith('.json'):  # Ensure it's a JSON file
        # Download content as string
        content = blob.download_as_string()
        try:
            data = json.loads(content)
            score = data.get('score')
            if score is not None:
                scores.append(score)
        except json.JSONDecodeError:
            print(f"Error parsing {blob.name}")

# Compute distribution
distribution = Counter(scores)

# Sort by score for nice output
sorted_dist = sorted(distribution.items())

# Print results
print("Score Distribution:")
for score, count in sorted_dist:
    print(f"Score {score}: {count} files")

print(f"Total files processed: {len(scores)}")