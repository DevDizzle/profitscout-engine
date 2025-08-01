import base64
import json
import functions_framework
from google.cloud import firestore, bigquery

# Initialize clients globally to be reused across function invocations
db = firestore.Client()
bq_client = bigquery.Client()

def delete_collection(coll_ref, batch_size):
    """
    Deletes all documents in a collection in batches.
    """
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f"Deleting doc: {doc.id}")
        doc.reference.delete()
        deleted += 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

@functions_framework.cloud_event
def run(cloud_event):
    """
    This function is triggered by Pub/Sub. It deletes all data in the 'stock'
    Firestore collection and then syncs it with data from a BigQuery table.

    Args:
        cloud_event (cloudevents.http.CloudEvent): The event payload from Pub/Sub.
        The message data is expected to be a JSON string with BigQuery table details:
        '{"project_id": "your-project", "dataset_id": "your_dataset", "table_id": "your_table"}'
    """
    try:
        # --- 1. Decode the Pub/Sub message to get BigQuery table info ---
        message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        table_info = json.loads(message_data)
        
        project_id = table_info["project_id"]
        dataset_id = table_info["dataset_id"]
        table_id = table_info["table_id"]
        
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        print(f"Received request to sync from BigQuery table: {full_table_id}")

        # --- 2. Delete all existing documents in the 'stock' collection ---
        stock_collection_ref = db.collection("stock")
        print("Starting deletion of all documents in 'stock' collection...")
        delete_collection(stock_collection_ref, 100) # Deleting in batches of 100
        print("Deletion complete.")

        # --- 3. Query the BigQuery table ---
        print(f"Fetching rows from {full_table_id}...")
        rows_iterator = bq_client.list_rows(full_table_id)

        # --- 4. Write new data from BigQuery to Firestore in batches ---
        batch = db.batch()
        docs_in_batch = 0
        total_docs_written = 0
        
        print("Starting batch write to Firestore...")
        for row in rows_iterator:
            # Convert BigQuery Row to a dictionary
            doc_data = dict(row)
            
            # Use a unique field from your data (e.g., 'sku', 'id') as the document ID.
            # If no obvious unique ID exists, you can let Firestore auto-generate one.
            # Here, we assume a unique 'product_id' field exists in your BigQuery table.
            if "product_id" in doc_data:
                doc_ref = stock_collection_ref.document(str(doc_data["product_id"]))
            else:
                # If no unique ID is available, let Firestore generate one.
                doc_ref = stock_collection_ref.document()

            batch.set(doc_ref, doc_data)
            docs_in_batch += 1
            
            # Commit the batch every 500 documents to stay within limits
            if docs_in_batch >= 500:
                batch.commit()
                total_docs_written += docs_in_batch
                print(f"Committed a batch of {docs_in_batch} documents.")
                # Start a new batch
                batch = db.batch()
                docs_in_batch = 0

        # Commit any remaining documents in the last batch
        if docs_in_batch > 0:
            batch.commit()
            total_docs_written += docs_in_batch
            print(f"Committed the final batch of {docs_in_batch} documents.")

        print(f"✅ Successfully synced {total_docs_written} documents to Firestore.")

    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON from Pub/Sub message: {e}")
        raise
    except KeyError as e:
        print(f"❌ Error: Missing expected key {e} in the Pub/Sub message data.")
        raise
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        raise