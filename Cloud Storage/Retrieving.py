import json
import os

from google.cloud import storage

client = storage.Client(project=os.getenv("GCP_PROJECT"))
try:
    bucket = client.get_bucket(os.getenv("GCP_STORAGE_BUCKET"))
    try:
        file = bucket.blob("Hello_World.json")
        data = json.loads(file.download_as_string())
        print(data)
    except Exception as e:
        print("Error loading file:")
        print(e)
except FileNotFoundError:
    print("Bucket not found.")
