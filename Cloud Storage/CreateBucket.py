import os

from google.cloud import storage

try:
    client = storage.Client(project=os.getenv("GCP_PROJECT"))
    client.create_bucket(os.getenv("GCP_STORAGE_BUCKET"))
    print("Storage Created.")
except Exception as e:
    print("Do not run within PyCharm. Ensure Environmental Variable is set.")
    print(e)
