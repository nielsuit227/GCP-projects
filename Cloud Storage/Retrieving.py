import json
from google.cloud import storage


client = storage.Client(project='amplo-301021')
try:
    bucket = client.get_bucket('amplo-storage')
except:
    print('Bucket not found.')
try:
    file = bucket.blob('Hello_World.json')
    data = json.loads(file.download_as_string())
    print(data)
except Exception as e:
    print('Error loading file:')
    print(e)

