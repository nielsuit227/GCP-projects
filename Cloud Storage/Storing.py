from google.cloud import storage


try:
    client = storage.Client()
    bucket = client.bucket('amplo-storage')
    destination = bucket.blob('/Docs/Hello_World.json')
    destination.upload_from_filename('data.json')
    print('File uploaded.')
except Exception as e:
    print(e)
