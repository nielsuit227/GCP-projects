from google.cloud import storage


try:
    client = storage.Client(
        project='amplo-301021',
    )
    client.create_bucket('amplo-storage')
    print('Storage Created.')
except Exception as e:
    print(e)