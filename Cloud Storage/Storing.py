from google.cloud import storage


# Params
file = 'data.json'
bucket_name = 'amplo-storage'
file_destination = 'Hello_World.json'

try:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    destination = bucket.blob(file_destination)
    destination.upload_from_filename(file)
    print('%s uploaded to gs://%s%s' % (file, bucket_name, file_destination))
except Exception as e:
    print('Do not run within PyCharm. Ensure Environmental Variable is set.')
    print(e)
