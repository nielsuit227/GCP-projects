import json, joblib, pickle, cantools, re
import numpy as np
import pandas as pd
from google.cloud import storage

# Global (avoids reloading, makes hot calls cheaper :)
model = None
normalization = None
features = None
dbc = None
bucket = 'amplo-storage'
project = 'tritium/'
error_message = 'Internal error, please contact info@amplo.ch'
api_key = 'ya29.a0AfH6SMAzzvEoeIvYmyL4J66QHU6ES_-K37sDVF4QNeDmEiA2xXonRi4lRM0Qz0UDW4rCucL775TNGqS__JGXvO3zF7UYdCvqAf' \
          '6Ahcoxwf5DzJVzSkbQrW68sewDmoPUD1tIiJfCCrbszfEcXbMPi_IniYEsFuKRpzmCCDd9o0U'


def load_file(file_loc):
    """
    Loads file from storage bucket into local memory.
    :param file_loc: String with file location in project folder.
    :return:
    """
    global error_message, bucket, project
    # Connect
    client = storage.Client()
    try:
        bucket_class = client.get_bucket(bucket)
    except Exception as e:
        print('Connecting to bucket %s failed.' % bucket)
        print(e)
        return error_message
    # Get file
    try:
        blob = bucket_class.blob(project + file_loc)
        blob.download_to_filename('/tmp/' + file_loc)
    except Exception as e:
        print('Loading %s into memory failed.' % file_loc)
        print(e)
        return error_message


def predict_iso(request):
    """
    Automated Diagnostics API for Tritium.
    Triggered with HTTPS Post. Requires API key for authentication and csv for prediction.
    """
    global model, features, normalization, dbc, api_key, error_message

    # Checks
    if request.method != 'POST':
        return 'Please use HTTPS POST.'
    if 'X-API-Key' not in request.headers:
        return 'Please provide X-API-Key.'
    if request.headers['X-API-Key'] != api_key:
        return 'Incorrect API Key.'
    # todo check csv

    # Load Prediction files
    if model is None:
        load_file('model.joblib')
        model = joblib.load('/tmp/model.joblib')
    if features is None:
        load_file('features.json')
        features = json.load(open('/tmp/features.json', 'r'))
    if normalization is None:
        load_file('normalization.pickle')
        normalization = pickle.load(open('/tmp/normalization.pickle', 'rb'))
    if dbc is None:
        load_file('tri93-rt.dbc')
        dbc = cantools.database.load_file('/tmp/tri93-rt.dbc')

    # Load CSV
    try:
        df = pd.read_csv(request.file.get('file'))
        for key in df.keys():
            df = df.rename(columns={key: key.replace(' ', '')})
    except Exception as e:
        print('File not loaded')
        print(e)
        return error_message

    # Data check
    if 'ID' not in df.keys():
        return "No CAN ID's provided. Please ensure an 'ID' column is present in the CSV header."
    if 'data' not in df.keys():
        return "No CAN data provided. Please ensure a 'data' column is present in the CSV header."

    # Decode
    try:
        dec_list = []
        ind_list = [m.frame_id for m in dbc.messages]
        for i in range(10000):
            row = df.iloc[i]
            if row['ID'] in ind_list:
                dec = dbc.decode_message(int(row['ID'], 0), row['data'].encode('ascii'))
                dec['ts'] = row['Recvtime']
                dec_list.append(dec)
        data = pd.DataFrame(dec_list)
    except Exception as e:
        print('Decoding failed.')
        print(e)
        return error_message

    # Convert to timeseries
    try:
        data = data.set_index(pd.to_datetime(data['ts']))
        data = data.mean(level=0)
        data = data.resample('ms').interpolate(limit_direction='both')
        data = data.resample('s').asfreq()
    except Exception as e:
        print('Conversion to timeseries failed.')
        print(e)
        return error_message

    # Cleaning Keys
    try:
        new_keys = {}
        for key in data.keys():
            new_keys[key] = re.sub('[^a-zA-Z0-9]', '_', key.lower())
        data = data.rename(columns=new_keys)
        data = data.loc[:, ~data.columns.duplicated()]
    except Exception as e:
        print('Key Cleaning Failed.')
        print(e)
        return error_message

    # Categorical Variables
    try:
        for i in range(4):
            key = 'chargererrorevent%i' % (i + 1)
            dummies = pd.get_dummies(data[key])
            for dummy_key in dummies.keys():
                dummies = dummies.rename(columns={dummy_key: key + '_' + str(dummy_key)})
            data = data.drop(key, axis=1).join(dummies)
    except Exception as e:
        print('Categorical Variables Failed')
        print(e)
        return error_message

    # Normalizing
    try:
        data = normalization.convert(data)
    except Exception as e:
        print('Normalization Failed.')
        print(e)
        return error_message

    # Decoded data check
    if not set(features).issubset(set(data.keys())):
        return 'Required features for prediction missing, please contact info@amplo.ch'

    # Select features
    try:
        for key in [x for x in features if x not in list(data.keys())]:
            data.loc[:, key] = np.zeros(len(data))
        data = data[features]
    except Exception as e:
        print('Selecting features failed.')
        print(e)
        return error_message

    # Making predictions
    try:
        predictions = model.decision_function(data.iloc[1:])
        return 'Probability of faulty ISO Board: %.2f %%' % min(100, max(0, (sum(predictions) / len(predictions) * 50 + 50)))
    except Exception as e:
        print('Making predictions failed.')
        print(e)
        return error_message

    # todo store prediction
