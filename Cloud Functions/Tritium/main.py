import datetime
import json
import logging
import os
import pickle
import re
import struct
from io import StringIO

import cantools
import joblib
import numpy as np
import pandas as pd
from google.cloud import storage

# Global (avoids reloading, makes hot calls cheaper :)
model = None
normalization = None
norm_features = None
features = None
dbc = None
api_key = None
BUCKET_ID = os.getenv("TRITIUM_BUCKET_ID")
PROJECT_ID = os.getenv("TRITIUM_PROJECT_ID")
SECRET_ID = "tritium-apikey"
PROJECT = os.getenv("TRITIUM_PROJECT_NAME")
ERROR_MESSAGE = "Internal error, please contact info@amplo.ch"
CAT_COLUMNS = os.getenv("TRITIUM_CAT_COLS")
FLOAT_COLUMNS = os.getenv("TRITIUM_FLOAT_COLS")
SKIP_COLUMNS = os.getenv("TRITIUM_SKIP_COLS")


def load_file(file_loc):
    """
    Loads file from storage bucket into local memory.
    :param file_loc: String with file location in project folder.
    :return:
    """
    global ERROR_MESSAGE, BUCKET_ID, PROJECT
    # Connect
    client = storage.Client()
    try:
        bucket = client.get_bucket(BUCKET_ID)
    except Exception as e:
        print("Connecting to bucket %s failed." % BUCKET_ID)
        logging.error(e)
        return ERROR_MESSAGE
    # Get file
    try:
        blob = bucket.blob(PROJECT + file_loc)
        blob.download_to_filename("/tmp/" + file_loc)
    except Exception as e:
        print("Loading %s into memory failed." % file_loc)
        logging.error(e)
        return ERROR_MESSAGE


def predict_iso(request):
    """
    Automated Diagnostics API for Tritium.
    Triggered with HTTPS Post. Requires API key for authentication and csv for prediction.
    """
    global model, norm_features, features, normalization, dbc, api_key, ERROR_MESSAGE, PROJECT_ID, SECRET_ID, BUCKET_ID
    sn = None

    # Load API Key
    if api_key is None:
        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(
            request={
                "name": "projects/{}/secrets/{}/versions/latest".format(
                    PROJECT_ID, SECRET_ID
                )
            }
        )
        api_key = response.payload.data.decode("utf-8")

    # Checks
    if request.method != "POST":
        return "Please use HTTPS POST."
    if "X-API-Key" not in request.headers:
        return "Please provide X-API-Key."
    if request.headers["X-API-Key"] != api_key:
        return "Incorrect API Key."

    # Load Prediction files
    try:
        if model is None:
            load_file("model.joblib")
            model = joblib.load("/tmp/model.joblib")
        if norm_features is None:
            load_file("norm_features.json")
            norm_features = json.load(open("/tmp/norm_features.json", "r"))
        if features is None:
            load_file("features.json")
            features = json.load(open("/tmp/features.json", "r"))
        if normalization is None:
            load_file("normalization.pickle")
            normalization = pickle.load(open("/tmp/normalization.pickle", "rb"))
        if dbc is None:
            load_file("tri93-rt.dbc")
            dbc = cantools.db.load_file("/tmp/tri93-rt.dbc")
    except Exception as e:
        print("Loading files failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Load CSV
    try:
        df = pd.read_csv(StringIO(request.data.decode("utf-8")))
        for key in df.keys():
            df = df.rename(columns={key: key.replace(" ", "")})
    except Exception as e:
        print("File not loaded")
        logging.error(e)
        return ERROR_MESSAGE

    # Data check
    if "ID" not in df.keys():
        return "No CAN ID's provided. Please ensure an 'ID' column is present in the CSV header."
    if "data" not in df.keys():
        return "No CAN data provided. Please ensure a 'data' column is present in the CSV header."

    # Decode
    try:
        dec_list = []
        for i in range(10000):
            # Organise data
            row = df.iloc[i]
            row_id = int(row["ID"], 0)
            can_bytes = bytes.fromhex(row["data"].strip()[2:])[::-1]

            # Skip unnecessary IDs
            if row_id not in SKIP_COLUMNS:
                continue

            # Check & Get SN
            if row_id == 807:
                if sn is None:
                    sn = dbc.decode_message(row_id, can_bytes)["OverallChargerSerial"]

            # Else Decode
            else:
                try:
                    # Decode
                    dec = dbc.decode_message(row_id, can_bytes)
                    dec["ts"] = row["Recvtime"]
                    # Check Floats
                    if row_id in FLOAT_COLUMNS:
                        if len(dbc.get_message_by_frame_id(id).signals) == 2:
                            x, y = struct.unpack("<ff", can_bytes)
                            dec[list(dec.keys())[0]] = x
                            dec[list(dec.keys())[1]] = y
                        else:
                            signal = [
                                s
                                for s in dbc.get_message_by_frame_id(id).signals
                                if s.length == 32
                            ][0]
                            x, y = struct.unpack("<ff", can_bytes)
                            if signal.start == 32:
                                dec[signal.name] = y
                            elif signal.start == 0:
                                dec[signal.name] = x
                    # Append
                    dec_list.append(dec)
                except:
                    # Bare exception is no issue. This exception is triggered for legacy messages not in the DBC.
                    pass
        data = pd.DataFrame(dec_list)
        del dec_list, row, df
    except Exception as e:
        print("Decoding failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Convert to time series
    try:
        # Change index to TS
        data["ts"] = pd.to_datetime(data["ts"])
        data = data.set_index("ts")
        # Drop duplicates
        data = data.drop_duplicates()
        data = data.loc[:, ~data.columns.duplicated()]
        # Merge rows
        new_data = pd.DataFrame(
            columns=[], index=data.index.drop_duplicates(keep="first")
        )
        for key in data.keys():
            key_series = pd.Series(data[~data[key].isna()][key])
            new_data[key] = key_series[~key_series.index.duplicated()]
        data = new_data
        # Cat cols
        for key in CAT_COLUMNS:
            if key in data.keys():
                dummies = pd.get_dummies(data[key], prefix=key).replace(0, 1)
                data = data.drop(key, axis=1).join(dummies)
        # Re-sample
        data = data.resample("ms").interpolate(limit_direction="both")
        data = data.resample("s").asfreq()
    except Exception as e:
        print("Conversion to timeseries failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Cleaning Keys
    try:
        new_keys = {}
        for key in data.keys():
            new_keys[key] = re.sub("[^a-zA-Z0-9]", "_", key.lower())
        data = data.rename(columns=new_keys)
        del new_keys
    except Exception as e:
        print("Key Cleaning Failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Normalizing
    try:
        for key in [x for x in norm_features if x not in list(data.keys())]:
            data.loc[:, key] = np.zeros(len(data))
        data = data[norm_features]
        data[data.keys()] = normalization.transform(data)
    except Exception as e:
        print("Normalization Failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Select features
    try:
        for key in [x for x in features if x not in list(data.keys())]:
            data.loc[:, key] = np.zeros(len(data))
        data = data[features]
    except Exception as e:
        print("Adding missing features failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Making predictions
    try:
        predictions = model.predict_proba(data.iloc[1:])[:, 1]
        prediction = sum(predictions) / len(predictions) * 100
    except Exception as e:
        print("Making predictions failed.")
        logging.error(e)
        return ERROR_MESSAGE

    # Store file
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(BUCKET_ID)
        if prediction > 80:
            blob = bucket.blob(
                "tritium/logs/ISO Fault/"
                + str(sn)
                + datetime.datetime.now().strftime("_%Y_%m_%d_%H_%M")
                + ".csv"
            )
        else:
            blob = bucket.blob(
                "tritium/logs/Random/"
                + str(sn)
                + datetime.datetime.now().strftime("_%Y_%m_%d_%H_%M")
                + ".csv"
            )
        blob.upload_from_string(request.data.decode("utf-8"), content_type="text/csv")
    except Exception as e:
        print("Storing predictions failed.")
        logging.error(e)
        return ERROR_MESSAGE

    return "\n\nProbability of faulty ISO Board: %.2f %%\n\n" % prediction
