import argparse
import base64
import datetime
import json

import jwt
import requests
from google.api_core import retry

_BASE_URL = "https://cloudiotdevice.googleapis.com/v1"
_BACKOFF_DURATION = 60


def create_jwt(project_id, private_key_file, algorithm):
    token = {
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
        "aud": project_id,
    }
    with open(private_key_file, "r") as f:
        private_key = f.read()
    return jwt.encode(token, private_key, algorithm=algorithm).decode("ascii")


@retry.Retry(
    predicate=retry.if_exception_type(AssertionError), deadline=_BACKOFF_DURATION
)
def publish_message(
    message, base_url, project_id, cloud_region, registry_id, device_id, jwt_token
):
    headers = {
        "authorization": "Bearer {}".format(jwt_token),
        "content-type": "application/json",
        "cache-control": "no-cache",
    }
    url_suffix = "publishEvent"
    publish_url = ("{}/projects/{}/locations/{}/registries/{}/devices/{}:{}").format(
        base_url, project_id, cloud_region, registry_id, device_id, url_suffix
    )
    msg_bytes = base64.urlsafe_b64encode(message.encode("utf-8"))
    body = {"binary_data": msg_bytes.decode("ascii")}
    resp = requests.post(publish_url, data=json.dumps(body), headers=headers)
    if resp.status_code != 200:
        print("Response came back {}, retrying".format(resp.status_code))
    return resp


@retry.Retry(
    predicate=retry.if_exception_type(AssertionError), deadline=_BACKOFF_DURATION
)
def get_config(
    version, base_url, project_id, cloud_region, registry_id, device_id, jwt_token
):
    headers = {
        "authorization": "Bearer {}".format(jwt_token),
        "content-type": "application/json",
        "cache-control": "no-cache",
    }
    basepath = "{}/projects/{}/locations/{}/registries/{}/devices/{}/"
    template = basepath + "config?local_version={}"
    config_url = template.format(
        base_url, project_id, cloud_region, registry_id, device_id, version
    )
    resp = requests.get(config_url, headers=headers)
    if resp.status_code != 200:
        print("Error getting config: {}, retrying".format(resp.status_code))
        raise AssertionError("Not OK response: {}".format(resp.status_code))
    return resp


def parse_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--projectId", required=True, help="GCP cloud project name")
    parser.add_argument(
        "--registryId", required=True, help="Cloud IoT Core registry id"
    )
    parser.add_argument("--deviceId", required=True, help="Cloud IoT Core device id")
    parser.add_argument(
        "--privateKeyFile", required=True, help="Path to private key file."
    )
    parser.add_argument(
        "--algorithm",
        choices=("RS256", "ES256"),
        default="RS256",
        help="JWT encryption algorithm.",
    )
    parser.add_argument(
        "--cloudRegion", default="europe-west1", help="GCP cloud region"
    )
    parser.add_argument(
        "--caCerts",
        default="roots.pem",
        help=("CA root from https://pki.google.com/roots.pem"),
    )
    parser.add_argument("--message", required=True, help="Message to publish")
    parser.add_argument(
        "--baseUrl",
        default=_BASE_URL,
        help=("Base URL for the Cloud IoT Core Device Service API"),
    )
    return parser.parse_args()


def main():
    args = parse_command_line_args()
    print(args.message)
    jwtToken = create_jwt(args.projectId, args.privateKeyFile, args.algorithm)
    resp = publish_message(
        args.message,
        args.baseUrl,
        args.projectId,
        args.cloudRegion,
        args.registryId,
        args.deviceId,
        jwtToken,
    )
    print("HTTP response: ", resp)


if __name__ == "__main__":
    main()
