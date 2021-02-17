import argparse
import base64
import datetime
import json
import os

import jwt
import requests
from google.api_core import retry

_BASE_URL = "https://cloudiotdevice.googleapis.com/v1"
_BACKOFF_DURATION = 60
_PROJECT = os.getenv("GCP_PUBSUB_PROJECT")
_REGION = os.getenv("GCP_PUBSUB_REGION")
_REGISTRY = os.getenv("GCP_PUBSUB_REGISTRY")
_DEVICE = os.getenv("GCP_PUBSUB_DEVICE")


def create_jwt(project_id):
    token = {
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
        "aud": project_id,
    }
    private_key = os.getenv("GCP_PUBSUB_PRIVATE_KEY")
    return jwt.encode(token, private_key, algorithm="RS256").decode("ascii")


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
    parser.add_argument("--message", required=True, help="Message to publish")
    return parser.parse_args()


def main():
    args = parse_command_line_args()
    print(args.message)
    jwtToken = create_jwt(_PROJECT)
    resp = publish_message(
        args.message, _BASE_URL, _PROJECT, _REGION, _REGISTRY, _DEVICE, jwtToken
    )
    print("HTTP Reps: " + str(resp) + ", Msg: %s" % args.message)


if __name__ == "__main__":
    main()
