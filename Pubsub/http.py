import traceback, logging
import sys
sys.path.append('/home/niels/.local/lib/python3.7/site-packages')
import argparse
import base64
import datetime
import json
import time
from google.api_core import retry
import jwt
import requests



_BASE_URL = 'https://cloudiotdevice.googleapis.com/v1'
_BACKOFF_DURATION = 60
_PROJECT = 'responder-289707'
_REGION = 'europe-west1'
_REGISTRY = 'gateways'
_DEVICE = 'rpi-01'


def create_jwt(project_id):
    token = {
            'iat': datetime.datetime.utcnow(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'aud': project_id
    }
    private_key = '-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDX1StzU1U7MdYV\ndA48IH8Vi1jNz2Fi7egYfxOzRIIGXDCQuEIPXtnDplQwu9MiPCJPf33X+iSP+sI1\n6/Mr5sAtyRvua+5goNl1+usTWov+KHjxzEyV9bdgkSq+dnIV7mUVdx1stHU2UKwb\nLwivRLSkM0iOkM3p+zXoRgavH5IMG0hWLKcmbELI5Nxcwn0Wx1kOlQQvBeLZCZrK\nDgtFyycDsqwW8MveMBDvB6lzDrJ+/wgkX6JIQdlYLY0R0ptVou5d0/YSVzdBpd0E\nCsNFIC59H61PTVN6Mwi3bSWEB7xtY8LLqoJKvNK5faS97zS37jamYQm20cMFPqna\nf9h1DfaTAgMBAAECggEARWBmKRneSbrJP/ggIz+m2fwCvZUtjqk+c7FVWchpqzWy\n/rHbuikZAoTShx+4zEZcGQW4I9ZqLkXCa6a+cZwopg8BBXB4HWNWw0+2hHAUk8va\npI6xB3sGSOogvTxBi78nivDQ6oJPMvhXeh1yQzRohGdfqUPujImNWG4588zIG0+K\nY4Uv3WNRt55GWKWTVn+nCjWnE6wKdEKPzPZ3SScRXSAJzq7vfxRkusvO5ddVgNp4\nefhj0pbVKPGoFht5XNcdd02ygp8dDjUGzji3ei3nirnc3GfLXDlFyFc7pF2bc/Gq\nOcRciR2x1QH4IkNED0LgOzy5rUfShH7M1ReMakXGIQKBgQD4uCKkkuO36gX5D0cV\nN9t3i2ZeOOh3EwSAXSULxLwfUAfyAhDMcODsOI0Nm0ADQ1wey/mrZU4c8r2VorIi\noL4cHV5UowL8fw2Gi/mj/geszE5PL2HYnaXMXzkVdiSDR7cCcXXDaj/GEzGpQHWy\naSgcN48XiQAu4D5EnBqcGemmCQKBgQDeJpZaI3/NvtAirYKmELWdSotOU8uidIOu\nQGe+TLPPC6gdbgoXUXUcBYkAM2iQju2uNPncNmfwaSVjOktHPjV1ysl1leeqOqic\nCa5szGO1VIpnQou/QR3GX/fu9NB8ktAjAvIM2jDX08Xnjrz5fauX19QZFyxC8Got\nWDPILq++uwKBgHIFF/ySSkqZwjs3QcL8ZGQdR8SSGh/cXAfsq3sFqahBLCNJxyGx\n7ardEezW6zWTv7tujvp/6ptivH7Ioxk0z3JcFE6AnHHcXPr9WGhRoHsa/htXAWgo\nfUV/sc+g5YQ5cDByiyYWoz3Otsl8f3hWtMiav8JuT+Mtcd5KIfjF+FLhAoGBAIOg\n+ZWPUzrlxQ3HIZKkc/gvIzvWCuLHhv7Iyq/HYwNbNNG7Ud9fNLTV0sd7rol5JvwC\nB8qjshKROsYA1HnyMlsJPTWfDRWgjCdo1SFCOhJHdXqZw5QfUTUpyA6eoKovk4Qr\nSqCy6B36LVl3CiIKhJIEIHh9cox3R3J2wLfscdXhAoGBAL3HkvbmYloglv1W85Ix\npCPBTcpG0Yl2ycO3gj5oGPghUnEnOPcz/J91T/TTaUs0bhXhUgn9wL1se974cX9G\nYAkHAOEN5MhtathlkWhdhR3geE7dqRWKt4+2+e/dOmfqlSCUvvcBenajpBHfX0Wv\ntLRNXG8cbmPlCp2+USLAne8i\n-----END PRIVATE KEY-----'
    return jwt.encode(token, private_key, algorithm='RS256').decode('ascii')


@retry.Retry(predicate=retry.if_exception_type(AssertionError), deadline=_BACKOFF_DURATION)
def publish_message(message, base_url, project_id, cloud_region, registry_id, device_id, jwt_token):
    headers = {
            'authorization': 'Bearer {}'.format(jwt_token),
            'content-type': 'application/json',
            'cache-control': 'no-cache'
    }
    url_suffix = 'publishEvent'
    publish_url = (
        '{}/projects/{}/locations/{}/registries/{}/devices/{}:{}').format(
            base_url, project_id, cloud_region, registry_id, device_id,
            url_suffix)
    msg_bytes = base64.urlsafe_b64encode(message.encode('utf-8'))
    body = {'binary_data': msg_bytes.decode('ascii')}
    resp = requests.post(publish_url, data=json.dumps(body), headers=headers)
    if resp.status_code != 200:
        print('Response came back {}, retrying'.format(resp.status_code))
    return resp


@retry.Retry(
    predicate=retry.if_exception_type(AssertionError),
    deadline=_BACKOFF_DURATION)
def get_config(
        version, base_url, project_id, cloud_region, registry_id,
        device_id, jwt_token):
    headers = {
            'authorization': 'Bearer {}'.format(jwt_token),
            'content-type': 'application/json',
            'cache-control': 'no-cache'
    }
    basepath = '{}/projects/{}/locations/{}/registries/{}/devices/{}/'
    template = basepath + 'config?local_version={}'
    config_url = template.format(
        base_url, project_id, cloud_region, registry_id, device_id, version)
    resp = requests.get(config_url, headers=headers)
    if resp.status_code != 200:
        print('Error getting config: {}, retrying'.format(resp.status_code))
        raise AssertionError('Not OK response: {}'.format(resp.status_code))
    return resp


def parse_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--message', required=True, help='Message to publish')
    return parser.parse_args()


def main():
    args = parse_command_line_args()
    print(args.message)
    jwtToken = create_jwt(_PROJECT)
    resp = publish_message(args.message, _BASE_URL, _PROJECT, _REGION, _REGISTRY, _DEVICE, jwtToken)
    print('HTTP Reps: '+str(resp)+', Msg: %s' % args.message)


if __name__ == '__main__':
    main()
