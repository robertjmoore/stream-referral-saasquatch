import os
import argparse
import logging
import requests
import stitchstream
import sys
import json
import datetime

session = requests.Session()
logger = logging.getLogger()
bookmark = {}

def get_env_or_throw(key):
    value = os.environ.get(key)

    if value == None:
        raise Exception('Missing ' + key + ' environment variable!')

    return value

def configure_logging(level=logging.DEBUG):
    global logger
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def authed_get(url):
    return session.request(method='get', url=url)

def authed_get_all_pages(url):
    while True:
        r = authed_get(url)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break

response_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                         'key': True
                     },
                     'created_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'updated_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'score': {
                         'type': 'integer',
                     },
                     'text': {
                         'type': 'string',
                     },
                     'ip_address': {
                         'type': 'string',
                     },
                     'origin_url': {
                         'type': 'string',
                     },
                     'end_user_id': {
                         'type': 'integer',
                     },
                     'survey_id': {
                         'type': 'integer',
                     },
                     'completed': {
                         'type': 'boolean',
                     },
                     'excluded_from_calculations': {
                         'type': 'boolean',
                     },
                     'tags': {
                         'type': 'array',
                         'items': {
                             "type": "string"
                         }
                     },
                 },
                 'required': ['id']
             }

def get_all_responses(since_date):
    global bookmark
    query_string = '&created[gt]={}'.format(since_date) if since_date else ''

    last_response_time = None
    requestUrl = 'https://api.wootric.com/v1/responses?per_page=50&sort_order=asc{}'.format(query_string)
    logger.info(requestUrl)
    for response in authed_get_all_pages(requestUrl):
        responses = response.json()
        if len(responses) > 0:
            last_created_at_string = responses[-1]['created_at']
            last_created_at_datetime = datetime.datetime.strptime(last_created_at_string, '%Y-%m-%d %H:%M:%S %z')
            last_response_time = last_created_at_datetime.datetime.total_seconds()
            logger.info('lct is ' + last_response_time)
        stitchstream.write_records('responses', responses)

    bookmark['responses'] = last_response_time

def get_access_token(client_id, client_secret):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post('https://api.wootric.com/oauth/token', data=data).json();
    if 'access_token' in response:
        return response['access_token']
    raise Exception('Access Token Retrieval Failed: ' + str(response))


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(prog='Wootric Streamer')
    parser.add_argument('FILENAME', help='File containing the last bookmark value', nargs='?')
    args = parser.parse_args()

    client_id = get_env_or_throw('WOOTRIC_CLIENT_ID')
    client_secret = get_env_or_throw('WOOTRIC_CLIENT_SECRET')
    access_token = get_access_token(client_id, client_secret)
    session.headers.update({'authorization': 'Bearer ' + access_token})

    bookmark = {}
    if args.FILENAME:
        with open(args.FILENAME, 'r') as file:
            for line in file:
                bookmark = json.loads(line.strip())

    if bookmark:
        logger.info('Replicating responses since %s', bookmark)
    else:
        logger.info('Replicating all responses')

    stitchstream.write_schema('responses', response_schema)
    get_all_responses(bookmark.get('responses', None))
    stitchstream.write_bookmark(bookmark)
