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

def authed_get_all_pages(baseUrl, bookmarkName):
    global bookmark
    while True:
        url = baseUrl
        if bookmark.get(bookmarkName, None):
            url = baseUrl + '&created[gt]=' + bookmark[bookmarkName]
        r = authed_get(url)
        rJson = r.json();
        yield r
        if len(rJson) <= 1:
            break

response_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                         'key': True
                     },
                     'created_at': {
                         'type': 'string'
                     },
                     'updated_at': {
                         'type': 'string'
                     },
                     'score': {
                         'type': 'integer'
                     },
                     'text': {
                         'type': ['string','null']
                     },
                     'ip_address': {
                         'type': 'string'
                     },
                     'origin_url': {
                         'type': 'string'
                     },
                     'end_user_id': {
                         'type': 'integer'
                     },
                     'survey_id': {
                         'type': 'integer'
                     },
                     'completed': {
                         'type': 'boolean'
                     },
                     'excluded_from_calculations': {
                         'type': 'boolean'
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
    
    last_response_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/responses?per_page=50&sort_order=asc'
    for response in authed_get_all_pages(requestUrl, 'responses'):
        responses = response.json()
        if len(responses) > 0:
            last_created_at_string = responses[-1]['created_at']
            last_created_at_datetime = datetime.datetime.strptime(last_created_at_string, '%Y-%m-%d %H:%M:%S %z')
            last_response_unixtime = int(last_created_at_datetime.timestamp())
        stitchstream.write_records('responses', responses)

        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if bookmark.get('responses', None) == str(last_response_unixtime):
            logger.error('Breaking retrieval loop for responses at unixtime ' + str(last_response_unixtime) + ', will cause missing data')
            last_response_unixtime = last_response_unixtime + 1

        bookmark['responses'] = str(last_response_unixtime)


decline_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                         'key': True
                     },
                     'created_at': {
                         'type': 'string'
                     },
                     'updated_at': {
                         'type': 'string'
                     },
                     'end_user_id': {
                         'type': 'integer'
                     },
                     'survey_id': {
                         'type': 'integer'
                     }
                 },
                 'required': ['id']
             }

def get_all_declines(since_date):
    global bookmark
    
    last_decline_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/declines?per_page=50&sort_order=asc'
    for response in authed_get_all_pages(requestUrl, 'declines'):
        declines = response.json()
        if len(declines) > 0:
            last_created_at_string = declines[-1]['created_at']
            last_created_at_datetime = datetime.datetime.strptime(last_created_at_string, '%Y-%m-%d %H:%M:%S %z')
            last_decline_unixtime = int(last_created_at_datetime.timestamp())
        stitchstream.write_records('declines', declines)
        
        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if bookmark.get('declines', None) == str(last_decline_unixtime):
            logger.error('Breaking retrieval loop for declines at unixtime ' + str(last_decline_unixtime) + ', will cause missing data')
            last_decline_unixtime = last_decline_unixtime + 1

        bookmark['declines'] = str(last_decline_unixtime)

enduser_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                         'key': True
                     },
                     'created_at': {
                         'type': 'string'
                     },
                     'updated_at': {
                         'type': 'string'
                     },
                     'email': {
                         'type': 'string'
                     },
                     'last_surveyed': {
                         'type': ['string','null']
                     },
                     'external_created_at': {
                         'type': ['integer','null']
                     },
                     'page_views_count': {
                         'type': 'integer'
                     }
                 },
                 'required': ['id']
             }

def get_all_endusers(since_date):
    global bookmark
    
    last_enduser_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/end_users?per_page=50&sort_order=asc'
    for response in authed_get_all_pages(requestUrl, 'endusers'):
        endusers = response.json()
        if len(endusers) > 0:
            last_created_at_string = endusers[-1]['created_at']
            last_created_at_datetime = datetime.datetime.strptime(last_created_at_string, '%Y-%m-%d %H:%M:%S %z')
            last_enduser_unixtime = int(last_created_at_datetime.timestamp())
        stitchstream.write_records('endusers', endusers)

        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if bookmark.get('endusers', None) == str(last_enduser_unixtime):
            logger.error('Breaking retrieval loop for enduers at unixtime ' + str(last_enduser_unixtime) + ', will cause missing data')
            last_enduser_unixtime = last_enduser_unixtime + 1

        bookmark['endusers'] = str(last_enduser_unixtime)

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

    if bookmark.get('endusers', None):
        logger.info('Replicating endusers since %s', bookmark.get('endusers', None))
    else:
        logger.info('Replicating all endusers')
    stitchstream.write_schema('endusers', enduser_schema)
    get_all_endusers(bookmark.get('endusers', None))

    if bookmark.get('responses', None):
        logger.info('Replicating responses since %s', bookmark.get('responses', None))
    else:
        logger.info('Replicating all responses')
    stitchstream.write_schema('responses', response_schema)
    get_all_responses(bookmark.get('responses', None))

    if bookmark.get('declines', None):
        logger.info('Replicating declines since %s', bookmark.get('declines', None))
    else:
        logger.info('Replicating all declines')
    stitchstream.write_schema('declines', decline_schema)
    get_all_declines(bookmark.get('declines', None))

    stitchstream.write_bookmark(bookmark)
