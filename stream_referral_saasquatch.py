import os
import argparse
import logging
import requests
import stitchstream
import sys
import json
import time
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

def is_export_completed(tenant_alias, api_key, exportId):
    auth = ('', api_key)
    headers = { 'Content-Type' : 'application/json' }
    lookupUrl = 'https://app.referralsaasquatch.com/api/v1/' + tenant_alias + '/export/' + exportId
    rLookup = requests.get(lookupUrl, auth=auth, headers=headers)
    resultLookupJson = rLookup.json()
    if resultLookupJson['status'] == 'COMPLETED':
        return True
    return False

def get_id_of_completed_export(exportType, startingTimestamp, tenant_alias, api_key):
    #this function is a multistep process of requesting an export, waiting for it to be generated
    #and then eventually returning the URL of the results once the generation is complete
    createUrl = 'https://app.referralsaasquatch.com/api/v1/' + tenant_alias + '/export'
    data = {'type' : exportType,
            'format' : 'CSV',
            'name' : 'Stitch Streams ' + exportType + ' Request ' + str(datetime.datetime.now()),
    }
    auth = ('', api_key)
    headers = { 'Content-Type' : 'application/json' }
    if startingTimestamp:
        data.params = { 'createdOrUpdatedSince' : startingTimestamp }

    r = requests.post(createUrl, auth=auth, json=data, headers=headers)
    resultJson = r.json()

    if resultJson['id']:
        waited = 0;
        while True:
            if is_export_completed(tenant_alias, api_key, resultJson['id']):
                return resultJson['id']
            if(waited > 3600):
                raise Exception('Waited over an hour, ' + exportType + ' export ' + resultJson['id'] + ' never completed')
            time.sleep(5)
            waited += 5
    else:
        raise Exception('Request to create ' + exportType + ' export failed')

user_schema = {'type': 'object',
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
                     'email': {
                         'type': 'string'
                     },
                     'last_surveyed': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "string",
                                 'format': 'date-time'
                             }
                         ]
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

def get_all_new_users(tenant_alias, api_key):
    global bookmark
    
    startingTimestamp = bookmark.get('users', None) 
    exportId = get_id_of_completed_export('USER', startingTimestamp, tenant_alias, api_key)

    logger.info('GOT EXPORT FILE ID: ' + exportId)

    #UP NEXT: STREAM IN THE FILE IN PIECES AND PUSH IT TO STITCH...

    return exportId

    last_user_unixtime = None
    url = 'https://app.referralsaasquatch.com/api/v1/' + tenant_alias + '/users'
    r = requests.get(url, auth=('', api_key))
    logger.info(r.json())
    requestUrl = '/api/v1/{tenant_alias}/export'
    for response in authed_get_all_pages(requestUrl, 'users'):
        users = response.json()
        if len(users) > 0:
            last_created_at_datetime = wootricdate_to_datetime(users[-1]['created_at'])
            last_user_unixtime = int(last_created_at_datetime.timestamp())

        for index, item in enumerate(users):
            users[index]['created_at'] = wootricdate_to_datetime(users[index]['created_at']).isoformat()
            users[index]['updated_at'] = wootricdate_to_datetime(users[index]['updated_at']).isoformat()
            if users[index]['last_surveyed']:
                users[index]['last_surveyed'] = wootricdate_to_datetime(users[index]['last_surveyed']).isoformat()

        stitchstream.write_records('users', users)

        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if bookmark.get('users', None) == str(last_user_unixtime) and len(users) > 1:
            logger.error('Breaking retrieval loop for enduers at unixtime ' + str(last_user_unixtime) + ', will cause missing data')
            last_user_unixtime = last_user_unixtime + 1

        if last_user_unixtime: #can be None if no new users
            bookmark['users'] = str(last_user_unixtime)


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(prog='Referral SaaSquatch Streamer')
    parser.add_argument('FILENAME', help='File containing the last bookmark value', nargs='?')
    args = parser.parse_args()

    tenant_alias = get_env_or_throw('REFERRAL_SAASQUATCH_TENANT_ALIAS')
    api_key = get_env_or_throw('REFERRAL_SAASQUATCH_API_KEY')

    bookmark = {}
    if args.FILENAME:
        with open(args.FILENAME, 'r') as file:
            for line in file:
                bookmark = json.loads(line.strip())

    if bookmark.get('users', None):
        logger.info('Replicating users since %s', bookmark.get('users', None))
    else:
        logger.info('Replicating all users')
    #stitchstream.write_schema('users', user_schema)
    get_all_new_users(tenant_alias, api_key)

    stitchstream.write_bookmark(bookmark)
