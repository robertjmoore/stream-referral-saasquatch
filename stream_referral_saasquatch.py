import os
import argparse
import logging
import requests
import stitchstream
import sys
import json
import time
import datetime
import csv

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

def run_and_get_id_of_completed_export(exportType, tenant_alias, api_key):
    #this function is a multistep process of requesting an export, waiting for it to be generated
    #and then eventually returning the URL of the results once the generation is complete
    startingTimestamp = bookmark.get(exportType, None) 
    createUrl = 'https://app.referralsaasquatch.com/api/v1/' + tenant_alias + '/export'
    data = {'type' : exportType,
            'format' : 'CSV',
            'name' : 'Stitch Streams ' + exportType + ' Request ' + str(datetime.datetime.now()),
            'params' : {}
    }
    auth = ('', api_key)
    headers = { 'Content-Type' : 'application/json' }
    if startingTimestamp:
        data['params'] = { 'createdOrUpdatedSince' : startingTimestamp }
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

def cleanse_value(name, value):
    if name == "amount":
        value = int(value)
    elif "date" in name: #consistent pattern that all dates have "date" in their name
        if value != '':
            #Referral SaaSquatch returns timestamps in ms, so we convert to timestamp in seconds
            valueSeconds = int(int(value)/1000)
            dt = datetime.datetime.utcfromtimestamp(valueSeconds)
            value = dt.isoformat() + '+00:00'
        else:
            value = None
    return value

def stream_export_contents_to_stitch(streamName, exportId, tenant_alias, api_key):
    downloadUrl = 'https://app.referralsaasquatch.com/api/v1/' + tenant_alias + '/export/' + exportId + '/download'
    auth = ('', api_key)
    headers = { 'Content-Type' : 'application/json' }
    r = requests.get(downloadUrl, stream=True, auth=auth, headers=headers)

    #iterate through the rest of the lines, persisting to Stitch
    headerMapping = None
    for line in r.iter_lines():
        text = line.decode('utf-8')
        csvReader = csv.reader([text])
        for csvLine in csvReader:
            if headerMapping is None: #first line contains column names, save it
                headerMapping = csvLine
            else: #bundle up the row and write to Stitch
                dataRows = [{}]
                for index, value in enumerate(csvLine, start=0):
                    dataRows[0][headerMapping[index]] = cleanse_value(headerMapping[index], value)
                stitchstream.write_records(streamName, dataRows)

def current_utc_timestamp_ms():
    dts = time.time()
    return int(dts)*1000

def update_stream(streamType, tenant_alias, api_key):
    global bookmark
    if bookmark.get(streamType, None):
        logger.info('Replicating ' + streamType + ' since %s', bookmark.get(streamType, None))
    else:
        logger.info('Replicating all ' + streamType)

    processStartTimestamp = current_utc_timestamp_ms() #must bookmark in utc timestamp in ms
    exportId = run_and_get_id_of_completed_export(streamType, tenant_alias, api_key)
    stream_export_contents_to_stitch(streamType, exportId, tenant_alias, api_key)
    bookmark[streamType] = processStartTimestamp #we don't get timestamps in the data so we bookmark by when the process ran

user_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'string',
                         'key': True
                     },
                     'accountId': {
                         'type': 'string',
                     },
                     'email': {
                         'type': 'string',
                     },
                     'firstName': {
                         'type': 'string'
                     },
                     'lastName': {
                         'type': 'string'
                     },
                     'imageUrl': {
                         'type': 'string'
                     },
                     'firstSeenIP': {
                         'type': 'string'
                     },
                     'lastSeenIP': {
                         'type': 'string'
                     },
                     'dateCreated': {
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
                     'emailHash': {
                         'type': 'string'
                     },
                     'referralSource': {
                         'type': 'string'
                     },
                     'locale': {
                         'type': 'string'
                     },
                     'shareLink': {
                         'type': 'string'
                     },
                     'facebookShareLink': {
                         'type': 'string'
                     },
                     'twitterShareLink': {
                         'type': 'string'
                     },
                     'emailShareLink': {
                         'type': 'string'
                     },
                     'linkedinShareLink': {
                         'type': 'string'
                     }
                 },
                 'required': ['id']
             }

reward_balance_schema = {'type': 'object',
                 'properties': {
                     'userId': {
                         'type': 'string',
                         'key': True
                     },
                     'accountId': {
                         'type': 'string',
                     },
                     'type': {
                         'type': 'string',
                     },
                     'amount': {
                         'type': 'integer'
                     },
                     'unit': {
                         'type': 'string'
                     }
                 },
                 'required': ['userId']
             }

referral_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'string',
                         'key': True
                     },
                     'referredUser': {
                         'type': 'string',
                     },
                     'referredAccount': {
                         'type': 'string',
                     },
                     'referrerUser': {
                         'type': 'string'
                     },
                     'referrerAccount': {
                         'type': 'string'
                     },
                     'referredReward': {
                         'type': 'string'
                     },
                     'referrerReward': {
                         'type': 'string'
                     },
                     'dateReferralStarted': {
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
                     'dateReferralPaid': {
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
                     'dateReferralEnded': {
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
                     'dateModerated': {
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
                     'referredModerationStatus': {
                         'type': 'string'
                     },
                     'referrerModerationStatus': {
                         'type': 'string'
                     },
                 },
                 'required': ['id']
             }

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

    stitchstream.write_schema('USER', user_schema)
    update_stream('USER', tenant_alias, api_key)

    stitchstream.write_schema('REWARD_BALANCE', reward_balance_schema)
    update_stream('REWARD_BALANCE', tenant_alias, api_key)

    stitchstream.write_schema('REFERRAL', referral_schema)
    update_stream('REFERRAL', tenant_alias, api_key)

    stitchstream.write_bookmark(bookmark)
