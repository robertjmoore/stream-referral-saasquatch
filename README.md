# stream-github

A data stream generator for the Wootric API, written in python 3.

## Limitations

### Creation Date Clustering

The Wootric API limits results to 50 per request and only allows sorting by created_at. For instances where more than 50 records have an identical created_at date and time, it is therefore impossible to access certain data points. In this cse, the streamer increments its bookmark by one second and proceeds with its replication in order to avoid a crash or infinite loop.

### Record Updates

The Wootric API does not allow you to order results by updated_at or filter based on updated_at date. As a result, there is no way to incrementally upsert updated records. To capture any changes in records that have been previously updated, conduct a full replication.

## Install

Clone this repository, and then:

```bash
› pip install -r requirements.txt
```

## Run

#### Run the application

`stream-wootric` can be run with:

```bash

WOOTRIC_CLIENT_ID=<clientid> WOOTRIC_CLIENT_SECRET=<clientsecret> python stream_wootric.py [FILENAME]

```

Where `clientid` and `clientsecret` are the 64-character strings retrieved from the API section of your Wootric account settings page.
