# stream-referral-saasquatch

A data stream generator for the Referral SaaSquatch API, written in python 3.

## Install

Clone this repository, and then:

```bash
› pip install -r requirements.txt
```

## Run

#### Run the application

`stream-referral-saasquatch` can be run with:

```bash

REFERRAL_SAASQUATCH_TENANT_ALIAS=<tenantalias> REFERRAL_SAASQUATCH_API_KEY=<apikey> python stream_referral_saasquatch.py [FILENAME]

```

Where `clientid` and `clientsecret` are the 64-character strings retrieved from the API section of your Wootric account settings page.
