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

## Testing

A python script that pre-populates an account with fictional users and referral activity can be run with:

```bash

REFERRAL_SAASQUATCH_TENANT_ALIAS=<tenantalias> REFERRAL_SAASQUATCH_API_KEY=<apikey> python seed_referral_saasquatch.py

```

The web interface for Referral SaaSquatch allows for the manual modification/update of users and referrals, creating the ability to modify the seed data and test incremental updates.
