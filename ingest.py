
import os
import requests
import pandas as pd

import sqlalchemy

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import intrinio_sdk
from intrinio_sdk.rest import ApiException
intrinio_sdk.ApiClient().configuration.api_key['api_key'] = os.getenv('INTRINIO_PROD_KEY')
security_api = intrinio_sdk.SecurityApi()

from helper import ( ingest_security, parse_wikipedia )

# open sqllite db
engine = sqlalchemy.create_engine('sqlite:///securities.db')
db_session = sqlalchemy.orm.Session(bind=engine)

# Ingest  ETF Data
for ETF in ['SPY', 'IEF', 'GLD']:
    ingest_security(intrinio_security =  security_api, db_session = db_session, ticker = ETF, name = None, type='etf' )

# parse s&p 500 companies from wikipedia
companies = parse_wikipedia()

# iterate through companies
for company in companies:
    ingest_security(intrinio_security =  security_api, db_session = db_session, ticker = company['Symbol'], name = company['Name'], type = 'stock' )
