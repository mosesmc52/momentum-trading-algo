
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

from helper import ( ingest_security )

# open sqllite db
engine = sqlalchemy.create_engine('sqlite:///securities.db')
db_session = sqlalchemy.orm.Session(bind=engine)

# Ingest  ETF Data
for ETF in ['SPY', 'IEF', 'GLD']:
    ingest_security(intrinio_security =  security_api, db_session = db_session, ticker = ETF, name = None, type='etf' )

# write s&p 500 companies
resp = requests.get('https://datahub.io/core/s-and-p-500-companies/r/0.csv')
if resp.status_code == 200:
    with open('s-and-p-500-companies.csv', 'w') as f:
        f.write(resp.text)
else:
    print("S & P 500 Companies not found")
    raise

# read s&p 500 companies into pandas dataframe
companies = pd.read_csv('s-and-p-500-companies.csv')

# iterate through companies
for _, company in companies.iterrows():
    ingest_security(intrinio_security =  security_api, db_session = db_session, ticker = company['Symbol'], name = company['Name'], type = 'stock' )
