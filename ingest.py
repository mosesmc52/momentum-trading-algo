
import os
import requests
import pandas as pd

import sqlalchemy

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
alpaca_api = tradeapi.REST(os.getenv('ALPACA_KEY_ID'), os.getenv('ALPACA_SECRET_KEY'), base_url=os.getenv('ALPACA_BASE_URL'))

from helper import ( ingest_security, parse_wiki_sp_consituents )

# open sqllite db
engine = sqlalchemy.create_engine('sqlite:///securities.db')
db_session = sqlalchemy.orm.Session(bind=engine)

# Ingest  ETF Data
for ETF in ['SPY', 'IEI', 'IEF', 'TLH','TLT', 'SHY']:
    ingest_security(alpaca_api =  alpaca_api, db_session = db_session, ticker = ETF, name = None, type='etf' )

# parse s&p 500 companies from wikipedia
companies = parse_wiki_sp_consituents(sources = ['500', '400', '600'])

# iterate through companies
for company in companies:
    ingest_security(alpaca_api =  alpaca_api, db_session = db_session, ticker = company['Symbol'], name = company['Name'], type = 'stock' )
