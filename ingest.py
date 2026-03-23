import os

import pandas as pd
import requests
import sqlalchemy
from dotenv import find_dotenv, load_dotenv
from alpaca_adapter import AlpacaAPI

load_dotenv(find_dotenv())

alpaca_api = AlpacaAPI.from_env(
    os.getenv("ALPACA_KEY_ID"),
    os.getenv("ALPACA_SECRET_KEY"),
    paper="paper" in os.getenv("ALPACA_BASE_URL", "").lower(),
)

from helper import ingest_security, parse_wiki_sp_consituents

# open sqllite db
engine = sqlalchemy.create_engine("sqlite:///securities.db")
db_session = sqlalchemy.orm.Session(bind=engine)

# Ingest  ETF Data
for ETF in ["SPY", "SPMD", "IEI", "IEF", "TLH", "TLT", "SHY"]:
    ingest_security(
        alpaca_api=alpaca_api, db_session=db_session, ticker=ETF, name=None, type="etf"
    )

# parse s&p 500 companies from wikipedia
companies = parse_wiki_sp_consituents(sources=["500", "400", "600"])

# iterate through companies
for company in companies:
    ingest_security(
        alpaca_api=alpaca_api,
        db_session=db_session,
        ticker=company["Symbol"],
        name=company["Name"],
        type="stock",
    )
