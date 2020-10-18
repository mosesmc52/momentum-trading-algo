import os
import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
import configparser

import models
import sqlalchemy

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from helper import ( momentum_quality, momentum_score, volatility, history, TMOM )
from log import log

# constants
DAYS_IN_YEAR = 365

# initialize Alpaca Trader
api = tradeapi.REST(os.getenv('ALPACA_KEY_ID'), os.getenv('ALPACA_SECRET_KEY'), base_url=os.getenv('ALPACA_BASE_URL')) # or use ENV Vars shown below
account = api.get_account()


# open sqllite db
engine = sqlalchemy.create_engine('sqlite:///securities.db')
db_session = sqlalchemy.orm.Session(bind=engine)

# retreive configuration parameters
config = configparser.ConfigParser()
config.read('algo_settings.cfg')

# read S&P etf
market_history = history(db_session = db_session, tickers = [config['model']['market']],  days = config['model']['trend_window_days'])
cash_history = history(db_session = db_session, tickers = [config['model']['cash']],  days = config['model']['trend_window_days'])

is_bull_market =  (market_history['close'].tail(1).iloc[0] > market_history['close'].mean()) and (TMOM(market_history['close']) > TMOM(cash_history['close']))
if is_bull_market:
    log('Bull Market', 'success')
else:
    log('Bear Market', 'warning')

# read s&p 500 companies into pandas dataframe
companies = pd.read_csv('s-and-p-500-companies.csv')

mom_equities = pd.DataFrame(columns=['ticker','inf_discr', 'score'])
for _, company in companies.iterrows():

    # calculate inference
    equity_history = history(db_session = db_session, tickers = [company['Symbol']],  days = DAYS_IN_YEAR)
    if not len(equity_history):
        log('{0}, no data'.format(company['Symbol']))
        continue


    inf_discr, is_quality = momentum_quality(equity_history['close'], min_inf_discr = config['model']['min_inf_discr'])
    if not is_quality:
        log('{0}, quality failed'.format(company['Symbol']))
        continue

    # calculate momentum score
    data_end = -1 * (int(config['model']['score_exclude_days']) + 1) # exclude most recent data
    momentum_start = -1 * (int(config['model']['score_window_days']) + int(config['model']['score_exclude_days']))
    momentum_hist = equity_history[momentum_start:data_end]
    score = momentum_score(equity_history['close']).mean()
    if score <= float(config['model']['minimum_score_momentum']):
        log('{0}, score {0} less than minimum'.format(company['Symbol'], score))
        continue

    log(company['Symbol'], 'success')
    mom_equities = mom_equities.append({'ticker': company['Symbol'],
                 'inf_discr': inf_discr,
                 'score': score}, ignore_index=True)

mom_equities = mom_equities.set_index(['ticker'])
ranking_table = mom_equities.sort_values(by=['inf_discr', 'score'], ascending=[True, False])
print('ticker count: {0}'.format(len(ranking_table)))
print(ranking_table)


# https://github.com/alpacahq/alpaca-trade-api-python
kept_positions =  []
for position in api.list_positions():
    if (position['symbol'] in ['IEF', 'GLD']) or \
        ( position['symbol'] not in filter.index.tolist() and today.month in [3, 6, 9, 12]):
        #https://alpaca.markets/docs/api-documentation/api-v2/orders/
        #api.submit_order(
        #    symbol=position['symbol'],
        #    side='sell',
        #    type='market',
        #    qty=position['qty'],
        #)
        pass
    else:
        kept_positions.append(position['symbol'])


replacement_stocks = int(config['model']['portfolio_size']) - len(kept_positions)

buy_list = ranking_table.loc[
    ~ranking_table.index.isin(kept_positions)][:replacement_stocks]

new_portfolio = pd.concat(
    (buy_list,
     ranking_table.loc[ranking_table.index.isin(kept_positions)])
)

# calculate equity inverse volatility
position_volatility = pd.DataFrame(columns=['ticker', 'volatility'])
for ticker, _ in new_portfolio.iterrows():
    equity_history = history(db_session = db_session, tickers = [ticker],  days = int(config['model']['hist_window_days']))
    position_volatility = position_volatility.append({
                'ticker': ticker,
                 'volatility':  volatility(equity_history['close'], vola_window = int(config['model']['vola_window']))
                 }, ignore_index=True)

# calculate weights
position_volatility = position_volatility.set_index(['ticker'])
inv_vola = 1 / position_volatility
sum_inv_vola = np.sum(inv_vola)
vola_target_weights = inv_vola / sum_inv_vola

# order market positions
market_weight = 0.0
for security, weight in vola_target_weights.iterrows():
    if security in kept_positions:
        if security in kept_positions:
            #api.submit_order(
            #    symbol=position['symbol'],
            #    side='buy',
            #    type='market',
            #    qty=position['qty'],
            #)
            market_weight += weight[0]
    else:
        if is_bull_market:
            #api.submit_order(
            #    symbol=position['symbol'],
            #    side='buy',
            #    type='market',
            #    qty=position['qty'],
            #)
            market_weight += weight[0]

print('position size: {0}'.format(len(new_portfolio)))

if market_weight:
    print('Market weight: {0}'.format( round(market_weight, 3) ))

# if not bull market invest in cash
if round(market_weight, 3) < 1.0 and not is_bull_market:  # this section manages bear market
    gld_history = history(db_session = db_session, tickers = config['model']['gold'],  days=hist_market_window_days)
    weight = 1.0 - market_weight
    if (TMOM(gld_history['close']) > TMOM(cash_history['close'])) and (gld_history['close'].tail(1).iloc[0] > gld_history['close'].mean()):
        print('gold [%s]' % ( weight ))
        # buy gold
        #api.submit_order(
        #    symbol=position['symbol'],
        #    side='buy',
        #    type='market',
        #    qty=position['qty'],
        #)
    else:
        cash_history = history(db_session = db_session, tickers = config['model']['cash'],  days=hist_market_window_days)
        print('cash [%s]' % ( weight ))
        # buy cash
        #api.submit_order(
        #    symbol=position['symbol'],
        #    side='buy',
        #    type='market',
        #    qty=position['qty'],
        #)

# Email Positions
