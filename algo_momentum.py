import os
from datetime import datetime
import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
import configparser

import models
import sqlalchemy

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import sentry_sdk
from sentry_sdk import capture_exception

import sendgrid
from sendgrid.helpers.mail import *

# find on https://docs.sentry.io/error-reporting/quickstart/?platform=python
sentry_sdk.init(dsn=os.getenv('SENTRY_DSN'))

from helper import (str2bool, parse_wiki_sp_consituents, share_quantity, momentum_quality, momentum_score, volatility, history, TMOM )
from log import log

# constants
DAYS_IN_YEAR = 365

# live trade
LIVE_TRADE = str2bool(os.getenv('LIVE_TRADE', False))

# initialize Alpaca Trader
api = tradeapi.REST(os.getenv('ALPACA_KEY_ID'), os.getenv('ALPACA_SECRET_KEY'), base_url=os.getenv('ALPACA_BASE_URL')) # or use ENV Vars shown below
account = api.get_account()

current_positions = []
not_tradeable_positions = []
for position in api.list_positions():
    asset = api.get_asset(position.symbol)
    if asset.tradable == True:
        current_positions.append(position.symbol)
    else:
        log('{0} is not tradable, skipping'.format(position.symbol), 'error')
        not_tradeable_positions.append(position.symbol)

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

# read s&p 500, 400 companies into pandas dataframe
companies = parse_wiki_sp_consituents(sources = ['500', '400'])

mom_equities = pd.DataFrame(columns=['ticker','inf_discr', 'score'])
for company in companies:
    # if stock traded > 100 day MA

    # if stock moved > 15% in the past 90 days remove

    # calculate inference
    equity_history = history(db_session = db_session, tickers = [company['Symbol']],  days = DAYS_IN_YEAR)
    if not len(equity_history):
        log('{0}, no data'.format(company['Symbol']))
        continue

    # if stock traded > 100 day MA
    if market_history['close'].tail(1).iloc[0] <= equity_history['close'][len(equity_history['close']) - 100:].mean():
        log('{0} is trading below 100 day moving average, skipping'.format(company['Symbol']), 'warning')
        continue

    # if stock moved > 15% in the past 90 days remove
    returns = equity_history['close'][len(equity_history['close']) - 90:].pct_change()
    if len(returns[ (returns <= -.15) | (returns > .15)]):
        log('{0} moved greater than 15% in the past 90 days, skipping'.format(company['Symbol']), 'warning')
        continue

    inf_discr, is_quality = momentum_quality(equity_history['close'], min_inf_discr = config['model']['min_inf_discr'])
    if not is_quality and company['Symbol'] not in current_positions:
        log('{0}, quality failed'.format(company['Symbol']))
        continue

    # calculate momentum score
    data_end = -1 * (int(config['model']['score_exclude_days']) + 1) # exclude most recent data
    momentum_start = -1 * (int(config['model']['score_window_days']) + int(config['model']['score_exclude_days']))
    momentum_hist = equity_history[momentum_start:data_end]
    score = momentum_score(equity_history['close']).mean()
    if score <= float(config['model']['minimum_score_momentum']) and company['Symbol'] not in current_positions:
        log('{0}, score {0} less than minimum'.format(company['Symbol'], score))
        continue

    log(company['Symbol'], 'success')
    mom_equities = mom_equities.append({'ticker': company['Symbol'],
                 'inf_discr': inf_discr,
                 'score': score}, ignore_index=True)

mom_equities = mom_equities.set_index(['ticker'])
ranking_table = mom_equities.sort_values(by=['inf_discr', 'score'], ascending=[True, False])

log('Ranking Table', 'success')
print(ranking_table)

kept_positions =  []
today = datetime.now()
for position in api.list_positions():

    if position.symbol in not_tradeable_positions:
        log('{0} is not tradable, skipping'.format(position.symbol), 'error')
        continue

    bear_etfs = config['model']['bear_etfs'].split(',')
    bear_etfs.append(config['model']['cash'])

    if (position.symbol in bear_etfs and is_bull_market) or \
        ( position.symbol not in mom_equities.index.tolist() and today.month in [3, 6, 9, 12]):
        if LIVE_TRADE:
            api.submit_order(
                symbol=position.symbol,
                time_in_force='day',
                side='sell',
                type='market',
                qty=position.qty,
            )
        log('drop postion {0}'.format(position.symbol), 'info')
    else:
        kept_positions.append(position.symbol)

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
    equity_history = history(db_session = db_session, tickers = [ticker],  days = DAYS_IN_YEAR)

    position_volatility = position_volatility.append({
                'ticker': ticker,
                 'volatility':  volatility(equity_history['close'], vola_window = int(config['model']['vola_window'])),
                 'price': equity_history.tail(1)['close'][0]
                 }, ignore_index=True)

# calculate weights
position_volatility = position_volatility.set_index(['ticker'])
inv_vola = 1 / position_volatility['volatility']
sum_inv_vola = np.sum(inv_vola)
position_volatility['weight']= inv_vola / sum_inv_vola

# order market positions
log('Positions', 'success')
market_weight = 0.0
portfolio_value = round( float(account.equity), 3)
positions = 0

updated_positions = []
for security, data in position_volatility.iterrows():

    asset = api.get_asset(security)
    if asset.tradable == False:
        log('{0} is not tradable, skipping'.format(security), 'error')
    elif security in kept_positions:
        qty = share_quantity(price = data['price'], weight = data['weight'],portfolio_value = portfolio_value)

        if qty:
            diff = qty - int(api.get_position(security).qty)
            if LIVE_TRADE:
                # check quanity for existing position

                # buy or sell the difference
                if diff > 0:
                    api.submit_order(
                        symbol=security,
                        time_in_force='day',
                        side='buy',
                        type='market',
                        qty=diff,
                    )


                elif diff < 0:
                    api.submit_order(
                        symbol=security,
                        time_in_force='day',
                        side='sell',
                        type='market',
                        qty=abs(diff),
                    )

            updated_positions.append({
                'security': security,
                'action':'buy' if diff > 0 else 'sell',
                'qty': qty,
                'diff': diff
            })

            market_weight += data['weight']
            log('{0}: {1}'.format(security, qty), 'info')
            positions+= 1

        else:
            updated_positions.append({
                'security': security,
                'action':'buy' if diff > 0 else 'sell',
                'qty': 0,
                'diff': - int(api.get_position(security).qty)
            })

            log('{0}: 0'.format(security), 'warning')
    elif is_bull_market:
            qty = share_quantity(price = data['price'], weight = data['weight'],portfolio_value = portfolio_value)
            if qty:
                if LIVE_TRADE:
                    api.submit_order(
                        symbol=security,
                        time_in_force='day',
                        side='buy',
                        type='market',
                        qty=qty,
                    )

                updated_positions.append({
                'security': security,
                'action':'buy',
                'qty': qty,
                'diff': qty
                })

                market_weight += data['weight']
                log('{0}: {1}'.format(security, qty), 'info')
                positions+= 1
            else:
                updated_positions.append({
                    'security': security,
                    'action':'buy',
                    'qty': 0,
                    'diff': 0
                })

                log('{0}: 0'.format(security), 'warning')

print('desired portfolio size: {0}'.format(len(new_portfolio)))
print('position size: {0}'.format(positions))

if market_weight:
    print('Market weight: {0}'.format( round(market_weight, 3) ))

# if not bull market invest in cash
if round(market_weight, 3) < 1.0 and not is_bull_market:  # this section manages bear market

    etf_history = history(db_session = db_session, tickers = config['model']['bear_etfs'],  days=config['model']['trend_window_days'])

    # retrieve bear etf with the highest momentum score
    data_end = -1 * (config['model']['bear_score_exclude_days'] + 1) # exclude most recent data
    etf_mom_start = -1 * (config['model']['bear_score_window_days'] + config['model']['bear_score_exclude_days'])
    etf_mom_start = etf_history[etf_mom_start:data_end]

    momentum_list = etf_mom_start.apply(momentum_score)
    momentum_list = momentum_list.dropna()
    sel_etf = momentum_list.sort_values(ascending = False).index[0]

    weight = 1.0 - market_weight
    bear_weight = 0.0

    if (TMOM(etf_history[sel_etf]['close']) > TMOM(cash_history['close'])):
        bear_weight += weight / 2.0

    if (etf_history[sel_etf]['close'].tail(1).iloc[0] > etf_history[sel_etf]['close'].mean()):
        bear_weight += weight / 2.0

    print('{0} weight: {1}'.format( sel_etf , bear_weight ))
    price = etf_history[sel_etf].tail(1)['close'][0]
    qty = share_quantity(price = price, weight = bear_weight, portfolio_value = portfolio_value)

    # buy bear_etf
    if sel_etf in current_positions:
        # bear ETF alread has a position, update the position
        diff = qty - int(api.get_position(sel_etf).qty)

        updated_positions.append({
            'security': sel_etf,
            'action':'buy' if diff > 0 else 'sell',
            'qty': qty,
            'diff': diff
        })

        if LIVE_TRADE:
            # check quanity for existing position

            # buy or sell the difference
            if diff > 0:
                api.submit_order(
                    symbol=sel_etf,
                    time_in_force='day',
                    side='buy',
                    type='market',
                    qty=diff,
                )

            elif diff < 0:
                api.submit_order(
                    symbol=sel_etf,
                    time_in_force='day',
                    side='sell',
                    type='market',
                    qty=abs(diff),
                )

    else:
        # bear ETF doesn't have a position
        updated_positions.append({
        'security': sel_etf,
        'action':'buy',
        'qty': qty,
        'diff': qty
        })

        if LIVE_TRADE:
            api.submit_order(
                symbol=sel_etf,
                time_in_force='day',
                side='buy',
                type='market',
                qty=qty,
            )

    # insert in cash
    cash_history = history(db_session = db_session, tickers = config['model']['cash'],  days=config['model']['trend_window_days'])

    cash_weight = weight - bear_weight
    print('cash weight: %s' % ( cash_weight ))
    price = cash_history.tail(1)['close'][0]
    qty = share_quantity(price = price, weight = cash_weight,portfolio_value = portfolio_value)
    # buy cash

    # check quanity for existing position
    if config['model']['cash'] in current_positions:
        # cash has a position update
        diff = qty - int(api.get_position(config['model']['cash']).qty)

        updated_positions.append({
            'security': config['model']['cash'],
            'action':'buy' if diff > 0 else 'sell',
            'qty': qty,
            'diff': diff
        })

        if LIVE_TRADE:
            # buy or sell the difference
            if diff > 0:
                api.submit_order(
                    symbol=config['model']['cash'],
                    time_in_force='day',
                    side='buy',
                    type='market',
                    qty=diff,
                )

            elif diff < 0:
                api.submit_order(
                    symbol=config['model']['cash'],
                    time_in_force='day',
                    side='sell',
                    type='market',
                    qty=abs(diff),
                )

    else:
        # cash doesn't have a position
        updated_positions.append({
        'security': config['model']['cash'],
        'action':'buy',
        'qty': qty,
        'diff': qty
        })

        if LIVE_TRADE:
            api.submit_order(
                symbol=config['model']['cash'],
                time_in_force='day',
                side='buy',
                type='market',
                qty=qty,
            )

# Email Positions
EMAIL_POSITIONS = str2bool(os.getenv('EMAIL_POSITIONS', False))

# too lazy to write better
message_body_html = 'Market Condition: {0}<br>'.format('Bull' if is_bull_market else 'Bear' )
message_body_plain = 'Market Condition: {0}\n'.format('Bull' if is_bull_market else 'Bear' )

message_body_html += 'Total Positions: {0}<br>'.format(len(updated_positions))
message_body_plain += 'Total Positions: {0}\n'.format(len(updated_positions))

message_body_html += '---------------------------------------------------<br>'
message_body_plain += '---------------------------------------------------\n'

for position in updated_positions:
    diff = ''

    if position['diff'] >= 0:
        diff = '[+{0}]'.format(  position['diff'] )
    elif position['diff'] < 0:
        diff = '[{0}]'.format(  position['diff'] )

    message_body_html += '<a clicktracking=off href="https://finviz.com/quote.ashx?t={0}">{1}</a>: {2} {3}<br>'.format(position['security'] , position['security'], position['qty'], diff)
    message_body_plain += '{0}: {1} {2}\n'.format(position['security'], position['qty'], diff )

if EMAIL_POSITIONS:
    TO_ADDRESSES = os.getenv('TO_ADDRESSES', '').split(',')
    FROM_ADDRESS = os.getenv('FROM_ADDRESS', '')
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))

    from_email = Email(FROM_ADDRESS)
    subject = "Your Monthly Momentum Algo Position Report"
    for to_address in TO_ADDRESSES:
        to_email = To(to_address)
        content = Content("text/html", message_body_html)
        mail = Mail(from_email, to_email, subject, content)

        response = sg.client.mail.send.post(request_body=mail.get())

print('---------------------------------------------------\n')
print(message_body_plain)
