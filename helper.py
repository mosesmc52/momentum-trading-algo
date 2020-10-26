"""
Helper functions.
"""
import math
import time
from datetime import datetime, timedelta

import numpy as np
from scipy import stats
import pandas as pd
from intrinio_sdk.rest import ApiException

import sqlalchemy
import models

from log import log

def str2bool(value):
    valid = {'true': True, 't': True, '1': True, 'on': True,
             'false': False, 'f': False, '0': False,
             }

    if isinstance(value, bool):
        return value

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)


def price_history(security_api, ticker, start_date, end_date, frequency='daily', page_size = 10000, print_test = False ):
    has_next_page = True
    next_page= ''
    page = 1
    df = pd.DataFrame()
    while has_next_page:
        try:
            api_response = security_api.get_security_stock_prices(identifier = ticker, start_date=start_date, end_date=end_date, frequency=frequency, page_size=page_size, next_page=next_page)
            #if not api_response.to_dict()['next_page'] and page == 1:
            #    has_next_page = False
            #    continue

        except ApiException as e:
            print(ticker)
            print("Exception when calling SecurityApi->get_security_stock_prices: %s\r\n" % e)
            has_next_page = False
            continue

        if print_test:
            print('{ticker} price rows: {rows}, page: {page}, next page: {nextpage}'.format(ticker = ticker, rows = len(api_response.to_dict()['stock_prices']), page= page, nextpage = api_response.to_dict()['next_page']) )

        has_another_page = 'no'
        if bool(api_response.to_dict()['next_page'] == None):
             has_next_page = False
        else:
            has_another_page = 'yes'
            next_page = api_response.to_dict()['next_page']
            page +=1

        if print_test:
            print('save_price_history another page: {has_another_page}'.format(has_another_page = has_another_page ))


        for price in api_response.to_dict()['stock_prices']:

            df = df.append({'date': price['date'].strftime("%Y-%m-%d"),
                            'open': price['adj_open'],
                            'high': price['adj_high'],
                            'low': price['adj_low'],
                            'close': price['adj_close'],
                            'volume': price['adj_volume'],
                            'dividend': 0.0,
                            'split': 1.0
                            }, ignore_index=True)

    df = df.dropna()
    return df


def ingest_security(intrinio_security, db_session, ticker, name = '', type = 'stock'):
    now = datetime.now()
    end_date  =  now.strftime('%Y-%m-%d')

    log(ticker , 'success')
    # insert security in database if doesn't exist
    security = db_session.query(models.Security).filter(models.Security.ticker == ticker).first()
    if not security:
        security = models.Security(
            ticker = ticker,
            name = name,
            type = type
        )

        db_session.add(security)
        db_session.commit()
        start_date = now - timedelta( days = 730)
    else:
        # retrieve latest price data from sql database
        last_price = db_session.query(models.Price).order_by(sqlalchemy.desc('date')).first()
        if not last_price:
            start_date = now - timedelta( days = 730)
        else:
            start_date = last_price.date + timedelta( days = 1)
            if start_date > now:
                return True

    # retrieve price history since latest price
    hist = price_history(intrinio_security, ticker, start_date, end_date)
    if not len(hist):
        log('No History found since {0}'.format(last_price.date.strftime('%m-%d-%Y')), 'info')
        time.sleep(1)
        return True

    # save to database
    for _, price in hist.sort_values(by = 'date', ascending = True).iterrows():
        object = models.Price(
            close =price['close'],
            date = datetime.strptime(price['date'], '%Y-%m-%d'),
            security_id = security.id
        )
        db_session.add(object)
        db_session.commit()

    print('{0} prices inserted'.format(len(hist)))
    if len(hist) < 500:
        time.sleep(1)

    return True

def _pos_neg( pct_change):
    if pct_change > 0:
        return 1
    else:
        return 0

def momentum_quality( ts, min_inf_discr = 0.0):
    # use momentum quality calculation

    df = pd.DataFrame()
    lookback_months = 12

    df['return'] = ts.resample('M').last().pct_change()[-lookback_months:-1]
    df['pos_neg'] = df.apply(lambda row: _pos_neg(row['return']) ,axis=1)
    df['pos_sum'] = df['pos_neg'].cumsum()

    positive_sum = df['pos_sum'].iloc[-1]
    consist_indicator = df['pos_sum'].iloc[-1] >= lookback_months * 2 / 3

    if positive_sum == 0:
        pos_percent = 0
        neg_percent = 1
    elif positive_sum >= lookback_months - 1:
         pos_percent = 1
         neg_percent = 0
    else:
        pos_percent, neg_percent = df['pos_neg'].value_counts(normalize=True)
    perc_diff = neg_percent - pos_percent

    pret = ((df['return']+1).cumprod()-1).iloc[-1]
    inf_discr =  np.sign(pret) * perc_diff
    if inf_discr < float(min_inf_discr) and consist_indicator:
        return inf_discr, True

    return inf_discr, False


def momentum_score(ts):
    """
    Input:  Price time series.
    Output: Annualized exponential regression slope,
            multiplied by the R2
    """
    # Make a list of consecutive numbers
    x = np.arange(len(ts))
    # Get logs
    log_ts = np.log(ts)
    # Calculate regression values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, log_ts)
    # Annualize percent
    annualized_slope = (np.power(np.exp(slope), 252) - 1) * 100
    #Adjust for fitness
    score = annualized_slope * (r_value ** 2)
    return score

def volatility(ts, vola_window = 20):
    return ts.pct_change().rolling(vola_window).std().mean()


def history(db_session, tickers, days):

    # build sqlite queries
    security_query = db_session.query(models.Security).filter(models.Security.ticker.in_(tuple(tickers)))

    security_ids = []
    for security in security_query.all():
        security_ids.append(security.id)

    past = datetime.now() - timedelta(days = int(days))
    price_query = db_session.query(models.Price).filter(models.Price.security_id.in_(tuple(security_ids)), models.Price.date >= past)

    # build pandas dataframe
    security_df = pd.read_sql(security_query.statement, db_session.bind)
    price_df = pd.read_sql(price_query.statement, db_session.bind)

    # merge both dataframes
    df = security_df.merge( price_df, left_on='id', right_on='security_id')

    # remove unnessary columns
    df = df.drop(['security_id', 'id_x','id_y'], axis = 1)

    # convert date to datetime object
    df['date'] = pd.to_datetime(df['date'])

    # set date to index
    df = df.set_index(['date'])

    return df

def TMOM(prices_df):
    return prices_df.pct_change().cumsum().tail(1)[0]

def share_quantity(price, weight, portfolio_value):
    return math.floor( (portfolio_value * weight) / price)
