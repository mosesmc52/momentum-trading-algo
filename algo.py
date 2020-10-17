import pandas as pd
import configparser

import models
import sqlalchemy

from helper import (momentum_quality, momentum_score, volatility, history, TMOM )
from log import log

# open sqllite db
engine = sqlalchemy.create_engine('sqlite:///securities.db')
db_session = sqlalchemy.orm.Session(bind=engine)

# retreive configuration parameters
config = configparser.ConfigParser()
config.read('algo_settings.cfg')

# read S&P etf
market_history = history(db_session = db_session, tickers = [config['model']['market']],  bar_count = config['model']['trend_window_days'])
cash_history = history(db_session = db_session, tickers = [config['model']['cash']],  bar_count = config['model']['trend_window_days'])

is_bull_market =  (market_history['close'].tail(1).iloc[0] > market_history['close'].mean()) and (TMOM(market_history['close']) > TMOM(cash_history['close']))
if not is_bull_market:
    log('Bear Market Exiting Algo', 'info')
    quit()
else:
    log('Bull Market', 'success')

# read s&p 500 companies into pandas dataframe
companies = pd.read_csv('s-and-p-500-companies.csv')

# retrieve equities from database
equities_history = history(db_session = db_session, tickers = companies['Symbol'].to_list(),  bar_count = config['model']['hist_window_days'])

import pdb; pdb.set_trace()

# get momentum quality
quality = equities_history.apply(momentum_quality).T
quality = quality[(quality[1] == True)]

#data_end = -1 * (context.score_exclude_days + 1) # exclude most recent data

#momentum_start = -1 * (context.score_window + context.score_exclude_days)
#momentum_hist = hist[momentum_start:data_end]

# Calculate momentum scores for all stocks.
#momentum_list = momentum_hist.apply(momentum_score)  # Mom Window 1

#score = momentum_list.mean()

# get momentum score
#score = score[score >= minimum_score_momentum]

# merge both quality and score
#filter = pd.concat([quality, score], axis = 1)
#filter = filter.dropna()

# drop consistency column because all tickers filter at this point should be consistent
#filter = filter.drop([1], axis=1)
#filter.columns = ['inf_discr', 'score']

# rank by score and inf_discr
#ranking_table = filter.sort_values(by=['inf_discr', 'score'], ascending=[True, False])
#print('ticker count: {0}'.format(len(ranking_table)))

#print(ranking_table)
#"""
#   First we check if any existing position should be sold.
#* Sell if stock is no longer part of index.
#* Sell if stock has too low momentum value.
#"""

#kept_positions = list(context.portfolio.positions.keys())
#for security in context.portfolio.positions:
#    if security in [symbol('IEF'), symbol('GLD')]:
#        kept_positions.remove(security)

    #elif (security not in todays_universe):
#    elif (security not in todays_universe) or ( security not in filter.index.tolist() and today.month in [3, 6, 9, 12]):
    #elif (security not in filter.index.tolist()):
#        order_target_percent(security, 0.0)
#        kept_positions.remove(security)

#        if security in context.positions:
            # delete security from purchase date
#            del context.positions[security]

#"""
#Stock Selection Logic

#Check how many stocks we are keeping from last month.
#Fill from top of ranking list, until we reach the
#desired total number of portfolio holdings.
#"""
#replacement_stocks = portfolio_size - len(kept_positions)

#buy_list = ranking_table.loc[
#    ~ranking_table.index.isin(kept_positions)][:replacement_stocks]

#new_portfolio = pd.concat(
#    (buy_list,
#     ranking_table.loc[ranking_table.index.isin(kept_positions)])
#)

#"""
#Calculate inverse volatility for stocks,
#and make target position weights.
#"""

#vola_table = hist[new_portfolio.index].apply(volatility)
#inv_vola_table = 1 / vola_table
#sum_inv_vola = np.sum(inv_vola_table)
#vola_target_weights = inv_vola_table / sum_inv_vola
#market_weight = 0.0

#for security, rank in new_portfolio.iterrows():
#    weight = vola_target_weights[security]
#    if security in kept_positions:

#        if security in context.positions:
#            order_target_percent(security, weight)
#            market_weight += weight
#            context.positions[security]['weight'] = weight
#    else:
#        if context.is_bull_market:
#            order_target_percent(security, weight)
#            market_weight += weight

#            if security not in context.positions:
#                context.positions[security] = {'purchase_date': today, 'weight': weight }

#print('position size: {size}'.format(size = len(context.positions)))
#if len(context.positions) > portfolio_size:
#    import pdb; pdb.set_trace()

#if context.is_bull_market:
#    print('Bull Market')
    #print( ranking_table )
#else:
#    print('Bear Market')

#if market_weight:
#    print('Market weight: %s' %  market_weight )

#if round(market_weight, 3) < 1.0 and not context.is_bull_market:  # this section manages bear market
#    gld_history = data.history(context.gold, 'price', bar_count=hist_market_window_days, frequency="1d")
#    weight = 1.0 - market_weight
#    if (gld_history.pct_change().cumsum().tail(1)[0] > TMOM[context.cash][0]) and (gld_history.tail(1).iloc[0] > gld_history.mean()):
#        print('gold [%s]' % ( weight ))
#        order_target_percent(context.gold, weight)
#    else:
#        cash_history = data.history(context.cash, 'price', bar_count=hist_market_window_days, frequency="1d")
#        print('cash [%s]' % ( weight ))
#        order_target_percent(context.cash, weight )
#else:
#    order_target_percent( context.cash,0.0 )
#    order_target_percent( context.gold,0.0 )
