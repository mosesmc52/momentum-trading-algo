"""
Helper functions.
"""
import math
import time
from datetime import datetime, timedelta

import models
import numpy as np
import pandas as pd
import requests
import sqlalchemy
from alpaca_trade_api.rest import TimeFrame
from dateutil import parser as time_parser
from log import log
from lxml import html
from scipy import stats


def str2bool(value):
    valid = {
        "true": True,
        "t": True,
        "1": True,
        "on": True,
        "false": False,
        "f": False,
        "0": False,
    }

    if isinstance(value, bool):
        return value

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)


def parse_wiki_sp_consituents(sources=[]):
    companies = []
    if "500" in sources:
        log("\nParsing S&P 500 Large-Cap Wiki Constituents", "info")
        response = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        )
        mainTree = html.fromstring(response.text)

        companies500 = []
        for row in mainTree.xpath('//table[contains(@id, "constituents")]/tbody/tr'):
            if len(row.xpath("td")):
                companies500.append(
                    {
                        "Symbol": row.xpath("td/a/text()")[0],
                        "Name": row.xpath("td/a/text()")[1],
                    }
                )

        log(
            "{0} Companies found on Wikipedia: S&P 500 Constituents Page".format(
                len(companies500)
            ),
            "success",
        )
        companies += companies500

    if "400" in sources:
        log("\nParsing S&P 400 Mid-Cap Wiki Constituents", "info")
        response = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
        )
        mainTree = html.fromstring(response.text)

        companies400 = []
        for row in mainTree.xpath('//table[contains(@id, "constituents")]/tbody/tr'):
            if len(row.xpath("td")):
                companies400.append(
                    {
                        "Symbol": row.xpath("td[1]/a/text()")[0].strip(),
                        "Name": row.xpath("td[2]/a/text()")[0],
                    }
                )

        log(
            "{0} Companies found on Wikipedia: S&P 400 Constituents Page".format(
                len(companies400)
            ),
            "success",
        )
        companies += companies400

    if "600" in sources:
        log("\nParsing S&P 600 Small-Cap Wiki Constituents", "info")
        response = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
        )
        mainTree = html.fromstring(response.text)

        companies600 = []
        for row in mainTree.xpath('//table[contains(@id, "constituents")]/tbody/tr'):
            if len(row.xpath("td")):
                companies600.append(
                    {
                        "Symbol": row.xpath("td[2]/descendant::text()")[0].strip(),
                        "Name": row.xpath("td[1]/descendant::text()")[0].strip(),
                    }
                )

        log(
            "{0} Companies found on Wikipedia: S&P 600 Constituents Page".format(
                len(companies600)
            ),
            "success",
        )
        companies += companies600

    return companies


def price_history(api, ticker, start_date, end_date, print_test=False):
    try:
        return api.get_bars(
            ticker,
            TimeFrame.Day,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
    except TypeError as te:
        log("{}\n".format(te), "error")

    return []


def ingest_security(alpaca_api, db_session, ticker, name="", type="stock"):
    now = datetime.now()
    end_date = now - timedelta(hours=24)

    log("\n{0}".format(ticker), "success")
    # insert security in database if doesn't exist
    security = (
        db_session.query(models.Security)
        .filter(models.Security.ticker == ticker)
        .first()
    )
    if not security:
        security = models.Security(ticker=ticker, name=name, type=type)

        db_session.add(security)
        db_session.commit()
        start_date = now - timedelta(days=730)
    else:
        # retrieve latest price data from sql database
        last_price = (
            db_session.query(models.Price)
            .filter(models.Price.security_id == security.id)
            .order_by(sqlalchemy.desc("date"))
            .first()
        )
        if not last_price:
            start_date = now - timedelta(days=730)
        else:
            start_date = last_price.date + timedelta(days=1)
            if start_date > now:
                return True

    # retrieve price history since latest price
    if start_date > end_date:
        log("0 day prices inserted", "info")
        return True

    hist = price_history(alpaca_api, ticker, start_date, end_date)

    for price in hist:
        object = models.Price(
            close=price.c,  # retrieve close price
            date=time_parser.parse(str(price.t)),
            security_id=security.id,
        )
        db_session.add(object)
        db_session.commit()

    log("{0} day prices inserted".format(len(hist)))

    return True


def _pos_neg(pct_change):
    if pct_change > 0:
        return 1
    else:
        return 0


def momentum_quality(ts, min_inf_discr=0.0):
    # use momentum quality calculation

    df = pd.DataFrame()
    lookback_months = 12

    df["return"] = ts.resample("M").last().pct_change()[-lookback_months:-1]
    if not len(df["return"]):
        return False, False

    df["pos_neg"] = df.apply(lambda row: _pos_neg(row["return"]), axis=1)
    df["pos_sum"] = df["pos_neg"].cumsum()

    positive_sum = 0
    if len(df["pos_sum"]) > 0:
        positive_sum = df["pos_sum"].iloc[-1]
        consist_indicator = df["pos_sum"].iloc[-1] >= lookback_months * 2 / 3

    if positive_sum == 0:
        pos_percent = 0
        neg_percent = 1
    elif positive_sum >= lookback_months - 1:
        pos_percent = 1
        neg_percent = 0
    else:
        pos_percent, neg_percent = df["pos_neg"].value_counts(normalize=True)
    perc_diff = neg_percent - pos_percent

    pret = ((df["return"] + 1).cumprod() - 1).iloc[-1]
    inf_discr = np.sign(pret) * perc_diff
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
    # Adjust for fitness
    score = annualized_slope * (r_value**2)
    return score


def volatility(ts, vola_window=20):
    return ts.pct_change().rolling(vola_window).std().mean()


def history(db_session, tickers, days):
    # build sqlite queries
    security_query = db_session.query(models.Security).filter(
        models.Security.ticker.in_(tuple(tickers))
    )

    security_ids = []
    for security in security_query.all():
        security_ids.append(security.id)

    past = datetime.now() - timedelta(days=int(days))
    price_query = db_session.query(models.Price).filter(
        models.Price.security_id.in_(tuple(security_ids)), models.Price.date >= past
    )

    # build pandas dataframe
    security_df = pd.read_sql(security_query.statement, db_session.bind)
    price_df = pd.read_sql(price_query.statement, db_session.bind)

    # merge both dataframes
    df = security_df.merge(price_df, left_on="id", right_on="security_id")

    # remove unnessary columns
    df = df.drop(["security_id", "id_x", "id_y"], axis=1)

    # convert date to datetime object
    df["date"] = pd.to_datetime(df["date"])

    # set date to index
    df = df.set_index(["date"])

    return df


def TMOM(prices_df):
    return prices_df.pct_change().cumsum().tail(1)[0]


def High52Week(price_df):
    return price_df["close"].rolling(window=52 * 7, min_periods=1).max()[0]


def NearHigh(price_df):
    return price_df["close"].tail(1).iloc[0] / High52Week(price_df)


def share_quantity(price, weight, portfolio_value):
    return math.floor((portfolio_value * weight) / price)
