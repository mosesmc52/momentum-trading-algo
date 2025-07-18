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
from sqlalchemy.sql import text


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
                        "Symbol": row.xpath("td[1]/descendant::text()")[0].strip(),
                        "Name": row.xpath("td[2]/descendant::text()")[0].strip(),
                    }
                )

        log(
            "{0} Companies found on Wikipedia: S&P 600 Constituents Page".format(
                len(companies600)
            ),
            "success",
        )
        companies += companies600
    if "aristocrats" in sources:
        log("\nParsing S&P 500 Dividend Aristocrats", "info")
        response = requests.get(
            "https://en.wikipedia.org/wiki/S%26P_500_Dividend_Aristocrats"
        )
        mainTree = html.fromstring(response.text)

        companiesAristocrats = []
        for row in mainTree.xpath('//table[contains(@id, "constituents")]/tbody/tr'):
            if len(row.xpath("td")):
                companiesAristocrats.append(
                    {
                        "Symbol": row.xpath("td[2]/descendant::text()")[0].strip(),
                        "Name": row.xpath("td[1]/descendant::text()")[0].strip(),
                    }
                )
        companies += companiesAristocrats
    return companies


def price_history(api, ticker, start_date, end_date, print_test=False):
    try:
        return api.get_bars(
            ticker,
            TimeFrame.Day,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            adjustment="all",
        )
    except TypeError as te:
        log("{}\n".format(te), "error")

    return []


def ingest_security(alpaca_api, db_session, ticker, name="", type="stock"):
    now = datetime.now()
    end_date = now - timedelta(hours=48)

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


def momentum_score(ts, trading_days=252):
    """
    Input:  Price time series.
    Output: Annualized exponential regression slope,
            multiplied by the R2
    """
    if len(ts) < 2 or ts.isnull().any():
        return np.nan

    # Make a list of consecutive numbers
    x = np.arange(len(ts))
    # Get logs
    log_ts = np.log(ts)
    # Calculate regression values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, log_ts)
    # Annualize percent
    annualized_slope = (np.power(np.exp(slope), trading_days) - 1) * 100
    # Adjust for fitness
    score = annualized_slope * (r_value**2)
    return score


def volatility(ts, vola_window=20):
    return ts.pct_change().rolling(vola_window).std().mean()


def history(engine, db_session, tickers, days):
    security_query = db_session.query(models.Security).filter(
        models.Security.ticker.in_(tuple(tickers))
    )

    security_ids = [s.id for s in security_query.all()]

    past = datetime.now() - timedelta(days=int(days))
    price_query = db_session.query(models.Price).filter(
        models.Price.security_id.in_(tuple(security_ids)), models.Price.date >= past
    )

    # Convert queries to raw SQL strings using text()
    security_sql = str(
        security_query.statement.compile(compile_kwargs={"literal_binds": True})
    )
    price_sql = str(
        price_query.statement.compile(compile_kwargs={"literal_binds": True})
    )

    security_df = pd.read_sql(text(security_sql), con=engine)
    price_df = pd.read_sql(text(price_sql), con=engine)

    df = security_df.merge(price_df, left_on="id", right_on="security_id")
    df = df.drop(columns=["security_id", "id_x", "id_y"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    return df


def share_quantity(price, weight, portfolio_value):
    return math.floor((portfolio_value * weight) / price)


def yoy(current_yr, previous_yr):
    return current_yr - previous_yr
