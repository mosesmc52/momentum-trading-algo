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
from pandas.tseries.offsets import BDay
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


def last_trading_day_of_week(ref_date=None):
    """
    Returns the last trading day (Friday) of the current week based on ref_date.
    If today is a weekend (Saturday or Sunday), returns the previous Friday.
    """
    if ref_date is None:
        ref_date = datetime.now()

    weekday = ref_date.weekday()  # Monday=0, Sunday=6

    if weekday >= 5:
        # Saturday (5) or Sunday (6): go back to last Friday
        days_to_subtract = weekday - 4
        last_friday = ref_date - timedelta(days=days_to_subtract)
    else:
        # Weekday: compute Friday of current week
        days_to_add = 4 - weekday
        last_friday = ref_date + timedelta(days=days_to_add)

    return last_friday.date()


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


def ingest_security(
    alpaca_api, db_session, ticker, name="", type="stock", trading_days=252 * 2
):
    now = datetime.now()
    # Use last trading day of the current week as end_date
    end_date = last_trading_day_of_week(ref_date=now)
    # Convert to datetime at market close time (e.g., 16:00) for consistency if needed
    # Here, assuming you want midnight of that day:
    end_date = datetime.combine(end_date, datetime.min.time())

    log(f"\n{ticker}", "success")

    # insert security in database if it doesn't exist
    security = (
        db_session.query(models.Security)
        .filter(models.Security.ticker == ticker)
        .first()
    )
    if not security:
        security = models.Security(ticker=ticker, name=name, type="stock")
        db_session.add(security)
        db_session.commit()
        # Approximate calendar days for trading_days (e.g. 252 trading days ≈ 365 calendar days)
        # Set start_date some days back from end_date (you may want to implement a function to get trading days back)
        start_date = end_date - timedelta(days=int(trading_days * 1.5))
    else:
        # retrieve latest price data from sql database
        last_price = (
            db_session.query(models.Price)
            .filter(models.Price.security_id == security.id)
            .order_by(sqlalchemy.desc("date"))
            .first()
        )
        if not last_price:
            start_date = end_date - timedelta(days=int(trading_days * 1.5))
        else:
            start_date = last_price.date + timedelta(days=1)
            if start_date > end_date:
                return True

    if start_date > end_date:
        log("0 day prices inserted", "info")
        return True

    # Call price_history here (make sure it accepts datetime or date objects as arguments)
    hist = price_history(alpaca_api, ticker, start_date, end_date)

    for price in hist:
        db_session.add(
            models.Price(
                close=price.c,  # retrieve close price
                date=time_parser.parse(str(price.t)),
                security_id=security.id,
            )
        )
    db_session.commit()

    log(f"{len(hist)} day prices inserted")

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


def history(engine, db_session, tickers, trading_days):
    # Step 1: Query securities by ticker
    security_query = db_session.query(models.Security).filter(
        models.Security.ticker.in_(tuple(tickers))
    )
    security_ids = [s.id for s in security_query.all()]

    # Step 2: Subtract trading days using BDay (business days)
    past = datetime.now() - BDay(int(trading_days))

    # Step 3: Query prices after `past` date
    price_query = db_session.query(models.Price).filter(
        models.Price.security_id.in_(tuple(security_ids)), models.Price.date >= past
    )

    # Step 4: Compile to raw SQL strings
    security_sql = str(
        security_query.statement.compile(compile_kwargs={"literal_binds": True})
    )
    price_sql = str(
        price_query.statement.compile(compile_kwargs={"literal_binds": True})
    )

    # Step 5: Execute SQL queries and merge data
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
