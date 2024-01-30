import configparser
import os
from datetime import datetime

import alpaca_trade_api as tradeapi
import models
import numpy as np
import pandas as pd
import sqlalchemy
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

import sentry_sdk
from sentry_sdk import capture_exception
from SES import AmazonSES

# find on https://docs.sentry.io/error-reporting/quickstart/?platform=python
sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"))

from helper import (
    TMOM,
    NearHigh,
    history,
    momentum_quality,
    momentum_score,
    parse_wiki_sp_consituents,
    share_quantity,
    str2bool,
    volatility,
)
from log import log

# constants
DAYS_IN_YEAR = 365

# live trade
LIVE_TRADE = str2bool(os.getenv("LIVE_TRADE", False))

# initialize Alpaca Trader
api = tradeapi.REST(
    os.getenv("ALPACA_KEY_ID"),
    os.getenv("ALPACA_SECRET_KEY"),
    base_url=os.getenv("ALPACA_BASE_URL"),
)  # or use ENV Vars shown below
account = api.get_account()

current_positions = []
not_tradeable_positions = []
for position in api.list_positions():
    asset = api.get_asset(position.symbol)
    if asset.tradable is True:
        current_positions.append(position.symbol)
    else:
        log("{0} is not tradable, skipping".format(position.symbol), "error")
        not_tradeable_positions.append(position.symbol)

# open sqllite db
engine = sqlalchemy.create_engine("sqlite:///securities.db")
db_session = sqlalchemy.orm.Session(bind=engine)

# retreive configuration parameters
config = configparser.ConfigParser()
config.read("algo_settings.cfg")

# read S&P etf
market_history = history(
    db_session=db_session,
    tickers=[config["model"]["market"]],
    days=config["model"]["trend_window_days"],
)

is_bull_market = (
    market_history["close"].tail(1).iloc[0] > market_history["close"].mean()
)
market_mean_percent_difference = 1 - (
    market_history.tail(1).iloc[0] / market_history.mean()
)

if is_bull_market:
    log("Bull Market", "success")
else:
    log("Bear Market", "warning")

# read s&p 500, 400 companies into pandas dataframe
companies = parse_wiki_sp_consituents(os.getenv("SP_CONSITUENTS").split(","))


mom_equities = pd.DataFrame(columns=["ticker", "inf_discr", "score"])
for company in companies:
    # calculate inference
    equity_history = history(
        db_session=db_session, tickers=[company["Symbol"]], days=DAYS_IN_YEAR
    )
    if not len(equity_history):
        log("{0}, no data".format(company["Symbol"]))
        continue

    if equity_history["close"].tail(1).iloc[0] >= float(
        config["model"]["max_allowable_price"]
    ):
        log(
            "{0} greater than max allowable price, skipping".format(company["Symbol"]),
            "warning",
        )
        continue

    # check if stock traded > 100 day MA
    if (
        equity_history["close"].tail(1).iloc[0]
        <= equity_history["close"][len(equity_history["close"]) - 100 :].mean()
    ):
        log(
            "{0} is trading below {1} day moving average, skipping".format(
                company["Symbol"], config["model"]["slope_window_days"]
            ),
            "warning",
        )
        continue

    # if stock moved > 15% in the past 90 days remove
    returns = (
        equity_history["close"][
            len(equity_history["close"]) - config["model"]["slope_window_days"] :
        ]
        .pct_change()
        .abs()
    )
    if len(returns[(returns > config["model"]["max_stock_gap"])]):
        log(
            "{0} moved greater than 15% in the past {1} days, skipping".format(
                company["Symbol"], config["model"]["slope_window_days"]
            ),
            "warning",
        )
        continue

    inf_discr, is_quality = momentum_quality(
        equity_history["close"], min_inf_discr=config["model"]["min_inf_discr"]
    )
    if not is_quality:
        log("{0}, quality failed".format(company["Symbol"]))
        continue

    momentum_hist = equity_history[slope_window_days:data_end]
    score = momentum_score(equity_history["close"]).mean()
    if score <= float(config["model"]["minimum_score_momentum"]):
        log("{0}, score {0} less than minimum".format(company["Symbol"], score))
        continue

    log(company["Symbol"], "success")
    mom_equities = mom_equities.append(
        {
            "ticker": company["Symbol"],
            "inf_discr": inf_discr,
            "score": score,
            "near_high": NearHigh(equity_history),
            "volitility": volatility(
                equity_history["close"], vola_window=int(config["model"]["vola_window"])
            ),
        },
        ignore_index=True,
    )

# include equities lower than 0.8 near high
# if str2bool(os.getenv("FILTER_NEARHIGH", False)):
#     mom_equities = mom_equities[
#         mom_equities["near_high"] < config["model"]["maximum_near_high"]
#     ]

mom_equities = mom_equities.set_index(["ticker"])
ranking_table = mom_equities.sort_values(
    by=["volitility", "inf_discr", "score"], ascending=[True, True, False]
)

log("Ranking Table", "success")
print(ranking_table)

kept_positions = []
today = datetime.now()

for position in api.list_positions():
    if position.symbol in not_tradeable_positions:
        log("{0} is not tradable, skipping".format(position.symbol), "error")
        continue

    if position.symbol not in mom_equities.index.tolist():
        if LIVE_TRADE:
            api.submit_order(
                symbol=position.symbol,
                time_in_force="day",
                side="sell",
                type="market",
                qty=position.qty,
            )
        log("drop postion {0}".format(position.symbol), "info")
    else:
        kept_positions.append(position.symbol)

replacement_stocks = int(config["model"]["portfolio_size"]) - len(kept_positions)

buy_list = ranking_table.loc[~ranking_table.index.isin(kept_positions)][
    :replacement_stocks
]

new_portfolio = pd.concat(
    (buy_list, ranking_table.loc[ranking_table.index.isin(kept_positions)])
)

# calculate equity inverse volatility
position_volatility = pd.DataFrame(columns=["ticker", "volatility"])
for ticker, _ in new_portfolio.iterrows():
    equity_history = history(db_session=db_session, tickers=[ticker], days=DAYS_IN_YEAR)

    position_volatility = position_volatility.append(
        {
            "ticker": ticker,
            "volatility": volatility(
                equity_history["close"], vola_window=int(config["model"]["vola_window"])
            ),
            "price": equity_history.tail(1)["close"][0],
        },
        ignore_index=True,
    )


# calculate weights
position_volatility = position_volatility.set_index(["ticker"])
inv_vola = 1 / position_volatility["volatility"]
sum_inv_vola = np.sum(inv_vola)
position_volatility["weight"] = inv_vola / sum_inv_vola

# order market positions
log("Positions", "success")
market_weight = 0.0
portfolio_value = round(float(account.equity), 3)
positions = 0

if is_bull_market:
    updated_positions = []
    for security, data in position_volatility.iterrows():
        asset = api.get_asset(security)
        if asset.tradable == False:
            log("{0} is not tradable, skipping".format(security), "error")
        elif security in kept_positions:
            qty = share_quantity(
                price=data["price"],
                weight=data["weight"],
                portfolio_value=portfolio_value,
            )

            if qty:
                diff = qty - int(api.get_position(security).qty)
                if LIVE_TRADE:
                    # check quanity for existing position

                    # buy or sell the difference
                    if diff > 0:
                        api.submit_order(
                            symbol=security,
                            time_in_force="day",
                            side="buy",
                            type="market",
                            qty=diff,
                        )

                    elif diff < 0:
                        api.submit_order(
                            symbol=security,
                            time_in_force="day",
                            side="sell",
                            type="market",
                            qty=abs(diff),
                        )

                updated_positions.append(
                    {
                        "security": security,
                        "action": "buy" if diff > 0 else "sell",
                        "qty": qty,
                        "diff": diff,
                    }
                )

                market_weight += data["weight"]
                log("{0}: {1}".format(security, qty), "info")
                positions += 1

            else:
                updated_positions.append(
                    {
                        "security": security,
                        "action": "buy" if diff > 0 else "sell",
                        "qty": 0,
                        "diff": -int(api.get_position(security).qty),
                    }
                )

                log("{0}: 0".format(security), "warning")
        elif is_bull_market:
            qty = share_quantity(
                price=data["price"],
                weight=data["weight"],
                portfolio_value=portfolio_value,
            )
            if qty:
                if LIVE_TRADE:
                    api.submit_order(
                        symbol=security,
                        time_in_force="day",
                        side="buy",
                        type="market",
                        qty=qty,
                    )

                updated_positions.append(
                    {"security": security, "action": "buy", "qty": qty, "diff": qty}
                )

                market_weight += data["weight"]
                log("{0}: {1}".format(security, qty), "info")
                positions += 1
            else:
                updated_positions.append(
                    {"security": security, "action": "buy", "qty": 0, "diff": 0}
                )

                log("{0}: 0".format(security), "warning")

    print("desired portfolio size: {0}".format(len(new_portfolio)))
    print("position size: {0}".format(positions))
else:
    if (
        market_mean_percent_difference
        > config["model"]["maximum_market_mean_percent_difference"]
    ):
        # drop all market positions
        for position in kept_positions:
            if LIVE_TRADE:
                api.submit_order(
                    symbol=position.symbol,
                    time_in_force="day",
                    side="sell",
                    type="market",
                    qty=position.qty,
                )
            log("drop postion {0}".format(position.symbol), "info")
    else:
        # keep positions the same
        print("portfolio size: {0}".format(len(kept_positions)))
        updated_positions = kept_positions

if market_weight:
    print("Market weight: {0}".format(round(market_weight, 3)))

# Email Positions
EMAIL_POSITIONS = str2bool(os.getenv("EMAIL_POSITIONS", False))

# too lazy to write better
message_body_html = "Market Condition: {0}<br>".format(
    "Bull" if is_bull_market else "Bear"
)
message_body_plain = "Market Condition: {0}\n".format(
    "Bull" if is_bull_market else "Bear"
)

message_body_html += "Total Positions: {0}<br>".format(len(updated_positions))
message_body_plain += "Total Positions: {0}\n".format(len(updated_positions))

message_body_html += "---------------------------------------------------<br>"
message_body_plain += "---------------------------------------------------\n"

for position in updated_positions:
    diff = ""

    if position["diff"] >= 0:
        diff = "[+{0}]".format(position["diff"])
    elif position["diff"] < 0:
        diff = "[{0}]".format(position["diff"])

    message_body_html += '<a clicktracking=off href="https://finviz.com/quote.ashx?t={0}">{1}</a>: {2} {3}<br>'.format(
        position["security"], position["security"], position["qty"], diff
    )
    message_body_plain += "{0}: {1} {2}\n".format(
        position["security"], position["qty"], diff
    )

if EMAIL_POSITIONS:
    TO_ADDRESSES = os.getenv("TO_ADDRESSES", "").split(",")
    FROM_ADDRESS = os.getenv("FROM_ADDRESS", "")
    ses = AmazonSES(
        region=os.environ.get("AWS_SES_REGION_NAME"),
        access_key=os.environ.get("AWS_SES_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SES_SECRET_ACCESS_KEY"),
        from_address=os.environ.get("FROM_ADDRESS"),
    )
    if LIVE_TRADE:
        status = "Live"
    else:
        status = "Test"

    subject = "Your Monthly Momentum Algo Position Report - {}".format(status)

    for to_address in TO_ADDRESSES:
        ses.send_html_email(
            to_address=to_address, subject=subject, content=message_body_html
        )

print("---------------------------------------------------\n")
print(message_body_plain)
