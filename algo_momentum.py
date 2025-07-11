import configparser
import os
from datetime import datetime, timedelta

import alpaca_trade_api as tradeapi
import models
import numpy as np
import pandas as pd
import sqlalchemy
from dotenv import find_dotenv, load_dotenv
from fredapi import Fred

load_dotenv(find_dotenv())

import sentry_sdk
from sentry_sdk import capture_exception
from SES import AmazonSES

# find on https://docs.sentry.io/error-reporting/quickstart/?platform=python
sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"))

from helper import (
    history,
    momentum_score,
    parse_wiki_sp_consituents,
    share_quantity,
    str2bool,
    volatility,
    yoy,
)
from log import log

# constants
TRADING_DAYS_IN_YEAR = 252
BUY = "buy"
SELL = "sell"

# load macro-economic event check for bull market
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

now = datetime.now()
extra_parameters = {
    "observation_start": (now - timedelta(days=600)).strftime("%Y-%m-%d"),
    "observation_end": now.strftime("%Y-%m-%d"),
}

MACRO = fred.get_series("RRSFS", **extra_parameters)
MACRO.name = "MACRO"

df = pd.concat([MACRO], axis=1)
full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="D")
df = df.reindex(full_range)
df.ffill(inplace=True)

MACRO_YOY = yoy(
    df["MACRO"].tail(1).iloc[0],
    df.loc[df["MACRO"].tail(1).index - pd.DateOffset(years=1), "MACRO"].iloc[0],
)


# live trade
LIVE_TRADE = str2bool(os.getenv("LIVE_TRADE", False))
log(f"Running in {'LIVE' if LIVE_TRADE else 'TEST'} mode", "info")

# initialize Alpaca Trader
api = tradeapi.REST(
    os.getenv("ALPACA_KEY_ID"),
    os.getenv("ALPACA_SECRET_KEY"),
    base_url=os.getenv("ALPACA_BASE_URL"),
)  # or use ENV Vars shown below
account = api.get_account()

# retrieve all tradable positions
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
Session = sqlalchemy.orm.sessionmaker(bind=engine)
db_session = Session()

# retreive configuration parameters
config = configparser.ConfigParser()
config.read(f'{os.getenv("CONFIG_FILE_ABSOLUTE_PATH")}/algo_settings.cfg')

# read S&P etf

market_history = history(
    engine=engine,
    db_session=db_session,
    tickers=[config["model"]["market"]],
    days=config["model"]["trend_window_days"],
)

is_bull_market = (
    market_history["close"].tail(1).iloc[0] > market_history["close"].mean()
    and MACRO_YOY > 0.0
)


if is_bull_market:
    log("Bull Market", "success")
else:
    log("Bear Market", "warning")

# read s&p 500, 400 companies into pandas dataframe
companies = parse_wiki_sp_consituents(os.getenv("SP_CONSITUENTS").split(","))


mom_equities_data = []
for company in companies:
    # calculate inference
    equity_history = history(
        engine=engine,
        db_session=db_session,
        tickers=[company["Symbol"]],
        days=TRADING_DAYS_IN_YEAR,
    )
    if not len(equity_history):
        log("{0}, no data".format(company["Symbol"]))
        continue

    # check if stock traded > 100 day MA
    if (
        equity_history["close"].tail(1).iloc[0]
        <= equity_history["close"].iloc[-100:].mean()
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
            len(equity_history["close"]) - int(config["model"]["slope_window_days"]) :
        ]
        .pct_change()
        .abs()
    )
    if len(returns[(returns > float(config["model"]["max_stock_gap"]))]):
        log(
            "{0} moved greater than 15% in the past {1} days, skipping".format(
                company["Symbol"], config["model"]["slope_window_days"]
            ),
            "warning",
        )
        continue

    score = momentum_score(equity_history["close"])
    if score <= float(config["model"]["minimum_score_momentum"]):
        log("{0}, score {1} less than minimum".format(company["Symbol"], score))
        continue

    log(company["Symbol"], "success")
    mom_equities_data.append(
        {
            "ticker": company["Symbol"],
            "score": score,
        },
    )

mom_equities = pd.DataFrame(mom_equities_data)

if mom_equities.empty:
    log("No equities passed momentum screening. Exiting.", "error")
    exit(1)

if "ticker" not in mom_equities.columns:
    log(f"Missing 'ticker' column. Columns found: {mom_equities.columns}", "error")
    exit(1)

mom_equities = mom_equities.set_index(["ticker"])

ranking_table = mom_equities.sort_values(by=["score"], ascending=[False])

log("Ranking Table", "success")
if str2bool(os.getenv("VERBOSE", False)):
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
                side=SELL,
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
position_volatility_data = []
for ticker, _ in new_portfolio.iterrows():
    equity_history = history(
        engine=engine,
        db_session=db_session,
        tickers=[ticker],
        days=TRADING_DAYS_IN_YEAR,
    )

    position_volatility_data.append(
        {
            "ticker": ticker,
            "volatility": volatility(
                equity_history["close"], vola_window=int(config["model"]["vola_window"])
            ),
            "price": equity_history["close"].tail(1).iloc[0],
        },
    )


# calculate weights
position_volatility = pd.DataFrame(position_volatility_data).set_index("ticker")
inv_vola = 1 / position_volatility["volatility"]
sum_inv_vola = np.sum(inv_vola)
position_volatility["weight"] = inv_vola / sum_inv_vola


def process_position(security, data, is_existing_position, current_qty=0):
    qty = share_quantity(
        price=data["price"],
        weight=data["weight"],
        portfolio_value=portfolio_value,
    )

    if qty:
        diff = qty - current_qty if is_existing_position else qty
        if LIVE_TRADE:
            if is_existing_position:
                if diff > 0:
                    api.submit_order(
                        symbol=security,
                        time_in_force="day",
                        side=BUY,
                        type="market",
                        qty=diff,
                    )
                elif diff < 0:
                    api.submit_order(
                        symbol=security,
                        time_in_force="day",
                        side=SELL,
                        type="market",
                        qty=abs(diff),
                    )
            else:
                api.submit_order(
                    symbol=security,
                    time_in_force="day",
                    side=BUY,
                    type="market",
                    qty=qty,
                )

        if is_existing_position:
            action = BUY if diff > 0 else SELL
        else:
            action = BUY

        updated_positions.append(
            {
                "security": security,
                "action": action,
                "qty": qty,
                "diff": diff,
            }
        )
        log(f"{security}: {qty}", "info")
    else:
        updated_positions.append(
            {
                "security": security,
                "action": BUY,
                "qty": 0,
                "diff": -current_qty if is_existing_position else 0,
            }
        )
        log(f"{security}: 0", "warning")

    return qty


# Main bull market execution
# order market positions
log("Positions", "success")
portfolio_value = round(float(account.equity), 3)
updated_positions = []
positions = 0
market_weight = 0.0

if is_bull_market:
    for security, data in position_volatility.iterrows():
        asset = api.get_asset(security)
        if not asset.tradable:
            log(f"{security} is not tradable, skipping", "error")
            continue

        if security in kept_positions:
            current_qty = int(api.get_position(security).qty)
            qty = process_position(
                security, data, is_existing_position=True, current_qty=current_qty
            )
        else:
            qty = process_position(security, data, is_existing_position=False)

        if qty:
            market_weight += data["weight"]
            positions += 1

    if str2bool(os.getenv("VERBOSE", False)):
        print(f"desired portfolio size: {len(new_portfolio)}")
        print(f"position size: {positions}")

else:

    for position in kept_positions:
        if LIVE_TRADE:
            api.submit_order(
                symbol=position,
                time_in_force="day",
                side=SELL,
                type="market",
                qty=api.get_position(position).qty,
            )
        log(f"drop position {position}", "info")


if market_weight:
    if str2bool(os.getenv("VERBOSE", False)):
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
    TO_ADDRESSES = [addr for addr in os.getenv("TO_ADDRESSES", "").split(",") if addr]
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
        log(f"Email sent to {to_address}", "info")
        ses.send_html_email(
            to_address=to_address, subject=subject, content=message_body_html
        )

if str2bool(os.getenv("VERBOSE", False)):
    print("---------------------------------------------------\n")
    print(message_body_plain)
