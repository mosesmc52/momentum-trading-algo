# trading-algo
Momentum Trading Algorithm

This repo is an adjusted implementation of momentum trading algorithm.  

# backtest
Python Algo Investment Strategy Backtests

#### Install App
1) Create Virtual Env
2) In console type the following command in Virtual Env
```console
pip install -r requirements.txt
````

#### Create SQLlite DB
1) open python in terminal
2) Insert the following commands
```
from database import ( init_db )
init_db()
````

#### CRON Tab
0 7 1 * * [path]/invest.sh > [outputpath]/mom-algo.log 2>&1
