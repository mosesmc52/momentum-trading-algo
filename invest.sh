#!/bin/bash

cd ~/mom-trading-algo/
source ~/mom-trading-algo/venv/bin/activate
python ingest.py
python algo_momentum.py
