#!/bin/bash

# create sqllitedb
echo "create sqllitedb"
python -c '
from database import ( init_db )
init_db()
'

# Start the run once job.
echo "Momentum Algo Docker container has been started"

declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

echo "start" >> /var/log/cron.log 2>&1

# Setup a cron schedule to run 1st of every month
echo "SHELL=/bin/bash
BASH_ENV=/container.env
0 0 1 * * python /app/ingest.py; python /app/algo_momentum.py >> /var/log/cron.log 2>&1
# This extra line makes it a valid cron" > scheduler.txt

crontab scheduler.txt
cron

touch /var/log/cron.log
echo "running....."
