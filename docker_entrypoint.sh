#!/bin/bash

# create SQLite DB
echo "create sqllitedb"
python -c '
from database import init_db
init_db()
'

# ingest Equities
echo "ingest stock equities"
python ingest.py

# Write env
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Create log file early
touch /var/log/cron.log

# Write cron schedule
echo "SHELL=/bin/bash
BASH_ENV=/container.env
PATH=/usr/local/bin:/usr/bin:/bin
*/15 * * * * cd /app && /usr/bin/python3 ingest.py && /usr/bin/python3 algo_momentum.py >> /var/log/cron.log 2>&1
" > scheduler.txt

# Set and start cron
crontab scheduler.txt
cron -f
