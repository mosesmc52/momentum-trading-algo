#!/bin/bash
set -e

# create SQLite DB
echo "create sqllitedb"
python -c '
from database import init_db
init_db()
'

# ingest Equities
echo "ingest stock equities"
python ingest.py

# Create log file early
touch /var/log/cron.log

# Export environment
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Write cron schedule
echo "SHELL=/bin/bash
BASH_ENV=/container.env
PATH=$PATH
*/15 * * * * cd /app && /usr/bin/python3 ingest.py && /usr/bin/python3 algo_momentum.py >> /var/log/cron.log 2>&1
" > scheduler.txt

# Install cron job
crontab scheduler.txt

# Start cron in foreground
cron && tail -f /var/log/cron.log
