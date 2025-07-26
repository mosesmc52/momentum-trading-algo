#!/bin/bash
set -e

# Create log file early
touch /var/log/cron.log

# Export environment (optional for real jobs)
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

# Write cron schedule
cat <<EOF > scheduler.txt
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
* * * * * echo "Cron job ran at \$(date)" >> /var/log/cron.log 2>&1
EOF

# Install cron job
crontab scheduler.txt

# Start cron in foreground
cron -f
