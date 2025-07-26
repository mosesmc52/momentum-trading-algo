#!/bin/bash
set -e

echo "Creating cron log..."
touch /var/log/cron.log

echo "Writing scheduler.txt to /app..."
cat <<EOF > /app/scheduler.txt
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
* * * * * echo "Cron job ran at \$(date)" >> /var/log/cron.log 2>&1
EOF

echo "Installing crontab:"
cat /app/scheduler.txt
crontab /app/scheduler.txt

echo "Crontab after install:"
crontab -l

echo "Starting cron..."
cron -f
