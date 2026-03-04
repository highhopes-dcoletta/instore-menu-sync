#!/bin/bash
# deploy.sh — provision or update the sync script on the High Hopes VPS
# Usage: ./deploy.sh

set -e

HOST="highhopes@104.236.29.111"
REMOTE_DIR="/home/highhopes/sync"
REPO="https://github.com/highhopes-dcoletta/instore-menu-sync.git"
CRON="*/5 9-21 * * * cd $REMOTE_DIR && venv/bin/python sync.py --target main >> $REMOTE_DIR/sync.log 2>&1"

echo "==> Copying .env to server..."
scp .env "$HOST:$REMOTE_DIR/.env" 2>/dev/null || {
    # Directory might not exist yet — create it first then retry
    ssh "$HOST" "mkdir -p $REMOTE_DIR"
    scp .env "$HOST:$REMOTE_DIR/.env"
}

echo "==> Setting up server..."
ssh "$HOST" bash << EOF
set -e

# Clone or update repo
if [ -d "$REMOTE_DIR/.git" ]; then
    echo "  Pulling latest..."
    cd $REMOTE_DIR
    git pull
else
    echo "  Cloning repo..."
    git clone $REPO $REMOTE_DIR
    cd $REMOTE_DIR
fi

cd $REMOTE_DIR

# Set up Python venv
if [ ! -d "venv" ]; then
    echo "  Creating virtual environment..."
    python3 -m venv venv
fi

echo "  Installing dependencies..."
venv/bin/pip install --quiet -r requirements.txt

# Set up cron job (idempotent)
echo "  Setting up cron job..."
( crontab -l 2>/dev/null | grep -v "sync.py" ; echo "$CRON" ) | crontab -

echo "  Cron jobs:"
crontab -l

# Set last_count.json if it doesn't exist
if [ ! -f "last_count.json" ]; then
    echo '{"last_count": null, "first_run_complete": true, "consecutive_failures": 0, "last_errors": []}' > last_count.json
fi

echo "  Done."
EOF

echo ""
echo "==> Deploy complete. Running a test sync..."
ssh "$HOST" "cd $REMOTE_DIR && venv/bin/python sync.py --target main 2>&1 | tail -5"
