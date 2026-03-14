#!/bin/bash

# ==========================
# USER SETTINGS (EDIT HERE)
# ==========================

BACKUP_BASE="/home/tom/grafana-backup"   # <-- Change this path anytime
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="grafana-backup-$DATE"
WORK_DIR="$BACKUP_BASE/$BACKUP_NAME"
ARCHIVE="$BACKUP_BASE/$BACKUP_NAME.tar.gz"

# ==========================
# START BACKUP
# ==========================

echo "Stopping Grafana..."
systemctl stop grafana-server || { echo "Failed to stop Grafana"; exit 1; }

echo "Creating backup directory..."
mkdir -p "$WORK_DIR"

echo "Backing up SQLite database..."
cp /var/lib/grafana/grafana.db "$WORK_DIR/" || { echo "Database copy failed"; exit 1; }

echo "Backing up configuration..."
cp -r /etc/grafana "$WORK_DIR/" || { echo "Config copy failed"; exit 1; }

echo "Restarting Grafana..."
systemctl start grafana-server

echo "Creating compressed archive..."
tar -czf "$ARCHIVE" -C "$BACKUP_BASE" "$BACKUP_NAME"

echo "Cleaning up working directory..."
rm -rf "$WORK_DIR"

echo "Backup completed successfully."
echo "Backup stored at: $ARCHIVE"
