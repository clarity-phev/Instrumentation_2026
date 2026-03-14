#!/bin/bash

# ==========================
# USER SETTINGS (EDIT HERE)
# ==========================

BACKUP_BASE="/home/tom/grafana-backup"   # <-- Change if needed

# ==========================
# INPUT VALIDATION
# ==========================

if [ -z "$1" ]; then
  echo "Usage: sudo ./restore-grafana.sh grafana-backup-YYYYMMDD_HHMMSS.tar.gz"
  exit 1
fi

ARCHIVE="$BACKUP_BASE/$1"

if [ ! -f "$ARCHIVE" ]; then
  echo "Backup file not found: $ARCHIVE"
  exit 1
fi

# Extract folder name from archive filename
FOLDER_NAME="${1%.tar.gz}"
WORK_DIR="$BACKUP_BASE/$FOLDER_NAME"

echo "WARNING: This will overwrite your current Grafana database and configuration."
read -p "Type YES to continue: " CONFIRM

if [ "$CONFIRM" != "YES" ]; then
  echo "Restore cancelled."
  exit 0
fi

# ==========================
# RESTORE PROCESS
# ==========================

echo "Stopping Grafana..."
systemctl stop grafana-server || { echo "Failed to stop Grafana"; exit 1; }

echo "Extracting archive..."
tar -xzf "$ARCHIVE" -C "$BACKUP_BASE" || { echo "Extraction failed"; exit 1; }

echo "Restoring database..."
cp "$WORK_DIR/grafana.db" /var/lib/grafana/ || { echo "Database restore failed"; exit 1; }

echo "Restoring configuration..."
rm -rf /etc/grafana
cp -r "$WORK_DIR/grafana" /etc/grafana || { echo "Config restore failed"; exit 1; }

echo "Fixing permissions..."
chown -R grafana:grafana /var/lib/grafana
chown -R grafana:grafana /etc/grafana

echo "Starting Grafana..."
systemctl start grafana-server

echo "Cleaning up extracted files..."
rm -rf "$WORK_DIR"

echo "Restore completed successfully."
