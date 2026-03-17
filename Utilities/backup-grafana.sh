#!/bin/bash

# Must be run as root

# To run as a cron job, 
# 0 9 * * * /opt/logger/collectors/Utilities/backup-grafana.sh >> /mnt/ssd/backup-grafana.log 2>&1


# ==========================
# CONFIGURATION
# ==========================

BACKUP_BASE="/mnt/ssd/grafana-backups"
RETENTION=7
BUSY_TIMEOUT=30000   # milliseconds (30 seconds)

# ==========================
# SETUP
# ==========================

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="grafana-backup-$DATE"
WORK_DIR="$BACKUP_BASE/$BACKUP_NAME"
ARCHIVE="$BACKUP_BASE/$BACKUP_NAME.tar.gz"

set -e  # exit on error

mkdir -p "$WORK_DIR"

echo "Starting Grafana backup: $DATE"

# ==========================
# DATABASE BACKUP (ONLINE)
# ==========================

echo "Backing up SQLite database (online)..."

sqlite3 /var/lib/grafana/grafana.db <<EOF
PRAGMA busy_timeout=$BUSY_TIMEOUT;
.backup '$WORK_DIR/grafana.db'
EOF

echo "Database backup complete."

# ==========================
# CONFIG BACKUP
# ==========================

echo "Backing up /etc/grafana ..."
cp -a /etc/grafana "$WORK_DIR/"

# ==========================
# ARCHIVE CREATION
# ==========================

echo "Creating compressed archive..."
tar -czf "$ARCHIVE" -C "$BACKUP_BASE" "$BACKUP_NAME"

# Cleanup working directory
rm -rf "$WORK_DIR"

# ==========================
# RETENTION POLICY
# ==========================

echo "Applying retention policy (keeping last $RETENTION backups)..."

ls -1t "$BACKUP_BASE"/grafana-backup-[0-9]*.tar.gz 2>/dev/null | \
tail -n +$((RETENTION+1)) | \
xargs -r rm --

echo "Backup completed successfully."
echo "Stored at: $ARCHIVE"
