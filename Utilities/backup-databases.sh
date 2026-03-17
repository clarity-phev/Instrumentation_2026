#!/bin/bash

# Invoke this with a 'cron' job.  Here is an example (runs at 9:00 AM every day):
# 0 9 * * * /opt/logger/collectors/Utilities/backup-databases.sh >> /mnt/ssd/backup-databases.log 2>&1

# ====== CONFIGURATION ======

SOURCE_DIR="/opt/logger/datastores"
DEST_DIR="/mnt/ssd/instrumentation-backups"

# Number of backup copies to retain
RETENTION=7

# Compression options per DB (true/false)
COMPRESS_energy=false
COMPRESS_furnace=false
COMPRESS_temperature=false

# Busy timeout in milliseconds
BUSY_TIMEOUT=20000

# ===========================

DATE=$(date +"%Y-%m-%d_%H-%M-%S")

mkdir -p "$DEST_DIR"

backup_database () {

    DB_NAME=$1
    COMPRESS=$2

    SRC_DB="$SOURCE_DIR/$DB_NAME"
    BASE_NAME="${DB_NAME%.db}"
    BACKUP_FILE="$DEST_DIR/${BASE_NAME}_${DATE}.db"

    echo "Backing up $DB_NAME..."

    sqlite3 "$SRC_DB" <<EOF
PRAGMA busy_timeout=$BUSY_TIMEOUT;
.backup '$BACKUP_FILE'
EOF

    if [ $? -ne 0 ]; then
        echo "Backup failed for $DB_NAME"
        return 1
    fi

    if [ "$COMPRESS" = true ]; then
        gzip "$BACKUP_FILE"
        BACKUP_FILE="${BACKUP_FILE}.gz"
    fi

    # Retention pruning
    if [ "$COMPRESS" = true ]; then
        ls -1t "$DEST_DIR/${BASE_NAME}_"*.db.gz 2>/dev/null | tail -n +$((RETENTION+1)) | xargs -r rm --
    else
        ls -1t "$DEST_DIR/${BASE_NAME}_"*.db 2>/dev/null | tail -n +$((RETENTION+1)) | xargs -r rm --
    fi

    echo "Backup complete for $DB_NAME"
}

# ===== Run backups =====

backup_database "energy.db" $COMPRESS_energy
backup_database "furnace.db" $COMPRESS_furnace
backup_database "temperature.db" $COMPRESS_temperature

echo "All backups completed."
