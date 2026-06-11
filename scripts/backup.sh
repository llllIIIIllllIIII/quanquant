#!/usr/bin/env bash
# Daily Postgres backup -> GCS. Runs ON THE VM via cron:
#   15 21 * * * BACKUP_BUCKET=gs://<bucket> /home/<user>/quanquant/scripts/backup.sh >> /home/<user>/backup.log 2>&1
# (21:15 UTC = 05:15 CST, right after the night session closes.)
set -euo pipefail

cd "$(dirname "$0")/.."
BUCKET="${BACKUP_BUCKET:?set BACKUP_BUCKET=gs://<bucket>}"
F="quanquant-$(date +%F).dump"

docker compose exec -T postgres pg_dump -U quanquant -Fc quanquant > "/tmp/$F"
gcloud storage cp "/tmp/$F" "$BUCKET/pg/$F"
rm -f "/tmp/$F"
echo "$(date -u +%FT%TZ) backed up $F"
