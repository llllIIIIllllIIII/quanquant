#!/usr/bin/env bash
# Deploy latest main to the GCP VM: git pull + rebuild the app container.
# Postgres/Caddy containers and the pgdata volume are untouched.
# Usage: ./scripts/deploy.sh   (after git push)
set -euo pipefail

ZONE="${ZONE:-asia-east1-b}"
VM="${VM:-quanquant}"

gcloud compute ssh "$VM" --zone "$ZONE" --command \
  'cd ~/quanquant && git pull --ff-only && docker compose up -d --build && docker compose ps'
