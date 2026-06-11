#!/bin/bash
# Deploy (or update) all UFC pipeline Cloud Run Jobs.
# Usage: bash deploy_jobs.sh
set -euo pipefail

PROJECT="ultra-acre-443816-g8"
REGION="europe-central2"
IMAGE="europe-central2-docker.pkg.dev/${PROJECT}/ufc-pipeline/ufc-jobs:latest"

# ── helpers ────────────────────────────────────────────────────────────────────

create_or_update() {
  local name="$1"; shift
  if gcloud run jobs describe "$name" --region "$REGION" --quiet &>/dev/null; then
    gcloud run jobs update "$name" --region "$REGION" "$@" --quiet
    echo "  updated  $name"
  else
    gcloud run jobs create "$name" --region "$REGION" "$@" --quiet
    echo "  created  $name"
  fi
}

BASE=(
  --image       "$IMAGE"
  --task-timeout 30m
  --max-retries  1
  --memory       2Gi
  --set-env-vars "GCP_PROJECT_ID=${PROJECT}"
)

# ── scrapers ───────────────────────────────────────────────────────────────────

echo "Deploying Cloud Run Jobs to $REGION..."
echo ""

create_or_update scrape-event-urls     "${BASE[@]}" --command python --args scrape_event_urls.py
create_or_update scrape-event-data     "${BASE[@]}" --command python --args scrape_event_data.py
create_or_update scrape-fight-urls     "${BASE[@]}" --command python --args scrape_fight_urls.py
create_or_update scrape-fight-data     "${BASE[@]}" --command python --args scrape_fight_data.py
create_or_update scrape-fight-stats    "${BASE[@]}" --command python --args scrape_fight_stats.py
create_or_update scrape-fighter-urls   "${BASE[@]}" --command python --args scrape_fighter_urls.py
create_or_update scrape-fighter-data   "${BASE[@]}" --command python --args scrape_fighter_data.py
create_or_update scrape-upcoming-event "${BASE[@]}" --command python --args scrape_upcoming_event.py
create_or_update scrape-rankings       "${BASE[@]}" --command python --args scrape_rankings.py
create_or_update scrape-odds           "${BASE[@]}" --command python --args scrape_bfo_odds.py
create_or_update odds-watcher          "${BASE[@]}" --command python --args scrape_bfo_odds_watcher.py

# build-features: extra CLI arg --mode historical
create_or_update build-features \
  "${BASE[@]}" \
  --command python \
  --args "build_features.py,--mode,historical"

# publish-to-kaggle: Kaggle credentials as env vars
# TODO: replace with Secret Manager references once secrets are configured
create_or_update publish-to-kaggle \
  --image       "$IMAGE" \
  --task-timeout 30m \
  --max-retries  1 \
  --memory       2Gi \
  --command python \
  --args "publish_to_kaggle.py" \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT},KAGGLE_USERNAME=placeholder,KAGGLE_KEY=placeholder"

# ── summary ────────────────────────────────────────────────────────────────────

echo ""
echo "Done. Current jobs in $REGION:"
gcloud run jobs list --region "$REGION" \
  --format "table(name,region,metadata.annotations.'run.googleapis.com/lastModifier':label=LAST_MODIFIED)"
