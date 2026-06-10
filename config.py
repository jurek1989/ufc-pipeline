import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "ultra-acre-443816-g8")

DATASET = "UFC_data"
MODEL_DATASET = "UFC_model"

# Tables — UFC_data
TABLE_EVENTS_URLS = "UFC_events_urls"
TABLE_EVENTS_DATA = "UFC_events_data"
TABLE_FIGHTS_URLS = "UFC_fights_urls"
TABLE_FIGHTS_DATA = "UFC_fights_data"
TABLE_FIGHTS_STATS = "UFC_fights_stats_data"
TABLE_FIGHTERS_URLS = "UFC_fighters_urls"
TABLE_FIGHTERS_DATA = "UFC_fighters_data"
TABLE_RANKINGS = "UFC_rankings"
TABLE_COMING_EVENT = "UFC_coming_event"
TABLE_BETTING_ODDS_API = "UFC_betting_odds_api"
TABLE_ODDS_SNAPSHOTS = "odds_snapshots"

# Tables — UFC_model
TABLE_MODEL_ROUNDS = "UFC_model_full_analysis_rounds"
TABLE_FEATURES = "UFC_features"

# Views / legacy tables — UFC_data (read-only)
VIEW_FULL_DATA = "full_data"
VIEW_FULL_DATA_SILVER_PLUS = "full_data_silver_plus"
VIEW_COMING_EVENT_WITH_RANKINGS = "UFC_coming_event_with_rankings_placeholder"
TABLE_MODEL_ANALYSIS = "UFC_model_full_analysis"
TABLE_MODEL_PREDICTION_INPUT = "UFC_model_prediction_input"

# Kaggle
KAGGLE_DATASET_SLUG = "jerzyszocik/ufc-fight-forecast-complete-gold-modeling-dataset"

# Secrets — environment only, never hardcoded
ODDS_API_KEY   = os.environ.get("ODDS_API_KEY", "")
KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME", "")
KAGGLE_KEY      = os.environ.get("KAGGLE_KEY", "")
