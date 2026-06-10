"""
predict_upcoming.py — Cloud Run Job: generate fight outcome predictions.

Sources (in priority order):
  1. UFC_features WHERE split='upcoming'   — fully engineered features (future)
  2. UFC_model_prediction_input            — fallback: maps rolling stats on-the-fly

Output: UFC_data.predictions (WRITE_TRUNCATE)

Columns written:
  fight_url, event_date, event_name, fighter_1, fighter_2, weight_class,
  model_prob_f1, model_prob_f2, market_prob_f1, market_prob_f2,
  edge, recommended, predicted_at, model_version
"""

import logging
import re
import sys
import warnings
from datetime import timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery

from config import DATASET, PROJECT_ID, TABLE_FEATURES
from model_utils import IsotonicCalibratedClassifier  # noqa: F401 — required for joblib deserialization

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

MODEL_PATH        = Path("models/xgb_v2.joblib")
TABLE_PREDICTIONS = "predictions"
TABLE_PRED_INPUT  = "UFC_model_prediction_input"
EDGE_THRESHOLD    = 0.10
CAT_COLS          = ["weight_class", "f_1_fighter_stance", "f_2_fighter_stance"]

META_COLS = {
    "fight_url", "event_date", "split", "as_of_date",
    "market_prob_f1", "market_prob_f2", "winner_encoded",
}


# ── Model loading ──────────────────────────────────────────────────────────────

def load_model():
    if not MODEL_PATH.exists():
        log.error("Model not found: %s  — run train_model.py first", MODEL_PATH)
        sys.exit(1)
    payload = joblib.load(MODEL_PATH)
    log.info("Loaded model %s  (%d features)", MODEL_PATH, len(payload["feat_names"]))
    return payload


# ── Data loading ───────────────────────────────────────────────────────────────

def _bq():
    return bigquery.Client(project=PROJECT_ID)


def _load_upcoming_features(client: bigquery.Client) -> pd.DataFrame:
    """Try UFC_features WHERE split='upcoming' (proper engineered features)."""
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_FEATURES}`
        WHERE split = 'upcoming'
    """
    df = client.query(sql).to_dataframe()
    log.info("UFC_features upcoming rows: %d", len(df))
    return df


def _load_prediction_input(client: bigquery.Client) -> pd.DataFrame:
    """Load UFC_model_prediction_input (fallback feature source + fighter names)."""
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_PRED_INPUT}`
    """
    df = client.query(sql).to_dataframe()
    log.info("UFC_model_prediction_input rows: %d", len(df))
    return df


def _american_to_prob(odds_series: pd.Series) -> pd.Series:
    """Convert American odds (+150 / -200) to implied probability."""
    odds = pd.to_numeric(odds_series, errors="coerce")
    pos  = odds >= 0
    prob = pd.Series(np.nan, index=odds.index)
    prob[pos]  = 100.0 / (odds[pos]  + 100.0)
    prob[~pos] = odds[~pos].abs() / (odds[~pos].abs() + 100.0)
    return prob.clip(0.01, 0.99)


# ── Feature matrix construction ────────────────────────────────────────────────

def _prepare_from_features(df: pd.DataFrame, feat_cols: list[str], encoder) -> np.ndarray:
    """Standard path: UFC_features rows have all columns needed."""
    X = df[feat_cols].copy()
    for c in X.select_dtypes(include="Int64").columns:
        X[c] = X[c].astype("float64")
    active_cats = [c for c in CAT_COLS if c in feat_cols]
    if active_cats:
        X[active_cats] = encoder.transform(X[active_cats].fillna("__missing__"))
    return X.values.astype(np.float32)


def _map_features_from_prediction_input(
    meta_df: pd.DataFrame, feat_cols: list[str], encoder
) -> np.ndarray:
    """
    Fallback path: map UFC_model_prediction_input columns to model features.

    Patterns handled:
      sapm_12_f_1        → f_1_sapm_12         (role-swapped rolling stats)
      diff_sapm_5        → f_1_sapm_5 - f_2_sapm_5
      diff_age           → f_1_age - f_2_age    (special no-window diffs)
      diff_reach_cm      → f_1_fighter_reach_cm - f_2_fighter_reach_cm
      td_def_r5_12_f_1   → NaN                  (round-specific, not in source)
    """
    result: dict[str, pd.Series] = {}

    for feat in feat_cols:
        # Direct column match
        if feat in meta_df.columns:
            result[feat] = pd.to_numeric(meta_df[feat], errors="coerce")
            continue

        # {stat}_{n}_{role}  →  {role}_{stat}_{n}
        m = re.fullmatch(r"(.+?)_(\d+)_(f_[12])$", feat)
        if m:
            stat, n, role = m.groups()
            col = f"{role}_{stat}_{n}"
            result[feat] = (
                pd.to_numeric(meta_df[col], errors="coerce")
                if col in meta_df.columns else np.nan
            )
            continue

        # Skip round-specific features (rN in name) — not computable from source
        if re.search(r"_r\d+_", feat):
            result[feat] = np.nan
            continue

        # diff_{stat}_{n}  →  f_1_{stat}_{n} - f_2_{stat}_{n}
        m = re.fullmatch(r"diff_(.+?)_(\d+)$", feat)
        if m:
            stat, n = m.groups()
            c1, c2 = f"f_1_{stat}_{n}", f"f_2_{stat}_{n}"
            if c1 in meta_df.columns and c2 in meta_df.columns:
                result[feat] = (
                    pd.to_numeric(meta_df[c1], errors="coerce") -
                    pd.to_numeric(meta_df[c2], errors="coerce")
                )
            else:
                result[feat] = np.nan
            continue

        # diff_{stat}  (no window number)
        m = re.fullmatch(r"diff_(.+)$", feat)
        if m:
            stat = m.group(1)
            c1, c2 = f"f_1_{stat}", f"f_2_{stat}"
            if c1 in meta_df.columns and c2 in meta_df.columns:
                result[feat] = (
                    pd.to_numeric(meta_df[c1], errors="coerce") -
                    pd.to_numeric(meta_df[c2], errors="coerce")
                )
            else:
                result[feat] = np.nan
            continue

        result[feat] = np.nan

    X = pd.DataFrame(result, index=meta_df.index)

    for c in X.select_dtypes(include="Int64").columns:
        X[c] = X[c].astype("float64")

    active_cats = [c for c in CAT_COLS if c in feat_cols]
    if active_cats:
        X[active_cats] = encoder.transform(X[active_cats].fillna("__missing__"))

    mapped = sum(1 for v in result.values() if not (isinstance(v, float) and np.isnan(v)))
    log.info("  Fallback feature mapping: %d / %d features resolved", mapped, len(feat_cols))

    return X.values.astype(np.float32)


# ── Prediction pipeline ────────────────────────────────────────────────────────

def build_predictions(
    model_payload: dict,
    feat_df: pd.DataFrame | None,
    meta_df: pd.DataFrame,
    source: str,
) -> pd.DataFrame:
    """
    Run model and assemble the output table.

    feat_df : fully engineered rows (UFC_features) or None if fallback
    meta_df : UFC_model_prediction_input (fighter names, odds, event info)
    source  : "UFC_features" | "UFC_model_prediction_input"
    """
    model     = model_payload["model"]
    encoder   = model_payload["encoder"]
    feat_cols = model_payload["feat_names"]
    version   = model_payload["meta"].get("version", "xgb_v2")

    if source == "UFC_features" and feat_df is not None and len(feat_df) > 0:
        log.info("Using UFC_features as feature source (%d rows)", len(feat_df))
        X = _prepare_from_features(feat_df, feat_cols, encoder)
        base_df = feat_df
    else:
        log.info("Using UFC_model_prediction_input as feature source (fallback)")
        X = _map_features_from_prediction_input(meta_df, feat_cols, encoder)
        base_df = meta_df

    proba     = model.predict_proba(X)
    prob_f1   = proba[:, 1]
    prob_f2   = proba[:, 0]

    # ── Market probabilities ──────────────────────────────────────────────────
    if "market_prob_f1" in base_df.columns:
        mkt_f1 = pd.to_numeric(base_df["market_prob_f1"], errors="coerce")
        mkt_f2 = pd.to_numeric(base_df["market_prob_f2"], errors="coerce")
    elif "f_1_odds" in meta_df.columns:
        mkt_f1 = _american_to_prob(meta_df["f_1_odds"])
        mkt_f2 = _american_to_prob(meta_df["f_2_odds"])
        log.info("  Converted American odds → implied probabilities")
    else:
        mkt_f1 = pd.Series(np.nan, index=meta_df.index)
        mkt_f2 = pd.Series(np.nan, index=meta_df.index)
        log.info("  No market odds available — edge will be null")

    edge = pd.Series(prob_f1 - mkt_f1.values, index=meta_df.index)
    edge = edge.where(mkt_f1.notna())   # NaN where market odds absent

    # ── Fighter names ─────────────────────────────────────────────────────────
    def _name(col_a, col_b):
        if col_a in meta_df.columns:
            return meta_df[col_a].fillna("").str.strip()
        a = meta_df.get(col_b.split("|")[0], pd.Series("", index=meta_df.index)).fillna("")
        return a

    f1_name = _name("f_1_full_name", "f_1_fighter_f_name")
    f2_name = _name("f_2_full_name", "f_2_fighter_f_name")

    fight_url  = base_df.get("fight_url",  pd.Series(pd.NA, index=base_df.index))
    event_name = meta_df.get("event_name", pd.Series(pd.NA, index=meta_df.index))
    event_date = pd.to_datetime(
        meta_df.get("event_date", pd.Series(pd.NaT, index=meta_df.index)),
        utc=True, errors="coerce",
    )
    wc = meta_df.get("weight_class", pd.Series(pd.NA, index=meta_df.index))

    now = pd.Timestamp.now(tz=timezone.utc)

    out = pd.DataFrame({
        "fight_url":      fight_url.values if len(fight_url) == len(meta_df) else [pd.NA] * len(meta_df),
        "event_date":     event_date.values,
        "event_name":     event_name.values,
        "fighter_1":      f1_name.values,
        "fighter_2":      f2_name.values,
        "weight_class":   wc.values,
        "model_prob_f1":  np.round(prob_f1, 4),
        "model_prob_f2":  np.round(prob_f2, 4),
        "market_prob_f1": np.round(mkt_f1.values.astype(float), 4),
        "market_prob_f2": np.round(mkt_f2.values.astype(float), 4),
        "edge":           np.round(edge.values.astype(float), 4),
        "recommended":    (edge > EDGE_THRESHOLD).values,
        "predicted_at":   now,
        "model_version":  version,
    })

    # Replace NaN / inf with None for BigQuery
    out = out.replace([np.inf, -np.inf], np.nan)

    return out


# ── BigQuery write ─────────────────────────────────────────────────────────────

def save_predictions(client: bigquery.Client, df: pd.DataFrame) -> None:
    dest = f"{PROJECT_ID}.{DATASET}.{TABLE_PREDICTIONS}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, dest, job_config=job_config)
    job.result()
    log.info("Saved %d prediction rows → %s", len(df), dest)


# ── Display ────────────────────────────────────────────────────────────────────

def print_predictions(df: pd.DataFrame) -> None:
    display_cols = [
        "fighter_1", "fighter_2", "weight_class",
        "model_prob_f1", "model_prob_f2",
        "market_prob_f1", "market_prob_f2",
        "edge", "recommended",
    ]
    present = [c for c in display_cols if c in df.columns]
    print("\n── Predictions ─────────────────────────────────────────────────")
    print(df[present].to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    n_rec = int(df["recommended"].sum())
    print(f"\n  Total fights: {len(df)}   Recommended (edge > {EDGE_THRESHOLD*100:.0f}%): {n_rec}")


# ── Entry ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("predict_upcoming.py  —  UFC fight predictions")
    log.info("=" * 60)

    model_payload = load_model()
    client        = _bq()

    # Load both sources (UFC_features is the preferred path)
    feat_df = _load_upcoming_features(client)
    meta_df = _load_prediction_input(client)

    if len(feat_df) == 0 and len(meta_df) == 0:
        log.warning("No upcoming data in either source — nothing to predict.")
        log.warning(
            "Run process_upcoming_stats.py to populate UFC_model_prediction_input, "
            "then build_features.py to populate UFC_features."
        )
        sys.exit(0)

    source = "UFC_features" if len(feat_df) > 0 else "UFC_model_prediction_input"
    if source == "UFC_model_prediction_input":
        log.info(
            "UFC_features has no upcoming rows — using UFC_model_prediction_input (fallback). "
            "Run build_features.py to enable full feature set."
        )

    df_out = build_predictions(model_payload, feat_df, meta_df, source)

    print_predictions(df_out)
    save_predictions(client, df_out)

    log.info("=" * 60)
    log.info("predict_upcoming complete.")


if __name__ == "__main__":
    main()
