"""
train_model.py — Cloud Run Job: train XGBoost fight outcome predictor.

v1  models/xgb_v1.joblib  — baseline, all features, 1994+, light regularization
v2  models/xgb_v2.joblib  — filtered 2016+, top-100 features from v1, strong regularization

Time splits:
  train_xgb  : DATA_CUTOFF ≤ event_date < CAL_CUTOFF
  val_cal    : CAL_CUTOFF  ≤ event_date < TRAIN_CUTOFF  (early stopping + calibration)
  test       : event_date  ≥ TRAIN_CUTOFF               (held-out evaluation)
"""

import logging
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.calibration import calibration_curve
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBClassifier

from config import DATASET, PROJECT_ID, TABLE_FEATURES
from model_utils import IsotonicCalibratedClassifier

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DATA_CUTOFF  = "2016-01-01"   # v2: drop pre-modern-UFC era noise
CAL_CUTOFF   = "2022-01-01"   # start of val/calibration window (~1000 rows)
TRAIN_CUTOFF = "2024-01-01"   # start of held-out test set

TOP_N_FEATURES     = 100
PROB_CLIP          = (0.05, 0.95)   # v2 never outputs certainty
CLV_EDGE_THRESHOLD = 0.10           # v2 uses 10% (less noise than 5%)

META_COLS = {
    "fight_url", "event_date", "split", "as_of_date",
    "market_prob_f1", "market_prob_f2", "winner_encoded",
}
CAT_COLS = ["weight_class", "f_1_fighter_stance", "f_2_fighter_stance"]

MODEL_DIR      = Path("models")
V1_MODEL_PATH  = MODEL_DIR / "xgb_v1.joblib"
V2_MODEL_PATH  = MODEL_DIR / "xgb_v2.joblib"

# ── v1 params (kept for reference / quick pre-fit) ────────────────────────────
XGB_PARAMS_V1 = dict(
    n_estimators=500,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,
    eval_metric="logloss",
    early_stopping_rounds=50,
    tree_method="hist",
    random_state=42,
    n_jobs=-1,
)

# ── v2 params: strong regularization ─────────────────────────────────────────
XGB_PARAMS_V2 = dict(
    n_estimators=1000,
    max_depth=3,
    learning_rate=0.05,
    subsample=0.7,
    colsample_bytree=0.3,
    min_child_weight=10,
    reg_alpha=1.0,
    reg_lambda=5.0,
    eval_metric="logloss",
    early_stopping_rounds=50,
    tree_method="hist",
    random_state=42,
    n_jobs=-1,
)


# ── Data loading ───────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    log.info("Loading UFC_features from BigQuery (%s.%s)…", DATASET, TABLE_FEATURES)
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_FEATURES}`
        WHERE split = 'train'
    """
    df = client.query(sql).to_dataframe()
    log.info("  raw rows: %d  cols: %d", len(df), len(df.columns))
    return df


# ── Preprocessing ──────────────────────────────────────────────────────────────

def _all_feature_cols(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in META_COLS and "legacy" not in c.lower()
    ]


def _prepare(df: pd.DataFrame, feat_cols: list[str], encoder: OrdinalEncoder, fit: bool) -> np.ndarray:
    """Return float32 array for XGBoost, using only feat_cols."""
    X = df[feat_cols].copy()

    for c in X.select_dtypes(include="Int64").columns:
        X[c] = X[c].astype("float64")

    active_cats = [c for c in CAT_COLS if c in feat_cols]
    if active_cats:
        if fit:
            encoder.fit(X[active_cats].fillna("__missing__"))
        X[active_cats] = encoder.transform(X[active_cats].fillna("__missing__"))

    return X.values.astype(np.float32)


# ── Feature selection ──────────────────────────────────────────────────────────

def select_top_features(df: pd.DataFrame, feat_names_all: list[str]) -> list[str]:
    """
    Return top-N feature names by XGBoost importance.
    Source priority:
      1. v1 model importances (fast, no re-fit needed)
      2. Quick pre-fit on the full feature set (fallback)
    """
    if V1_MODEL_PATH.exists():
        log.info("Loading v1 feature importances from %s…", V1_MODEL_PATH)
        v1 = joblib.load(V1_MODEL_PATH)
        v1_xgb   = v1["model"].base
        v1_feats = v1["feat_names"]
        imp_map  = dict(zip(v1_feats, v1_xgb.feature_importances_))
        ranked   = sorted(
            [f for f in feat_names_all if f in imp_map],
            key=lambda f: -imp_map[f],
        )
        top = ranked[:TOP_N_FEATURES]
        log.info("  Feature selection (v1): %d → %d features", len(feat_names_all), len(top))
        return top

    log.info("v1 not found — quick pre-fit for feature selection…")
    enc_q = OrdinalEncoder(
        handle_unknown="use_encoded_value",
        unknown_value=-1,
        encoded_missing_value=-1,
    )
    df_clean = df[df["winner_encoded"] != -1].copy()
    df_clean["event_date"] = pd.to_datetime(df_clean["event_date"], utc=True)
    df_clean = df_clean.sort_values("event_date")

    X_all = _prepare(df_clean, feat_names_all, enc_q, fit=True)
    y_all = df_clean["winner_encoded"].astype(int).values

    quick = XGBClassifier(
        n_estimators=100, max_depth=4, learning_rate=0.1,
        tree_method="hist", n_jobs=-1, random_state=42,
    )
    quick.fit(X_all, y_all, verbose=False)
    top_idx = np.argsort(quick.feature_importances_)[-TOP_N_FEATURES:][::-1]
    top = [feat_names_all[i] for i in top_idx]
    log.info("  Feature selection (quick pre-fit): %d → %d features", len(feat_names_all), len(top))
    return top


# ── Dataset builder ────────────────────────────────────────────────────────────

def build_datasets(df: pd.DataFrame, feat_cols: list[str], data_cutoff: str | None = None):
    df = df[df["winner_encoded"] != -1].copy()
    df["event_date"] = pd.to_datetime(df["event_date"], utc=True)
    df = df.sort_values("event_date").reset_index(drop=True)
    log.info("  after dropping draws/NC: %d rows", len(df))

    if data_cutoff:
        cutoff_ts = pd.Timestamp(data_cutoff, tz="UTC")
        df = df[df["event_date"] >= cutoff_ts].reset_index(drop=True)
        log.info("  after DATA_CUTOFF (%s): %d rows", data_cutoff, len(df))

    cal_ts   = pd.Timestamp(CAL_CUTOFF,   tz="UTC")
    train_ts = pd.Timestamp(TRAIN_CUTOFF, tz="UTC")

    mask_train = df["event_date"] < cal_ts
    mask_val   = (df["event_date"] >= cal_ts) & (df["event_date"] < train_ts)
    mask_test  = df["event_date"] >= train_ts

    train_df = df[mask_train]
    val_df   = df[mask_val]
    test_df  = df[mask_test]

    log.info(
        "  splits — train: %d  val_cal: %d  test: %d",
        len(train_df), len(val_df), len(test_df),
    )
    log.info(
        "  train %s → %s",
        train_df["event_date"].min().date(), train_df["event_date"].max().date(),
    )
    log.info(
        "  test  %s → %s",
        test_df["event_date"].min().date(), test_df["event_date"].max().date(),
    )

    encoder = OrdinalEncoder(
        handle_unknown="use_encoded_value",
        unknown_value=-1,
        encoded_missing_value=-1,
    )

    X_train = _prepare(train_df, feat_cols, encoder, fit=True)
    X_val   = _prepare(val_df,   feat_cols, encoder, fit=False)
    X_test  = _prepare(test_df,  feat_cols, encoder, fit=False)

    y_train = train_df["winner_encoded"].astype(int).values
    y_val   = val_df["winner_encoded"].astype(int).values
    y_test  = test_df["winner_encoded"].astype(int).values

    test_meta = test_df[
        ["event_date", "market_prob_f1", "market_prob_f2", "winner_encoded"]
    ].reset_index(drop=True)

    return (
        X_train, y_train,
        X_val,   y_val,
        X_test,  y_test,
        encoder,
        test_meta,
        test_df,   # raw test df — needed for v1 comparison
    )


# ── Training ───────────────────────────────────────────────────────────────────

def train_xgboost(X_train, y_train, X_val, y_val, params: dict) -> XGBClassifier:
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    spw = neg / pos
    log.info(
        "Class balance — pos(f1): %d  neg(f2): %d  scale_pos_weight=%.3f",
        pos, neg, spw,
    )
    xgb = XGBClassifier(**params, scale_pos_weight=spw)
    log.info("Training XGBClassifier…")
    xgb.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=100)
    log.info("Best iteration: %d", xgb.best_iteration)
    return xgb


# ── Calibration ────────────────────────────────────────────────────────────────


def calibrate(
    xgb: XGBClassifier,
    X_val, y_val,
    prob_clip: tuple[float, float] | None = None,
) -> IsotonicCalibratedClassifier:
    log.info("Calibrating (IsotonicRegression, clip=%s)…", prob_clip)
    cal = IsotonicCalibratedClassifier(xgb, prob_clip=prob_clip)
    cal.fit(X_val, y_val)
    return cal


# ── Evaluation helpers ─────────────────────────────────────────────────────────

def _metrics_row(label: str, y_true, y_prob) -> dict:
    y_pred = (y_prob >= 0.5).astype(int)
    return {
        "split":    label,
        "n":        len(y_true),
        "accuracy": accuracy_score(y_true, y_pred),
        "log_loss": log_loss(y_true, y_prob),
        "brier":    brier_score_loss(y_true, y_prob),
    }


def _reliability_table(y_true, y_prob, n_bins: int = 10) -> pd.DataFrame:
    prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=n_bins, strategy="quantile")
    step = len(y_prob) // n_bins
    sorted_prob = np.sort(y_prob)
    rows = []
    for i, (pp, pt) in enumerate(zip(prob_pred, prob_true)):
        lo = sorted_prob[i * step] if i * step < len(sorted_prob) else 0
        hi = sorted_prob[min((i + 1) * step - 1, len(sorted_prob) - 1)]
        rows.append({
            "bin":         f"{lo:.2f}–{hi:.2f}",
            "mean_pred":   round(pp, 4),
            "actual_rate": round(pt, 4),
            "n":           step,
            "gap":         round(pt - pp, 4),
        })
    return pd.DataFrame(rows)


def _feature_importance(xgb: XGBClassifier, feat_names: list[str], top_n: int = 30) -> pd.DataFrame:
    imp = xgb.feature_importances_
    df  = pd.DataFrame({"feature": feat_names, "importance": imp})
    return df.nlargest(top_n, "importance").reset_index(drop=True)


def _clv_roi(test_meta: pd.DataFrame, y_prob_test, edge_threshold: float) -> dict | None:
    meta = test_meta.copy()
    meta["model_prob_f1"] = y_prob_test
    meta["edge_f1"]       = meta["model_prob_f1"] - meta["market_prob_f1"]
    meta["winner"]        = meta["winner_encoded"].astype(int)

    has_market = meta["market_prob_f1"].notna() & meta["market_prob_f2"].notna()
    meta_m     = meta[has_market].copy()
    log.info("  Test fights with market odds: %d / %d", len(meta_m), len(meta))

    if len(meta_m) == 0:
        log.warning("  No market odds in test set — skipping CLV/ROI")
        return None

    meta_m["value_f1"] = meta_m["edge_f1"] > 0
    correct = (
        (meta_m["value_f1"]  & (meta_m["winner"] == 1)) |
        (~meta_m["value_f1"] & (meta_m["winner"] == 0))
    )
    clv_acc = correct.mean()

    bets = meta_m[meta_m["edge_f1"].abs() > edge_threshold].copy()
    roi = win_rate = None
    if len(bets) > 0:
        dec_f1  = 1.0 / bets["market_prob_f1"].clip(0.01, 0.99)
        dec_f2  = 1.0 / bets["market_prob_f2"].clip(0.01, 0.99)
        profit  = np.where(
            bets["edge_f1"] > 0,
            np.where(bets["winner"] == 1, dec_f1 - 1, -1.0),
            np.where(bets["winner"] == 0, dec_f2 - 1, -1.0),
        )
        bets = bets.copy()
        bets["profit"] = profit
        roi      = float(profit.mean())
        win_rate = float((profit > 0).mean())
    else:
        log.warning("  No bets with |edge| > %.0f%%", edge_threshold * 100)

    return {
        "n_with_odds":   len(meta_m),
        "clv_accuracy":  clv_acc,
        "threshold":     edge_threshold,
        "n_bets":        len(bets),
        "roi":           roi,
        "win_rate":      win_rate,
        "bets_df":       bets if len(bets) > 0 else None,
    }


# ── Full evaluation ────────────────────────────────────────────────────────────

def evaluate(
    label: str,
    cal_model: IsotonicCalibratedClassifier,
    xgb: XGBClassifier,
    X_train, y_train,
    X_val,   y_val,
    X_test,  y_test,
    feat_names: list[str],
    test_meta: pd.DataFrame,
):
    log.info("=" * 60)
    log.info("EVALUATION — %s", label)
    log.info("=" * 60)

    prob_train = cal_model.predict_proba(X_train)[:, 1]
    prob_val   = cal_model.predict_proba(X_val)[:, 1]
    prob_test  = cal_model.predict_proba(X_test)[:, 1]

    metrics = pd.DataFrame([
        _metrics_row("train", y_train, prob_train),
        _metrics_row("val",   y_val,   prob_val),
        _metrics_row("test",  y_test,  prob_test),
    ])
    print(f"\n── Core Metrics [{label}] ───────────────────────────────")
    print(metrics.to_string(index=False, float_format=lambda x: f"{x:.4f}"))

    rel = _reliability_table(y_test, prob_test)
    print(f"\n── Reliability Curve [{label}] (test, 10 quantile bins) ─")
    print(rel.to_string(index=False))

    fi = _feature_importance(xgb, feat_names, top_n=30)
    print(f"\n── Top 30 Feature Importances [{label}] ─────────────────")
    print(fi.to_string(index=False))

    print(f"\n── CLV & ROI [{label}] (test set, edge > {CLV_EDGE_THRESHOLD*100:.0f}%) ──")
    clv = _clv_roi(test_meta, prob_test, edge_threshold=CLV_EDGE_THRESHOLD)
    if clv:
        print(f"  Test fights with market odds : {clv['n_with_odds']}")
        print(f"  CLV accuracy                 : {clv['clv_accuracy']:.4f}")
        print(f"  Value bets (edge>{clv['threshold']*100:.0f}%)       : {clv['n_bets']}")
        if clv["roi"] is not None:
            print(f"  ROI (flat staking)           : {clv['roi']:+.4f}  ({clv['roi']*100:+.2f}%)")
            print(f"  Win rate on bets             : {clv['win_rate']:.4f}")
            if clv["bets_df"] is not None:
                top = clv["bets_df"].nlargest(10, "edge_f1")[
                    ["event_date", "market_prob_f1", "model_prob_f1", "edge_f1", "winner", "profit"]
                ]
                print("\n  Top 10 value bets (by edge):")
                print(top.to_string(index=False))

    return prob_test, clv


# ── v1 vs v2 comparison ────────────────────────────────────────────────────────

def compare_v1_v2(
    test_df_raw: pd.DataFrame,
    prob_v2:     np.ndarray,
    y_test:      np.ndarray,
    test_meta:   pd.DataFrame,
):
    print("\n" + "=" * 60)
    print("MODEL COMPARISON — v1 (baseline) vs v2")
    print("=" * 60)

    rows = []

    # ── v2 metrics (already computed) ────────────────────────────────────────
    clv_v2 = _clv_roi(test_meta, prob_v2, edge_threshold=CLV_EDGE_THRESHOLD)
    rows.append({
        "model":    "v2",
        **{k: v for k, v in _metrics_row("test", y_test, prob_v2).items() if k != "split"},
        "clv_acc":  round(clv_v2["clv_accuracy"], 4) if clv_v2 else None,
        "roi":      round(clv_v2["roi"], 4)           if clv_v2 and clv_v2["roi"] else None,
        "n_bets":   clv_v2["n_bets"]                  if clv_v2 else None,
    })

    # ── v1 metrics (reconstruct from saved model) ─────────────────────────────
    if not V1_MODEL_PATH.exists():
        log.warning("v1 model not found at %s — skipping v1 comparison", V1_MODEL_PATH)
    else:
        v1       = joblib.load(V1_MODEL_PATH)
        v1_feats = v1["feat_names"]
        v1_enc   = v1["encoder"]
        v1_model = v1["model"]

        # Prepare test data using v1's encoder + full feature set (no DATA_CUTOFF matters — same test rows)
        X_test_v1 = _prepare(test_df_raw, v1_feats, v1_enc, fit=False)
        prob_v1   = v1_model.predict_proba(X_test_v1)[:, 1]

        clv_v1 = _clv_roi(test_meta, prob_v1, edge_threshold=CLV_EDGE_THRESHOLD)
        rows.insert(0, {
            "model":   "v1",
            **{k: v for k, v in _metrics_row("test", y_test, prob_v1).items() if k != "split"},
            "clv_acc": round(clv_v1["clv_accuracy"], 4) if clv_v1 else None,
            "roi":     round(clv_v1["roi"], 4)           if clv_v1 and clv_v1["roi"] else None,
            "n_bets":  clv_v1["n_bets"]                  if clv_v1 else None,
        })

    cmp = pd.DataFrame(rows)[["model", "n", "accuracy", "log_loss", "brier", "clv_acc", "roi", "n_bets"]]
    print()
    print(cmp.to_string(index=False, float_format=lambda x: f"{x:.4f}" if isinstance(x, float) else str(x)))
    print()

    if len(rows) == 2:
        v1r, v2r = rows[0], rows[1]
        print("  Δ accuracy  :", f"{v2r['accuracy']  - v1r['accuracy']:+.4f}")
        print("  Δ log_loss  :", f"{v2r['log_loss']  - v1r['log_loss']:+.4f}")
        print("  Δ brier     :", f"{v2r['brier']     - v1r['brier']:+.4f}")
        print("  Δ clv_acc   :", f"{v2r['clv_acc']   - v1r['clv_acc']:+.4f}" if v1r["clv_acc"] and v2r["clv_acc"] else "  n/a")
        print("  Δ roi       :", f"{v2r['roi']       - v1r['roi']:+.4f}"     if v1r["roi"] and v2r["roi"] else "  n/a")


# ── Save ───────────────────────────────────────────────────────────────────────

def save_model(
    cal_model: IsotonicCalibratedClassifier,
    encoder: OrdinalEncoder,
    feat_names: list[str],
    path: Path,
):
    MODEL_DIR.mkdir(exist_ok=True)
    payload = {
        "model":      cal_model,
        "encoder":    encoder,
        "feat_names": feat_names,
        "cat_cols":   CAT_COLS,
        "meta": {
            "version":       path.stem,
            "data_cutoff":   DATA_CUTOFF,
            "cal_cutoff":    CAL_CUTOFF,
            "train_cutoff":  TRAIN_CUTOFF,
            "prob_clip":     PROB_CLIP,
            "top_n_features": TOP_N_FEATURES,
        },
    }
    joblib.dump(payload, path)
    log.info("Model saved → %s  (%.1f MB)", path, path.stat().st_size / 1_048_576)


# ── Entry ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("train_model.py  —  UFC fight outcome predictor  [v2]")
    log.info("=" * 60)

    # 1. Load raw data
    df = load_features()

    # 2. Feature selection: top-100 from v1 (or quick pre-fit)
    feat_names_all = _all_feature_cols(df)
    selected_feats = select_top_features(df, feat_names_all)

    # 3. Build time-aware datasets (filtered to DATA_CUTOFF, top-100 features)
    (
        X_train, y_train,
        X_val,   y_val,
        X_test,  y_test,
        encoder,
        test_meta,
        test_df_raw,
    ) = build_datasets(df, selected_feats, data_cutoff=DATA_CUTOFF)

    # 4. Train v2
    xgb_v2 = train_xgboost(X_train, y_train, X_val, y_val, params=XGB_PARAMS_V2)

    # 5. Calibrate (with clipping to avoid 0/1 certainty)
    cal_v2 = calibrate(xgb_v2, X_val, y_val, prob_clip=PROB_CLIP)

    # 6. Full evaluation
    prob_v2_test, _ = evaluate(
        "v2",
        cal_v2, xgb_v2,
        X_train, y_train,
        X_val,   y_val,
        X_test,  y_test,
        feat_names=selected_feats,
        test_meta=test_meta,
    )

    # 7. v1 vs v2 comparison
    compare_v1_v2(test_df_raw, prob_v2_test, y_test, test_meta)

    # 8. Save v2 (v1 untouched)
    save_model(cal_v2, encoder, selected_feats, path=V2_MODEL_PATH)

    log.info("=" * 60)
    log.info("train_model complete.  v1=%s  v2=%s", V1_MODEL_PATH, V2_MODEL_PATH)


if __name__ == "__main__":
    main()
