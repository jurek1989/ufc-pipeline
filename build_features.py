"""
build_features.py — unified feature module (train == serve).

mode='historical'  source: full_data_silver_plus → UFC_features  WRITE_TRUNCATE
mode='upcoming'    source: UFC_coming_event_with_rankings_placeholder
                   + rolling from full history → UFC_features  WRITE_APPEND

Rolling via cumsum+shift (windows 3-15).  Same code path for both modes.
Anti-leakage contract: market_prob_f1/f2 are reference columns, not features.
"""

from __future__ import annotations

import sys
import logging
import multiprocessing as mp
from datetime import date

import numpy as np
import pandas as pd
from google.cloud import bigquery
from pandas.api.types import is_bool_dtype

from config import (
    PROJECT_ID, DATASET,
    TABLE_FEATURES,
    VIEW_FULL_DATA_SILVER_PLUS,
    VIEW_COMING_EVENT_WITH_RANKINGS,
    TABLE_FIGHTERS_DATA,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

client = bigquery.Client(project=PROJECT_ID)
DS = f"{PROJECT_ID}.{DATASET}"
TABLE_REF = f"{DS}.{TABLE_FEATURES}"

ROLLING_WINDOWS: list[int] = list(range(3, 16))

# Each entry: metric_name → (numerator_col_in_long_df, denominator_col, invert)
BASE_METRICS: dict[str, tuple[str, str, bool]] = {
    "slpm":       ("fighter_sig_strikes_succ", "fight_duration_minutes", False),
    "str_acc":    ("fighter_sig_strikes_succ", "fighter_sig_strikes_att",  False),
    "sapm":       ("opp_sig_strikes_succ",     "fight_duration_minutes",  False),
    "str_def":    ("opp_sig_strikes_succ",     "opp_sig_strikes_att",     True),
    "td_avg":     ("fighter_td_succ",          "fight_duration_minutes",  False),
    "td_acc":     ("fighter_td_succ",          "fighter_td_att",          False),
    "td_def":     ("opp_td_succ",              "opp_td_att",              True),
    "sub_avg":    ("fighter_sub_att",          "fight_duration_minutes",  False),
    "ctrl_ratio": ("fighter_ctrl_sec",         "fight_duration_minutes",  False),
}

OUTCOME_NAMES = ["wins", "losses", "finish_wins", "sub_wins", "finish_losses"]
STREAK_NAMES  = ["streak", "losing_streak"]

# Excluded from feature matrix X (kept as reference or dropped entirely)
_LEAKAGE_COLS     = {"f_1_implied_prob", "f_2_implied_prob", "odds_source"}
_DROP_FROM_OUTPUT = _LEAKAGE_COLS | {"winner", "result"}

_META_COLS = ["fight_url", "event_date", "split", "as_of_date", "winner_encoded"]
_REF_COLS  = ["market_prob_f1", "market_prob_f2"]

_STATIC_COLS = [
    "f_1_age", "f_2_age",
    "f_1_ranking", "f_2_ranking",
    "f_1_fighter_height_cm", "f_2_fighter_height_cm",
    "f_1_fighter_reach_cm",  "f_2_fighter_reach_cm",
    "f_1_fighter_stance",    "f_2_fighter_stance",
    "f_1_fighter_weight_lbs","f_2_fighter_weight_lbs",
    "weight_class",
]
_SPREAD_COLS = ["spread_f1_american", "spread_f2_american", "odds_n_books"]


def _build_all_metrics() -> dict[str, tuple[str, str, bool]]:
    """9 whole-fight + 9×5 per-round = 54 rolling metrics."""
    m: dict[str, tuple[str, str, bool]] = dict(BASE_METRICS)
    for r in range(1, 6):
        dur = f"r{r}_duration"
        m[f"slpm_r{r}"]       = (f"fighter_sig_strikes_succ_r{r}", dur, False)
        m[f"str_acc_r{r}"]    = (f"fighter_sig_strikes_succ_r{r}", f"fighter_sig_strikes_att_r{r}", False)
        m[f"sapm_r{r}"]       = (f"opp_sig_strikes_succ_r{r}",     dur, False)
        m[f"str_def_r{r}"]    = (f"opp_sig_strikes_succ_r{r}",     f"opp_sig_strikes_att_r{r}", True)
        m[f"td_avg_r{r}"]     = (f"fighter_td_succ_r{r}",          dur, False)
        m[f"td_acc_r{r}"]     = (f"fighter_td_succ_r{r}",          f"fighter_td_att_r{r}", False)
        m[f"td_def_r{r}"]     = (f"opp_td_succ_r{r}",              f"opp_td_att_r{r}", True)
        m[f"sub_avg_r{r}"]    = (f"fighter_sub_att_r{r}",          dur, False)
        m[f"ctrl_ratio_r{r}"] = (f"fighter_ctrl_sec_r{r}",         dur, False)
    return m


ALL_METRICS = _build_all_metrics()   # 54 entries
_ALL_ROLLING_NAMES = list(ALL_METRICS.keys()) + OUTCOME_NAMES + STREAK_NAMES  # 61 total


# ── Helpers ────────────────────────────────────────────────────────────────────

def _time_to_minutes(t: str | None) -> float:
    try:
        mins, secs = t.split(":")
        return (int(mins) * 60 + int(secs)) / 60.0
    except Exception:
        return 0.0


def _parallel_apply(
    df: pd.DataFrame, group_col: str, func, n_jobs: int = 4
) -> pd.DataFrame:
    groups = [g for _, g in df.groupby(group_col, sort=False)]
    with mp.Pool(n_jobs) as pool:
        results = pool.map(func, groups)
    return pd.concat(results, ignore_index=True)


def _coerce_booleans(df: pd.DataFrame) -> None:
    for col in df.columns:
        s = df[col]
        if is_bool_dtype(s):
            df[col] = s.astype("boolean").fillna(False)
        elif s.dtype == object:
            non_na = s.dropna()
            if len(non_na) > 0 and non_na.isin([True, False]).all():
                df[col] = s.astype("boolean").fillna(False)


def _to_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def _american_to_novig(a1, a2) -> tuple[float, float]:
    """Convert a pair of American odds to multiplicative no-vig implied probs."""
    def _impl(a: float) -> float:
        if a > 0:
            return 100.0 / (a + 100.0)
        if a < 0:
            return abs(a) / (abs(a) + 100.0)
        return np.nan

    try:
        p1, p2 = _impl(float(a1)), _impl(float(a2))
        if np.isnan(p1) or np.isnan(p2):
            return np.nan, np.nan
        total = p1 + p2
        return round(p1 / total, 6), round(p2 / total, 6)
    except (TypeError, ValueError):
        return np.nan, np.nan


# ── Data loading ───────────────────────────────────────────────────────────────

def load_historical() -> pd.DataFrame:
    sql = f"SELECT * FROM `{DS}.{VIEW_FULL_DATA_SILVER_PLUS}` ORDER BY event_date"
    logging.info("Loading historical fights from full_data_silver_plus ...")
    return client.query(sql).to_dataframe()


def load_upcoming() -> pd.DataFrame:
    sql = f"SELECT * FROM `{DS}.{VIEW_COMING_EVENT_WITH_RANKINGS}`"
    logging.info("Loading upcoming card ...")
    return client.query(sql).to_dataframe()


def _load_fighters_lookup() -> pd.DataFrame:
    """UFC_fighters_data indexed by fighter_url, for debutant static features."""
    sql = f"""
    SELECT fighter_url, fighter_f_name, fighter_l_name,
           fighter_height_cm, fighter_reach_cm, fighter_stance,
           fighter_dob, fighter_weight_lbs
    FROM `{DS}.{TABLE_FIGHTERS_DATA}`
    """
    return client.query(sql).to_dataframe().set_index("fighter_url")


def _load_odds_spread() -> pd.DataFrame:
    """MAX − MIN American odds at the closing snapshot per fight (bookmaker disagreement)."""
    sql = f"""
    WITH latest AS (
        SELECT event_url, fighter_1, fighter_2, MAX(scraped_at) AS last_scraped
        FROM `{DS}.odds_snapshots`
        GROUP BY 1, 2, 3
    )
    SELECT
        LOWER(TRIM(os.fighter_1)) AS f1_norm,
        LOWER(TRIM(os.fighter_2)) AS f2_norm,
        DATE(os.scraped_at)       AS fight_date,
        MAX(os.odds_f1_american) - MIN(os.odds_f1_american) AS spread_f1_american,
        MAX(os.odds_f2_american) - MIN(os.odds_f2_american) AS spread_f2_american,
        COUNT(DISTINCT os.bookmaker)                        AS odds_n_books
    FROM `{DS}.odds_snapshots` os
    JOIN latest
        ON  os.event_url   = latest.event_url
        AND os.fighter_1   = latest.fighter_1
        AND os.fighter_2   = latest.fighter_2
        AND os.scraped_at  = latest.last_scraped
    WHERE os.odds_f1_american IS NOT NULL
      AND os.odds_f2_american IS NOT NULL
    GROUP BY 1, 2, 3
    """
    return client.query(sql).to_dataframe()


# ── Per-fight prep ─────────────────────────────────────────────────────────────

def add_winner_encoded(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["winner_encoded"] = np.select(
        [df["winner"] == df["f_1_name"],
         df["winner"] == df["f_2_name"],
         df["winner"].isin(["Draw", "No Contest"])],
        [1, 0, -1],
        default=-1,
    )
    return df


def prepare_base_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add fight_duration_minutes and r{1..5}_duration columns."""
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["fight_duration_minutes"] = df.apply(
        lambda r: (r["finish_round"] - 1) * 5.0 + _time_to_minutes(r["finish_time"]),
        axis=1,
    )
    finish_round = df["finish_round"]
    finish_min   = df["finish_time"].apply(_time_to_minutes)
    for r in range(1, 6):
        df[f"r{r}_duration"] = np.where(
            r < finish_round, 5.0,
            np.where(r == finish_round, finish_min, 0.0),
        )
    return df


# ── Long format ────────────────────────────────────────────────────────────────

def to_long(df: pd.DataFrame, split: str = "train") -> pd.DataFrame:
    """
    Expand fight-level df to 2 rows per fight (one per fighter perspective).
    Columns are generic (fighter / opp), not f_1_ / f_2_ prefixed.
    """
    halves: list[pd.DataFrame] = []
    for prefix, opp in [("f_1", "f_2"), ("f_2", "f_1")]:
        t: dict[str, pd.Series] = {}
        t["fighter"]    = df[f"{prefix}_url"]
        t["opponent"]   = df[f"{opp}_url"]
        t["event_date"] = df["event_date"]
        t["fight_url"]  = df["fight_url"]
        t["split"]      = split
        t["role"]       = prefix

        # Outcome flags
        t["is_winner"]   = (df["winner"] == df[f"{prefix}_name"]).astype(int)
        t["is_loss"]     = (df["winner"] == df[f"{opp}_name"]).astype(int)
        t["finish_win"]  = (
            (df["winner"] == df[f"{prefix}_name"])
            & df["result"].str.contains("KO|TKO", na=False)
        ).astype(int)
        t["sub_win"]     = (
            (df["winner"] == df[f"{prefix}_name"])
            & (df["result"] == "Submission")
        ).astype(int)
        t["finish_loss"] = (
            (df["winner"] == df[f"{opp}_name"])
            & df["result"].str.contains("KO|TKO", na=False)
        ).astype(int)

        t["fight_duration_minutes"] = df["fight_duration_minutes"]

        # Whole-fight raw stats (fighter perspective)
        t["fighter_sig_strikes_succ"] = df[f"{prefix}_sig_strikes_succ"]
        t["fighter_sig_strikes_att"]  = df[f"{prefix}_sig_strikes_att"]
        t["opp_sig_strikes_succ"]     = df[f"{opp}_sig_strikes_succ"]
        t["opp_sig_strikes_att"]      = df[f"{opp}_sig_strikes_att"]
        t["fighter_td_succ"]          = df[f"{prefix}_takedown_succ"]
        t["fighter_td_att"]           = df[f"{prefix}_takedown_att"]
        t["opp_td_succ"]              = df[f"{opp}_takedown_succ"]
        t["opp_td_att"]               = df[f"{opp}_takedown_att"]
        t["fighter_sub_att"]          = df[f"{prefix}_submission_att"]
        t["fighter_ctrl_sec"]         = df[f"{prefix}_ctrl_time_sec"]

        # Per-round raw stats
        for r in range(1, 6):
            t[f"fighter_sig_strikes_succ_r{r}"] = df[f"{prefix}_r{r}_sig_strikes_succ"]
            t[f"fighter_sig_strikes_att_r{r}"]  = df[f"{prefix}_r{r}_sig_strikes_att"]
            t[f"opp_sig_strikes_succ_r{r}"]     = df[f"{opp}_r{r}_sig_strikes_succ"]
            t[f"opp_sig_strikes_att_r{r}"]      = df[f"{opp}_r{r}_sig_strikes_att"]
            t[f"fighter_td_succ_r{r}"]          = df[f"{prefix}_r{r}_td_1_succ"]
            t[f"fighter_td_att_r{r}"]           = df[f"{prefix}_r{r}_td_1_att"]
            t[f"opp_td_succ_r{r}"]              = df[f"{opp}_r{r}_td_1_succ"]
            t[f"opp_td_att_r{r}"]               = df[f"{opp}_r{r}_td_1_att"]
            t[f"fighter_sub_att_r{r}"]          = df[f"{prefix}_r{r}_submission_att"]
            t[f"fighter_ctrl_sec_r{r}"]         = df[f"{prefix}_r{r}_ctrl"]
            t[f"r{r}_duration"]                 = df[f"r{r}_duration"]

        halves.append(pd.DataFrame(t))

    result = pd.concat(halves, ignore_index=True).sort_values("event_date").reset_index(drop=True)
    # BigQuery returns nullable Int64 columns; pd.to_numeric alone keeps them nullable.
    # Force all stat columns to plain numpy float64 so np.where comparisons work.
    _id_cols = {"fighter", "opponent", "event_date", "fight_url", "split", "role"}
    for col in result.columns:
        if col not in _id_cols:
            try:
                result[col] = result[col].astype("float64")
            except (TypeError, ValueError):
                pass
    return result


# ── Rolling — cumsum+shift, called per-fighter group ──────────────────────────

def _rolling_stats_for_group(grp: pd.DataFrame) -> pd.DataFrame:
    """
    For every (metric, window) combination compute rolling ratio metrics
    using cumsum+shift — the window is [i-n .. i-1], excluding the current fight.
    """
    grp = grp.sort_values("event_date").reset_index(drop=True)
    new: dict[str, np.ndarray] = {}
    for n in ROLLING_WINDOWS:
        for name, (num_col, den_col, invert) in ALL_METRICS.items():
            if num_col not in grp.columns or den_col not in grp.columns:
                new[f"{name}_{n}"] = np.full(len(grp), np.nan)
                continue
            cs_num = grp[num_col].fillna(0).cumsum()
            cs_den = grp[den_col].fillna(0).cumsum()
            roll_num = cs_num.shift(1) - cs_num.shift(n + 1).fillna(0)
            roll_den = cs_den.shift(1) - cs_den.shift(n + 1).fillna(0)
            with np.errstate(divide="ignore", invalid="ignore"):
                val = np.where(roll_den.values > 0, roll_num.values / roll_den.values, np.nan)
            if invert:
                val = np.where(~np.isnan(val), 1.0 - val, np.nan)
            new[f"{name}_{n}"] = val
    return pd.concat([grp, pd.DataFrame(new, index=grp.index)], axis=1)


def _rolling_outcomes_for_group(grp: pd.DataFrame) -> pd.DataFrame:
    """
    Wins / losses / finish_wins / sub_wins / finish_losses via cumsum+shift.
    Streak / losing_streak via backward cumprod loop (can't vectorise with cumsum).
    """
    grp = grp.sort_values("event_date").reset_index(drop=True)
    n_rows = len(grp)
    new: dict[str, np.ndarray] = {}

    for n in ROLLING_WINDOWS:
        # Count outcomes: same cumsum+shift pattern
        for src, out in [
            ("is_winner",   "wins"),
            ("is_loss",     "losses"),
            ("finish_win",  "finish_wins"),
            ("sub_win",     "sub_wins"),
            ("finish_loss", "finish_losses"),
        ]:
            cs   = grp[src].fillna(0).astype(float).cumsum()
            roll = (cs.shift(1) - cs.shift(n + 1).fillna(0)).clip(lower=0)
            new[f"{out}_{n}"] = roll.fillna(0).values.astype(int)

        # Streaks: last consecutive wins/losses ending just before fight i
        streak_w = np.zeros(n_rows, dtype=int)
        streak_l = np.zeros(n_rows, dtype=int)
        wins_arr = grp["is_winner"].fillna(0).values
        loss_arr = grp["is_loss"].fillna(0).values
        for i in range(1, n_rows):
            w = wins_arr[max(0, i - n):i][::-1]
            l = loss_arr[max(0, i - n):i][::-1]
            streak_w[i] = int((w == 1).cumprod().sum())
            streak_l[i] = int((l == 1).cumprod().sum())
        new[f"streak_{n}"]        = streak_w
        new[f"losing_streak_{n}"] = streak_l

    return pd.concat([grp, pd.DataFrame(new, index=grp.index)], axis=1)


# ── Wide format ────────────────────────────────────────────────────────────────

def to_wide(df: pd.DataFrame, long_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long rolling features to wide: {metric}_{n}_f_1 / {metric}_{n}_f_2."""
    long_df = long_df.copy()
    # fight_ordinal: cumcount within each fighter sorted by date
    long_df = long_df.sort_values(["fighter", "event_date"]).reset_index(drop=True)
    long_df["fight_ordinal"] = long_df.groupby("fighter", sort=False).cumcount() + 1

    rolling_cols = [
        c for c in long_df.columns
        if any(c.endswith(f"_{n}") for n in ROLLING_WINDOWS)
    ]
    pivot_cols = rolling_cols + ["fight_ordinal"]

    pivot = long_df.pivot(index="fight_url", columns="role", values=pivot_cols)
    pivot.columns = [f"{col}_{role}" for col, role in pivot.columns]
    pivot = pivot.reset_index()

    return df.merge(pivot, on="fight_url", how="left")


# ── Feature augmentation ───────────────────────────────────────────────────────

def add_static_features(df: pd.DataFrame) -> pd.DataFrame:
    """Age from DOB; height/reach/stance/weight already present via the view."""
    df = df.copy()
    for p in ("f_1", "f_2"):
        dob = pd.to_datetime(df[f"{p}_fighter_dob"], errors="coerce")
        df[f"{p}_age"] = (df["event_date"] - dob).dt.days // 365
    return df


def add_odds_spread(df: pd.DataFrame, spread_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join closing bookmaker spread (MAX−MIN American odds) onto fights.
    Handles both f_1/f_2 orderings and date ±1 day.
    """
    df = df.copy()
    df["_f1n"]  = df["f_1_name"].str.lower().str.strip()
    df["_f2n"]  = df["f_2_name"].str.lower().str.strip()
    df["_date"] = pd.to_datetime(df["event_date"]).dt.date

    spread = spread_df.copy()
    spread["fight_date"] = pd.to_datetime(spread["fight_date"]).dt.date

    # Both fighter orderings
    fwd = spread.rename(columns={"f1_norm": "_f1n", "f2_norm": "_f2n"})
    rev = pd.DataFrame({
        "_f1n":               spread["f2_norm"],
        "_f2n":               spread["f1_norm"],
        "fight_date":         spread["fight_date"],
        "spread_f1_american": spread["spread_f2_american"],
        "spread_f2_american": spread["spread_f1_american"],
        "odds_n_books":       spread["odds_n_books"],
    })
    all_spread = pd.concat(
        [fwd[["_f1n", "_f2n", "fight_date", "spread_f1_american", "spread_f2_american", "odds_n_books"]],
         rev[["_f1n", "_f2n", "fight_date", "spread_f1_american", "spread_f2_american", "odds_n_books"]]],
        ignore_index=True,
    )

    matched = df[["fight_url", "_f1n", "_f2n", "_date"]].merge(
        all_spread, on=["_f1n", "_f2n"], how="left"
    )
    date_diff = abs(
        (pd.to_datetime(matched["_date"]) - pd.to_datetime(matched["fight_date"])).dt.days
    )
    matched = (
        matched[date_diff <= 1]
        .drop_duplicates(subset="fight_url", keep="first")
        [["fight_url", "spread_f1_american", "spread_f2_american", "odds_n_books"]]
    )

    return df.merge(matched, on="fight_url", how="left").drop(
        columns=["_f1n", "_f2n", "_date"], errors="ignore"
    )


def add_market_reference(df: pd.DataFrame) -> pd.DataFrame:
    """
    Carry implied probs as reference columns.
    These are EXCLUDED from feature matrix X; used for CLV / calibration only.
    """
    df = df.copy()
    df["market_prob_f1"] = df.get("f_1_implied_prob")
    df["market_prob_f2"] = df.get("f_2_implied_prob")
    return df


def add_diff_features(df: pd.DataFrame) -> pd.DataFrame:
    """f_1_xxx − f_2_xxx for every numeric rolling and static pair."""
    # Collect all new columns in a dict first, then concat once to avoid
    # the O(n²) fragmentation from inserting 800 columns one by one.
    new: dict[str, pd.Series] = {}
    # Rolling diffs
    for metric in _ALL_ROLLING_NAMES:
        for n in ROLLING_WINDOWS:
            c1, c2 = f"{metric}_{n}_f_1", f"{metric}_{n}_f_2"
            if c1 in df.columns and c2 in df.columns:
                new[f"diff_{metric}_{n}"] = (
                    pd.to_numeric(df[c1], errors="coerce")
                    - pd.to_numeric(df[c2], errors="coerce")
                )
    # Static diffs
    for tag, c1, c2 in [
        ("age",        "f_1_age",                "f_2_age"),
        ("fight_ord",  "fight_ordinal_f_1",       "fight_ordinal_f_2"),
        ("ranking",    "f_1_ranking",             "f_2_ranking"),
        ("height_cm",  "f_1_fighter_height_cm",   "f_2_fighter_height_cm"),
        ("reach_cm",   "f_1_fighter_reach_cm",    "f_2_fighter_reach_cm"),
        ("weight_lbs", "f_1_fighter_weight_lbs",  "f_2_fighter_weight_lbs"),
    ]:
        if c1 in df.columns and c2 in df.columns:
            new[f"diff_{tag}"] = (
                pd.to_numeric(df[c1], errors="coerce")
                - pd.to_numeric(df[c2], errors="coerce")
            )
    return pd.concat([df, pd.DataFrame(new, index=df.index)], axis=1)


# ── Upcoming helper ────────────────────────────────────────────────────────────

def _build_upcoming_rows(
    upcoming_df: pd.DataFrame,
    long_df: pd.DataFrame,
    fighters_lut: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each fight in the upcoming card:
      - Look up each fighter's latest rolling features in long_df (by fighter URL).
      - Debutant (no history in long_df): rolling = NaN; static from fighters_lut.
    Returns a wide feature DataFrame ready for write_to_bq.
    """
    rolling_cols = [
        c for c in long_df.columns
        if any(c.endswith(f"_{n}") for n in ROLLING_WINDOWS)
    ]

    # Latest rolling row per fighter URL, and total fight count
    latest: dict[str, pd.Series] = {}
    fight_count: dict[str, int] = {}
    for furl, grp in long_df.groupby("fighter"):
        sorted_grp = grp.sort_values("event_date")
        latest[furl]      = sorted_grp.iloc[-1]
        fight_count[furl] = len(sorted_grp)

    today = date.today()
    rows: list[dict] = []

    for _, fight in upcoming_df.iterrows():
        f1_url  = fight.get("fighter1_link", "")
        f2_url  = fight.get("fighter2_link", "")
        ev_date = pd.to_datetime(fight.get("event_date"))

        a1 = _to_float(fight.get("f_1_odds"))
        a2 = _to_float(fight.get("f_2_odds"))
        mprob_f1, mprob_f2 = _american_to_novig(a1, a2)

        row: dict = {
            "fight_url":      f"{f1_url}_vs_{f2_url}",
            "event_date":     ev_date,
            "split":          "upcoming",
            "as_of_date":     today.isoformat(),
            "winner_encoded": np.nan,
            "market_prob_f1": mprob_f1,
            "market_prob_f2": mprob_f2,
            "weight_class":   fight.get("weight_class"),
        }

        for role, furl in [("f_1", f1_url), ("f_2", f2_url)]:
            hist = latest.get(furl)

            # Rolling features
            if hist is not None:
                for col in rolling_cols:
                    row[f"{col}_{role}"] = hist.get(col, np.nan)
                row[f"fight_ordinal_{role}"] = fight_count[furl] + 1
            else:
                # Debutant: all rolling = NaN
                for col in rolling_cols:
                    row[f"{col}_{role}"] = np.nan
                row[f"fight_ordinal_{role}"] = 1

            # Static features — prefer the upcoming view (already has fighter profile)
            row[f"{role}_ranking"]              = _to_float(fight.get(f"{role}_ranking"))
            row[f"{role}_fighter_height_cm"]    = _to_float(fight.get(f"{role}_fighter_height_cm"))
            row[f"{role}_fighter_reach_cm"]     = _to_float(fight.get(f"{role}_fighter_reach_cm"))
            row[f"{role}_fighter_stance"]       = fight.get(f"{role}_fighter_stance")
            row[f"{role}_fighter_weight_lbs"]   = _to_float(fight.get(f"{role}_fighter_weight_lbs"))
            dob = pd.to_datetime(fight.get(f"{role}_fighter_dob"), errors="coerce")
            row[f"{role}_age"] = (
                int((ev_date - dob).days // 365) if pd.notna(dob) else np.nan
            )

            # Debutant fallback: fill any NaN statics from fighters_lut
            if furl in fighters_lut.index:
                fd = fighters_lut.loc[furl]
                for src, dest in [
                    ("fighter_height_cm",   f"{role}_fighter_height_cm"),
                    ("fighter_reach_cm",    f"{role}_fighter_reach_cm"),
                    ("fighter_weight_lbs",  f"{role}_fighter_weight_lbs"),
                ]:
                    if pd.isna(row.get(dest)):
                        row[dest] = _to_float(fd.get(src))
                if row.get(f"{role}_fighter_stance") is None:
                    row[f"{role}_fighter_stance"] = fd.get("fighter_stance")
                if pd.isna(row.get(f"{role}_age")):
                    fdob = pd.to_datetime(fd.get("fighter_dob"), errors="coerce")
                    if pd.notna(fdob):
                        row[f"{role}_age"] = int((ev_date - fdob).days // 365)

        rows.append(row)

    if not rows:
        logging.warning("No upcoming fights found — returning empty DataFrame")
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = add_diff_features(out)
    return _select_output_columns(out)


# ── Output selection ───────────────────────────────────────────────────────────

def _select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only: meta, reference, rolling features (both roles), static,
    odds spread, and diff columns. Drop raw source stats and leakage cols.
    """
    keep: set[str] = set()
    keep.update(c for c in _META_COLS     if c in df.columns)
    keep.update(c for c in _REF_COLS      if c in df.columns)
    keep.update(c for c in _STATIC_COLS   if c in df.columns)
    keep.update(c for c in _SPREAD_COLS   if c in df.columns)
    keep.update(c for c in df.columns if c.startswith("diff_"))

    for role in ("f_1", "f_2"):
        keep.add(f"fight_ordinal_{role}")
        for name in _ALL_ROLLING_NAMES:
            for n in ROLLING_WINDOWS:
                col = f"{name}_{n}_{role}"
                if col in df.columns:
                    keep.add(col)

    return df[[c for c in df.columns if c in keep]]


def write_to_bq(df: pd.DataFrame, mode: str) -> None:
    if df.empty:
        logging.warning("Nothing to write — DataFrame is empty")
        return
    disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "historical"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    _coerce_booleans(df)
    job_config = bigquery.LoadJobConfig(write_disposition=disposition)
    client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config).result(timeout=300)
    logging.info(f"Wrote {len(df)} rows ({mode}) → {TABLE_REF}")


# ── Entry ──────────────────────────────────────────────────────────────────────

def main(mode: str = "historical") -> None:
    if mode not in ("historical", "upcoming"):
        logging.error(f"Unknown mode: {mode!r}. Use 'historical' or 'upcoming'.")
        sys.exit(1)

    logging.info("=" * 60)
    logging.info(f"build_features  mode={mode}")
    logging.info("=" * 60)

    # ── 1. Historical rolling (needed for both modes) ──────────────────────────
    df = load_historical()
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = add_winner_encoded(df)
    df = prepare_base_features(df)

    long_df = to_long(df, split="train")
    long_df = _parallel_apply(long_df, "fighter", _rolling_outcomes_for_group)
    long_df = _parallel_apply(long_df, "fighter", _rolling_stats_for_group)
    logging.info(f"Rolling complete — {len(long_df)} long rows, {long_df['fighter'].nunique()} fighters")

    # ── 2. Mode-specific output ────────────────────────────────────────────────
    if mode == "historical":
        df = to_wide(df, long_df)
        df = add_static_features(df)

        spread_df = _load_odds_spread()
        df = add_odds_spread(df, spread_df)
        df = add_market_reference(df)
        df = add_diff_features(df)

        df["split"]      = "train"
        df["as_of_date"] = df["event_date"].dt.date.astype(str)

        output_df = _select_output_columns(df)
        write_to_bq(output_df, mode)

    else:  # upcoming
        upcoming_df  = load_upcoming()
        fighters_lut = _load_fighters_lookup()
        output_df    = _build_upcoming_rows(upcoming_df, long_df, fighters_lut)
        write_to_bq(output_df, mode)

    # ── 3. Log summary ─────────────────────────────────────────────────────────
    n_rows = len(output_df)
    meta_ref = set(_META_COLS + _REF_COLS + ["winner_encoded"])
    n_features = len([c for c in output_df.columns if c not in meta_ref])
    logging.info(f"Output: {n_rows} rows | {n_features} feature columns")
    logging.info("=" * 60)

    # ── 4. Data quality sanity check ───────────────────────────────────────────
    import data_quality_checks as dqc
    failures = []
    if not dqc.check_stats_rows():     failures.append(1)
    dqc.check_odds_coverage()
    dqc.check_rankings_coverage()
    if not dqc.check_view_row_count(): failures.append(4)
    if not dqc.check_orphan_fights():  failures.append(5)

    if failures:
        logging.error(f"Quality gate failures: {failures}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build UFC feature table")
    parser.add_argument(
        "--mode", default="historical", choices=["historical", "upcoming"],
        help="historical: WRITE_TRUNCATE full table; upcoming: WRITE_APPEND upcoming rows",
    )
    args = parser.parse_args()
    main(args.mode)
