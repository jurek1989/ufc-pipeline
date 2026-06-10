"""
This module extends the original UFC model feature engineering pipeline to
include round‑level statistics and eight new qualitative features for each
fighter (physical strength, punching power, explosiveness, speed, timing,
footwork, chin and cardio).  It ingests the `full_data_silver_plus` view
which contains per‑round strike, takedown and control statistics and
produces a wide table with rolling aggregates across multiple window
lengths.  All new metrics are computed on a per‑fight basis and then
rolled across the same window sizes as the original model (3 through 15).

Key enhancements relative to the baseline script include:

* Dynamic construction of per‑round metrics (SLpM, StrAcc, SApM, StrDef,
  TDAvg, TDAcc, TDDef, SubAvg, CtrlRatio) for rounds 1–5.
* Computation of aggregate strike breakdowns by target (head, body, leg)
  and position (distance, clinch, ground) across the entire fight and per
  round, expressed both as accuracies (landed/attempted) and as shares of
  total significant strikes landed.
* Derivation of eight qualitative scores from the raw statistics: strength,
  power, dynamika (explosiveness), speed, timing, footwork, chin and
  cardio.  These scores combine various ratios and counting statistics to
  capture how a fighter performed in a given bout.  We intentionally
  avoid scaling these scores to a strict 0–1 range, leaving them in their
  natural units so the downstream model can learn appropriate weights.
* Rolling averages of all new metrics across the same windows used for
  historical outcomes and base statistics (3 to 15 fights), computed
  separately for each fighter.

Note that this script does not write to BigQuery directly—it exposes
`process_new_fights()` for integration with Cloud Functions.  The
`ufc_model_update` entrypoint calls this function and returns a HTTP
status.  To deploy this code you should replace your existing
`process_model_stats.py` with this module.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import multiprocessing as mp
import warnings
import time

# Configuration
PROJECT_ID = "ultra-acre-443816-g8"
DATASET_ID = "UFC_model"
# Name of the table this script writes to
TABLE_ID = "UFC_model_full_analysis_rounds"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
SOURCE_TABLE = "ultra-acre-443816-g8.UFC_data.full_data_silver_plus"

# BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Suppress some pandas warnings about chained assignment
warnings.filterwarnings("ignore", category=RuntimeWarning)
pd.options.mode.chained_assignment = None

# Rolling window lengths.  We compute rolling metrics over the last N
# fights, where N ranges from 3 up to 15.  Larger windows smooth recent
# performance over a longer history.
ROLLING_WINDOWS = list(range(3, 16))


def time_to_minutes(t: str | None) -> float:
    """Convert a "M:S" formatted time string into minutes as a float.

    Returns 0 if the input is None or not parseable.
    """
    try:
        mins, secs = t.split(":")
        return (int(mins) * 60 + int(secs)) / 60.0
    except Exception:
        return 0.0


def time_to_seconds(t: str | None) -> int:
    """Convert a "M:S" formatted time string into seconds.

    Returns 0 if the input is None or not parseable.
    """
    try:
        mins, secs = t.split(":")
        return int(mins) * 60 + int(secs)
    except Exception:
        return 0


def parallel_apply(df: pd.DataFrame, group_col: str, func, n_jobs: int = 4) -> pd.DataFrame:
    """Apply a function to each group of a DataFrame in parallel.

    The DataFrame is split on `group_col`, each group is passed to `func`
    and the results are concatenated.  Use this for CPU intensive per‑group
    processing such as rolling calculations.
    """
    groups = [group for _, group in df.groupby(group_col)]
    with mp.Pool(n_jobs) as pool:
        results = pool.map(func, groups)
    return pd.concat(results)


def get_full_fight_data() -> pd.DataFrame:
    """Fetch all fights from the silver plus view in chronological order."""
    query = f"SELECT * FROM `{SOURCE_TABLE}` ORDER BY event_date"
    return client.query(query).to_dataframe()


def add_winner_encoded(df: pd.DataFrame) -> pd.DataFrame:
    """Encode the fight winner as 1 for fighter 1, 0 for fighter 2, -1 for no contest/draw."""
    df['winner_encoded'] = np.select(
        [df['winner'] == df['f_1_name'], df['winner'] == df['f_2_name'], df['winner'].isin(['Draw', 'No Contest'])],
        [1, 0, -1],
        default=-1
    )
    return df


def prepare_base_features(df: pd.DataFrame) -> pd.DataFrame:
    """Convert times to numeric, compute fight duration and per‑round durations.

    The input DataFrame contains fight‑level fields for each fighter and round.  This
    function creates the following columns:

      - f_1_ctrl_time_sec, f_2_ctrl_time_sec: total control time in seconds for
        each fighter.
      - event_date: parsed to pandas datetime.
      - fight_duration_minutes: total fight length in minutes derived from
        finish_round and finish_time.
      - r1_duration ... r5_duration: length of each round in minutes.  Rounds
        prior to the finish round are assumed to be a full 5 minutes, the
        finish round duration uses finish_time, and later rounds are zero.
    """
    # # Convert control time strings (formatted like "M:S" or missing) into seconds
    # for prefix in ['f_1', 'f_2']:
    #     df[f'{prefix}_ctrl_time_sec'] = df[f'{prefix}_ctrl_time_sec'].astype(float)
    #     # Some older datasets stored control time as string; if that's the case
    #     # fall back to converting from minutes:seconds.
    #     if df[f'{prefix}_ctrl_time_sec'].isna().any():
    #         df[f'{prefix}_ctrl_time_sec'] = df[f'{prefix}_ctrl_time'].apply(time_to_seconds)

    # Ensure event_date is a datetime
    df['event_date'] = pd.to_datetime(df['event_date'])

    # Compute total fight duration in minutes
    df['fight_duration_minutes'] = df.apply(
        lambda row: (row['finish_round'] - 1) * 5 + time_to_minutes(row['finish_time']), axis=1
    )

    # Compute per‑round durations: each round before the finish round is 5 minutes,
    # the finishing round uses finish_time, rounds after finish are zero.
    def compute_round_durations(row):
        durations = {}
        finish_round = row['finish_round']
        finish_time_min = time_to_minutes(row['finish_time'])
        for r in range(1, 6):
            if r < finish_round:
                durations[f'r{r}_duration'] = 5.0
            elif r == finish_round:
                durations[f'r{r}_duration'] = finish_time_min
            else:
                durations[f'r{r}_duration'] = 0.0
        return pd.Series(durations)

    df = pd.concat([df, df.apply(compute_round_durations, axis=1)], axis=1)
    return df


def compute_strike_breakdowns(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate accuracy and share of significant strikes by target and position.

    For each fighter prefix (f_1 and f_2), this function computes total
    significant strike counts across all rounds for each target (head, body, leg)
    and position (distance, clinch, ground).  It then derives accuracy
    (landed/attempted) and share (landed / total landed) metrics both at fight
    level and per round.

    The resulting columns follow these naming patterns:

      * {prefix}_{cat}_succ_total, {prefix}_{cat}_att_total
      * {prefix}_{cat}_acc: accuracy across fight (succ_total / att_total)
      * {prefix}_{cat}_share: share of total significant strikes landed by category
      * {prefix}_r{n}_{cat}_acc: per round accuracy
      * {prefix}_r{n}_{cat}_share: per round share of significant strikes

    where prefix is f_1 or f_2, cat is one of head, body, leg, distance,
    clinch or ground, and n is the round number 1–5.
    """
    categories = ['head', 'body', 'leg', 'distance', 'clinch', 'ground']
    for prefix in ['f_1', 'f_2']:
        # Aggregate totals across rounds
        for cat in categories:
            succ_cols = [f'{prefix}_r{r}_{cat}_succ' for r in range(1, 6) if f'{prefix}_r{r}_{cat}_succ' in df.columns]
            att_cols = [f'{prefix}_r{r}_{cat}_att' for r in range(1, 6) if f'{prefix}_r{r}_{cat}_att' in df.columns]
            df[f'{prefix}_{cat}_succ_total'] = df[succ_cols].sum(axis=1)
            df[f'{prefix}_{cat}_att_total'] = df[att_cols].sum(axis=1)
            # Compute accuracy for the whole fight
            df[f'{prefix}_{cat}_acc'] = np.divide(
                df[f'{prefix}_{cat}_succ_total'],
                df[f'{prefix}_{cat}_att_total'],
                out=np.full(len(df), np.nan),
                where=df[f'{prefix}_{cat}_att_total'] > 0
            )
        # Compute share of landed strikes among head/body/leg categories.  Total
        # landed is only head/body/leg (distance/clinch/ground break down is
        # orthogonal), so we sum those three to get total significant strikes.
        total_succ = sum(df[f'{prefix}_{cat}_succ_total'] for cat in ['head', 'body', 'leg'])
        for cat in ['head', 'body', 'leg']:
            df[f'{prefix}_{cat}_share'] = np.divide(
                df[f'{prefix}_{cat}_succ_total'],
                total_succ,
                out=np.full(len(df), np.nan),
                where=total_succ > 0
            )
        # Similarly for positions distance/clinch/ground.  Use sum of those three.
        total_pos_succ = sum(df[f'{prefix}_{cat}_succ_total'] for cat in ['distance', 'clinch', 'ground'])
        for cat in ['distance', 'clinch', 'ground']:
            df[f'{prefix}_{cat}_share'] = np.divide(
                df[f'{prefix}_{cat}_succ_total'],
                total_pos_succ,
                out=np.full(len(df), np.nan),
                where=total_pos_succ > 0
            )
        # Per‑round breakdowns: accuracy and share per round.
        for r in range(1, 6):
            # Total landed in round r across head/body/leg for share
            total_round_succ = sum(df[f'{prefix}_r{r}_{cat}_succ'] for cat in ['head', 'body', 'leg'])
            for cat in ['head', 'body', 'leg']:
                # Accuracy per round
                df[f'{prefix}_r{r}_{cat}_acc'] = np.divide(
                    df[f'{prefix}_r{r}_{cat}_succ'],
                    df[f'{prefix}_r{r}_{cat}_att'],
                    out=np.full(len(df), np.nan),
                    where=df[f'{prefix}_r{r}_{cat}_att'] > 0
                )
                # Share per round
                df[f'{prefix}_r{r}_{cat}_share'] = np.divide(
                    df[f'{prefix}_r{r}_{cat}_succ'],
                    total_round_succ,
                    out=np.full(len(df), np.nan),
                    where=total_round_succ > 0
                )
            # For positions (distance/clinch/ground)
            total_round_pos_succ = sum(df[f'{prefix}_r{r}_{cat}_succ'] for cat in ['distance', 'clinch', 'ground'])
            for cat in ['distance', 'clinch', 'ground']:
                df[f'{prefix}_r{r}_{cat}_acc'] = np.divide(
                    df[f'{prefix}_r{r}_{cat}_succ'],
                    df[f'{prefix}_r{r}_{cat}_att'],
                    out=np.full(len(df), np.nan),
                    where=df[f'{prefix}_r{r}_{cat}_att'] > 0
                )
                df[f'{prefix}_r{r}_{cat}_share'] = np.divide(
                    df[f'{prefix}_r{r}_{cat}_succ'],
                    total_round_pos_succ,
                    out=np.full(len(df), np.nan),
                    where=total_round_pos_succ > 0
                )
    return df


def compute_quality_features(row: pd.Series, prefix: str) -> dict[str, float]:
    """Derive eight high‑level qualitative scores from raw fight statistics.

    Given a Series representing one fight row for both fighters, compute scores
    for the fighter specified by `prefix` (either 'f_1' or 'f_2').  The
    formulas combine strike, takedown and control metrics along with the
    outcome of the fight.  We intentionally avoid normalising these values to
    [0, 1] so that the model can learn appropriate scaling.
    """
    opp_prefix = 'f_2' if prefix == 'f_1' else 'f_1'
    # Basic fields
    fight_duration = row['fight_duration_minutes'] if row['fight_duration_minutes'] > 0 else 1.0
    max_duration = row['num_rounds'] * 5.0  # scheduled number of rounds * 5 minutes
    # Takedown statistics
    td_succ = row.get(f'{prefix}_takedown_succ', 0)
    td_att = row.get(f'{prefix}_takedown_att', 0)
    td_ratio = td_succ / td_att if td_att and td_att > 0 else 0.0
    opp_td_succ = row.get(f'{opp_prefix}_takedown_succ', 0)
    opp_td_att = row.get(f'{opp_prefix}_takedown_att', 0)
    td_def_ratio = 1.0 - (opp_td_succ / opp_td_att) if opp_td_att and opp_td_att > 0 else 1.0
    # Control time ratio (seconds of control per minute of fight)
    ctrl_time_sec = row.get(f'{prefix}_ctrl_time_sec', 0)
    ctrl_ratio = ctrl_time_sec / fight_duration if fight_duration > 0 else 0.0
    # Reversals – use relative share of reversals in this fight
    rev_self = row.get(f'{prefix}_reversals', 0)
    rev_opp = row.get(f'{opp_prefix}_reversals', 0)
    rev_total = rev_self + rev_opp
    rev_share = (rev_self / rev_total) if rev_total and rev_total > 0 else 0.0
    # Physical strength combines takedown offence/defence, control and reversals
    physical_strength = 0.4 * td_ratio + 0.3 * td_def_ratio + 0.2 * ctrl_ratio + 0.1 * rev_share
    # Knockdowns and KO outcomes for power and timing scores
    knockdowns = row.get(f'{prefix}_knockdowns', 0)
    sig_succ = row.get(f'{prefix}_sig_strikes_succ', 0)
    sig_att = row.get(f'{prefix}_sig_strikes_att', 0)
    # Determine KO win and finish round factor for punching power
    fighter_name = row.get(f'{prefix}_name')
    winner = row.get('winner')
    result = row.get('result', '') or ''
    is_win = (winner == fighter_name)
    ko_win = is_win and (('KO' in result) or ('TKO' in result))
    # Finish bonus counts only if KO/TKO victory
    finish_round = row.get('finish_round', 0)
    num_rounds = row.get('num_rounds', 3)
    finish_bonus = ((num_rounds - finish_round + 1) / num_rounds) if (ko_win and num_rounds) else 0.0
    power_ratio = (knockdowns / sig_succ) if sig_succ and sig_succ > 0 else 0.0
    punching_power = 0.5 * power_ratio + 0.3 * (1.0 if ko_win else 0.0) + 0.2 * finish_bonus
    # Explosiveness (dynamika) – finishing early, high action rate and knockdowns per minute
    remaining = (max_duration - fight_duration) if max_duration > fight_duration else 0.0
    finish_factor = (remaining / max_duration) if (result != 'Decision' and max_duration > 0) else 0.0
    actions = sig_succ + td_succ + row.get(f'{prefix}_submission_att', 0) + knockdowns
    actions_per_min = actions / fight_duration if fight_duration > 0 else 0.0
    kd_per_min = knockdowns / fight_duration if fight_duration > 0 else 0.0
    dynamika = 0.5 * finish_factor + 0.3 * actions_per_min + 0.2 * kd_per_min
    # Speed – strike rate, differential and defence
    opp_sig_succ = row.get(f'{opp_prefix}_sig_strikes_succ', 0)
    opp_sig_att = row.get(f'{opp_prefix}_sig_strikes_att', 0)
    slpm = sig_succ / fight_duration if fight_duration > 0 else 0.0
    strike_diff_per_min = (sig_succ - opp_sig_succ) / fight_duration if fight_duration > 0 else 0.0
    strike_def = 1.0 - (opp_sig_succ / opp_sig_att) if opp_sig_att and opp_sig_att > 0 else 1.0
    speed = 0.6 * slpm + 0.2 * strike_diff_per_min + 0.2 * strike_def
    # Timing – accuracy, knockdown efficiency, takedown accuracy
    strike_acc = sig_succ / sig_att if sig_att and sig_att > 0 else 0.0
    kd_efficiency = (knockdowns / sig_succ) if sig_succ and sig_succ > 0 else 0.0
    td_acc = td_ratio  # already computed
    timing = 0.7 * strike_acc + 0.2 * kd_efficiency + 0.1 * td_acc
    # Footwork – strike defence, takedown defence and low absorption per minute
    td_def = td_def_ratio
    sapm = opp_sig_succ / fight_duration if fight_duration > 0 else 0.0
    # The SAPM normalisation constant (10) reflects a rough upper bound on
    # significant strikes absorbed per minute in typical bouts.  Higher
    # numbers reduce the score.
    footwork = 0.5 * strike_def + 0.3 * td_def + 0.2 * (1.0 - (sapm / 10.0))
    # Chin – ability to absorb punishment without being stopped
    opp_knockdowns = row.get(f'{opp_prefix}_knockdowns', 0)
    # If the fighter lost by KO/TKO, chin is zero; otherwise use ratio of
    # strikes absorbed to knockdowns incurred.
    fighter_lost_by_ko = (not is_win) and (('KO' in result) or ('TKO' in result))
    if fighter_lost_by_ko:
        chin = 0.0
    else:
        # Avoid division by zero by adding 1 to knockdown count
        chin = opp_sig_succ / (opp_knockdowns + 1) if (opp_knockdowns + 1) > 0 else 0.0
    # Cardio – ability to maintain output over time
    duration_ratio = fight_duration / max_duration if max_duration > 0 else 0.0
    cardio = 0.7 * slpm + 0.3 * duration_ratio
    return {
        'physical_strength': physical_strength,
        'punching_power': punching_power,
        'dynamika': dynamika,
        'speed': speed,
        'timing': timing,
        'footwork': footwork,
        'chin': chin,
        'cardio': cardio,
    }


def prepare_long_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the fight‑level DataFrame into a long format keyed by fighter.

    Each row in the returned DataFrame corresponds to one fighter in one fight.  We
    produce two rows per bout: one for fighter 1 and one for fighter 2.  The
    function copies basic fight metadata, outcome indicators, aggregate
    statistics and newly computed qualitative scores.  Per‑round strike
    statistics and control durations are also brought across.
    """
    long_rows = []
    for prefix, opp_prefix, role in [('f_1', 'f_2', 'f_1'), ('f_2', 'f_1', 'f_2')]:
        temp = pd.DataFrame()
        # Metadata
        temp['event_date'] = df['event_date']
        temp['fight_url'] = df['fight_url']
        temp['fighter'] = df[f'{prefix}_url']
        temp['opponent'] = df[f'{opp_prefix}_url']
        temp['role'] = role
        # Outcome flags
        temp['is_winner'] = (df['winner'] == df[f'{prefix}_name']).astype(int)
        temp['is_loss'] = (df['winner'] == df[f'{opp_prefix}_name']).astype(int)
        temp['finish_win'] = np.where(
            (temp['is_winner'] == 1) & df['result'].str.contains('KO|TKO', na=False), 1, 0
        )
        temp['submission_win'] = np.where(
            (temp['is_winner'] == 1) & (df['result'] == 'Submission'), 1, 0
        )
        temp['finish_loss'] = np.where(
            (temp['is_loss'] == 1) & df['result'].str.contains('KO|TKO', na=False), 1, 0
        )
        temp['submission_loss'] = np.where(
            (temp['is_loss'] == 1) & (df['result'] == 'Submission'), 1, 0
        )
        # Fight duration and round durations
        temp['fight_duration_minutes'] = df['fight_duration_minutes']
        for r in range(1, 6):
            temp[f'r{r}_duration'] = df[f'r{r}_duration']
        # Aggregate fight statistics used in traditional UFC metrics
        for stat in ['sig_strikes_succ', 'sig_strikes_att', 'takedown_succ', 'takedown_att', 'submission_att', 'reversals']:
            temp[f'fighter_{stat}'] = df[f'{prefix}_{stat}']
            temp[f'opp_{stat}'] = df[f'{opp_prefix}_{stat}']
        temp['fighter_ctrl_time_sec'] = df[f'{prefix}_ctrl_time_sec']
        temp['opp_ctrl_time_sec'] = df[f'{opp_prefix}_ctrl_time_sec']
        # Per‑round striking, takedown, submission and control counts
        for r in range(1, 6):
            # Significant strikes
            temp[f'fighter_sig_strikes_succ_r{r}'] = df[f'{prefix}_r{r}_sig_strikes_succ']
            temp[f'fighter_sig_strikes_att_r{r}'] = df[f'{prefix}_r{r}_sig_strikes_att']
            temp[f'opp_sig_strikes_succ_r{r}'] = df[f'{opp_prefix}_r{r}_sig_strikes_succ']
            temp[f'opp_sig_strikes_att_r{r}'] = df[f'{opp_prefix}_r{r}_sig_strikes_att']
            # Takedowns (we use td_1 as attempts/success – second takedown fields are ignored)
            temp[f'fighter_takedown_succ_r{r}'] = df[f'{prefix}_r{r}_td_1_succ']
            temp[f'fighter_takedown_att_r{r}'] = df[f'{prefix}_r{r}_td_1_att']
            temp[f'opp_takedown_succ_r{r}'] = df[f'{opp_prefix}_r{r}_td_1_succ']
            temp[f'opp_takedown_att_r{r}'] = df[f'{opp_prefix}_r{r}_td_1_att']
            # Submissions
            temp[f'fighter_submission_att_r{r}'] = df[f'{prefix}_r{r}_submission_att']
            temp[f'opp_submission_att_r{r}'] = df[f'{opp_prefix}_r{r}_submission_att']
            # Reversals
            temp[f'fighter_reversals_r{r}'] = df[f'{prefix}_r{r}_reversals']
            temp[f'opp_reversals_r{r}'] = df[f'{opp_prefix}_r{r}_reversals']
            # Control
            temp[f'fighter_ctrl_r{r}'] = df[f'{prefix}_r{r}_ctrl']
            temp[f'opp_ctrl_r{r}'] = df[f'{opp_prefix}_r{r}_ctrl']
            # Knockdowns
            temp[f'fighter_knockdowns_r{r}'] = df[f'{prefix}_r{r}_knockdowns']
            temp[f'opp_knockdowns_r{r}'] = df[f'{opp_prefix}_r{r}_knockdowns']
        # Knockdowns and other fight level stats
        temp['fighter_knockdowns'] = df[f'{prefix}_knockdowns']
        temp['opp_knockdowns'] = df[f'{opp_prefix}_knockdowns']
        # Bring per‑round durations again for convenience
        for r in range(1, 6):
            temp[f'duration_r{r}'] = df[f'r{r}_duration']
        # Bring breakdown accuracies and shares across fight and per round
        # Across fight: accuracy and share for head/body/leg and distance/clinch/ground
        breakdown_cats = ['head', 'body', 'leg', 'distance', 'clinch', 'ground']
        for cat in breakdown_cats:
            temp[f'{cat}_acc'] = df[f'{prefix}_{cat}_acc']
            temp[f'{cat}_share'] = df[f'{prefix}_{cat}_share']
        # Per‑round breakdowns
        for r in range(1, 6):
            for cat in breakdown_cats:
                temp[f'{cat}_acc_r{r}'] = df[f'{prefix}_r{r}_{cat}_acc']
                temp[f'{cat}_share_r{r}'] = df[f'{prefix}_r{r}_{cat}_share']
        # Compute qualitative scores
        quality_scores = df.apply(lambda row: compute_quality_features(row, prefix), axis=1, result_type='expand')
        for col in ['physical_strength', 'punching_power', 'dynamika', 'speed', 'timing', 'footwork', 'chin', 'cardio']:
            temp[col] = quality_scores[col]
        long_rows.append(temp)
    long_df = pd.concat(long_rows, ignore_index=True)
    # Assign fighter names for potential reference
    long_df['fighter_name'] = long_df['fighter'].apply(lambda x: x.split('/')[-1] if isinstance(x, str) else x)
    return long_df


def calculate_outcomes(group: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling outcome statistics (wins, finish wins, submissions, streaks).

    This mirrors the original behaviour: for each fighter, we calculate the
    number of wins, finish wins, submission wins, losses and streak lengths
    over rolling windows defined in `ROLLING_WINDOWS`.  The results are
    shifted by one so that the current fight does not leak into the history.
    """
    group = group.sort_values('event_date').reset_index(drop=True)
    # Initialise new columns with zeros
    new_cols = {
        f'{metric}_{n}': np.zeros(len(group), dtype=int)
        for n in ROLLING_WINDOWS
        for metric in ['wins', 'finish_wins', 'sub_wins', 'streak', 'losses', 'finish_losses', 'sub_losses', 'losing_streak']
    }
    group = pd.concat([group, pd.DataFrame(new_cols, index=group.index)], axis=1)
    # Compute outcomes per fight
    for i in range(1, len(group)):
        for n in ROLLING_WINDOWS:
            prev = group.iloc[max(0, i - n):i]
            group.at[i, f'wins_{n}'] = prev['is_winner'].sum()
            group.at[i, f'finish_wins_{n}'] = prev['finish_win'].sum()
            group.at[i, f'sub_wins_{n}'] = prev['submission_win'].sum()
            group.at[i, f'losses_{n}'] = prev['is_loss'].sum()
            group.at[i, f'finish_losses_{n}'] = prev['finish_loss'].sum()
            group.at[i, f'sub_losses_{n}'] = prev['submission_loss'].sum()
            # Streak of wins: reverse cumulative product of last values of is_winner
            group.at[i, f'streak_{n}'] = (prev['is_winner'][::-1] == 1).cumprod().sum()
            group.at[i, f'losing_streak_{n}'] = (prev['is_loss'][::-1] == 1).cumprod().sum()
    return group


def calculate_group(group: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling statistics for a single fighter.

    This function extends the original calculation to handle round‑level
    metrics, strike breakdowns and qualitative scores.  For each metric, we
    compute a rolling aggregation over the previous N fights where N is in
    `ROLLING_WINDOWS`.  Two kinds of metrics are supported:

      * Ratio metrics are defined by (numerator_column, denominator_column,
        invert_flag).  We sum numerator and denominator across the window
        (shifted by one fight), divide and optionally invert (1 − value).
      * Average metrics are defined by (column_name, None).  We compute the
        simple average of the column over the window (again shifted by one).

    The returned DataFrame contains new columns for each rolling metric
    labelled "{metric}_{window}".
    """
    group = group.sort_values('event_date').reset_index(drop=True)
    # Determine per‑fight denominator for average metrics.  We'll create a
    # constant column of ones to count fights in a window.
    group['ones'] = 1
    # Metrics definition: map metric names to (numerator, denominator, invert, is_avg)
    metrics = {}
    # Traditional fight metrics (slpm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg, ctrl_ratio)
    metrics.update({
        'slpm': ('fighter_sig_strikes_succ', 'fight_duration_minutes', False, False),
        'str_acc': ('fighter_sig_strikes_succ', 'fighter_sig_strikes_att', False, False),
        'sapm': ('opp_sig_strikes_succ', 'fight_duration_minutes', False, False),
        'str_def': ('opp_sig_strikes_succ', 'opp_sig_strikes_att', True, False),
        'td_avg': ('fighter_takedown_succ', 'fight_duration_minutes', False, False),
        'td_acc': ('fighter_takedown_succ', 'fighter_takedown_att', False, False),
        'td_def': ('opp_takedown_succ', 'opp_takedown_att', True, False),
        'sub_avg': ('fighter_submission_att', 'fight_duration_minutes', False, False),
        'ctrl_ratio': ('fighter_ctrl_time_sec', 'fight_duration_minutes', False, False),
    })
    # Per‑round metrics: for each round we define similar metrics
    for r in range(1, 6):
        suffix = f'r{r}'
        metrics.update({
            f'slpm_{suffix}': (f'fighter_sig_strikes_succ_{suffix}', f'duration_{suffix}', False, False),
            f'str_acc_{suffix}': (f'fighter_sig_strikes_succ_{suffix}', f'fighter_sig_strikes_att_{suffix}', False, False),
            f'sapm_{suffix}': (f'opp_sig_strikes_succ_{suffix}', f'duration_{suffix}', False, False),
            f'str_def_{suffix}': (f'opp_sig_strikes_succ_{suffix}', f'opp_sig_strikes_att_{suffix}', True, False),
            f'td_avg_{suffix}': (f'fighter_takedown_succ_{suffix}', f'duration_{suffix}', False, False),
            f'td_acc_{suffix}': (f'fighter_takedown_succ_{suffix}', f'fighter_takedown_att_{suffix}', False, False),
            f'td_def_{suffix}': (f'opp_takedown_succ_{suffix}', f'opp_takedown_att_{suffix}', True, False),
            f'sub_avg_{suffix}': (f'fighter_submission_att_{suffix}', f'duration_{suffix}', False, False),
            f'ctrl_ratio_{suffix}': (f'fighter_ctrl_{suffix}', f'duration_{suffix}', False, False),
        })
    # Strike breakdown accuracies and shares across fight and per round.  These
    # metrics are treated as averages (simple mean) because they are already
    # ratios.  We'll create average metrics by setting denominator to None.
    breakdown_metrics = []
    cats = ['head', 'body', 'leg', 'distance', 'clinch', 'ground']
    for cat in cats:
        breakdown_metrics.append(f'{cat}_acc')
        breakdown_metrics.append(f'{cat}_share')
        for r in range(1, 6):
            breakdown_metrics.append(f'{cat}_acc_r{r}')
            breakdown_metrics.append(f'{cat}_share_r{r}')
    for m in breakdown_metrics:
        metrics[m] = (m, None, False, True)
    # Qualitative scores are also averaged across the window
    for q in ['physical_strength', 'punching_power', 'dynamika', 'speed', 'timing', 'footwork', 'chin', 'cardio']:
        metrics[q] = (q, None, False, True)
    # Compute rolling aggregations
    new_cols = {}
    for n in ROLLING_WINDOWS:
        for m_name, (num_col, denom_col, invert, is_avg) in metrics.items():
            col_name = f'{m_name}_{n}'
            # Initialise column
            new_cols[col_name] = np.zeros(len(group))
            # Rolling sums/averages shifted by one to exclude current fight
            if is_avg:
                roll_sum = group[num_col].rolling(n, min_periods=1).sum().shift(1)
                roll_count = group[num_col].rolling(n, min_periods=1).count().shift(1)
                # Avoid division by zero: where count==0, result stays zero
                val = np.divide(
                    roll_sum,
                    roll_count,
                    out=np.zeros_like(roll_sum),
                    where=roll_count > 0
                )
                new_cols[col_name] = val
            else:
                roll_num = group[num_col].rolling(n, min_periods=1).sum().shift(1)
                roll_den = group[denom_col].rolling(n, min_periods=1).sum().shift(1)
                val = np.divide(
                    roll_num,
                    roll_den,
                    out=np.zeros_like(roll_num),
                    where=roll_den > 0
                )
                if invert:
                    val = 1.0 - val
                new_cols[col_name] = val
    new_df = pd.concat([group, pd.DataFrame(new_cols, index=group.index)], axis=1)
    return new_df


def merge_rolling_and_outcomes(df: pd.DataFrame, long_df: pd.DataFrame) -> pd.DataFrame:
    """Merge rolling metrics from the long format back into the fight‑level table.

    We pivot the long DataFrame on fight_url and role to spread rolling
    features across fighter columns.  Missing values are filled with zero
    (because rolling aggregates are undefined for the first few fights).
    """
    # Identify all rolling metric columns (they end with an integer window)
    rolling_cols = [c for c in long_df.columns if any(c.endswith(f'_{n}') for n in ROLLING_WINDOWS)]
    # Pivot by role
    pivot = long_df.pivot(index='fight_url', columns='role', values=rolling_cols)
    # Flatten MultiIndex columns
    pivot.columns = [f'{col}_{role}' for col, role in pivot.columns]
    merged = df.merge(pivot, on='fight_url', how='left')
    # Fill NaNs in rolling columns with zero
    merged[rolling_cols] = merged[rolling_cols].fillna(0)
    return merged


def add_fight_ordinal(df: pd.DataFrame, long_df: pd.DataFrame) -> pd.DataFrame:
    """Assign sequential fight numbers to each fighter and pivot into fight‑level columns."""
    long_df = long_df.copy()
    long_df['fight_ordinal'] = long_df.groupby('fighter').cumcount() + 1
    pivot = long_df.pivot(index='fight_url', columns='role', values='fight_ordinal')
    pivot.columns = [f'fight_ordinal_{c}' for c in pivot.columns]
    return df.merge(pivot, on='fight_url', how='left')


def add_additional_features(df: pd.DataFrame, long_df: pd.DataFrame) -> pd.DataFrame:
    """Compute static fighter attributes (age, fight count) and merge into fight table."""
    df = df.copy()
    # Convert dates of birth to datetime
    df['fighter_dob_f_1'] = pd.to_datetime(df['f_1_fighter_dob'])
    df['fighter_dob_f_2'] = pd.to_datetime(df['f_2_fighter_dob'])
    df['f_1_age'] = (df['event_date'] - df['fighter_dob_f_1']).dt.days // 365
    df['f_2_age'] = (df['event_date'] - df['fighter_dob_f_2']).dt.days // 365
    # Number of previous fights per fighter
    long_df = long_df.copy()
    long_df['fight_count'] = long_df.groupby('fighter').cumcount()
    counts = long_df.pivot(index='fight_url', columns='role', values='fight_count')
    counts.columns = [f'{c}_fight_number' for c in counts.columns]
    df = df.merge(counts, on='fight_url', how='left')
    return df


def add_diff_and_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute pairwise differences and simple interactions for select rolling metrics.

    The model often benefits from knowing how two fighters compare on certain
    statistics.  Here we compute differences for a subset of metrics across
    the rolling windows and store them in new columns.  We also include
    existing differences (age, fight ordinal, ranking and betting odds).
    """
    df = df.copy()
    # Difference of ages and fight count
    df['diff_age'] = df['f_1_age'] - df['f_2_age']
    df['diff_fight_number'] = df['fight_ordinal_f_1'] - df['fight_ordinal_f_2']
    df['diff_ranking'] = df['f_1_ranking'] - df['f_2_ranking']
    df['diff_odds'] = df['f_1_odds'] - df['f_2_odds']
    # For each rolling metric, compute difference between fighter 1 and fighter 2
    for metric in ['slpm', 'str_acc', 'sapm', 'str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg', 'ctrl_ratio',
                   'physical_strength', 'punching_power', 'dynamika', 'speed', 'timing', 'footwork', 'chin', 'cardio']:
        for n in ROLLING_WINDOWS:
            col1 = f'{metric}_{n}_f_1'
            col2 = f'{metric}_{n}_f_2'
            if col1 in df.columns and col2 in df.columns:
                df[f'diff_{metric}_{n}'] = df[col1] - df[col2]
    # Differences for strike breakdown accuracies and shares across fight and per round
    cats = ['head', 'body', 'leg', 'distance', 'clinch', 'ground']
    for base in cats:
        for suffix in ['', '_r1', '_r2', '_r3', '_r4', '_r5']:
            for t in ['acc', 'share']:
                metric_name = f'{base}_{t}{suffix}'
                for n in ROLLING_WINDOWS:
                    col1 = f'{metric_name}_{n}_f_1'
                    col2 = f'{metric_name}_{n}_f_2'
                    if col1 in df.columns and col2 in df.columns:
                        df[f'diff_{metric_name}_{n}'] = df[col1] - df[col2]
    return df


from pandas.api.types import is_bool_dtype

# --- FIX booleany (także hidden: boolean[pyarrow] / object True/False/None) ---
def _coerce_boolean_like(df):
    touched = []
    for c in df.columns:
        s = df[c]
        # 1) natywne bool/BooleanDtype
        if is_bool_dtype(s):
            df[c] = s.astype("boolean").fillna(False)  # zostaje BooleanDtype (NA-aware)
            touched.append(c)
            continue
        # 2) object z True/False/None
        non_na = s.dropna()
        # jeśli wszystkie nie-NA to dokładnie True/False -> też potraktuj jako bool
        if len(non_na) and non_na.isin([True, False]).all():
            df[c] = s.astype("boolean").fillna(False)
            touched.append(c)
    return touched

def process_new_fights() -> None:
    """Main entrypoint for feature engineering pipeline.

    This function orchestrates the loading of raw data, computation of
    additional features, rolling aggregations and saving the resulting
    DataFrame to BigQuery.  It may take several minutes to run due to the
    extensive per‑round calculations and rolling window operations.
    """
    total_start = time.time()
    # Load fight data
    df = get_full_fight_data()
    # Add encoded winner flag
    df = add_winner_encoded(df)
    # Compute base features such as durations
    df = prepare_base_features(df)
    # Compute strike breakdowns (accuracy and share) across fight and per round
    df = compute_strike_breakdowns(df)
    # Prepare long format: two rows per fight, one for each fighter
    long_df = prepare_long_format(df)
    # Compute rolling outcomes (wins/losses etc.)
    long_df = parallel_apply(long_df, 'fighter', calculate_outcomes, n_jobs=8)
    # Compute rolling statistics for all metrics defined in calculate_group
    long_df = parallel_apply(long_df, 'fighter', calculate_group, n_jobs=8)
    # Merge rolling statistics back into wide fight‑level table
    df = merge_rolling_and_outcomes(df, long_df)
    # Add sequential fight ordinal numbers and age/fight count features
    df = add_fight_ordinal(df, long_df)
    df = add_additional_features(df, long_df)
    # Compute difference and interaction features
    df = add_diff_and_interaction_features(df)
    # Remove any remaining NaNs in rolling columns by replacing with zero
    rolling_like = [c for c in df.columns if any(c.endswith(f'_{n}') for n in ROLLING_WINDOWS)]
    df[rolling_like] = df[rolling_like].fillna(0)
    # Write to BigQuery, replacing existing table
    client.delete_table(FULL_TABLE_ID, not_found_ok=True)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    _touched = _coerce_boolean_like(df)
    client.load_table_from_dataframe(df, FULL_TABLE_ID, job_config=job_config).result()
    print(f"✅ Updated {FULL_TABLE_ID} — {len(df)} records | {time.time() - total_start:.2f}s")


def ufc_model_update(request):
    """Cloud Function handler to trigger the pipeline.  Returns HTTP status."""
    try:
        process_new_fights()
        return "OK", 200
    except Exception as e:
        return f"Error: {str(e)}", 500


if __name__ == '__main__':
    process_new_fights()