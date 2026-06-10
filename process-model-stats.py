from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import time
import multiprocessing as mp

# Konfiguracja
PROJECT_ID = "ultra-acre-443816-g8"
DATASET_ID = "UFC_model"
TABLE_ID = "UFC_model_full_analysis"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
SOURCE_TABLE = f"ultra-acre-443816-g8.UFC_data.full_data"

client = bigquery.Client(project=PROJECT_ID)
warnings.filterwarnings("ignore", category=RuntimeWarning)
pd.options.mode.chained_assignment = None

ROLLING_WINDOWS = list(range(3, 16))

# Utils

def time_to_minutes(s):
    try: return sum(x * int(t) for x, t in zip([60, 1], s.split(":"))) / 60
    except: return 0

def time_to_seconds(s):
    try: return sum(x * int(t) for x, t in zip([60, 1], s.split(":")))
    except: return 0

def parallel_apply(df, group_col, func, n_jobs=4):
    groups = [group for _, group in df.groupby(group_col)]
    with mp.Pool(n_jobs) as pool:
        results = pool.map(func, groups)
    return pd.concat(results)

# Data prep

def get_full_fight_data():
    query = f"SELECT * FROM `{SOURCE_TABLE}` ORDER BY event_date"
    return client.query(query).to_dataframe()

def add_winner_encoded(df):
    df['winner_encoded'] = np.select([
        df['winner'] == df['f_1_name'],
        df['winner'] == df['f_2_name'],
        df['winner'].isin(['Draw', 'No Contest'])
    ], [1, 0, -1], default=-1)
    return df

def prepare_base_features(df):
    df['f_1_ctrl_time_sec'] = df['f_1_ctrl_time'].apply(time_to_seconds)
    df['f_2_ctrl_time_sec'] = df['f_2_ctrl_time'].apply(time_to_seconds)
    df['event_date'] = pd.to_datetime(df['event_date'])
    df['fight_duration_minutes'] = df.apply(
        lambda row: (row['finish_round'] - 1) * 5 + time_to_minutes(row['finish_time']), axis=1)
    return df

def prepare_long_format(df):
    def create_view(prefix, role):
        opp_prefix = 'f_2' if prefix == 'f_1' else 'f_1'
        view = df[['event_date', 'fight_url', 'f_1_url', 'f_2_url', 'result', 'winner']].copy()
        view['fighter'] = df[f'{prefix}_url']
        view['opponent'] = df[f'{opp_prefix}_url']
        view['role'] = role
        view['is_winner'] = (df['winner'] == df[f'{prefix}_name']).astype(int)
        view['is_loss'] = (df['winner'] == df[f'{opp_prefix}_name']).astype(int)
        view['finish_win'] = np.where(view['is_winner'] & df['result'].str.contains("KO|TKO"), 1, 0)
        view['submission_win'] = np.where(view['is_winner'] & (df['result'] == 'Submission'), 1, 0)
        view['finish_loss'] = np.where(view['is_loss'] & df['result'].str.contains("KO|TKO"), 1, 0)
        view['submission_loss'] = np.where(view['is_loss'] & (df['result'] == 'Submission'), 1, 0)
        for stat in ['sig_strikes_succ', 'sig_strikes_att', 'takedown_succ', 'takedown_att', 'submission_att', 'ctrl_time_sec']:
            view[f'fighter_{stat}'] = df[f'{prefix}_{stat}']
            view[f'opp_{stat}'] = df[f'{opp_prefix}_{stat}']
        view['fight_duration_minutes'] = df['fight_duration_minutes']
        return view

    return pd.concat([
        create_view('f_1', 'f_1'),
        create_view('f_2', 'f_2')
    ]).sort_values(['fighter', 'event_date'])

# Rolling + Outcomes

def calculate_outcomes(group):
    group = group.sort_values('event_date').reset_index(drop=True)
    new_cols = {
        f'{m}_{n}': np.zeros(len(group), dtype=int)
        for n in ROLLING_WINDOWS
        for m in ['wins', 'finish_wins', 'sub_wins', 'streak', 'losses', 'finish_losses', 'sub_losses', 'losing_streak']
    }
    group = pd.concat([group, pd.DataFrame(new_cols, index=group.index)], axis=1)

    for i in range(1, len(group)):
        for n in ROLLING_WINDOWS:
            prev = group.iloc[max(0, i-n):i]
            group.at[i, f'wins_{n}'] = prev['is_winner'].sum()
            group.at[i, f'finish_wins_{n}'] = prev['finish_win'].sum()
            group.at[i, f'sub_wins_{n}'] = prev['submission_win'].sum()
            group.at[i, f'losses_{n}'] = prev['is_loss'].sum()
            group.at[i, f'finish_losses_{n}'] = prev['finish_loss'].sum()
            group.at[i, f'sub_losses_{n}'] = prev['submission_loss'].sum()
            group.at[i, f'streak_{n}'] = (prev['is_winner'][::-1] == 1).cumprod().sum()
            group.at[i, f'losing_streak_{n}'] = (prev['is_loss'][::-1] == 1).cumprod().sum()
    return group

def calculate_group(group):
    group = group.sort_values('event_date')
    metrics = {
        'slpm': ('fighter_sig_strikes_succ', 'fight_duration_minutes'),
        'str_acc': ('fighter_sig_strikes_succ', 'fighter_sig_strikes_att'),
        'sapm': ('opp_sig_strikes_succ', 'fight_duration_minutes'),
        'str_def': ('opp_sig_strikes_succ', 'opp_sig_strikes_att', True),
        'td_avg': ('fighter_takedown_succ', 'fight_duration_minutes'),
        'td_acc': ('fighter_takedown_succ', 'fighter_takedown_att'),
        'td_def': ('opp_takedown_succ', 'opp_takedown_att', True),
        'sub_avg': ('fighter_submission_att', 'fight_duration_minutes'),
        'ctrl_ratio': ('fighter_ctrl_time_sec', 'fight_duration_minutes')
    }
    new_cols = {}
    for n in ROLLING_WINDOWS:
        for m, args in metrics.items():
            invert = args[2] if len(args) == 3 else False
            num, denom = args[0], args[1]
            roll_num = group[num].rolling(n, min_periods=1).sum().shift(1)
            roll_denom = group[denom].rolling(n, min_periods=1).sum().shift(1)
            val = np.divide(roll_num, roll_denom, out=np.full_like(roll_num, np.nan), where=(roll_denom != 0))
            new_cols[f'{m}_{n}'] = 1 - val if invert else val
    return pd.concat([group, pd.DataFrame(new_cols, index=group.index)], axis=1)

def merge_rolling_and_outcomes(df, long_df):
    metrics = [
        f'{m}_{n}' for n in ROLLING_WINDOWS for m in [
            'wins', 'finish_wins', 'sub_wins', 'streak', 'losses', 'finish_losses', 'sub_losses', 'losing_streak',
            'slpm', 'str_acc', 'sapm', 'str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg', 'ctrl_ratio'
        ]
    ]
    pivot = long_df.pivot(index='fight_url', columns='role', values=[col for col in metrics if col in long_df.columns])
    pivot.columns = [f'{role}_{col}' for col, role in pivot.columns]
    df = df.merge(pivot, on='fight_url', how='left')
    existing_metrics = [col for col in pivot.columns if col in df.columns]
    df[existing_metrics] = df[existing_metrics].fillna(0)
    return df

# Inżynieria cech

def add_fight_ordinal(df, long_df):
    long_df['fight_ordinal'] = long_df.groupby('fighter').cumcount() + 1
    pivot = long_df.pivot(index='fight_url', columns='role', values='fight_ordinal')
    pivot.columns = [f'fight_ordinal_{c}' for c in pivot.columns]
    return df.merge(pivot, on='fight_url', how='left')

def add_additional_features(df, long_df):
    df['fighter_dob_f_1'] = pd.to_datetime(df['f_1_fighter_dob'])
    df['fighter_dob_f_2'] = pd.to_datetime(df['f_2_fighter_dob'])
    df['f_1_age'] = (df['event_date'] - df['fighter_dob_f_1']).dt.days // 365
    df['f_2_age'] = (df['event_date'] - df['fighter_dob_f_2']).dt.days // 365
    long_df['fight_count'] = long_df.groupby('fighter').cumcount()
    counts = long_df.pivot(index='fight_url', columns='role', values='fight_count')
    counts.columns = [f'{c}_fight_number' for c in counts.columns]
    return df.merge(counts, on='fight_url', how='left')

def add_diff_and_interaction_features(df):
    for n in range(5, 16, 5):
        df[f"diff_slpm_{n}"] = df[f"f_1_slpm_{n}"] - df[f"f_2_slpm_{n}"]
        df[f"td_effectiveness_{n}"] = df[f"f_1_td_avg_{n}"] * (1 - df[f"f_2_td_def_{n}"])
    df['diff_age'] = df['f_1_age'] - df['f_2_age']
    df['diff_fight_number'] = df['fight_ordinal_f_1'] - df['fight_ordinal_f_2']
    df['diff_ranking'] = df['f_1_ranking'] - df['f_2_ranking']
    df['diff_odds'] = df['f_1_odds'] - df['f_2_odds']
    return df

# Główna funkcja

def process_new_fights():
    total_start = time.time()
    df = get_full_fight_data()
    df = add_winner_encoded(df)
    df = prepare_base_features(df)

    long_df = prepare_long_format(df)
    long_df = parallel_apply(long_df, 'fighter', calculate_outcomes, n_jobs=8)
    long_df = parallel_apply(long_df, 'fighter', calculate_group, n_jobs=8)

    df = merge_rolling_and_outcomes(df, long_df)
    df = add_fight_ordinal(df, long_df)
    df = add_additional_features(df, long_df)
    df = add_diff_and_interaction_features(df)

    client.delete_table(FULL_TABLE_ID, not_found_ok=True)
    client.load_table_from_dataframe(df, FULL_TABLE_ID, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
    print(f"✅ Zaktualizowano {FULL_TABLE_ID} — {len(df)} rekordów | {time.time()-total_start:.2f}s")

# Entry point

def ufc_model_update(request):
    try:
        process_new_fights()
        return "OK", 200
    except Exception as e:
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    process_new_fights()
