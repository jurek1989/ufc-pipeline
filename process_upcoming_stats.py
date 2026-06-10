from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
from tqdm import tqdm

PROJECT_ID = "ultra-acre-443816-g8"
client = bigquery.Client(project=PROJECT_ID)

warnings.filterwarnings("ignore", category=RuntimeWarning)
pd.options.mode.chained_assignment = None
tqdm.pandas(desc="Processing")

def get_upcoming_event_fighters():
    df = client.query("""
        SELECT *
        FROM UFC_data.UFC_coming_event_with_rankings_placeholder
    """).to_dataframe()
    fighter_urls = pd.concat([df["fighter1_link"], df["fighter2_link"]]).dropna().unique().tolist()
    return df, fighter_urls

def get_fighters_history(fighter_urls):
    query = f"""
        SELECT * FROM UFC_data.full_data
        WHERE f_1_url IN UNNEST(@fighter_urls)
           OR f_2_url IN UNNEST(@fighter_urls)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("fighter_urls", "STRING", fighter_urls)
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

def time_to_minutes(time_str):
    try: return sum(x * int(t) for x, t in zip([60, 1], time_str.split(":"))) / 60
    except: return 0

def time_to_seconds(time_str):
    try: return sum(x * int(t) for x, t in zip([60, 1], time_str.split(":")))
    except: return 0

def prepare_base_features(df):
    df['f_1_ctrl_time_sec'] = df['f_1_ctrl_time'].apply(time_to_seconds)
    df['f_2_ctrl_time_sec'] = df['f_2_ctrl_time'].apply(time_to_seconds)
    df['event_date'] = pd.to_datetime(df['event_date'])
    df['fight_duration_minutes'] = df.apply(
        lambda row: (row['finish_round'] - 1) * 5 + time_to_minutes(row['finish_time']), axis=1
    )
    return df

def prepare_long_format(df):
    def create_view(prefix, role):
        view = df[['event_date', 'fight_url', 'f_1_url', 'f_2_url', 'result', 'winner']].copy()
        view['fighter'] = df[f'{prefix}_url']
        view['opponent'] = df[f'f_{2 if role == "f_1" else 1}_url']
        view['role'] = role
        view['is_winner'] = (df['winner'] == df[prefix]).astype(int)
        view['is_loss'] = (df['winner'] == df[f'f_{2 if role == "f_1" else 1}']).astype(int)
        view['finish_win'] = np.where(view['is_winner'] & df['result'].isin(['KO/TKO', "TKO - Doctor's Stoppage"]), 1, 0)
        view['submission_win'] = np.where(view['is_winner'] & (df['result'] == 'Submission'), 1, 0)
        view['finish_loss'] = np.where(view['is_loss'] & df['result'].isin(['KO/TKO', "TKO - Doctor's Stoppage"]), 1, 0)
        view['submission_loss'] = np.where(view['is_loss'] & (df['result'] == 'Submission'), 1, 0)

        for stat in ['sig_strikes_succ', 'sig_strikes_att', 'takedown_succ', 'takedown_att',
                     'submission_att', 'ctrl_time_sec']:
            view[f'fighter_{stat}'] = df[f'{prefix}_{stat}']
            view[f'opp_{stat}'] = df[f'f_{2 if role == "f_1" else 1}_{stat}']

        view['fight_duration_minutes'] = df['fight_duration_minutes']
        return view

    f1 = create_view('f_1', 'f_1')
    f2 = create_view('f_2', 'f_2')
    combined = pd.concat([f1, f2], ignore_index=True).sort_values(['fighter', 'event_date'])
    combined['fight_ordinal'] = combined.groupby('fighter').cumcount() + 1
    return combined

def calculate_rolling_stats(long_df, n_values):
    metrics = {
        'slpm': {'num': 'fighter_sig_strikes_succ', 'denom': 'fight_duration_minutes'},
        'str_acc': {'num': 'fighter_sig_strikes_succ', 'denom': 'fighter_sig_strikes_att'},
        'sapm': {'num': 'opp_sig_strikes_succ', 'denom': 'fight_duration_minutes'},
        'str_def': {'num': 'opp_sig_strikes_succ', 'denom': 'opp_sig_strikes_att', 'invert': True},
        'td_avg': {'num': 'fighter_takedown_succ', 'denom': 'fight_duration_minutes'},
        'td_acc': {'num': 'fighter_takedown_succ', 'denom': 'fighter_takedown_att'},
        'td_def': {'num': 'opp_takedown_succ', 'denom': 'opp_takedown_att', 'invert': True},
        'sub_avg': {'num': 'fighter_submission_att', 'denom': 'fight_duration_minutes'},
        'ctrl_ratio': {'num': 'fighter_ctrl_time_sec', 'denom': 'fight_duration_minutes'}
    }

    def process_group(group):
        group = group.sort_values('event_date')
        for n in n_values:
            for m, p in metrics.items():
                num = group[p['num']].rolling(n, min_periods=1).sum()
                denom = group[p['denom']].rolling(n, min_periods=1).sum()
                result = np.divide(num, denom, out=np.full_like(num, np.nan), where=denom != 0)
                if p.get('invert'): result = 1 - result
                group[f'{m}_{n}'] = result

            group[f'sub_wins_{n}'] = group['submission_win'].rolling(n, min_periods=1).sum()
            group[f'finish_wins_{n}'] = group['finish_win'].rolling(n, min_periods=1).sum()
            group[f'wins_{n}'] = group['is_winner'].rolling(n, min_periods=1).sum()
            group[f'streak_{n}'] = group['is_winner'].rolling(n, min_periods=1).apply(lambda x: (x[::-1] == 1).cumprod().sum(), raw=True)
        return group

    return long_df.groupby('fighter', group_keys=False).progress_apply(process_group)

def prepare_final_dataset(upcoming_df, long_df):
    latest_stats = long_df.sort_values('event_date').groupby('fighter').tail(1).copy()
    f1_df = latest_stats.copy().add_prefix('f_1_')
    f2_df = latest_stats.copy().add_prefix('f_2_')

    merged = upcoming_df.copy()
    merged = merged.merge(f1_df, left_on='fighter1_link', right_on='f_1_fighter', how='left')
    merged = merged.merge(f2_df, left_on='fighter2_link', right_on='f_2_fighter', how='left')

    # Oblicz wiek
    merged['f_1_age'] = (pd.to_datetime(merged['event_date']) - pd.to_datetime(merged['f_1_fighter_dob'])).dt.days // 365
    merged['f_2_age'] = (pd.to_datetime(merged['event_date']) - pd.to_datetime(merged['f_2_fighter_dob'])).dt.days // 365

    # Sklej imię i nazwisko
    merged['f_1_full_name'] = merged['f_1_fighter_f_name'].fillna('') + ' ' + merged['f_1_fighter_l_name'].fillna('')
    merged['f_2_full_name'] = merged['f_2_fighter_f_name'].fillna('') + ' ' + merged['f_2_fighter_l_name'].fillna('')

    # Ustal gender
    def map_gender(weight_class):
        if pd.isnull(weight_class): return 'Unknown'
        return 'Female' if "Women" in weight_class else 'Male'

    merged['gender'] = merged['weight_class'].apply(map_gender)

    for col in ['f_1_ko_odds', 'f_1_sub_odds', 'f_2_ko_odds', 'f_2_sub_odds']:
        merged[col] = None

    merged = merged.rename(columns={
        'f_1_fight_ordinal': 'fight_ordinal_f_1',
        'f_2_fight_ordinal': 'fight_ordinal_f_2'
    })

    return merged

def process_upcoming_fight_stats():
    upcoming_df, fighter_urls = get_upcoming_event_fighters()
    print(f"[INFO] Fighterów: {len(fighter_urls)}")

    history_df = get_fighters_history(fighter_urls)
    print(f"[INFO] Walk historycznych: {len(history_df)}")

    history_df = prepare_base_features(history_df)
    long_df = prepare_long_format(history_df)
    long_df = calculate_rolling_stats(long_df, range(3, 16))

    final_df = prepare_final_dataset(upcoming_df, long_df)

    try:
        table_ref = client.get_table("UFC_data.UFC_model_full_analysis")
        bq_columns = set([field.name for field in table_ref.schema])
        df_columns = set(final_df.columns)
        extra_columns = df_columns - bq_columns
        if extra_columns:
            print(f"Usuwam kolumny nieobecne w schemacie BigQuery: {extra_columns}")
            final_df = final_df.drop(columns=list(extra_columns))
    except Exception as e:
        print(f"[WARN] Nie udało się pobrać schematu tabeli UFC_model_full_analysis. Powód: {e}")

    final_df.to_gbq("UFC_data.UFC_model_prediction_input", project_id=PROJECT_ID, if_exists="replace")
    print(f"[SUCCESS] Zapisano {len(final_df)} rekordów do UFC_model_prediction_input")

def ufc_upcoming_model_processor(request):
    try:
        process_upcoming_fight_stats()
        return "OK", 200
    except Exception as e:
        print(f"[ERROR] {e}")
        return f"Error: {e}", 500

if __name__ == "__main__":
    process_upcoming_fight_stats()
