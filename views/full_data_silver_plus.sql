-- KANONICZNY widok bazowy (§7 ARCHITECTURE.md).
-- full_data.sql jest DEPRECATED — zastąpiony przez ten widok.

WITH bfo_closing AS (
  -- Closing no-vig consensus odds from BFO, one row per fight.
  -- event_url in odds_consensus is a BFO URL (different from UFCStats),
  -- so we join by fighter names + event date (±1 day for timezone offset).
  SELECT
    fighter_1,
    fighter_2,
    DATE(scraped_at)       AS fight_date,
    consensus_prob_f1,
    consensus_prob_f2,
    bookmaker_count        AS bfo_bookmaker_count,
    best_dec_f1,
    best_dec_f2
  FROM `UFC_data.odds_consensus`
  WHERE is_closing = TRUE
)

SELECT
  -- Base fight info
  fights_data.* EXCEPT(f_1, f_2, f_1_url, f_2_url),
  events_data.event_date,
  events_data.event_city,
  events_data.event_state,
  events_data.event_country,
  events_data.event_url,

  -- Aliases to avoid overwriting URLs
  fights_data.f_1 AS f_1_name,
  fights_data.f_2 AS f_2_name,
  fights_data.f_1_url,
  fights_data.f_2_url,

  -- Fighter 1 profile
  fighters_data.fighter_f_name    AS f_1_fighter_f_name,
  fighters_data.fighter_l_name    AS f_1_fighter_l_name,
  fighters_data.fighter_nickname  AS f_1_fighter_nickname,
  fighters_data.fighter_height_cm AS f_1_fighter_height_cm,
  fighters_data.fighter_weight_lbs AS f_1_fighter_weight_lbs,
  fighters_data.fighter_reach_cm  AS f_1_fighter_reach_cm,
  fighters_data.fighter_stance    AS f_1_fighter_stance,
  fighters_data.fighter_dob       AS f_1_fighter_dob,
  fighters_data.fighter_w         AS f_1_fighter_w,
  fighters_data.fighter_l         AS f_1_fighter_l,
  fighters_data.fighter_d         AS f_1_fighter_d,
  fighters_data.fighter_nc_dq     AS f_1_fighter_nc_dq,
  fighters_data.fighter_SlpM      AS f_1_fighter_SlpM,
  fighters_data.fighter_Str_Acc   AS f_1_fighter_Str_Acc,
  fighters_data.fighter_SApM      AS f_1_fighter_SApM,
  fighters_data.fighter_Str_Def   AS f_1_fighter_Str_Def,
  fighters_data.fighter_TD_Avg    AS f_1_fighter_TD_Avg,
  fighters_data.fighter_TD_Acc    AS f_1_fighter_TD_Acc,
  fighters_data.fighter_TD_Def    AS f_1_fighter_TD_Def,
  fighters_data.fighter_Sub_Avg   AS f_1_fighter_Sub_Avg,
  fighters_data.fighter_url       AS f_1_fighter_url,

  -- Fighter 2 profile
  fighters_data2.fighter_f_name    AS f_2_fighter_f_name,
  fighters_data2.fighter_l_name    AS f_2_fighter_l_name,
  fighters_data2.fighter_nickname  AS f_2_fighter_nickname,
  fighters_data2.fighter_height_cm AS f_2_fighter_height_cm,
  fighters_data2.fighter_weight_lbs AS f_2_fighter_weight_lbs,
  fighters_data2.fighter_reach_cm  AS f_2_fighter_reach_cm,
  fighters_data2.fighter_stance    AS f_2_fighter_stance,
  fighters_data2.fighter_dob       AS f_2_fighter_dob,
  fighters_data2.fighter_w         AS f_2_fighter_w,
  fighters_data2.fighter_l         AS f_2_fighter_l,
  fighters_data2.fighter_d         AS f_2_fighter_d,
  fighters_data2.fighter_nc_dq     AS f_2_fighter_nc_dq,
  fighters_data2.fighter_SlpM      AS f_2_fighter_SlpM,
  fighters_data2.fighter_Str_Acc   AS f_2_fighter_Str_Acc,
  fighters_data2.fighter_SApM      AS f_2_fighter_SApM,
  fighters_data2.fighter_Str_Def   AS f_2_fighter_Str_Def,
  fighters_data2.fighter_TD_Avg    AS f_2_fighter_TD_Avg,
  fighters_data2.fighter_TD_Acc    AS f_2_fighter_TD_Acc,
  fighters_data2.fighter_TD_Def    AS f_2_fighter_TD_Def,
  fighters_data2.fighter_Sub_Avg   AS f_2_fighter_Sub_Avg,
  fighters_data2.fighter_url       AS f_2_fighter_url,

  -- Aggregate fight stats (flat; secs only)
  CAST(stats1.knockdowns AS INT64)          AS f_1_knockdowns,
  CAST(stats1.total_strikes_att AS INT64)   AS f_1_total_strikes_att,
  CAST(stats1.total_strikes_succ AS INT64)  AS f_1_total_strikes_succ,
  CAST(stats1.sig_strikes_att AS INT64)     AS f_1_sig_strikes_att,
  CAST(stats1.sig_strikes_succ AS INT64)    AS f_1_sig_strikes_succ,
  CAST(stats1.takedown_att AS INT64)        AS f_1_takedown_att,
  CAST(stats1.takedown_succ AS INT64)       AS f_1_takedown_succ,
  CAST(stats1.submission_att AS INT64)      AS f_1_submission_att,
  CAST(stats1.reversals AS INT64)           AS f_1_reversals,
  CAST(stats1.ctrl_time_sec AS INT64)       AS f_1_ctrl_time_sec,

  CAST(stats2.knockdowns AS INT64)          AS f_2_knockdowns,
  CAST(stats2.total_strikes_att AS INT64)   AS f_2_total_strikes_att,
  CAST(stats2.total_strikes_succ AS INT64)  AS f_2_total_strikes_succ,
  CAST(stats2.sig_strikes_att AS INT64)     AS f_2_sig_strikes_att,
  CAST(stats2.sig_strikes_succ AS INT64)    AS f_2_sig_strikes_succ,
  CAST(stats2.takedown_att AS INT64)        AS f_2_takedown_att,
  CAST(stats2.takedown_succ AS INT64)       AS f_2_takedown_succ,
  CAST(stats2.submission_att AS INT64)      AS f_2_submission_att,
  CAST(stats2.reversals AS INT64)           AS f_2_reversals,
  CAST(stats2.ctrl_time_sec AS INT64)       AS f_2_ctrl_time_sec,
  -- Fighter 1, Round 1
  CAST(stats1.round1_sig_pct AS FLOAT64) AS f_1_r1_sig_pct,
  CAST(stats1.round1_knockdowns AS INT64) AS f_1_r1_knockdowns,
  CAST(stats1.round1_sig_strikes_succ AS INT64) AS f_1_r1_sig_strikes_succ,
  CAST(stats1.round1_sig_strikes_att AS INT64) AS f_1_r1_sig_strikes_att,
  CAST(stats1.round1_total_strikes_succ AS INT64) AS f_1_r1_total_strikes_succ,
  CAST(stats1.round1_total_strikes_att AS INT64) AS f_1_r1_total_strikes_att,
  CAST(stats1.round1_td_1_succ AS INT64) AS f_1_r1_td_1_succ,
  CAST(stats1.round1_td_1_att AS INT64) AS f_1_r1_td_1_att,
  CAST(stats1.round1_td_2 AS INT64) AS f_1_r1_td_2,
  CAST(stats1.round1_submission_att AS INT64) AS f_1_r1_submission_att,
  CAST(stats1.round1_reversals AS INT64) AS f_1_r1_reversals,
  CAST(stats1.round1_ctrl AS INT64) AS f_1_r1_ctrl,
  CAST(stats1.round1_sig_pct_succ AS INT64) AS f_1_r1_sig_pct_succ,
  CAST(stats1.round1_sig_pct_att AS INT64) AS f_1_r1_sig_pct_att,
  CAST(stats1.round1_sig_strikes AS INT64) AS f_1_r1_sig_strikes,
  CAST(stats1.round1_head_succ AS INT64) AS f_1_r1_head_succ,
  CAST(stats1.round1_head_att AS INT64) AS f_1_r1_head_att,
  CAST(stats1.round1_body_succ AS INT64) AS f_1_r1_body_succ,
  CAST(stats1.round1_body_att AS INT64) AS f_1_r1_body_att,
  CAST(stats1.round1_leg_succ AS INT64) AS f_1_r1_leg_succ,
  CAST(stats1.round1_leg_att AS INT64) AS f_1_r1_leg_att,
  CAST(stats1.round1_distance_succ AS INT64) AS f_1_r1_distance_succ,
  CAST(stats1.round1_distance_att AS INT64) AS f_1_r1_distance_att,
  CAST(stats1.round1_clinch_succ AS INT64) AS f_1_r1_clinch_succ,
  CAST(stats1.round1_clinch_att AS INT64) AS f_1_r1_clinch_att,
  CAST(stats1.round1_ground_succ AS INT64) AS f_1_r1_ground_succ,
  CAST(stats1.round1_ground_att AS INT64) AS f_1_r1_ground_att,
  -- Fighter 1, Round 2
  CAST(stats1.round2_sig_pct AS FLOAT64) AS f_1_r2_sig_pct,
  CAST(stats1.round2_knockdowns AS INT64) AS f_1_r2_knockdowns,
  CAST(stats1.round2_sig_strikes_succ AS INT64) AS f_1_r2_sig_strikes_succ,
  CAST(stats1.round2_sig_strikes_att AS INT64) AS f_1_r2_sig_strikes_att,
  CAST(stats1.round2_total_strikes_succ AS INT64) AS f_1_r2_total_strikes_succ,
  CAST(stats1.round2_total_strikes_att AS INT64) AS f_1_r2_total_strikes_att,
  CAST(stats1.round2_td_1_succ AS INT64) AS f_1_r2_td_1_succ,
  CAST(stats1.round2_td_1_att AS INT64) AS f_1_r2_td_1_att,
  CAST(stats1.round2_td_2 AS INT64) AS f_1_r2_td_2,
  CAST(stats1.round2_submission_att AS INT64) AS f_1_r2_submission_att,
  CAST(stats1.round2_reversals AS INT64) AS f_1_r2_reversals,
  CAST(stats1.round2_ctrl AS INT64) AS f_1_r2_ctrl,
  CAST(stats1.round2_sig_pct_succ AS INT64) AS f_1_r2_sig_pct_succ,
  CAST(stats1.round2_sig_pct_att AS INT64) AS f_1_r2_sig_pct_att,
  CAST(stats1.round2_sig_strikes AS INT64) AS f_1_r2_sig_strikes,
  CAST(stats1.round2_head_succ AS INT64) AS f_1_r2_head_succ,
  CAST(stats1.round2_head_att AS INT64) AS f_1_r2_head_att,
  CAST(stats1.round2_body_succ AS INT64) AS f_1_r2_body_succ,
  CAST(stats1.round2_body_att AS INT64) AS f_1_r2_body_att,
  CAST(stats1.round2_leg_succ AS INT64) AS f_1_r2_leg_succ,
  CAST(stats1.round2_leg_att AS INT64) AS f_1_r2_leg_att,
  CAST(stats1.round2_distance_succ AS INT64) AS f_1_r2_distance_succ,
  CAST(stats1.round2_distance_att AS INT64) AS f_1_r2_distance_att,
  CAST(stats1.round2_clinch_succ AS INT64) AS f_1_r2_clinch_succ,
  CAST(stats1.round2_clinch_att AS INT64) AS f_1_r2_clinch_att,
  CAST(stats1.round2_ground_succ AS INT64) AS f_1_r2_ground_succ,
  CAST(stats1.round2_ground_att AS INT64) AS f_1_r2_ground_att,
  -- Fighter 1, Round 3
  CAST(stats1.round3_sig_pct AS FLOAT64) AS f_1_r3_sig_pct,
  CAST(stats1.round3_knockdowns AS INT64) AS f_1_r3_knockdowns,
  CAST(stats1.round3_sig_strikes_succ AS INT64) AS f_1_r3_sig_strikes_succ,
  CAST(stats1.round3_sig_strikes_att AS INT64) AS f_1_r3_sig_strikes_att,
  CAST(stats1.round3_total_strikes_succ AS INT64) AS f_1_r3_total_strikes_succ,
  CAST(stats1.round3_total_strikes_att AS INT64) AS f_1_r3_total_strikes_att,
  CAST(stats1.round3_td_1_succ AS INT64) AS f_1_r3_td_1_succ,
  CAST(stats1.round3_td_1_att AS INT64) AS f_1_r3_td_1_att,
  CAST(stats1.round3_td_2 AS INT64) AS f_1_r3_td_2,
  CAST(stats1.round3_submission_att AS INT64) AS f_1_r3_submission_att,
  CAST(stats1.round3_reversals AS INT64) AS f_1_r3_reversals,
  CAST(stats1.round3_ctrl AS INT64) AS f_1_r3_ctrl,
  CAST(stats1.round3_sig_pct_succ AS INT64) AS f_1_r3_sig_pct_succ,
  CAST(stats1.round3_sig_pct_att AS INT64) AS f_1_r3_sig_pct_att,
  CAST(stats1.round3_sig_strikes AS INT64) AS f_1_r3_sig_strikes,
  CAST(stats1.round3_head_succ AS INT64) AS f_1_r3_head_succ,
  CAST(stats1.round3_head_att AS INT64) AS f_1_r3_head_att,
  CAST(stats1.round3_body_succ AS INT64) AS f_1_r3_body_succ,
  CAST(stats1.round3_body_att AS INT64) AS f_1_r3_body_att,
  CAST(stats1.round3_leg_succ AS INT64) AS f_1_r3_leg_succ,
  CAST(stats1.round3_leg_att AS INT64) AS f_1_r3_leg_att,
  CAST(stats1.round3_distance_succ AS INT64) AS f_1_r3_distance_succ,
  CAST(stats1.round3_distance_att AS INT64) AS f_1_r3_distance_att,
  CAST(stats1.round3_clinch_succ AS INT64) AS f_1_r3_clinch_succ,
  CAST(stats1.round3_clinch_att AS INT64) AS f_1_r3_clinch_att,
  CAST(stats1.round3_ground_succ AS INT64) AS f_1_r3_ground_succ,
  CAST(stats1.round3_ground_att AS INT64) AS f_1_r3_ground_att,
  -- Fighter 1, Round 4
  CAST(stats1.round4_sig_pct AS FLOAT64) AS f_1_r4_sig_pct,
  CAST(stats1.round4_knockdowns AS INT64) AS f_1_r4_knockdowns,
  CAST(stats1.round4_sig_strikes_succ AS INT64) AS f_1_r4_sig_strikes_succ,
  CAST(stats1.round4_sig_strikes_att AS INT64) AS f_1_r4_sig_strikes_att,
  CAST(stats1.round4_total_strikes_succ AS INT64) AS f_1_r4_total_strikes_succ,
  CAST(stats1.round4_total_strikes_att AS INT64) AS f_1_r4_total_strikes_att,
  CAST(stats1.round4_td_1_succ AS INT64) AS f_1_r4_td_1_succ,
  CAST(stats1.round4_td_1_att AS INT64) AS f_1_r4_td_1_att,
  CAST(stats1.round4_td_2 AS INT64) AS f_1_r4_td_2,
  CAST(stats1.round4_submission_att AS INT64) AS f_1_r4_submission_att,
  CAST(stats1.round4_reversals AS INT64) AS f_1_r4_reversals,
  CAST(stats1.round4_ctrl AS INT64) AS f_1_r4_ctrl,
  CAST(stats1.round4_sig_pct_succ AS INT64) AS f_1_r4_sig_pct_succ,
  CAST(stats1.round4_sig_pct_att AS INT64) AS f_1_r4_sig_pct_att,
  CAST(stats1.round4_sig_strikes AS INT64) AS f_1_r4_sig_strikes,
  CAST(stats1.round4_head_succ AS INT64) AS f_1_r4_head_succ,
  CAST(stats1.round4_head_att AS INT64) AS f_1_r4_head_att,
  CAST(stats1.round4_body_succ AS INT64) AS f_1_r4_body_succ,
  CAST(stats1.round4_body_att AS INT64) AS f_1_r4_body_att,
  CAST(stats1.round4_leg_succ AS INT64) AS f_1_r4_leg_succ,
  CAST(stats1.round4_leg_att AS INT64) AS f_1_r4_leg_att,
  CAST(stats1.round4_distance_succ AS INT64) AS f_1_r4_distance_succ,
  CAST(stats1.round4_distance_att AS INT64) AS f_1_r4_distance_att,
  CAST(stats1.round4_clinch_succ AS INT64) AS f_1_r4_clinch_succ,
  CAST(stats1.round4_clinch_att AS INT64) AS f_1_r4_clinch_att,
  CAST(stats1.round4_ground_succ AS INT64) AS f_1_r4_ground_succ,
  CAST(stats1.round4_ground_att AS INT64) AS f_1_r4_ground_att,
  -- Fighter 1, Round 5
  CAST(stats1.round5_sig_pct AS FLOAT64) AS f_1_r5_sig_pct,
  CAST(stats1.round5_knockdowns AS INT64) AS f_1_r5_knockdowns,
  CAST(stats1.round5_sig_strikes_succ AS INT64) AS f_1_r5_sig_strikes_succ,
  CAST(stats1.round5_sig_strikes_att AS INT64) AS f_1_r5_sig_strikes_att,
  CAST(stats1.round5_total_strikes_succ AS INT64) AS f_1_r5_total_strikes_succ,
  CAST(stats1.round5_total_strikes_att AS INT64) AS f_1_r5_total_strikes_att,
  CAST(stats1.round5_td_1_succ AS INT64) AS f_1_r5_td_1_succ,
  CAST(stats1.round5_td_1_att AS INT64) AS f_1_r5_td_1_att,
  CAST(stats1.round5_td_2 AS INT64) AS f_1_r5_td_2,
  CAST(stats1.round5_submission_att AS INT64) AS f_1_r5_submission_att,
  CAST(stats1.round5_reversals AS INT64) AS f_1_r5_reversals,
  CAST(stats1.round5_ctrl AS INT64) AS f_1_r5_ctrl,
  CAST(stats1.round5_sig_pct_succ AS INT64) AS f_1_r5_sig_pct_succ,
  CAST(stats1.round5_sig_pct_att AS INT64) AS f_1_r5_sig_pct_att,
  CAST(stats1.round5_sig_strikes AS INT64) AS f_1_r5_sig_strikes,
  CAST(stats1.round5_head_succ AS INT64) AS f_1_r5_head_succ,
  CAST(stats1.round5_head_att AS INT64) AS f_1_r5_head_att,
  CAST(stats1.round5_body_succ AS INT64) AS f_1_r5_body_succ,
  CAST(stats1.round5_body_att AS INT64) AS f_1_r5_body_att,
  CAST(stats1.round5_leg_succ AS INT64) AS f_1_r5_leg_succ,
  CAST(stats1.round5_leg_att AS INT64) AS f_1_r5_leg_att,
  CAST(stats1.round5_distance_succ AS INT64) AS f_1_r5_distance_succ,
  CAST(stats1.round5_distance_att AS INT64) AS f_1_r5_distance_att,
  CAST(stats1.round5_clinch_succ AS INT64) AS f_1_r5_clinch_succ,
  CAST(stats1.round5_clinch_att AS INT64) AS f_1_r5_clinch_att,
  CAST(stats1.round5_ground_succ AS INT64) AS f_1_r5_ground_succ,
  CAST(stats1.round5_ground_att AS INT64) AS f_1_r5_ground_att,
  -- Fighter 2, Round 1
  CAST(stats2.round1_sig_pct AS FLOAT64) AS f_2_r1_sig_pct,
  CAST(stats2.round1_knockdowns AS INT64) AS f_2_r1_knockdowns,
  CAST(stats2.round1_sig_strikes_succ AS INT64) AS f_2_r1_sig_strikes_succ,
  CAST(stats2.round1_sig_strikes_att AS INT64) AS f_2_r1_sig_strikes_att,
  CAST(stats2.round1_total_strikes_succ AS INT64) AS f_2_r1_total_strikes_succ,
  CAST(stats2.round1_total_strikes_att AS INT64) AS f_2_r1_total_strikes_att,
  CAST(stats2.round1_td_1_succ AS INT64) AS f_2_r1_td_1_succ,
  CAST(stats2.round1_td_1_att AS INT64) AS f_2_r1_td_1_att,
  CAST(stats2.round1_td_2 AS INT64) AS f_2_r1_td_2,
  CAST(stats2.round1_submission_att AS INT64) AS f_2_r1_submission_att,
  CAST(stats2.round1_reversals AS INT64) AS f_2_r1_reversals,
  CAST(stats2.round1_ctrl AS INT64) AS f_2_r1_ctrl,
  CAST(stats2.round1_sig_pct_succ AS INT64) AS f_2_r1_sig_pct_succ,
  CAST(stats2.round1_sig_pct_att AS INT64) AS f_2_r1_sig_pct_att,
  CAST(stats2.round1_sig_strikes AS INT64) AS f_2_r1_sig_strikes,
  CAST(stats2.round1_head_succ AS INT64) AS f_2_r1_head_succ,
  CAST(stats2.round1_head_att AS INT64) AS f_2_r1_head_att,
  CAST(stats2.round1_body_succ AS INT64) AS f_2_r1_body_succ,
  CAST(stats2.round1_body_att AS INT64) AS f_2_r1_body_att,
  CAST(stats2.round1_leg_succ AS INT64) AS f_2_r1_leg_succ,
  CAST(stats2.round1_leg_att AS INT64) AS f_2_r1_leg_att,
  CAST(stats2.round1_distance_succ AS INT64) AS f_2_r1_distance_succ,
  CAST(stats2.round1_distance_att AS INT64) AS f_2_r1_distance_att,
  CAST(stats2.round1_clinch_succ AS INT64) AS f_2_r1_clinch_succ,
  CAST(stats2.round1_clinch_att AS INT64) AS f_2_r1_clinch_att,
  CAST(stats2.round1_ground_succ AS INT64) AS f_2_r1_ground_succ,
  CAST(stats2.round1_ground_att AS INT64) AS f_2_r1_ground_att,
  -- Fighter 2, Round 2
  CAST(stats2.round2_sig_pct AS FLOAT64) AS f_2_r2_sig_pct,
  CAST(stats2.round2_knockdowns AS INT64) AS f_2_r2_knockdowns,
  CAST(stats2.round2_sig_strikes_succ AS INT64) AS f_2_r2_sig_strikes_succ,
  CAST(stats2.round2_sig_strikes_att AS INT64) AS f_2_r2_sig_strikes_att,
  CAST(stats2.round2_total_strikes_succ AS INT64) AS f_2_r2_total_strikes_succ,
  CAST(stats2.round2_total_strikes_att AS INT64) AS f_2_r2_total_strikes_att,
  CAST(stats2.round2_td_1_succ AS INT64) AS f_2_r2_td_1_succ,
  CAST(stats2.round2_td_1_att AS INT64) AS f_2_r2_td_1_att,
  CAST(stats2.round2_td_2 AS INT64) AS f_2_r2_td_2,
  CAST(stats2.round2_submission_att AS INT64) AS f_2_r2_submission_att,
  CAST(stats2.round2_reversals AS INT64) AS f_2_r2_reversals,
  CAST(stats2.round2_ctrl AS INT64) AS f_2_r2_ctrl,
  CAST(stats2.round2_sig_pct_succ AS INT64) AS f_2_r2_sig_pct_succ,
  CAST(stats2.round2_sig_pct_att AS INT64) AS f_2_r2_sig_pct_att,
  CAST(stats2.round2_sig_strikes AS INT64) AS f_2_r2_sig_strikes,
  CAST(stats2.round2_head_succ AS INT64) AS f_2_r2_head_succ,
  CAST(stats2.round2_head_att AS INT64) AS f_2_r2_head_att,
  CAST(stats2.round2_body_succ AS INT64) AS f_2_r2_body_succ,
  CAST(stats2.round2_body_att AS INT64) AS f_2_r2_body_att,
  CAST(stats2.round2_leg_succ AS INT64) AS f_2_r2_leg_succ,
  CAST(stats2.round2_leg_att AS INT64) AS f_2_r2_leg_att,
  CAST(stats2.round2_distance_succ AS INT64) AS f_2_r2_distance_succ,
  CAST(stats2.round2_distance_att AS INT64) AS f_2_r2_distance_att,
  CAST(stats2.round2_clinch_succ AS INT64) AS f_2_r2_clinch_succ,
  CAST(stats2.round2_clinch_att AS INT64) AS f_2_r2_clinch_att,
  CAST(stats2.round2_ground_succ AS INT64) AS f_2_r2_ground_succ,
  CAST(stats2.round2_ground_att AS INT64) AS f_2_r2_ground_att,
  -- Fighter 2, Round 3
  CAST(stats2.round3_sig_pct AS FLOAT64) AS f_2_r3_sig_pct,
  CAST(stats2.round3_knockdowns AS INT64) AS f_2_r3_knockdowns,
  CAST(stats2.round3_sig_strikes_succ AS INT64) AS f_2_r3_sig_strikes_succ,
  CAST(stats2.round3_sig_strikes_att AS INT64) AS f_2_r3_sig_strikes_att,
  CAST(stats2.round3_total_strikes_succ AS INT64) AS f_2_r3_total_strikes_succ,
  CAST(stats2.round3_total_strikes_att AS INT64) AS f_2_r3_total_strikes_att,
  CAST(stats2.round3_td_1_succ AS INT64) AS f_2_r3_td_1_succ,
  CAST(stats2.round3_td_1_att AS INT64) AS f_2_r3_td_1_att,
  CAST(stats2.round3_td_2 AS INT64) AS f_2_r3_td_2,
  CAST(stats2.round3_submission_att AS INT64) AS f_2_r3_submission_att,
  CAST(stats2.round3_reversals AS INT64) AS f_2_r3_reversals,
  CAST(stats2.round3_ctrl AS INT64) AS f_2_r3_ctrl,
  CAST(stats2.round3_sig_pct_succ AS INT64) AS f_2_r3_sig_pct_succ,
  CAST(stats2.round3_sig_pct_att AS INT64) AS f_2_r3_sig_pct_att,
  CAST(stats2.round3_sig_strikes AS INT64) AS f_2_r3_sig_strikes,
  CAST(stats2.round3_head_succ AS INT64) AS f_2_r3_head_succ,
  CAST(stats2.round3_head_att AS INT64) AS f_2_r3_head_att,
  CAST(stats2.round3_body_succ AS INT64) AS f_2_r3_body_succ,
  CAST(stats2.round3_body_att AS INT64) AS f_2_r3_body_att,
  CAST(stats2.round3_leg_succ AS INT64) AS f_2_r3_leg_succ,
  CAST(stats2.round3_leg_att AS INT64) AS f_2_r3_leg_att,
  CAST(stats2.round3_distance_succ AS INT64) AS f_2_r3_distance_succ,
  CAST(stats2.round3_distance_att AS INT64) AS f_2_r3_distance_att,
  CAST(stats2.round3_clinch_succ AS INT64) AS f_2_r3_clinch_succ,
  CAST(stats2.round3_clinch_att AS INT64) AS f_2_r3_clinch_att,
  CAST(stats2.round3_ground_succ AS INT64) AS f_2_r3_ground_succ,
  CAST(stats2.round3_ground_att AS INT64) AS f_2_r3_ground_att,
  -- Fighter 2, Round 4
  CAST(stats2.round4_sig_pct AS FLOAT64) AS f_2_r4_sig_pct,
  CAST(stats2.round4_knockdowns AS INT64) AS f_2_r4_knockdowns,
  CAST(stats2.round4_sig_strikes_succ AS INT64) AS f_2_r4_sig_strikes_succ,
  CAST(stats2.round4_sig_strikes_att AS INT64) AS f_2_r4_sig_strikes_att,
  CAST(stats2.round4_total_strikes_succ AS INT64) AS f_2_r4_total_strikes_succ,
  CAST(stats2.round4_total_strikes_att AS INT64) AS f_2_r4_total_strikes_att,
  CAST(stats2.round4_td_1_succ AS INT64) AS f_2_r4_td_1_succ,
  CAST(stats2.round4_td_1_att AS INT64) AS f_2_r4_td_1_att,
  CAST(stats2.round4_td_2 AS INT64) AS f_2_r4_td_2,
  CAST(stats2.round4_submission_att AS INT64) AS f_2_r4_submission_att,
  CAST(stats2.round4_reversals AS INT64) AS f_2_r4_reversals,
  CAST(stats2.round4_ctrl AS INT64) AS f_2_r4_ctrl,
  CAST(stats2.round4_sig_pct_succ AS INT64) AS f_2_r4_sig_pct_succ,
  CAST(stats2.round4_sig_pct_att AS INT64) AS f_2_r4_sig_pct_att,
  CAST(stats2.round4_sig_strikes AS INT64) AS f_2_r4_sig_strikes,
  CAST(stats2.round4_head_succ AS INT64) AS f_2_r4_head_succ,
  CAST(stats2.round4_head_att AS INT64) AS f_2_r4_head_att,
  CAST(stats2.round4_body_succ AS INT64) AS f_2_r4_body_succ,
  CAST(stats2.round4_body_att AS INT64) AS f_2_r4_body_att,
  CAST(stats2.round4_leg_succ AS INT64) AS f_2_r4_leg_succ,
  CAST(stats2.round4_leg_att AS INT64) AS f_2_r4_leg_att,
  CAST(stats2.round4_distance_succ AS INT64) AS f_2_r4_distance_succ,
  CAST(stats2.round4_distance_att AS INT64) AS f_2_r4_distance_att,
  CAST(stats2.round4_clinch_succ AS INT64) AS f_2_r4_clinch_succ,
  CAST(stats2.round4_clinch_att AS INT64) AS f_2_r4_clinch_att,
  CAST(stats2.round4_ground_succ AS INT64) AS f_2_r4_ground_succ,
  CAST(stats2.round4_ground_att AS INT64) AS f_2_r4_ground_att,
  -- Fighter 2, Round 5
  CAST(stats2.round5_sig_pct AS FLOAT64) AS f_2_r5_sig_pct,
  CAST(stats2.round5_knockdowns AS INT64) AS f_2_r5_knockdowns,
  CAST(stats2.round5_sig_strikes_succ AS INT64) AS f_2_r5_sig_strikes_succ,
  CAST(stats2.round5_sig_strikes_att AS INT64) AS f_2_r5_sig_strikes_att,
  CAST(stats2.round5_total_strikes_succ AS INT64) AS f_2_r5_total_strikes_succ,
  CAST(stats2.round5_total_strikes_att AS INT64) AS f_2_r5_total_strikes_att,
  CAST(stats2.round5_td_1_succ AS INT64) AS f_2_r5_td_1_succ,
  CAST(stats2.round5_td_1_att AS INT64) AS f_2_r5_td_1_att,
  CAST(stats2.round5_td_2 AS INT64) AS f_2_r5_td_2,
  CAST(stats2.round5_submission_att AS INT64) AS f_2_r5_submission_att,
  CAST(stats2.round5_reversals AS INT64) AS f_2_r5_reversals,
  CAST(stats2.round5_ctrl AS INT64) AS f_2_r5_ctrl,
  CAST(stats2.round5_sig_pct_succ AS INT64) AS f_2_r5_sig_pct_succ,
  CAST(stats2.round5_sig_pct_att AS INT64) AS f_2_r5_sig_pct_att,
  CAST(stats2.round5_sig_strikes AS INT64) AS f_2_r5_sig_strikes,
  CAST(stats2.round5_head_succ AS INT64) AS f_2_r5_head_succ,
  CAST(stats2.round5_head_att AS INT64) AS f_2_r5_head_att,
  CAST(stats2.round5_body_succ AS INT64) AS f_2_r5_body_succ,
  CAST(stats2.round5_body_att AS INT64) AS f_2_r5_body_att,
  CAST(stats2.round5_leg_succ AS INT64) AS f_2_r5_leg_succ,
  CAST(stats2.round5_leg_att AS INT64) AS f_2_r5_leg_att,
  CAST(stats2.round5_distance_succ AS INT64) AS f_2_r5_distance_succ,
  CAST(stats2.round5_distance_att AS INT64) AS f_2_r5_distance_att,
  CAST(stats2.round5_clinch_succ AS INT64) AS f_2_r5_clinch_succ,
  CAST(stats2.round5_clinch_att AS INT64) AS f_2_r5_clinch_att,
  CAST(stats2.round5_ground_succ AS INT64) AS f_2_r5_ground_succ,
  CAST(stats2.round5_ground_att AS INT64) AS f_2_r5_ground_att,

  -- Rankings
  ranking.rank  AS f_1_ranking,
  ranking2.rank AS f_2_ranking,

  -- Odds: BFO (preferred, de-vigged) with legacy fallback
  COALESCE(
    CASE WHEN LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_1))
         THEN bfo.consensus_prob_f1 ELSE bfo.consensus_prob_f2 END,
    SAFE_DIVIDE(1.0, betting_odds.f1_odds)
  ) AS f_1_implied_prob,

  COALESCE(
    CASE WHEN LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_1))
         THEN bfo.consensus_prob_f2 ELSE bfo.consensus_prob_f1 END,
    SAFE_DIVIDE(1.0, betting_odds.f2_odds)
  ) AS f_2_implied_prob,

  -- Source flag
  CASE WHEN bfo.consensus_prob_f1 IS NOT NULL THEN 'bfo'
       WHEN betting_odds.f1_odds IS NOT NULL THEN 'legacy'
       ELSE NULL END AS odds_source,

  -- Raw legacy odds (kept for backward compat)
  betting_odds.f1_odds         AS f_1_odds_legacy,
  betting_odds.f2_odds         AS f_2_odds_legacy,
  betting_odds.f1_ko_odds_avg  AS f_1_ko_odds,
  betting_odds.f1_sub_odds_avg AS f_1_sub_odds,
  betting_odds.f2_ko_odds_avg  AS f_2_ko_odds,
  betting_odds.f2_sub_odds_avg AS f_2_sub_odds,

  -- BFO detail columns
  CASE WHEN LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_1))
       THEN bfo.best_dec_f1 ELSE bfo.best_dec_f2 END AS f_1_bfo_best_decimal,
  CASE WHEN LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_1))
       THEN bfo.best_dec_f2 ELSE bfo.best_dec_f1 END AS f_2_bfo_best_decimal,
  bfo.bfo_bookmaker_count

FROM `UFC_data.UFC_fights_data` AS fights_data
LEFT JOIN `UFC_data.UFC_fights_urls`   AS fights_urls
  ON fights_data.fight_url = fights_urls.Fight_URL
LEFT JOIN `UFC_data.UFC_events_data`   AS events_data
  ON fights_urls.Event_URL = events_data.event_url

LEFT JOIN `UFC_data.UFC_fights_stats_data` AS stats1
  ON stats1.fight_url = fights_data.fight_url AND stats1.fighter_url = fights_data.f_1_url
LEFT JOIN `UFC_data.UFC_fights_stats_data` AS stats2
  ON stats2.fight_url = fights_data.fight_url AND stats2.fighter_url = fights_data.f_2_url

LEFT JOIN `UFC_data.UFC_fighters_data` AS fighters_data
  ON fights_data.f_1_url = fighters_data.fighter_url
LEFT JOIN `UFC_data.UFC_fighters_data` AS fighters_data2
  ON fights_data.f_2_url = fighters_data2.fighter_url

LEFT JOIN `UFC_data.events_with_full_rankings` AS ranking
  ON events_data.event_url = ranking.event_url
  AND fights_data.f_1 = ranking.fighter_normalized
  AND fights_data.weight_class = ranking.weightclass
LEFT JOIN `UFC_data.events_with_full_rankings` AS ranking2
  ON events_data.event_url = ranking2.event_url
  AND fights_data.f_2 = ranking2.fighter_normalized
  AND fights_data.weight_class = ranking2.weightclass

LEFT JOIN `UFC_data.final_event_view` AS betting_odds
  ON events_data.event_url = betting_odds.event_url
  AND fights_data.f_1 = betting_odds.f_1
  AND fights_data.f_2 = betting_odds.f_2

LEFT JOIN bfo_closing AS bfo
  ON ABS(DATE_DIFF(events_data.event_date, bfo.fight_date, DAY)) <= 1
  AND (
    (LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_1))
     AND LOWER(TRIM(bfo.fighter_2)) = LOWER(TRIM(fights_data.f_2)))
    OR
    (LOWER(TRIM(bfo.fighter_1)) = LOWER(TRIM(fights_data.f_2))
     AND LOWER(TRIM(bfo.fighter_2)) = LOWER(TRIM(fights_data.f_1)))
  )
