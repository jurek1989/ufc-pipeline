WITH latest_rank_dates AS (
  SELECT DISTINCT date AS ranking_date
  FROM `UFC_data.UFC_rankings`
),

events_with_rank_date AS (
  SELECT
    e.*,
    (
      SELECT MAX(ranking_date)
      FROM latest_rank_dates
      WHERE ranking_date <= DATE(e.event_date)
    ) AS closest_ranking_date
  FROM `UFC_data.UFC_coming_event` e
),

rankings_clean AS (
  SELECT date, weightclass, fighter, rank
  FROM `UFC_data.UFC_rankings`
  WHERE weightclass NOT IN (
    "Men's Pound-for-PoundTop Rank",
    "Women's Pound-for-PoundTop Rank",
    "Women's Pound-for-Pound",
    "Men's Pound-for-Pound",
    "Pound-for-Pound"
  )
)

SELECT
  e.event_name,
  e.event_date,
  e.event_city,
  e.event_state,
  e.event_country,
  e.weight_class,
  e.fighter1_name,
  e.fighter1_link,
  r1.rank AS f_1_ranking,
  e.fighter2_name,
  e.fighter2_link,
  r2.rank AS f_2_ranking,
      fighters_data.fighter_f_name AS f_1_fighter_f_name,
    fighters_data.fighter_l_name AS f_1_fighter_l_name,
    fighters_data.fighter_nickname AS f_1_fighter_nickname,
    fighters_data.fighter_height_cm AS f_1_fighter_height_cm,
    fighters_data.fighter_weight_lbs AS f_1_fighter_weight_lbs,
    fighters_data.fighter_reach_cm AS f_1_fighter_reach_cm,
    fighters_data.fighter_stance AS f_1_fighter_stance,
    fighters_data.fighter_dob AS f_1_fighter_dob,
    fighters_data.fighter_w AS f_1_fighter_w,
    fighters_data.fighter_l AS f_1_fighter_l,
    fighters_data.fighter_d AS f_1_fighter_d,
    fighters_data.fighter_nc_dq AS f_1_fighter_nc_dq,
    fighters_data.fighter_SlpM AS f_1_fighter_SlpM,
    fighters_data.fighter_Str_Acc AS f_1_fighter_Str_Acc,
    fighters_data.fighter_SApM AS f_1_fighter_SApM,
    fighters_data.fighter_Str_Def AS f_1_fighter_Str_Def,
    fighters_data.fighter_TD_Avg AS f_1_fighter_TD_Avg,
    fighters_data.fighter_TD_Acc AS f_1_fighter_TD_Acc,
    fighters_data.fighter_TD_Def AS f_1_fighter_TD_Def,
    fighters_data.fighter_Sub_Avg AS f_1_fighter_Sub_Avg,
    fighters_data.fighter_url AS f_1_fighter_url,
    fighters_data2.fighter_f_name AS f_2_fighter_f_name,
    fighters_data2.fighter_l_name AS f_2_fighter_l_name,
    fighters_data2.fighter_nickname AS f_2_fighter_nickname,
    fighters_data2.fighter_height_cm AS f_2_fighter_height_cm,
    fighters_data2.fighter_weight_lbs AS f_2_fighter_weight_lbs,
    fighters_data2.fighter_reach_cm AS f_2_fighter_reach_cm,
    fighters_data2.fighter_stance AS f_2_fighter_stance,
    fighters_data2.fighter_dob AS f_2_fighter_dob,
    fighters_data2.fighter_w AS f_2_fighter_w,
    fighters_data2.fighter_l AS f_2_fighter_l,
    fighters_data2.fighter_d AS f_2_fighter_d,
    fighters_data2.fighter_nc_dq AS f_2_fighter_nc_dq,
    fighters_data2.fighter_SlpM AS f_2_fighter_SlpM,
    fighters_data2.fighter_Str_Acc AS f_2_fighter_Str_Acc,
    fighters_data2.fighter_SApM AS f_2_fighter_SApM,
    fighters_data2.fighter_Str_Def AS f_2_fighter_Str_Def,
    fighters_data2.fighter_TD_Avg AS f_2_fighter_TD_Avg,
    fighters_data2.fighter_TD_Acc AS f_2_fighter_TD_Acc,
    fighters_data2.fighter_TD_Def AS f_2_fighter_TD_Def,
    fighters_data2.fighter_Sub_Avg AS f_2_fighter_Sub_Avg,
    fighters_data2.fighter_url AS f_2_fighter_url,
  e.f1_odds as f_1_odds,
  e.f2_odds as f_2_odds
FROM events_with_rank_date e
LEFT JOIN rankings_clean r1
  ON r1.date = e.closest_ranking_date
  AND r1.fighter = e.fighter1_name
  AND r1.weightclass = e.weight_class
LEFT JOIN rankings_clean r2
  ON r2.date = e.closest_ranking_date
  AND r2.fighter = e.fighter2_name
  AND r2.weightclass = e.weight_class
LEFT JOIN 
    UFC_data.UFC_fighters_data AS fighters_data
    ON e.fighter1_link = fighters_data.fighter_url
LEFT JOIN 
    UFC_data.UFC_fighters_data AS fighters_data2
    ON e.fighter2_link = fighters_data2.fighter_url



