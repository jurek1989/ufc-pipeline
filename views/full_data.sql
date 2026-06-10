-- DEPRECATED: użyj full_data_silver_plus (§7 ARCHITECTURE.md)
-- ✅ Poprawiona definicja widoku: full_data
-- Warstwa: Silver++ (łączenie finalne)

SELECT 
    fights_data.* EXCEPT(f_1, f_2, f_1_url, f_2_url), 
    events_data.event_date,
    events_data.event_city,
    events_data.event_state,
    events_data.event_country,
    events_data.event_url,

    -- ⚠️ Zmienione aliasy: nie nadpisujemy f_1_url/f_2_url
    fights_data.f_1 AS f_1_name,
    fights_data.f_2 AS f_2_name,
    fights_data.f_1_url,
    fights_data.f_2_url,

    -- 👤 Fighter 1
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

    -- 👤 Fighter 2
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

    -- 📊 Statystyki walki – z prefiksami
    stats1.knockdowns AS f_1_knockdowns,
    stats1.total_strikes_att AS f_1_total_strikes_att,
    stats1.total_strikes_succ AS f_1_total_strikes_succ,
    stats1.sig_strikes_att AS f_1_sig_strikes_att,
    stats1.sig_strikes_succ AS f_1_sig_strikes_succ,
    stats1.takedown_att AS f_1_takedown_att,
    stats1.takedown_succ AS f_1_takedown_succ,
    stats1.submission_att AS f_1_submission_att,
    stats1.reversals AS f_1_reversals,
    stats1.ctrl_time_sec AS f_1_ctrl_time,

    stats2.knockdowns AS f_2_knockdowns,
    stats2.total_strikes_att AS f_2_total_strikes_att,
    stats2.total_strikes_succ AS f_2_total_strikes_succ,
    stats2.sig_strikes_att AS f_2_sig_strikes_att,
    stats2.sig_strikes_succ AS f_2_sig_strikes_succ,
    stats2.takedown_att AS f_2_takedown_att,
    stats2.takedown_succ AS f_2_takedown_succ,
    stats2.submission_att AS f_2_submission_att,
    stats2.reversals AS f_2_reversals,
    stats2.ctrl_time_sec AS f_2_ctrl_time,

    -- 🏆 Rankingi
    ranking.rank AS f_1_ranking,
    ranking2.rank AS f_2_ranking,

    -- 💸 Kursy bukmacherskie – tylko z final_event_view
    betting_odds.f1_odds AS f_1_odds,
    betting_odds.f2_odds AS f_2_odds,
    betting_odds.f1_ko_odds_avg AS f_1_ko_odds,
    betting_odds.f1_sub_odds_avg AS f_1_sub_odds,
    betting_odds.f2_ko_odds_avg AS f_2_ko_odds,
    betting_odds.f2_sub_odds_avg AS f_2_sub_odds

FROM UFC_data.UFC_fights_data AS fights_data

LEFT JOIN UFC_data.UFC_fights_urls AS fights_urls
  ON fights_data.fight_url = fights_urls.Fight_URL

LEFT JOIN UFC_data.UFC_events_data AS events_data
  ON fights_urls.Event_URL = events_data.event_url

LEFT JOIN UFC_data.UFC_fights_stats_data AS stats1
  ON stats1.fight_url = fights_data.fight_url AND stats1.fighter_url = fights_data.f_1_url

LEFT JOIN UFC_data.UFC_fights_stats_data AS stats2
  ON stats2.fight_url = fights_data.fight_url AND stats2.fighter_url = fights_data.f_2_url

LEFT JOIN UFC_data.UFC_fighters_data AS fighters_data
  ON fights_data.f_1_url = fighters_data.fighter_url

LEFT JOIN UFC_data.UFC_fighters_data AS fighters_data2
  ON fights_data.f_2_url = fighters_data2.fighter_url

LEFT JOIN UFC_data.events_with_full_rankings AS ranking
  ON events_data.event_url = ranking.event_url
  AND fights_data.f_1 = ranking.fighter_normalized
  AND fights_data.weight_class = ranking.weightclass

LEFT JOIN UFC_data.events_with_full_rankings AS ranking2
  ON events_data.event_url = ranking2.event_url
  AND fights_data.f_2 = ranking2.fighter_normalized
  AND fights_data.weight_class = ranking2.weightclass

LEFT JOIN UFC_data.final_event_view AS betting_odds
  ON events_data.event_url = betting_odds.event_url
  AND fights_data.f_1 = betting_odds.f_1
  AND fights_data.f_2 = betting_odds.f_2

