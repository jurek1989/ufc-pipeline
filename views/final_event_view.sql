WITH base_data AS (
  SELECT 
    e.event_url,
    e.event_name,
    e.event_date,
    f.fight_url,
    f.f_1,
    f.f_2
  FROM `UFC_data.UFC_fights_data` f
  LEFT JOIN `UFC_data.UFC_fights_urls` fu ON f.fight_url = fu.Fight_URL
  LEFT JOIN `UFC_data.UFC_events_data` e ON fu.Event_URL = e.event_url
),

odds_joined AS (
  SELECT
    b.event_url,
    b.event_name,
    b.event_date,
    b.f_1,
    b.f_2,
    o.odds_1_avg AS f1_odds,
    o.odds_2_avg AS f2_odds,
    o.f1_ko_odds_avg,
    o.f2_ko_odds_avg,
    o.f1_sub_odds_avg,
    o.f2_sub_odds_avg,
    o.f1_dec_odds_avg,
    o.f2_dec_odds_avg,
    o.latest_quote_time,
    o.num_sources
  FROM base_data b
  LEFT JOIN `UFC_data.betting_odds_model_ready` o
    ON b.fight_url = o.fight_url
)

SELECT * FROM odds_joined
