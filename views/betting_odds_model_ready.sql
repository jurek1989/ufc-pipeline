WITH odds_before_18 AS (
  SELECT
    o.fight_url,
    e.event_url,
    e.event_date,
    o.odds_1,
    o.odds_2,
    o.f1_ko_odds,
    o.f2_ko_odds,
    o.f1_sub_odds,
    o.f2_sub_odds,
    o.f1_dec_odds,
    o.f2_dec_odds,
    o.adding_date,
    o.source
  FROM `UFC_data.betting_odds_clean` o
  JOIN `UFC_data.UFC_fights_urls` fu ON o.fight_url = fu.Fight_URL
  JOIN `UFC_data.UFC_events_data` e ON fu.Event_URL = e.event_url
  WHERE o.adding_date <= TIMESTAMP_ADD(TIMESTAMP(e.event_date), INTERVAL 18 HOUR)
    AND o.source NOT IN ('efortuna', 'zewnetrzne')
),

latest_before_18 AS (
  SELECT *
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY fight_url, source
        ORDER BY adding_date DESC
      ) AS rn
    FROM odds_before_18
  )
  WHERE rn = 1
),

aggregated_api AS (
  SELECT
    fight_url,
    event_url,
    event_date,
    AVG(odds_1) AS odds_1_avg,
    AVG(odds_2) AS odds_2_avg,
    AVG(f1_ko_odds) AS f1_ko_odds_avg,
    AVG(f2_ko_odds) AS f2_ko_odds_avg,
    AVG(f1_sub_odds) AS f1_sub_odds_avg,
    AVG(f2_sub_odds) AS f2_sub_odds_avg,
    AVG(f1_dec_odds) AS f1_dec_odds_avg,
    AVG(f2_dec_odds) AS f2_dec_odds_avg,
    MAX(adding_date) AS latest_quote_time,
    COUNT(DISTINCT source) AS num_sources
  FROM latest_before_18
  GROUP BY fight_url, event_url, event_date
)

-- 🟢 CZĘŚĆ 2: dane z UFC_betting_odds_old (1:1, nieagregowane)
SELECT
  o.fight_url,
  fu.Event_URL AS event_url,
  e.event_date,
  o.odds_1 AS odds_1_avg,
  o.odds_2 AS odds_2_avg,
  o.f1_ko_odds AS f1_ko_odds_avg,
  o.f2_ko_odds AS f2_ko_odds_avg,
  o.f1_sub_odds AS f1_sub_odds_avg,
  o.f2_sub_odds AS f2_sub_odds_avg,
  o.f1_dec_odds AS f1_dec_odds_avg,
  o.f2_dec_odds AS f2_dec_odds_avg,
  o.adding_date AS latest_quote_time,
  1 AS num_sources
FROM `UFC_data.betting_odds_clean` o
JOIN `UFC_data.UFC_fights_urls` fu ON o.fight_url = fu.Fight_URL
JOIN `UFC_data.UFC_events_data` e ON fu.Event_URL = e.event_url
WHERE o.source IN ('efortuna', 'zewnetrzne')

UNION ALL

SELECT * FROM aggregated_api
