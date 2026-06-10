SELECT
  fight_url,
  fighter_1_url,
  fighter_2_url,
  fighter_1,
  fighter_2,
  odds_1,
  odds_2,

  -- Przeliczone KO
  CASE 
    WHEN f1_ko_odds >= 0 THEN (f1_ko_odds / 100) + 1
    WHEN f1_ko_odds < 0 THEN (100 / ABS(f1_ko_odds)) + 1
    ELSE NULL 
  END AS f1_ko_odds,

  CASE 
    WHEN f2_ko_odds >= 0 THEN (f2_ko_odds / 100) + 1
    WHEN f2_ko_odds < 0 THEN (100 / ABS(f2_ko_odds)) + 1
    ELSE NULL 
  END AS f2_ko_odds,

  -- Przeliczone SUB
  CASE 
    WHEN f1_sub_odds >= 0 THEN (f1_sub_odds / 100) + 1
    WHEN f1_sub_odds < 0 THEN (100 / ABS(f1_sub_odds)) + 1
    ELSE NULL 
  END AS f1_sub_odds,

  CASE 
    WHEN f2_sub_odds >= 0 THEN (f2_sub_odds / 100) + 1
    WHEN f2_sub_odds < 0 THEN (100 / ABS(f2_sub_odds)) + 1
    ELSE NULL 
  END AS f2_sub_odds,

  -- Przeliczone DEC
  CASE 
    WHEN f1_dec_odds >= 0 THEN (f1_dec_odds / 100) + 1
    WHEN f1_dec_odds < 0 THEN (100 / ABS(f1_dec_odds)) + 1
    ELSE NULL 
  END AS f1_dec_odds,

  CASE 
    WHEN f2_dec_odds >= 0 THEN (f2_dec_odds / 100) + 1
    WHEN f2_dec_odds < 0 THEN (100 / ABS(f2_dec_odds)) + 1
    ELSE NULL 
  END AS f2_dec_odds,

  event_date,
  adding_date,
  source,
  region

FROM `UFC_data.UFC_betting_odds_old`

UNION ALL

-- 🟢 CZĘŚĆ 2: dane z API (np. DraftKings, Bet365, itd.)
SELECT
  f.fight_url,
  f.fighter_1_url,
  f.fighter_2_url,
  o.fighter_1,
  o.fighter_2,
  o.odds_1,
  o.odds_2,
  NULL AS f1_ko_odds,
  NULL AS f2_ko_odds,
  NULL AS f1_sub_odds,
  NULL AS f2_sub_odds,
  NULL AS f1_dec_odds,
  NULL AS f2_dec_odds,
  f.event_date,
  o.adding_date,
  o.bookmaker AS source,
  o.region

FROM `UFC_data.UFC_betting_odds_api` o
JOIN (
  -- dopasowanie fighterów i walk z pełnymi URL-ami
  SELECT
    f.fight_url,
    f.f_1_url AS fighter_1_url,
    f.f_2_url AS fighter_2_url,
    e.event_date,
    SOUNDEX(SPLIT(f.f_1, ' ')[OFFSET(0)]) AS f1_soundex,
    SOUNDEX(SPLIT(f.f_2, ' ')[OFFSET(0)]) AS f2_soundex
  FROM `UFC_data.UFC_fights_data` f
  JOIN `UFC_data.UFC_fights_urls` fu ON f.fight_url = fu.Fight_URL
  JOIN `UFC_data.UFC_events_data` e ON fu.Event_URL = e.event_url
) f
ON DATE_DIFF(f.event_date, DATE(o.event_date), DAY) BETWEEN -2 AND 2
AND (
  (SOUNDEX(SPLIT(o.fighter_1, ' ')[OFFSET(0)]) = f.f1_soundex AND SOUNDEX(SPLIT(o.fighter_2, ' ')[OFFSET(0)]) = f.f2_soundex) OR
  (SOUNDEX(SPLIT(o.fighter_1, ' ')[OFFSET(0)]) = f.f2_soundex AND SOUNDEX(SPLIT(o.fighter_2, ' ')[OFFSET(0)]) = f.f1_soundex)
)
