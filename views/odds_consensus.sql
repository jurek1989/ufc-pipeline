-- Widok: odds_consensus
-- Wejście: UFC_data.odds_snapshots (append-only surowe kursy American)
-- Wyjście: no-vig implied probability per walka per snapshot,
--          z flagami is_opening / is_closing.
--
-- Wzory:
--   decimal    = IF(american > 0, american/100 + 1, 100/ABS(american) + 1)
--   implied    = 1 / decimal
--   novig_prob = implied / SUM(implied) per fight+bookmaker+snapshot (multiplicative de-vig)

WITH

decimal_odds AS (
  SELECT
    event_name,
    event_url,
    event_date,
    fighter_1,
    fighter_2,
    bookmaker,
    scraped_at,
    odds_f1_american,
    odds_f2_american,
    -- American → decimal
    IF(odds_f1_american > 0,
       odds_f1_american / 100.0 + 1,
       100.0 / ABS(odds_f1_american) + 1)  AS dec_f1,
    IF(odds_f2_american > 0,
       odds_f2_american / 100.0 + 1,
       100.0 / ABS(odds_f2_american) + 1)  AS dec_f2
  FROM `UFC_data.odds_snapshots`
  WHERE odds_f1_american IS NOT NULL
    AND odds_f2_american IS NOT NULL
    AND odds_f1_american != 0
    AND odds_f2_american != 0
),

implied AS (
  SELECT
    *,
    1.0 / dec_f1 AS impl_f1,
    1.0 / dec_f2 AS impl_f2
  FROM decimal_odds
),

-- Multiplicative de-vig: divide each implied prob by the overround
novig AS (
  SELECT
    *,
    impl_f1 + impl_f2                       AS overround,
    impl_f1 / (impl_f1 + impl_f2)           AS novig_f1,
    impl_f2 / (impl_f1 + impl_f2)           AS novig_f2
  FROM implied
),

-- Consensus: average no-vig prob across bookmakers per fight+snapshot
consensus AS (
  SELECT
    event_name,
    event_url,
    event_date,
    fighter_1,
    fighter_2,
    scraped_at,
    COUNT(DISTINCT bookmaker)               AS bookmaker_count,
    ROUND(AVG(novig_f1), 6)                 AS consensus_prob_f1,
    ROUND(AVG(novig_f2), 6)                 AS consensus_prob_f2,
    -- Best available decimal odds (highest payout for f1/f2)
    ROUND(MAX(dec_f1), 4)                   AS best_dec_f1,
    ROUND(MAX(dec_f2), 4)                   AS best_dec_f2,
    ROUND(AVG(overround), 6)                AS avg_overround
  FROM novig
  GROUP BY 1, 2, 3, 4, 5, 6
),

-- Opening / closing flags per fight
fight_snapshots AS (
  SELECT
    event_url,
    fighter_1,
    fighter_2,
    MIN(scraped_at) AS first_seen,
    MAX(scraped_at) AS last_seen
  FROM consensus
  GROUP BY 1, 2, 3
)

SELECT
  c.*,
  c.scraped_at = fs.first_seen AS is_opening,
  c.scraped_at = fs.last_seen  AS is_closing
FROM consensus c
JOIN fight_snapshots fs
  USING (event_url, fighter_1, fighter_2)
ORDER BY c.event_date, c.fighter_1, c.scraped_at
