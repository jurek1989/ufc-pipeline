WITH 
unique_events AS (
  SELECT DISTINCT *
  FROM UFC_data.UFC_events_data
),

distinct_ranking_dates AS (
  SELECT DISTINCT date AS ranking_date 
  FROM UFC_data.UFC_rankings
),

events_with_closest_date AS (
  SELECT
    e.*,
    ARRAY_AGG(r.ranking_date 
      ORDER BY r.ranking_date DESC 
      LIMIT 1
    )[OFFSET(0)] AS closest_ranking_date
  FROM unique_events e
  LEFT JOIN distinct_ranking_dates r
    ON r.ranking_date <= e.event_date
  GROUP BY 1,2,3,4,5,6  -- wszystkie kolumny z unique_events
),

unique_rankings AS (
  SELECT DISTINCT date, weightclass, fighter, fighter_normalized, rank
  FROM UFC_data.UFC_rankings where weightclass not in ("""Men's Pound-for-PoundTop Rank""",
"""Women's Pound-for-PoundTop Rank""",
"""Women's Pound-for-Pound""",
"""Men's Pound-for-Pound""",
"""Pound-for-Pound""" 
)
)

SELECT
  e.event_name,
  e.event_date,
  e.event_city,
  e.event_state,
  e.event_country,
  e.event_url,
  r.date AS ranking_date,
  r.weightclass,
  r.fighter,
  r.fighter_normalized,
  r.rank
FROM events_with_closest_date e
LEFT JOIN unique_rankings r
  ON e.closest_ranking_date = r.date
