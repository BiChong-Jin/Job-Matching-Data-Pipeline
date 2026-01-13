WITH base AS (
  SELECT
    DATE(event_ts) AS event_date,
    user_id,
    event_name,
    rank_position
  FROM {{ ref('events') }}
  WHERE event_name IN ('click', 'apply')
    AND rank_position IS NOT NULL
),

k_buckets AS (
  SELECT
    event_date,
    user_id,
    event_name,
    CASE
      WHEN rank_position <= 1 THEN 'k01'
      WHEN rank_position <= 3 THEN 'k03'
      WHEN rank_position <= 5 THEN 'k05'
      WHEN rank_position <= 10 THEN 'k10'
      WHEN rank_position <= 20 THEN 'k20'
      ELSE 'k_gt20'
    END AS k_bucket
  FROM base
)

SELECT
  event_date,
  k_bucket,
  COUNT(DISTINCT IF(event_name = 'click', user_id, NULL)) AS users_click,
  COUNT(DISTINCT IF(event_name = 'apply', user_id, NULL)) AS users_apply,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(event_name = 'apply', user_id, NULL)),
    COUNT(DISTINCT IF(event_name = 'click', user_id, NULL))
  ) AS apply_per_click_user_level
FROM k_buckets
GROUP BY 1, 2
ORDER BY event_date DESC, k_bucket;

