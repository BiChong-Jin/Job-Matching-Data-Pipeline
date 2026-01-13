WITH base AS (
  SELECT
    DATE(event_ts) AS event_date,
    user_id,
    event_name
  FROM {{ ref('events') }}
  WHERE event_name IN ('impression', 'click', 'apply')
)

SELECT
  event_date,
  COUNT(DISTINCT IF(event_name = 'impression', user_id, NULL)) AS users_impression,
  COUNT(DISTINCT IF(event_name = 'click', user_id, NULL)) AS users_click,
  COUNT(DISTINCT IF(event_name = 'apply', user_id, NULL)) AS users_apply,

  SAFE_DIVIDE(
    COUNT(DISTINCT IF(event_name = 'click', user_id, NULL)),
    COUNT(DISTINCT IF(event_name = 'impression', user_id, NULL))
  ) AS ctr_user_level,

  SAFE_DIVIDE(
    COUNT(DISTINCT IF(event_name = 'apply', user_id, NULL)),
    COUNT(DISTINCT IF(event_name = 'click', user_id, NULL))
  ) AS apply_per_click_user_level
FROM base
GROUP BY 1
ORDER BY 1 DESC;

