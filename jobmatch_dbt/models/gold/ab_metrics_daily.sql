WITH base AS (
  SELECT
    DATE(event_ts) AS event_date,
    experiment_id,
    variant,
    user_id,
    event_name
  FROM {{ ref('events') }}
  WHERE experiment_id IS NOT NULL
    AND variant IS NOT NULL
    AND event_name IN ('impression', 'click', 'apply')
)

SELECT
  event_date,
  experiment_id,
  variant,

  COUNT(DISTINCT user_id) AS active_users,

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
GROUP BY 1, 2, 3
ORDER BY event_date DESC, experiment_id, variant;

