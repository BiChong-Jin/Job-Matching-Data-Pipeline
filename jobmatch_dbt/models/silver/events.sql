WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'events_raw') }}
)

SELECT
    event_id,
    event_name,
    event_ts,
    ingested_at,

    user_id,
    session_id,

    job_id,
    search_query,
    rank_position,

    experiment_id,
    variant,

    platform,
    device_type,

    source,
    schema_version
FROM dedup
WHERE rn = 1

