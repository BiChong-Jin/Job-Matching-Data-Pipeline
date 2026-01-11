import random
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

PROJECT_ID = "event-driven-job-matching"
DATASET = "job_matching_bronze"
TABLE = "events_raw"

EVENT_NAMES = ["session_start", "search", "impression", "click", "apply", "signup"]
PLATFORMS = ["web", "ios", "android"]
DEVICE_TYPES = ["desktop", "mobile"]
EXPERIMENTS = [("ranking_v1", "control"), ("ranking_v1", "treatment"), (None, None)]
JOB_IDS = [f"job_{i:05d}" for i in range(1, 2001)]
SEARCH_QUERIES = [
    "data engineer",
    "backend engineer",
    "sre",
    "ml engineer",
    "python",
    "kubernetes",
    "tokyo",
    "new grad",
]


def iso_ts(dt: datetime) -> str:
    # BigQuery accepts RFC3339 strings.
    return dt.astimezone(timezone.utc).isoformat()


def make_event(base_time: datetime, user_id: str, session_id: str) -> dict:
    event_name = random.choices(
        EVENT_NAMES,
        weights=[10, 12, 25, 15, 6, 2],  # more impressions/search than applies
        k=1,
    )[0]

    exp_id, variant = random.choice(EXPERIMENTS)

    job_id = None
    rank_position = None
    search_query = None

    if event_name == "search":
        search_query = random.choice(SEARCH_QUERIES)
    elif event_name in ("impression", "click", "apply"):
        job_id = random.choice(JOB_IDS)
        rank_position = random.randint(1, 20)

    # Simulate event_ts slightly earlier than ingested_at
    event_ts = base_time - timedelta(seconds=random.randint(0, 20))
    ingested_at = base_time

    return {
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "event_ts": iso_ts(event_ts),
        "ingested_at": iso_ts(ingested_at),
        "user_id": user_id,
        "session_id": session_id,
        "job_id": job_id,
        "search_query": search_query,
        "rank_position": rank_position,
        "experiment_id": exp_id,
        "variant": variant,
        "platform": random.choice(PLATFORMS),
        "device_type": random.choice(DEVICE_TYPES),
        "source": "python_insert",
        "schema_version": 1,
    }


def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    rows = []
    now = datetime.now(timezone.utc)

    # Generate ~5000 events across 200 users
    for u in range(200):
        user_id = f"user_{u:05d}"
        session_id = f"session_{uuid.uuid4().hex[:12]}"

        # each user produces 10~40 events
        for _ in range(random.randint(10, 40)):
            base_time = now - timedelta(minutes=random.randint(0, 60 * 24))
            rows.append(make_event(base_time, user_id, session_id))

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("Insert had errors:")
        for e in errors[:10]:
            print(e)
        raise SystemExit(1)

    print(f"Inserted {len(rows)} rows into {table_id} successfully.")


if __name__ == "__main__":
    main()
