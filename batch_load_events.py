import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = "event-driven-job-matching"
DATASET = "job_matching_bronze"
TABLE = "events_raw"

OUT_PATH = Path("events_raw.ndjson")

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
    return dt.astimezone(timezone.utc).isoformat()


def make_event(base_time: datetime, user_id: str, session_id: str) -> dict:
    event_name = random.choices(EVENT_NAMES, weights=[10, 12, 25, 15, 6, 2], k=1)[0]

    exp_id, variant = random.choice(EXPERIMENTS)

    job_id = None
    rank_position = None
    search_query = None

    if event_name == "search":
        search_query = random.choice(SEARCH_QUERIES)
    elif event_name in ("impression", "click", "apply"):
        job_id = random.choice(JOB_IDS)
        rank_position = random.randint(1, 20)

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
        "source": "python_load_job",
        "schema_version": 1,
    }


def generate_ndjson(out_path: Path, users: int = 200) -> int:
    now = datetime.now(timezone.utc)
    count = 0

    with out_path.open("w", encoding="utf-8") as f:
        for u in range(users):
            user_id = f"user_{u:05d}"
            session_id = f"session_{uuid.uuid4().hex[:12]}"
            for _ in range(random.randint(10, 40)):
                base_time = now - timedelta(minutes=random.randint(0, 60 * 24))
                f.write(json.dumps(make_event(base_time, user_id, session_id)) + "\n")
                count += 1
    return count


def load_to_bigquery(local_file: Path) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Use the table's existing schema (recommended); set autodetect=False.
        autodetect=False,
    )

    with local_file.open("rb") as f:
        load_job = client.load_table_from_file(
            f,
            destination=table_id,
            job_config=job_config,
        )

    print(f"Started load job: {load_job.job_id}")
    load_job.result()  # waits

    dest_table = client.get_table(table_id)
    print(
        f"Load complete. Table now has {dest_table.num_rows} rows. "
        f"Loaded {load_job.output_rows} rows in this job."
    )


def quick_validation():
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
    SELECT event_name, COUNT(*) AS c
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
    GROUP BY 1
    ORDER BY c DESC
    """
    for row in client.query(sql).result():
        print(row["event_name"], row["c"])


def main():
    n = generate_ndjson(OUT_PATH)
    print(f"Wrote {n} events to {OUT_PATH}")

    load_to_bigquery(OUT_PATH)

    print("\nRecent event distribution (last 2 days):")
    quick_validation()


if __name__ == "__main__":
    main()
