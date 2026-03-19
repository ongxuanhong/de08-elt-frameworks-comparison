from datetime import datetime, timezone

import dlt
from dlt.sources.filesystem import filesystem as src_filesystem
from dlt.sources.filesystem import read_jsonl
from dlt.destinations import filesystem as des_filesystem

# Column used for incremental loading and for deriving partition columns
TIMESTAMP_COLUMN = "timestamp"


def _parse_ts(value):
    """Parse timestamp (ISO string or Unix seconds) to datetime (UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        # ISO format; strip Z -> +00:00 for fromisoformat
        s = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            pass
    return None


def _add_partition_from_timestamp(record, meta=None):
    """Add year, month, day, hour from the timestamp column for Delta partitioning."""
    out = dict(record)
    ts_val = record.get(TIMESTAMP_COLUMN)
    dt = _parse_ts(ts_val)
    if dt is None:
        # Fallback to now if missing/invalid
        dt = datetime.now(timezone.utc)
    out["year"] = dt.year
    out["month"] = f"{dt.month:02d}"
    out["day"] = f"{dt.day:02d}"
    return out


src_files = src_filesystem(incremental=dlt.sources.incremental("modification_date"))
des_files = des_filesystem()
reader = (
    (src_files | read_jsonl())
    .with_name("metrics")
    .add_map(_add_partition_from_timestamp)
    .apply_hints(
        incremental=dlt.sources.incremental(TIMESTAMP_COLUMN),
        columns={
            "year": {"partition": True},
            "month": {"partition": True},
            "day": {"partition": True},
        },
    )
)
pipeline = dlt.pipeline(
    pipeline_name="bronze_to_silver_pipeline",
    dataset_name="event_streaming",
    destination=des_files,
)

info = pipeline.run(reader, write_disposition="replace", table_format="delta")
print(info)
