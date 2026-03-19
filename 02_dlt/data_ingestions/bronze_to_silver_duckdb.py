import dlt
from dlt.sources.filesystem import filesystem, read_jsonl

files = filesystem(incremental=dlt.sources.incremental("modification_date"))
reader = (files | read_jsonl()).with_name("metrics")
reader.apply_hints(incremental=dlt.sources.incremental("timestamp"))
pipeline = dlt.pipeline(
    pipeline_name="bronze_to_silver_pipeline",
    dataset_name="event_streaming",
    destination="duckdb",
)

info = pipeline.run(reader, write_disposition="replace")
print(info)
