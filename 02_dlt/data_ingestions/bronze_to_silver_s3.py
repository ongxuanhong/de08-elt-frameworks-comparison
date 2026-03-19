import dlt
from dlt.sources.filesystem import filesystem as src_filesystem
from dlt.sources.filesystem import read_jsonl
from dlt.destinations import filesystem as des_filesystem

src_files = src_filesystem(incremental=dlt.sources.incremental("modification_date"))
des_files = des_filesystem()
reader = (src_files | read_jsonl()).with_name("metrics")
reader.apply_hints(incremental=dlt.sources.incremental("timestamp"))
pipeline = dlt.pipeline(
    pipeline_name="bronze_to_silver_pipeline",
    dataset_name="event_streaming",
    destination=des_files,
)

info = pipeline.run(reader, write_disposition="replace")
print(info)
