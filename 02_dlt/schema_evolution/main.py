import dlt
from dlt.sources.filesystem import filesystem, read_jsonl
from dlt.common.typing import TDataItems

FILE_PATH = "/workspaces/de08-elt-frameworks-comparison/02_dlt/schema_evolution"


@dlt.resource(
    name="user_table",
    write_disposition="append",
    table_format="delta",
    schema_contract={"tables": "evolve", "columns": "evolve"},
)
def get_user_table() -> TDataItems:
    # trying: evolve, freeze, discard_value, discard_row
    files = filesystem(bucket_url=f"file://{FILE_PATH}/data", file_glob="after.jsonl")
    reader = files | read_jsonl()
    return reader


# Set pipeline name, destination, and dataset name
pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination=dlt.destinations.filesystem(
        bucket_url=f"file://{FILE_PATH}/output",
    ),
    dataset_name="mydata",
)

# Run the pipeline and print load info
load_info = pipeline.run(get_user_table)
print(pipeline.dataset().user_table.df())
