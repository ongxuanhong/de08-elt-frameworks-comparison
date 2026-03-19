import dlt
from dlt.sources.helpers.rest_client import paginate


@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/issues",
        params={
            "since": updated_at.last_value,
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
            "state": "open",
        },
    ):
        yield page


@dlt.resource(
    table_name="comments",
    write_disposition="merge",
    primary_key="id",
)
def get_comments(
    updated_at=dlt.sources.incremental(
        "updated_at", initial_value="1970-01-01T00:00:00Z"
    ),
):
    for page in paginate(
        "https://api.github.com/repos/dlt-hub/dlt/comments",
        params={
            "since": updated_at.last_value,
            "per_page": 100,
        },
    ):
        yield page


@dlt.source
def github_source():
    return [get_issues, get_comments]


pipeline = dlt.pipeline(
    pipeline_name="github_with_source",
    destination="duckdb",
    dataset_name="github_data",
)

load_info = pipeline.run(github_source())
print(load_info)
