"""
Kafka to S3 pipeline using dlt.

Reads messages from Kafka topic(s) and loads them into an S3 bucket using the
dlt filesystem destination. Uses Parquet as the loader format (dlt's filesystem
destination does not support Avro; supported formats: jsonl, parquet, csv, etc.
See https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem.)

Setup:
  1. Initialize the Kafka verified source and filesystem destination:
       dlt init kafka filesystem
  2. Install deps: pip install -r requirements.txt
  3. Configure .dlt/secrets.toml with Kafka and S3 credentials (see below).

Secrets example (.dlt/secrets.toml):

  [sources.kafka.credentials]
  bootstrap_servers = "your-broker:9092"
  group_id = "kafka_to_s3_consumer"
  security_protocol = "SASL_SSL"
  sasl_mechanisms = "PLAIN"
  sasl_username = "your_username"
  sasl_password = "your_password"

  [destination.filesystem]
  bucket_url = "s3://your-bucket-name"

  [destination.filesystem.credentials]
  aws_access_key_id = "your_key"
  aws_secret_access_key = "your_secret"
"""

from typing import List, Optional, Union

import dlt

try:
    from kafka import kafka_consumer
except ImportError:
    raise ImportError(
        "Kafka source not found. Run in this directory: dlt init kafka filesystem"
    ) from None

from s3_path_convention import S3PathConvention


def run_pipeline(
    topics: Union[str, List[str]],
    pipeline_name: str = "kafka_to_s3",
    dataset_name: str = "kafka_data",
    table_name: Optional[str] = None,
    path_convention: Optional[S3PathConvention] = None,
    batch_size: int = 3000,
    batch_timeout: int = 3,
    write_disposition: str = "append",
):
    """
    Run the Kafka → S3 pipeline.

    Args:
        topics: Kafka topic name or list of topic names to consume.
        pipeline_name: dlt pipeline name.
        dataset_name: Dataset (folder) name in S3. Ignored if path_convention is set.
        table_name: Override table name in S3 path (else derived from topic). Ignored if path_convention is set.
        path_convention: If set, use convention for path: {customer_id}/event_streaming/{schema}/{table}.
        batch_size: Max messages per batch (default from dlt Kafka source).
        batch_timeout: Max seconds to wait for a batch.
        write_disposition: 'append' or 'replace'.

    Returns:
        LoadInfo from pipeline.run().
    """
    if path_convention is not None:
        dataset_name = path_convention.dataset_name
        table_name = path_convention.table_name

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="filesystem",
        dataset_name=dataset_name,
    )

    resource = kafka_consumer(
        topics,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
    )

    run_kwargs = dict(
        write_disposition=write_disposition,
        loader_file_format="parquet",
    )
    if table_name is not None:
        run_kwargs["table_name"] = table_name

    load_info = pipeline.run(resource, **run_kwargs)
    return load_info


if __name__ == "__main__":
    # Option 1: default path (bucket_url + kafka_data + topic name)
    # TOPICS = ["postgres1_inventory_customers"]
    # load_info = run_pipeline(TOPICS)

    # Option 2: convention path bronze/customer_a/event_streaming/inventory/customer
    # (set bucket_url in .dlt/secrets.toml to s3://bronze)
    TOPICS = ["postgres1_inventory_customers"]
    path_convention = S3PathConvention(
        customer_id="customer_a",
        schema="inventory",
        table="customer",
    )
    load_info = run_pipeline(TOPICS, path_convention=path_convention)
    print(load_info)
