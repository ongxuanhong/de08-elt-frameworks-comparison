"""
S3 path convention for Kafka → filesystem (S3/MinIO) pipelines.

Target path pattern:
  {bucket_url}/{customer_id}/event_streaming/{schema}/{table}

Example:
  s3://bronze/customer_a/event_streaming/inventory/customer

Use with kafka_to_s3.run_pipeline(..., path_convention=...).
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class S3PathConvention:
    """
    Convention: {customer_id}/event_streaming/{schema} as dataset_name, {table} as table_name.
    Full path: bucket_url + / + dataset_name + / + table_name
    """

    customer_id: str
    schema: str
    table: str

    @property
    def dataset_name(self) -> str:
        """dlt dataset_name → {customer_id}/event_streaming/{schema}"""
        return f"{self.customer_id}/event_streaming/{self.schema}"

    @property
    def table_name(self) -> str:
        """dlt table name → {table} (overrides Kafka topic name as table)."""
        return self.table


def parse_topic_to_convention(topic: str, customer_id: str) -> Optional[S3PathConvention]:
    """
    Try to derive schema and table from a Kafka topic name.

    Example: "postgres1_inventory_customers" → schema="inventory", table="customers".
    Adjust the split logic to match your topic naming (e.g. {source}_{schema}_{table}).
    """
    parts = topic.split("_")
    if len(parts) >= 3:
        # assume last part = table, second-to-last = schema (or join rest)
        table = parts[-1]
        schema = parts[-2]
        return S3PathConvention(customer_id=customer_id, schema=schema, table=table)
    if len(parts) == 2:
        return S3PathConvention(customer_id=customer_id, schema=parts[0], table=parts[1])
    if len(parts) == 1:
        return S3PathConvention(customer_id=customer_id, schema="default", table=parts[0])
    return None
