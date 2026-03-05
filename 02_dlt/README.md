# Kafka → S3 with dlt

After `dlt init kafka filesystem`, you have:

- **`.dlt/`** – config and secrets
- **`kafka_pipeline.py`** – generated pipeline (Kafka → filesystem/S3)
- **`kafka_to_s3.py`** – same flow with Parquet and a simple `run_pipeline()` API

## Next steps

### 1. Configure secrets

Edit **`.dlt/secrets.toml`** and replace placeholders:

**Kafka (required):**

```toml
[sources.kafka.credentials]
bootstrap_servers = "your-broker:9092"
group_id = "your_consumer_group"
security_protocol = "SASL_SSL"   # or PLAINTEXT, SSL, SASL_PLAINTEXT
sasl_mechanisms = "PLAIN"
sasl_username = "your_username"
sasl_password = "your_password"
```

**S3 / filesystem (required):**

```toml
[destination.filesystem]
bucket_url = "s3://your-bucket-name"

[destination.filesystem.credentials]
aws_access_key_id = "your_key"
aws_secret_access_key = "your_secret"
```

If you use default AWS credentials (e.g. `~/.aws/credentials`), you can omit the `[destination.filesystem.credentials]` section.

**MinIO (S3-compatible):** use the same layout and set `endpoint_url` to your MinIO API endpoint:

```toml
[destination.filesystem]
bucket_url = "s3://your-bucket-name"

[destination.filesystem.credentials]
aws_access_key_id = "minio_access_key"
aws_secret_access_key = "minio_secret_key"
endpoint_url = "http://localhost:9000"
```

Use `http://` for plain MinIO (e.g. `http://localhost:9000`) or `https://` if MinIO is behind TLS. Replace host/port with your MinIO server (e.g. `https://minio.example.com`).

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Choose and run a pipeline

**Option A – Generated script** (`kafka_pipeline.py`):

- Edit the topic(s) and which function runs in `if __name__ == "__main__"` (e.g. `load_from_several_topics()` or `load_starting_from_date()`).
- Run:
  ```bash
  python kafka_pipeline.py

  # duckdb
  curl https://install.duckdb.org | sh
  ```

**Option B – Kafka → S3 helper** (`kafka_to_s3.py`):

- Set `TOPICS` in the `if __name__ == "__main__"` block.
- Run:
  ```bash
  python kafka_to_s3.py
  ```

Data is written under the dataset name (e.g. `kafka_messages` or `kafka_data`) in your S3 bucket, in **Parquet** format when using the updated `kafka_pipeline.py` or `kafka_to_s3.py`.

### 4. S3 path convention (e.g. `bronze/customer_a/event_streaming/inventory/customer`)

By default, dlt writes to `{bucket_url}/{dataset_name}/{table_name}` where the table name is the Kafka topic name. To use a fixed path layout like:

`bronze/customer_a/event_streaming/inventory/customer`

1. Set **bucket_url** in `.dlt/secrets.toml` to the bucket (and optional prefix) only, e.g. `s3://bronze` (no `/postgres/customers`).
2. Use **`kafka_to_s3.py`** with **`path_convention`**:

```python
from s3_path_convention import S3PathConvention
from kafka_to_s3 import run_pipeline

path_convention = S3PathConvention(
    customer_id="customer_a",
    schema="inventory",
    table="customer",
)
run_pipeline(
    ["postgres1_inventory_customers"],
    path_convention=path_convention,
)
```

Resulting path: `{bucket_url}/{customer_id}/event_streaming/{schema}/{table}` → e.g. `s3://bronze/customer_a/event_streaming/inventory/customer/`.

For multiple topics, run once per topic with the right `S3PathConvention`, or pass `table_name` only (no convention) to override the table name for a single topic.
