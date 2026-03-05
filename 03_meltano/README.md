# Kafka → S3 pipeline with Meltano

Reads from the Kafka topic `postgres1.inventory.customers` and loads data into AWS S3 using the [Meltano](https://meltano.com/) ELT framework.

This follows the [Meltano Getting Started](https://docs.meltano.com/getting-started/) guide: extractor (tap-kafka) → loader (target-s3).

## Prerequisites

- **Python** 3.10, 3.11, 3.12, 3.13, or 3.14
- **Kafka** broker (e.g. the `docker-compose-cdc.yaml` stack in this repo) with topic `postgres1.inventory.customers`
- **AWS S3** bucket and credentials

## 1. Install Meltano

From the [Meltano installation guide](https://docs.meltano.com/getting-started/installation/):

```bash
# Install pipx (if needed)
python3 -m pip install --user pipx
python3 -m pipx ensurepath
source ~/.bashrc   # or restart terminal

# Install Meltano
pipx install meltano
meltano --version
```

## 2. Project setup

```bash
cd iceberge-project/03_meltano
```

Copy environment template and set S3 (and optional Kafka) variables:

```bash
cp .env.example .env
# Edit .env: set TARGET_S3_CLOUD_PROVIDER_AWS_AWS_BUCKET, AWS keys, etc.
```

Install project plugins (tap-kafka and target-s3):

```bash
meltano install
```

## 3. Configure (optional)

Defaults in `meltano.yml`:

- **Kafka**: topic `postgres1.inventory.customers`, `localhost:9092`, group `meltano-kafka-to-s3`, `message_format: json`, `initial_start_time: earliest`
- **S3**: prefix `postgres1/inventory/customers`, Parquet, date in prefix/filename

Override via CLI or `.env`:

```bash
# List extractor settings
meltano config tap-kafka list

# Override topic or bootstrap servers
meltano config tap-kafka set topic postgres1.inventory.customers
meltano config tap-kafka set bootstrap_servers kafka:9092

# List loader settings
meltano config target-s3 list
```

Sensitive values (S3 bucket, AWS keys) should stay in `.env`; see [Configuration](https://docs.meltano.com/guide/configuration).

## 4. Run the pipeline

Run the EL pipeline (extract from Kafka, load to S3):

```bash
meltano run tap-kafka target-s3
```

To run from the earliest offset again (ignore saved state):

```bash
meltano run tap-kafka target-s3 --full-refresh
```

## 5. Verify

- **Extractor**: `meltano invoke tap-kafka --help`
- **Loader**: `meltano invoke target-s3 --help`
- **Config**: `meltano config tap-kafka` / `meltano config target-s3`
- **State**: `meltano state list` and `meltano state get dev:tap-kafka-to-target-s3`

## Plugins

| Role       | Plugin     | Variant     | Purpose                    |
|-----------|------------|------------|----------------------------|
| Extractor | [tap-kafka](https://hub.meltano.com/extractors/tap-kafka/)   | transferwise | Read from Kafka (Singer tap) |
| Loader    | [target-s3](https://hub.meltano.com/loaders/target-s3/)      | crowemi      | Write to S3 (Parquet/JSON/JSONL) |

## Scheduling (optional)

To run on a schedule (e.g. with Airflow), see [Schedule Pipelines](https://docs.meltano.com/getting-started/part2#schedule-pipelines-to-run-regularly):

```bash
meltano schedule add kafka-to-s3 --extractor tap-kafka --loader target-s3 --interval @hourly
```

## References

- [Meltano Getting Started](https://docs.meltano.com/getting-started/)
- [tap-kafka (Meltano Hub)](https://hub.meltano.com/extractors/tap-kafka/)
- [target-s3 (Meltano Hub)](https://hub.meltano.com/loaders/target-s3/)
