# Real-Time Flight Analytics Platform

End-to-end streaming data pipeline: **OpenSky Network → Azure Event Hubs (Kafka protocol) → Databricks Spark Structured Streaming → Delta Lake (Bronze/Silver/Gold) → Databricks Dashboard**.

For a full walkthrough of how to set this up and run it from scratch, see **[RUNBOOK.md](RUNBOOK.md)** — start there, not here, if you're taking this project over.

## Status: Core pipeline complete

- [x] OpenSky OAuth2 client with token auto-refresh (`producer/auth.py`)
- [x] OpenSky API client with correct state-vector parsing (`producer/opensky_client.py`)
- [x] Kafka producer (confluent-kafka) publishing to Azure Event Hubs (`producer/kafka_producer.py`)
- [x] Bronze layer — Spark Structured Streaming, Event Hubs → Delta (`notebooks/01_eventhub_bronze_ingestion.ipynb`)
- [x] Silver layer — parsing, data-quality rules, dead-letter routing, dedup (`notebooks/02_silver_layer.ipynb`)
- [x] Gold layer — `flight_kpis`, `streaming_health`, `longest_flights` (`notebooks/03_gold_layer.ipynb`)
- [x] Databricks Dashboard, reading from Gold tables
- [x] Databricks Job — scheduled Bronze → Silver → Gold execution
- [ ] End-to-end automated validation / CI
- [ ] Job-run alerting on failure

## Project layout

```
flight-analytics-platform/
├── RUNBOOK.md               # How to run this project end-to-end - read this first
├── config/                  # config.yaml + loader - single source of truth for all settings
├── producer/                # OpenSky OAuth2 client, state-vector parser, Kafka producer
├── notebooks/                # Databricks notebooks - the actual production pipeline
│   ├── 01_eventhub_bronze_ingestion.ipynb
│   ├── 02_silver_layer.ipynb
│   └── 03_gold_layer.ipynb
├── docker/                  # Local Kafka (Docker Compose) - optional, for local producer testing only
├── spark_jobs/               # Local-only Spark test scripts (not used in production - see RUNBOOK)
├── tests/                    # pytest unit tests
└── docs/                     # Architecture and status docs
```

## Quick start

See [RUNBOOK.md](RUNBOOK.md) for full setup and run instructions, including credentials, Databricks configuration, and troubleshooting. The short version:

1. `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and fill in real credentials (never commit `.env`)
3. Run the producer locally: `python -m producer.kafka_producer`
4. Run the three notebooks in order in Databricks: `01` → `02` → `03`
5. Or just trigger the scheduled Databricks Job, which runs all three in order

## Configuration

All tunable values (bounding box, poll interval, Kafka settings, watermark delay,
DQ thresholds) live in `config/config.yaml` — nothing is hardcoded in the source.
